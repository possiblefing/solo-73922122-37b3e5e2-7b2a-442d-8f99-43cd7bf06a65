"""写锁等待与恢复：其他连接持有 SQLite 写锁时的行为。

覆盖需求中的关键场景：
* 等锁期间读（首页、/healthz、旧目录下载）1 秒内返回已提交内容。
* 写库保留忙等待（测试用小超时代替 30 秒）；超时整页回滚，分页进度/水位不动。
* 轮次转为 paused(database_locked)，解锁后可从网页续采原窗口，重读不重复记录。
* 停机（request_stop / close）等写库结束；重启后仍可续采。
"""
from __future__ import annotations

import asyncio
from datetime import date
import os
import sqlite3
import sys
import threading
import time

import httpx
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db import Database, DatabaseBusy
from app.harvester import Harvester, Publisher
from app import oai as oai_mod
from source.app import create_source_app


# ---------------------------------------------------------------------- 工具
class HoldLock:
    """用另一条连接持有同一 DB 的写锁（模拟外部进程占库）。"""

    def __init__(self, path: str):
        self.conn = sqlite3.connect(path, timeout=5, isolation_level=None)
        self.conn.execute("BEGIN IMMEDIATE")
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES('test_lock', '1') "
            "ON CONFLICT(key) DO UPDATE SET value='1'"
        )

    def release(self):
        try:
            self.conn.execute("ROLLBACK")
        finally:
            self.conn.close()


def make(tmp_path, *, busy_ms=900, max_pages=None):
    db = Database(str(tmp_path / "catalog.db"), busy_timeout_ms=busy_ms)
    publisher = Publisher(
        db,
        str(tmp_path / "published"),
        fault="none",
        exit_process=lambda code: (_ for _ in ()).throw(SystemExit(code)),
    )
    h = Harvester(db, "http://source/oai", publisher, max_pages_per_call=max_pages, max_retries=0)
    return db, h


@pytest.fixture()
def env(tmp_path):
    src_app = create_source_app(str(tmp_path / "source.db"))
    transport = httpx.ASGITransport(app=src_app)
    orig_enter = oai_mod.OAIHttpClient.__aenter__

    async def patched_enter(self):
        if self._client is None:
            self._client = httpx.AsyncClient(transport=transport, base_url="http://source")
        return self

    oai_mod.OAIHttpClient.__aenter__ = patched_enter
    try:
        yield tmp_path, src_app, transport
    finally:
        oai_mod.OAIHttpClient.__aenter__ = orig_enter


# ------------------------------------------------------------------ DB 层
def test_reads_return_committed_state_while_write_lock_held(tmp_path):
    db, _ = make(tmp_path)
    run = db.create_run("full", None, date(2026, 11, 13))
    db.save_page(
        run.id,
        [{"identifier": "oai:x:1", "datestamp": "2026-11-13", "deleted": False,
          "metadata": {"title": ["甲"]}, "metadata_xml": None, "sets": []}],
        token="tok-1", pages_done=1, records_seen=1,
    )
    holder = HoldLock(db.path)
    try:
        for label, fn in (
            ("recent_runs", lambda: db.recent_runs(5)),
            ("get_run", lambda: db.get_run(run.id)),
            ("record_stats", db.record_stats),
            ("latest_publication", db.latest_publication),
            ("watermark", db.watermark),
        ):
            t0 = time.monotonic()
            out = fn()
            dt = time.monotonic() - t0
            assert dt < 1.0, f"{label} 读应在 1 秒内返回，实际 {dt:.2f}s"
        # 读到的是已提交内容
        assert db.get_run(run.id).pages_done == 1
        assert db.record_stats() == (1, 0)
    finally:
        holder.release()
    db.close()


def test_page_write_timeout_rolls_back_whole_page(tmp_path):
    db, _ = make(tmp_path, busy_ms=800)
    run = db.create_run("full", None, date(2026, 11, 13))
    holder = HoldLock(db.path)
    try:
        t0 = time.monotonic()
        with pytest.raises(DatabaseBusy):
            db.save_page(
                run.id,
                [{"identifier": "oai:x:1", "datestamp": "2026-11-13", "deleted": False,
                  "metadata": {"title": ["甲"]}, "metadata_xml": None, "sets": []}],
                token="tok-1", pages_done=1, records_seen=1,
            )
        assert time.monotonic() - t0 >= 0.7  # 确实忙等到超时
        # 超时不得推进该页分页进度；记录也不得写入
        run2 = db.get_run(run.id)
        assert run2.pages_done == 0 and run2.records_seen == 0
        assert run2.resumption_token is None
        assert db.record_stats() == (0, 0)
    finally:
        holder.release()
    # 解锁后同一页可写入（续采重读，幂等）
    db.save_page(
        run.id,
        [{"identifier": "oai:x:1", "datestamp": "2026-11-13", "deleted": False,
          "metadata": {"title": ["甲"]}, "metadata_xml": None, "sets": []}],
        token="tok-1", pages_done=1, records_seen=1,
    )
    assert db.get_run(run.id).pages_done == 1
    assert db.record_stats() == (1, 0)
    db.close()


def test_close_waits_for_inflight_write_to_finish(tmp_path):
    db, _ = make(tmp_path, busy_ms=1000)
    run = db.create_run("full", None, date(2026, 11, 13))
    holder = HoldLock(db.path)
    order: list[str] = []

    def writer():
        try:
            db.save_page(
                run.id,
                [{"identifier": "oai:x:1", "datestamp": "2026-11-13", "deleted": False,
                  "metadata": {}, "metadata_xml": None, "sets": []}],
                token="tok-1", pages_done=1, records_seen=1,
            )
            order.append("write-committed")
        except DatabaseBusy:
            order.append("write-rolled-back")

    t = threading.Thread(target=writer)
    t.start()
    time.sleep(0.3)  # 写线程已进入忙等

    closer = threading.Thread(target=lambda: (db.close(), order.append("closed")))
    closer.start()
    time.sleep(0.4)
    assert closer.is_alive(), "写库未结束前，close 必须等待"
    holder.release()  # 解锁；写线程此时已接近忙等超时，将回滚或提交
    t.join(10)
    closer.join(10)
    assert not closer.is_alive()
    assert order[-1] == "closed" and len(order) == 2
    assert order[0] in ("write-committed", "write-rolled-back")


# --------------------------------------------------------------- 采集编排层
def test_harvest_pauses_database_locked_then_resumes_without_duplicates(env):
    tmp_path, _, _ = env
    db, h = make(tmp_path, busy_ms=900)
    run = h.start(date(2026, 11, 13))

    holder = HoldLock(db.path)

    async def scenario():
        task = asyncio.create_task(h.resume(run.id))
        await asyncio.sleep(2.0)  # 第一页写库超时(0.9s)后，暂停状态正耐心等锁
        assert db.get_run(run.id).status == "running"  # 锁未释放前仍是已提交的旧状态
        holder.release()
        return await task

    outcome = asyncio.run(scenario())
    paused = db.get_run(run.id)
    assert outcome.run.status == "paused"
    assert paused.status == "paused"
    assert paused.error_code == "database_locked"
    assert paused.pages_done == 0 and paused.records_seen == 0  # 超时页未推进
    assert db.record_stats() == (0, 0)
    assert db.watermark() is None  # 水位未推进

    # 解锁后续采原窗口：重读不重复记录
    out2 = asyncio.run(h.resume(run.id))
    assert out2.run.status == "completed"
    assert db.record_stats() == (5, 0)
    assert len(db.active_records()) == 5
    assert db.watermark() == "2026-11-13"
    db.close()


def test_resume_flip_timeout_keeps_run_resumable(env):
    """续采的状态翻转写库超时：轮次保持 paused 并记 database_locked，可再次续采。"""
    tmp_path, _, _ = env
    db, h = make(tmp_path, busy_ms=900, max_pages=1)
    asyncio.run(h.start_in_background(date(2026, 11, 13)))
    st = db.unresolved_run()
    assert st is not None and st.status == "paused" and st.error_code == "chunk_limit"

    holder = HoldLock(db.path)

    async def scenario():
        task = asyncio.create_task(h.resume(st.id))
        await asyncio.sleep(2.0)
        holder.release()
        return await task

    asyncio.run(scenario())
    paused = db.get_run(st.id)
    assert paused.status == "paused"
    assert paused.error_code == "database_locked"
    assert paused.pages_done == 1  # 之前已提交的页进度保留

    # 再次续采可完成（max_pages=1，分段采完）
    while True:
        out = asyncio.run(h.resume(st.id))
        if out.run.status != "paused" or out.run.error_code != "chunk_limit":
            break
    assert db.get_run(st.id).status == "completed"
    assert db.record_stats() == (5, 0)
    db.close()


def test_stop_during_write_wait_then_restart_can_resume(env):
    """写库等待中停机：任务收尾退出；重启恢复后轮次可续采。"""
    tmp_path, _, _ = env
    db, h = make(tmp_path, busy_ms=900)
    run = h.start(date(2026, 11, 13))
    holder = HoldLock(db.path)

    async def scenario():
        task = asyncio.create_task(h.resume(run.id))
        await asyncio.sleep(0.4)  # 写库忙等中
        h.request_stop()          # 停机信号
        await asyncio.wait_for(task, timeout=10)  # 写库超时回滚后任务退出

    asyncio.run(scenario())
    # 停机时没来得及写入暂停状态：轮次仍是 running，由重启恢复兜底
    assert db.get_run(run.id).status == "running"
    db.close()
    holder.release()

    # 重启：恢复为可续采，续采完成且不重复
    db2, h2 = make(tmp_path, busy_ms=900)
    db2.recover_after_restart()
    st = db2.get_run(run.id)
    assert st.status == "paused" and st.error_code == "interrupted"
    out = asyncio.run(h2.resume(run.id))
    assert out.run.status == "completed"
    assert db2.record_stats() == (5, 0)
    db2.close()


# ------------------------------------------------------------------ 网页层
@pytest.fixture()
def web(tmp_path, monkeypatch):
    src_app = create_source_app(str(tmp_path / "source.db"))
    transport = httpx.ASGITransport(app=src_app)

    async def patched_enter(self):
        if self._client is None:
            self._client = httpx.AsyncClient(transport=transport, base_url="http://source")
        return self

    monkeypatch.setattr(oai_mod.OAIHttpClient, "__aenter__", patched_enter)
    monkeypatch.setenv("CATALOG_DATA_DIR", str(tmp_path / "web"))
    monkeypatch.setenv("OAI_ENDPOINT", "http://source/oai")
    monkeypatch.setenv("DB_BUSY_TIMEOUT_MS", "1000")  # 测试用小忙等代替 30 秒
    monkeypatch.setenv("MAX_PAGES_PER_CALL", "1")     # 便于制造"已暂停·可续采"
    monkeypatch.delenv("PUBLISH_FAILURE_MODE", raising=False)
    monkeypatch.delenv("PAGE_DELAY_MS", raising=False)

    import importlib

    import app.main as main_mod

    importlib.reload(main_mod)
    app = main_mod.build_app()
    with TestClient(app) as client:
        yield client, app, src_app


def _wait_status(db, run_id, statuses, timeout=30.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        run = db.get_run(run_id)
        if run is not None and run.status in statuses:
            return run
        time.sleep(0.1)
    return db.get_run(run_id)


def _resume_until_done(client, db, run_id, timeout=30.0):
    """反复续采（含 chunk_limit / database_locked 暂停），直到进入终态。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        run = db.get_run(run_id)
        if run.status in ("completed", "publish_failed", "canceled"):
            return run
        if run.status == "paused":
            client.post(f"/harvest/{run_id}/resume")
            # 等它离开 paused，避免重复提交续采请求
            t0 = time.time() + 10
            while time.time() < t0 and db.get_run(run_id).status == "paused":
                time.sleep(0.05)
        else:
            time.sleep(0.1)
    return db.get_run(run_id)


def test_web_lock_timeout_pause_and_resume(web):
    client, app, _ = web
    db = app.state.db

    # 先完整采一轮（分段续采），拿到旧目录
    client.post("/harvest/start", data={"until": "2026-11-13"})
    run1 = _wait_status(db, 1, {"paused", "completed"})
    run1 = _resume_until_done(client, db, run1.id)
    assert run1.status == "completed"
    old_xml = client.get("/catalog/download").text
    assert old_xml.count("<record ") == 5

    # 新一轮：采 1 页后按页数上限暂停
    client.post("/harvest/start", data={"until": "2026-11-13"})
    run2 = _wait_status(db, 2, {"paused"})
    assert run2.error_code == "chunk_limit"

    # 演示持锁 6 秒（> 1 秒忙等），然后点"继续采集"：续采写库将超时
    r = client.post("/demo/db-lock", data={"seconds": "6"}, follow_redirects=True)
    assert r.status_code == 200
    client.post(f"/harvest/{run2.id}/resume", follow_redirects=False)
    time.sleep(0.3)  # 续采的状态翻转写库正在忙等

    # 等锁期间：首页、/healthz、旧目录下载都在 1 秒内返回已提交内容
    for path in ("/", "/healthz", "/catalog/download"):
        t0 = time.monotonic()
        resp = client.get(path)
        dt = time.monotonic() - t0
        assert resp.status_code == 200, path
        assert dt < 1.0, f"{path} 应在 1 秒内返回，实际 {dt:.2f}s"
    assert client.get("/catalog/download").text == old_xml  # 仍是旧目录

    # 锁释放后：轮次显示 database_locked，进度未推进
    run2 = _wait_status(db, run2.id, {"paused"}, timeout=15)
    deadline = time.time() + 15
    while run2.error_code != "database_locked" and time.time() < deadline:
        time.sleep(0.1)
        run2 = db.get_run(run2.id)
    assert run2.error_code == "database_locked"
    assert run2.pages_done == 1  # 仍是已提交的第一页，超时未推进

    html = client.get("/").text
    assert "database_locked" in html
    assert f"/harvest/{run2.id}/resume" in html  # 页面可续采

    # 解锁后续采原窗口：完成且不重复记录
    run2 = _resume_until_done(client, db, run2.id)
    assert run2.status == "completed"
    assert db.record_stats() == (5, 0)
    assert len(db.active_records()) == 5
    new_xml = client.get("/catalog/download").text
    assert new_xml.count("<record ") == 5


def test_web_start_blocked_by_lock_returns_error_without_creating_run(web):
    client, app, _ = web
    db = app.state.db
    client.post("/demo/db-lock", data={"seconds": "4"})
    time.sleep(0.2)
    r = client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=True)
    assert "锁定" in r.text
    assert db.unresolved_run() is None  # 超时未创建轮次
    client.post("/demo/db-unlock")
    # 解锁后可正常开始
    client.post("/harvest/start", data={"until": "2026-11-13"})
    run = _wait_status(db, 1, {"paused", "completed"})
    assert run is not None


def test_web_shutdown_waits_for_write_and_restart_resumes(tmp_path, monkeypatch):
    """写库等待中停机：lifespan 等写库超时回滚、关闭连接后才退出；重启可续采。"""
    src_app = create_source_app(str(tmp_path / "source.db"))
    transport = httpx.ASGITransport(app=src_app)

    async def patched_enter(self):
        if self._client is None:
            self._client = httpx.AsyncClient(transport=transport, base_url="http://source")
        return self

    monkeypatch.setattr(oai_mod.OAIHttpClient, "__aenter__", patched_enter)
    monkeypatch.setenv("CATALOG_DATA_DIR", str(tmp_path / "web"))
    monkeypatch.setenv("OAI_ENDPOINT", "http://source/oai")
    monkeypatch.setenv("DB_BUSY_TIMEOUT_MS", "3000")  # 写库忙等 3 秒
    monkeypatch.setenv("PAGE_DELAY_MS", "800")        # 每页间隔，便于在采集途中持锁
    monkeypatch.delenv("PUBLISH_FAILURE_MODE", raising=False)
    monkeypatch.delenv("MAX_PAGES_PER_CALL", raising=False)

    import importlib

    import app.main as main_mod

    importlib.reload(main_mod)
    app = main_mod.build_app()

    client = TestClient(app)
    client.__enter__()
    db = app.state.db
    # 开始采集后立刻用外部连接持锁：下一页写库将进入 3 秒忙等
    client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=False)
    time.sleep(0.3)
    holder = HoldLock(db.path)
    time.sleep(1.5)  # 采集写库正在忙等（轮次仍是 running）

    # 停机：应等写库超时回滚（约 3 秒忙等）并关闭连接后才返回
    t0 = time.monotonic()
    client.__exit__(None, None, None)
    shutdown_s = time.monotonic() - t0
    assert shutdown_s >= 1.5, f"停机应等待写库结束，实际仅 {shutdown_s:.2f}s"
    assert shutdown_s < 20, f"停机不应无限等待，实际 {shutdown_s:.2f}s"
    holder.release()

    # 重启：遗留 running 恢复为可续采，续采完成且不重复
    app2 = main_mod.build_app()
    with TestClient(app2) as client2:
        db2 = app2.state.db
        run2 = db2.get_run(1)
        assert run2.status == "paused" and run2.error_code == "interrupted"
        run2 = _resume_until_done(client2, db2, run2.id)
        assert run2.status == "completed"
        assert db2.record_stats() == (5, 0)
        assert len(db2.active_records()) == 5
