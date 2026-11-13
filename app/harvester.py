"""采集编排：首次全量，此后按水位增量；分页进度落库，可续采。

规则
----
1. 首次（无水位）：ListRecords 不带 from，window_from=NULL。
2. 此后：window_from = 上次成功发布的水位（含当天），window_until 由页面选择
   （默认今天）。from/until 都是含当天，因此同日多次变更不会漏采。
3. 每页记录与分页进度在同一事务落库；进程重启/断流后从 token 续采。
   重复页只会产生幂等 UPSERT。
4. badResumptionToken：不推进水位、不清空已入库记录，从原窗口第一页重采。
5. 采集只改 records；发布独立进行，只有发布登记成功才产生新水位。

写锁等待（SQLite 被其他连接占用时）
----------------------------------
* 所有写库都放到工作线程执行，不堵事件循环：等锁期间首页、/healthz、
  旧目录下载照常返回最近一次已提交的内容。
* 单页写入与续采状态翻转只尝试一次（保留 30 秒忙等待）：超时则整页回滚，
  分页进度与发布水位都不推进；随后把轮次标记为 paused(database_locked)，
  解锁后可从网页续采原窗口，重读不产生重复记录。
* 状态类写库（暂停/完成/发布结果）会耐心重试，直到外部锁释放后写入成功；
  若进程此时停机则放弃写入，由重启恢复把遗留轮次转为可续采。
* 单轮互斥：同一时间最多一个采集/发布操作；新的续采请求要等当前操作
  （含其写库提交或回滚）结束后才会被接纳。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import asyncio
import logging
import random
import threading

from .db import Database, DatabaseBusy, Run
from .oai import OAIError, OAIHttpClient, Page

log = logging.getLogger("catalog.harvest")

LOCKED_CODE = "database_locked"
LOCKED_MESSAGE = (
    "数据库被其他连接锁定，写入忙等超时；本页未写入，分页进度与发布水位均未推进。"
    "锁释放后点“继续采集”即可从原窗口续采，已入库记录不会重复。"
)


class HarvestBlocked(Exception):
    pass


@dataclass
class HarvestOutcome:
    run: Run
    published: bool = False
    publication: dict | None = None
    message: str | None = None


@dataclass
class Publisher:
    """把"生成 + 原子发布"包成可替换的发布动作；失败抛异常，旧目录不动。"""

    db: Database
    output_dir: str
    fault: str = "none"
    exit_process: object = None

    def publish(self, run: Run, records: list, watermark: str) -> dict:
        from .catalog import write_publication
        import os

        return write_publication(
            self.db,
            self.output_dir,
            run.id,
            watermark,
            records,
            fault=self.fault,
            exit_process=self.exit_process or os._exit,
        )


class Harvester:
    def __init__(
        self,
        db: Database,
        endpoint: str,
        publisher: Publisher,
        *,
        max_pages_per_call: int | None = None,
        max_retries: int = 2,
        retry_delay: float = 0.4,
        page_delay: float = 0.0,
        max_token_resets: int = 3,
    ):
        self.db = db
        self.endpoint = endpoint
        self.publisher = publisher
        self.max_pages_per_call = max_pages_per_call
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.page_delay = page_delay
        self.max_token_resets = max_token_resets
        self._lock = asyncio.Lock()
        self._stop = threading.Event()

    # -------------------------------------------------------------- 停机控制
    def request_stop(self) -> None:
        """请求停止：进行中的写库完成提交/回滚后，采集循环尽快暂停退出。"""
        self._stop.set()

    @property
    def stop_requested(self) -> bool:
        return self._stop.is_set()

    # ------------------------------------------------------------------ 入口
    def start(self, window_until: date) -> Run:
        """创建轮次（同步写库，可能忙等 busy_timeout；调用方应放到线程中执行）。"""
        if self.db.unresolved_run() is not None:
            raise HarvestBlocked("已有未结束的轮次，请先继续/重试或放弃。")
        watermark = self.db.watermark()
        if watermark is None:
            return self.db.create_run("full", None, window_until)
        window_from = date.fromisoformat(watermark)
        if window_until < window_from:
            raise HarvestBlocked(
                f"本轮截止日 {window_until} 早于当前水位 {window_from}；"
                "增量窗口必须含上次成功截止日，不能回退。"
            )
        return self.db.create_run("incremental", window_from, window_until)

    async def resume(self, run_id: int) -> HarvestOutcome:
        """继续未结束轮次：paused/running 续采；publish_failed/publishing 重试发布。

        单轮互斥：同一时间最多一个采集/发布操作；新请求在本操作（含其写库
        提交或回滚）结束后才会被接纳。
        """
        async with self._lock:
            run = self.db.get_run(run_id)
            if run is None:
                raise HarvestBlocked(f"轮次 #{run_id} 不存在")
            if not run.is_unresolved:
                raise HarvestBlocked(f"轮次 #{run_id} 已结束（{run.status}），无需继续")

            if run.status in ("publish_failed", "publishing"):
                return await self._publish(run)

            if run.status == "paused":
                try:
                    await asyncio.to_thread(self.db.resume_paused, run_id)
                except DatabaseBusy:
                    # 续采的状态翻转写库超时：轮次保持 paused，改记 lock 错误
                    await self._pause_after_busy(run_id)
                    return await self._paused_outcome(run_id, LOCKED_CODE)
                run = self.db.get_run(run_id)
                assert run is not None
            return await self._harvest_loop(run_id)

    async def retry_publish(self, run_id: int) -> HarvestOutcome:
        async with self._lock:
            run = self.db.get_run(run_id)
            if run is None or run.status != "publish_failed":
                raise HarvestBlocked("该轮次当前不需要重试发布")
            if not await self._write_patient(self.db.begin_publish_retry, run_id):
                raise HarvestBlocked("服务正在停止，请稍后重试")
            run = self.db.get_run(run_id)
            assert run is not None
            return await self._publish(run)

    async def retry_publish_after_begin(self, run_id: int) -> HarvestOutcome:
        """页面已把状态置为 publishing，这里直接重试（避免再次 begin 失败）。"""
        async with self._lock:
            run = self.db.get_run(run_id)
            if run is None or run.status != "publishing":
                raise HarvestBlocked("该轮次不在发布中")
            return await self._publish(run)

    async def start_in_background(self, window_until: date, on_created=None) -> HarvestOutcome:
        """创建轮次后立即采集并发布（测试/脚本便捷入口）。"""
        run = await asyncio.to_thread(self.start, window_until)
        if on_created:
            on_created(run)
        return await self.resume(run.id)

    # --------------------------------------------------------- 写库辅助
    async def _write_patient(self, func, *args, **kwargs) -> bool:
        """状态类写库：外部锁释放前耐心重试；收到停机信号则放弃（重启恢复兜底）。

        返回是否写入成功。只有写库提交或回滚结束后，后续操作才会继续。
        """
        while True:
            try:
                await asyncio.to_thread(func, *args, **kwargs)
                return True
            except DatabaseBusy:
                if self._stop.is_set():
                    log.warning("停机中，放弃状态写库 %s", getattr(func, "__name__", func))
                    return False
                log.info("写库仍被外部连接锁定，等待释放后重试")
                await asyncio.sleep(0.2)

    async def _pause_after_busy(self, run_id: int) -> None:
        """写库超时后把轮次标记为 database_locked；等外部锁释放后落库。"""
        await self._write_patient(self.db.pause_run, run_id, LOCKED_CODE, LOCKED_MESSAGE)

    # -------------------------------------------------------------- 采集循环
    async def _harvest_loop(self, run_id: int) -> HarvestOutcome:
        try:
            return await self._harvest_loop_inner(run_id)
        except Exception as exc:
            # 兜底：任何意外都尽量把轮次标记为可续采，不让它停在"采集中"
            log.exception("harvest loop failed unexpectedly: %r", exc)
            await self._write_patient(
                self.db.pause_run, run_id, "interrupted", f"{type(exc).__name__}: {exc}"
            )
            return await self._paused_outcome(run_id, "interrupted")

    async def _harvest_loop_inner(self, run_id: int) -> HarvestOutcome:
        run = self.db.get_run(run_id)
        assert run is not None
        start_pages = run.pages_done
        token = run.resumption_token
        pages_done = run.pages_done
        records_seen = run.records_seen

        async with OAIHttpClient(self.endpoint, timeout=30.0) as client:
            while True:
                if self._stop.is_set():
                    await self._write_patient(
                        self.db.pause_run, run_id, "shutdown", "服务停止，本轮进度已保存，可继续采集。"
                    )
                    return await self._paused_outcome(run_id, "shutdown")

                if (
                    self.max_pages_per_call is not None
                    and (pages_done - start_pages) >= self.max_pages_per_call
                ):
                    await self._write_patient(
                        self.db.pause_run, run_id, "chunk_limit", "单次调用页数上限，进度已保存。"
                    )
                    run = self.db.get_run(run_id)
                    assert run is not None
                    return HarvestOutcome(run=run, message="progress saved (chunk limit)")

                try:
                    page = await self._fetch_page(client, run, token)
                except OAIError as exc:
                    if exc.code == "badResumptionToken":
                        if run.token_resets >= self.max_token_resets:
                            await self._write_patient(
                                self.db.pause_run,
                                run_id,
                                "badResumptionToken",
                                f"分页凭据连续失效 {run.token_resets} 次，已停止以免循环重采。",
                            )
                            return await self._paused_outcome(run_id, "badResumptionToken")
                        note = (
                            f"分页凭据失效（badResumptionToken），已从原窗口第一页重采"
                            f"（第 {run.token_resets + 1} 次重置）；水位保持不变。"
                        )
                        await self._write_patient(self.db.reset_for_relist, run_id, note)
                        token = None
                        pages_done = 0
                        records_seen = 0
                        start_pages = 0
                        run = self.db.get_run(run_id)
                        assert run is not None
                        continue
                    await self._write_patient(
                        self.db.pause_run, run_id, exc.code, exc.message or "OAI 错误"
                    )
                    return await self._paused_outcome(run_id, exc.code)
                except Exception as exc:
                    await self._write_patient(
                        self.db.pause_run, run_id, "interrupted", f"{type(exc).__name__}: {exc}"
                    )
                    return await self._paused_outcome(run_id, "interrupted")

                pages_done += 1
                records_seen += len(page.records)
                token = page.token
                try:
                    # 整页（记录 + 分页进度）一个事务：超时则整页回滚，进度不推进
                    await asyncio.to_thread(
                        self.db.save_page,
                        run_id,
                        [
                            {
                                "identifier": rec.identifier,
                                "datestamp": rec.datestamp,
                                "deleted": rec.deleted,
                                "metadata": rec.metadata,
                                "metadata_xml": rec.metadata_xml,
                                "sets": rec.sets,
                            }
                            for rec in page.records
                        ],
                        token=token,
                        pages_done=pages_done,
                        records_seen=records_seen,
                        complete_list_size=page.complete_list_size,
                        cursor=page.cursor,
                    )
                except DatabaseBusy:
                    log.warning("第 %s 轮第 %s 页写库超时（外部连接持锁），整页回滚", run_id, pages_done)
                    await self._pause_after_busy(run_id)
                    return await self._paused_outcome(run_id, LOCKED_CODE)

                if not token:
                    break
                if self.page_delay > 0:
                    await asyncio.sleep(self.page_delay)

        if not await self._write_patient(self.db.complete_harvest, run_id):
            return await self._paused_outcome(run_id, "interrupted")
        run = self.db.get_run(run_id)
        assert run is not None
        return await self._publish(run)

    async def _paused_outcome(self, run_id: int, code: str) -> HarvestOutcome:
        run = self.db.get_run(run_id)
        assert run is not None
        return HarvestOutcome(run=run, message=f"paused: {code}")

    async def _fetch_page(self, client: OAIHttpClient, run: Run, token: str | None) -> Page:
        window_from = date.fromisoformat(run.window_from) if run.window_from else None
        window_until = date.fromisoformat(run.window_until) if run.window_until else None
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                return await client.list_records(from_=window_from, until=window_until, token=token)
            except OAIError:
                raise
            except Exception as exc:  # 网络、超时、XML 损坏等：重试后交给上层暂停
                last_error = exc
            if attempt < self.max_retries:
                await asyncio.sleep(self.retry_delay * (2 ** attempt) + random.uniform(0, 0.2))
        assert last_error is not None
        raise last_error

    # ------------------------------------------------------------------ 发布
    async def _publish(self, run: Run) -> HarvestOutcome:
        watermark = run.window_until  # 只有发布登记成功，这个值才会成为新水位
        records = self.db.active_records()
        try:
            publication = await asyncio.to_thread(self.publisher.publish, run, records, watermark)
        except Exception as exc:
            # 故障注入的进程退出（SystemExit/os._exit）不落 publish_failed：
            # 进程重启后由启动恢复逻辑根据 publications 判断，保证水位与文件成对。
            await self._write_patient(self.db.mark_publish_failed, run.id, f"{type(exc).__name__}: {exc}")
            run2 = self.db.get_run(run.id)
            assert run2 is not None
            return HarvestOutcome(run=run2, published=False, message=str(exc))

        await self._write_patient(self.db.mark_completed, run.id)
        run2 = self.db.get_run(run.id)
        assert run2 is not None
        return HarvestOutcome(run=run2, published=True, publication=publication)
