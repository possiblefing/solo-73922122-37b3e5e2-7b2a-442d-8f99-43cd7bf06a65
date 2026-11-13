"""演示用写锁：用另一条连接对同一 SQLite 持有写锁，模拟"别的进程/连接占库"。

页面可设定持锁秒数或立即释放。持锁期间：
* 读（首页、/healthz、旧目录下载）走独立只读连接，WAL 快照读不受影响；
* 写（采集落库、创建轮次等）在 30 秒忙等待后超时，事务整体回滚，
  轮次转为"已暂停·可续采"（错误 database_locked）。

锁事务里写入的 meta 行最后会 ROLLBACK，不在库里留下任何痕迹。
"""
from __future__ import annotations

from dataclasses import dataclass, field
import logging
import sqlite3
import threading
import time

log = logging.getLogger("catalog.demolock")


@dataclass
class _Hold:
    stop: threading.Event = field(default_factory=threading.Event)
    acquired: threading.Event = field(default_factory=threading.Event)
    until: float = 0.0


class DemoWriteLock:
    """可控的"外部连接持写锁"模拟器（仅供演示/演练）。"""

    def __init__(self, path: str):
        self._path = path
        self._mu = threading.Lock()
        self._cur: _Hold | None = None

    def acquire(self, seconds: float) -> bool:
        """开始持锁 seconds 秒；已在持锁则返回 False。"""
        seconds = max(1.0, min(300.0, float(seconds)))
        with self._mu:
            if self._cur is not None:
                return False
            hold = _Hold(until=time.monotonic() + seconds)
            self._cur = hold
        threading.Thread(target=self._run, args=(hold, seconds), daemon=True).start()
        return True

    def release(self) -> None:
        """立即释放（未持锁时为空操作）。"""
        with self._mu:
            hold = self._cur
        if hold is not None:
            hold.stop.set()

    def remaining(self) -> float:
        """剩余持锁秒数；未持锁（或尚未真正获得锁）返回 0。"""
        with self._mu:
            hold = self._cur
        if hold is None or not hold.acquired.is_set():
            return 0.0
        return max(0.0, hold.until - time.monotonic())

    def _run(self, hold: _Hold, seconds: float) -> None:
        try:
            conn = sqlite3.connect(self._path, timeout=5, isolation_level=None)
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES('demo_write_lock', 'held') "
                    "ON CONFLICT(key) DO UPDATE SET value = 'held'"
                )
                hold.acquired.set()
                log.info("demo write lock held for %.0f s", seconds)
                hold.stop.wait(seconds)
                conn.execute("ROLLBACK")
                log.info("demo write lock released")
            finally:
                conn.close()
        except sqlite3.Error as exc:
            log.warning("demo write lock failed: %r", exc)
        finally:
            with self._mu:
                if self._cur is hold:
                    self._cur = None
