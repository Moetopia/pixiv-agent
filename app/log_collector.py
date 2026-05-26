"""
节点日志收集器：将关键日志写入 SQLite，供主服务器轮询。
使用内存环形缓冲 + 异步批量写入，不阻塞业务逻辑。
"""
import asyncio
import logging
from collections import deque
from datetime import datetime, timezone
from typing import List

_BUFFER_SIZE = 1000
_buffer: deque = deque(maxlen=_BUFFER_SIZE)
_write_queue: asyncio.Queue | None = None
_writer_task: asyncio.Task | None = None


class _NodeLogHandler(logging.Handler):
    """捕获 INFO+ 日志到内存缓冲 + 异步写入队列。"""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            entry = {
                "level": record.levelname,
                "logger": record.name,
                "message": self.format(record),
                "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            _buffer.append(entry)
            if _write_queue is not None:
                try:
                    _write_queue.put_nowait(entry)
                except asyncio.QueueFull:
                    pass
        except Exception:
            pass


async def _db_writer_loop() -> None:
    """将队列中的日志批量写入 SQLite。"""
    import aiosqlite
    from app.config import settings

    db_path = str(settings.DB_PATH)
    batch: list = []

    while True:
        try:
            entry = await asyncio.wait_for(_write_queue.get(), timeout=3.0)
            batch.append(entry)
            _write_queue.task_done()
            # 排空队列中剩余条目（批量写入）
            while not _write_queue.empty() and len(batch) < 50:
                try:
                    e = _write_queue.get_nowait()
                    batch.append(e)
                    _write_queue.task_done()
                except asyncio.QueueEmpty:
                    break
        except asyncio.TimeoutError:
            pass
        except asyncio.CancelledError:
            break

        if batch:
            try:
                async with aiosqlite.connect(db_path) as db:
                    await db.executemany(
                        "INSERT INTO node_logs (level, logger, message, created_at) VALUES (?, ?, ?, ?)",
                        [(e["level"], e["logger"], e["message"], e["ts"]) for e in batch],
                    )
                    await db.commit()
            except Exception:
                pass
            batch.clear()


async def setup_log_collector() -> None:
    global _write_queue, _writer_task

    _write_queue = asyncio.Queue(maxsize=500)
    handler = _NodeLogHandler(level=logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logging.getLogger().addHandler(handler)

    _writer_task = asyncio.create_task(_db_writer_loop())


async def teardown_log_collector() -> None:
    if _writer_task and not _writer_task.done():
        _writer_task.cancel()
        try:
            await asyncio.wait_for(_writer_task, timeout=3)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass


def get_recent_logs_memory(limit: int = 100) -> List[dict]:
    """从内存缓冲返回最近的日志（无需 DB 查询）。"""
    items = list(_buffer)
    return items[-limit:]


async def get_recent_logs_db(limit: int = 200, level: str | None = None) -> List[dict]:
    """从 SQLite 返回持久化日志。"""
    import aiosqlite
    from app.config import settings

    cond = "WHERE level=?" if level else ""
    params = (level, limit) if level else (limit,)
    try:
        async with aiosqlite.connect(str(settings.DB_PATH)) as db:
            db.row_factory = aiosqlite.Row
            rows = await db.execute_fetchall(
                f"SELECT level, logger, message, created_at FROM node_logs {cond} ORDER BY id DESC LIMIT ?",
                params,
            )
        return [dict(r) for r in rows]
    except Exception:
        return get_recent_logs_memory(limit)
