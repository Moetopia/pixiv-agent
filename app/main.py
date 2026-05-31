"""
pixiv-agent — Pixiv 数据代理节点
启动：uvicorn app.main:app --host 0.0.0.0 --port 8100
"""
import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.database import init_db
from app.queue import sync_queue
from app.routes import health, sync, artworks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

_worker_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _worker_task

    logger.info(f"🚀 pixiv-agent 节点 [{settings.NODE_NAME}] 启动中...")
    await init_db()
    logger.info("✅ SQLite 初始化完成")

    from app.log_collector import setup_log_collector
    await setup_log_collector()
    logger.info("✅ 日志收集器已启动")

    if not settings.PIXIV_REFRESH_TOKEN:
        logger.warning("⚠️  PIXIV_REFRESH_TOKEN 未配置，同步功能将不可用")

    from app.worker import worker_loop
    
    # 恢复异常中断或遗留的同步任务
    from app.database import get_db
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT DISTINCT pixiv_user_id FROM sync_jobs WHERE status IN ('running', 'pending', 'rate_limited', 'retry')"
        )
        rows = await cursor.fetchall()
        if rows:
            logger.info(f"🔄 发现 {len(rows)} 个异常中断的任务，正在恢复...")
            for row in rows:
                uid = row[0]
                await db.execute(
                    "UPDATE sync_jobs SET status='pending' WHERE pixiv_user_id=? AND status IN ('running', 'pending', 'rate_limited', 'retry')",
                    (uid,)
                )
                await db.execute(
                    "UPDATE authors SET status='pending' WHERE pixiv_user_id=?",
                    (uid,)
                )
                await db.commit()
                await sync_queue.enqueue(uid)
            logger.info("✅ 任务恢复完成，已重新加入内存队列")
            
    _worker_task = asyncio.create_task(worker_loop())
    logger.info("✅ 后台 worker 已启动")

    yield

    logger.info("🛑 正在关闭节点...")
    if _worker_task and not _worker_task.done():
        _worker_task.cancel()
        try:
            await asyncio.wait_for(_worker_task, timeout=5)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

    from app.log_collector import teardown_log_collector
    await teardown_log_collector()


app = FastAPI(
    title="pixiv-agent",
    version=settings.VERSION,
    description="Pixiv 数据代理节点 — 负责拉取并缓存 Pixiv 作品数据",
    lifespan=lifespan,
)

app.include_router(health.router, tags=["节点状态"])
app.include_router(sync.router,   tags=["同步控制"])
app.include_router(artworks.router, tags=["作品缓存"])


@app.get("/")
async def root():
    return {
        "node": settings.NODE_NAME,
        "version": settings.VERSION,
        "status": "online",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=settings.PORT)
