from fastapi import APIRouter, Depends, HTTPException, Query
from app.auth import require_api_key
from app.config import settings
from app.database import get_db
from app.queue import sync_queue

router = APIRouter()


@router.post("/sync/author/{pixiv_user_id}")
async def enqueue_author(
    pixiv_user_id: int,
    _: None = Depends(require_api_key),
):
    """将指定 Pixiv 作者加入同步队列。"""
    enqueued = await sync_queue.enqueue(pixiv_user_id)
    async with await get_db() as db:
        existing = await db.execute_fetchone(
            "SELECT pixiv_user_id, status FROM authors WHERE pixiv_user_id=?",
            (pixiv_user_id,),
        )
        if not existing:
            await db.execute(
                "INSERT OR IGNORE INTO authors (pixiv_user_id, status) VALUES (?, 'pending')",
                (pixiv_user_id,),
            )
            await db.commit()
    return {
        "pixiv_user_id": pixiv_user_id,
        "enqueued": enqueued,
        "message": "已加入同步队列" if enqueued else "已在队列中或正在同步",
        "queue": sync_queue.status(),
    }


@router.get("/sync/status")
async def sync_status(_: None = Depends(require_api_key)):
    """当前队列状态。"""
    return sync_queue.status()


@router.get("/sync/authors")
async def list_sync_authors(_: None = Depends(require_api_key)):
    """列出所有已追踪作者及同步状态。"""
    async with await get_db() as db:
        rows = await db.execute_fetchall(
            "SELECT pixiv_user_id, username, status, last_synced_at, artwork_count, created_at FROM authors ORDER BY created_at DESC"
        )
    return [dict(r) for r in rows]


@router.get("/sync/jobs")
async def list_jobs(
    limit: int = 50,
    _: None = Depends(require_api_key),
):
    """列出最近的同步任务记录。"""
    async with await get_db() as db:
        rows = await db.execute_fetchall(
            "SELECT * FROM sync_jobs ORDER BY created_at DESC LIMIT ?", (limit,)
        )
    return [dict(r) for r in rows]


@router.get("/logs")
async def get_logs(
    limit: int = 100,
    level: str = None,
    source: str = "db",
    _: None = Depends(require_api_key),
):
    """
    返回节点日志供主服务器轮询。
    source=memory: 内存缓冲（快，最近 1000 条）
    source=db: SQLite 持久化日志（默认，可按 level 过滤）
    """
    if source == "memory":
        from app.log_collector import get_recent_logs_memory
        logs = get_recent_logs_memory(limit)
    else:
        from app.log_collector import get_recent_logs_db
        logs = await get_recent_logs_db(limit=limit, level=level)
    return {"node_name": settings.NODE_NAME, "logs": logs}
