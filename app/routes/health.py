from fastapi import APIRouter, Depends, Query
from app.auth import require_api_key
from app.config import settings
from app.queue import sync_queue

router = APIRouter()


@router.get("/health")
async def health(
    include_logs: bool = Query(False, description="是否包含最近日志"),
    _: None = Depends(require_api_key),
):
    """节点状态：队列深度、冷却状态、版本、节点名称。"""
    from app.database import get_db
    from datetime import datetime, timezone

    # 基础统计
    async with get_db() as db:
        total_artworks = (await db.execute_fetchone("SELECT COUNT(*) FROM artworks"))[0]
        total_images = (await db.execute_fetchone("SELECT COUNT(*) FROM images"))[0]
        downloaded_images = (await db.execute_fetchone("SELECT COUNT(*) FROM images WHERE downloaded=1"))[0]
        failed_images = (await db.execute_fetchone(
            "SELECT COUNT(*) FROM images WHERE failed=1 OR (retry_count>=3 AND retry_after IS NOT NULL)"
        ))[0]
        pending_jobs = (await db.execute_fetchone(
            "SELECT COUNT(*) FROM sync_jobs WHERE status IN ('running','pending','rate_limited','retry')"
        ))[0]
        current_job = await db.execute_fetchone(
            "SELECT pixiv_user_id, started_at FROM sync_jobs WHERE status = 'running' ORDER BY started_at DESC LIMIT 1"
        )

    resp = {
        "status": "online",
        "node_name": settings.NODE_NAME,
        "version": settings.VERSION,
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "queue": sync_queue.status(),
        "stats": {
            "total_artworks": total_artworks,
            "total_images": total_images,
            "downloaded_images": downloaded_images,
            "failed_images": failed_images,
            "pending_jobs": pending_jobs,
            "current_job_user_id": current_job["pixiv_user_id"] if current_job else None,
            "current_job_started_at": current_job["started_at"] if current_job else None,
        },
    }
    if include_logs:
        from app.log_collector import get_recent_logs_memory
        resp["recent_logs"] = get_recent_logs_memory(50)
    return resp
