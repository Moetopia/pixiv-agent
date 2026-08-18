"""磁盘状态和图片缓存滚动清理。"""
import logging
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.config import settings
from app.database import get_db

logger = logging.getLogger(__name__)


def get_storage_status() -> dict:
    """返回 DATA_DIR 所在文件系统的空间状态。"""
    try:
        usage = shutil.disk_usage(settings.DATA_DIR)
    except OSError as exc:
        return {
            "state": "unknown",
            "path": str(settings.DATA_DIR),
            "error": str(exc),
        }

    free_percent = (usage.free / usage.total * 100) if usage.total else 0.0
    if free_percent <= settings.CACHE_PAUSE_SYNC_PERCENT:
        state = "full"
    elif free_percent <= settings.CACHE_CLEANUP_THRESHOLD_PERCENT:
        state = "degraded"
    else:
        state = "ok"

    return {
        "state": state,
        "path": str(settings.DATA_DIR),
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "free_percent": round(free_percent, 2),
    }


def _is_cache_path(path: Path) -> bool:
    """只允许清理图片缓存目录内的文件。"""
    try:
        path.resolve().relative_to(settings.IMAGES_DIR.resolve())
        return True
    except ValueError:
        return False


async def cleanup_old_images() -> dict:
    """空间不足时删除最老的已同步图片，并保留数据库元数据。"""
    status = get_storage_status()
    result = {"enabled": settings.CACHE_CLEANUP_ENABLED, "deleted": 0, "freed_bytes": 0, "storage": status}
    if not settings.CACHE_CLEANUP_ENABLED or status.get("state") not in {"degraded", "full"}:
        return result

    cutoff = datetime.now(timezone.utc) - timedelta(days=settings.CACHE_CLEANUP_MIN_AGE_DAYS)
    cutoff_iso = cutoff.isoformat(timespec="seconds")
    served_condition = "AND i.last_served_at IS NOT NULL" if settings.CACHE_CLEANUP_ONLY_SERVED else ""

    async with get_db() as db:
        rows = await db.execute_fetchall(
            f"""
            SELECT i.id, i.local_path, i.last_served_at, a.fetched_at
            FROM images i
            JOIN artworks a ON a.pixiv_id = i.pixiv_id
            WHERE i.downloaded=1 AND i.local_path IS NOT NULL
              {served_condition}
              AND COALESCE(i.last_served_at, a.fetched_at) <= ?
            ORDER BY COALESCE(i.last_served_at, a.fetched_at) ASC
            LIMIT ?
            """,
            (cutoff_iso, max(1, settings.CACHE_CLEANUP_BATCH_SIZE)),
        )

        for row in rows:
            current = get_storage_status()
            if current.get("free_percent", 0) >= settings.CACHE_CLEANUP_TARGET_PERCENT:
                break

            path = Path(row["local_path"])
            if not _is_cache_path(path):
                logger.warning("跳过缓存目录外的图片路径: %s", path)
                continue

            try:
                size = path.stat().st_size if path.exists() else 0
                path.unlink(missing_ok=True)
            except OSError as exc:
                logger.warning("清理图片失败 %s: %s", path, exc)
                continue

            await db.execute(
                """
                UPDATE images
                SET downloaded=0, local_path=NULL, failed=0,
                    retry_count=0, retry_after=NULL
                WHERE id=?
                """,
                (row["id"],),
            )
            result["deleted"] += 1
            result["freed_bytes"] += size

        if result["deleted"]:
            await db.commit()

    result["storage_after"] = get_storage_status()
    logger.warning(
        "图片缓存滚动清理完成: deleted=%s freed_bytes=%s state=%s",
        result["deleted"], result["freed_bytes"], result["storage_after"].get("state"),
    )
    return result
