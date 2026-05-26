"""
后台 Worker：从 SyncQueue 消费作者 ID，拉取 Pixiv 数据，下载图片，存入 SQLite。
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from app.config import settings
from app.database import get_db
from app.pixiv_client import fetch_user_detail, fetch_user_illusts_page, download_image, parse_illust, PixivRateLimitError
from app.queue import sync_queue, rate_limited_call

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


async def _upsert_author(db, author_data: dict) -> None:
    await db.execute("""
        INSERT INTO authors (pixiv_user_id, username, bio, website_url, twitter_url, avatar_url, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(pixiv_user_id) DO UPDATE SET
            username=excluded.username, bio=excluded.bio, website_url=excluded.website_url,
            twitter_url=excluded.twitter_url, avatar_url=excluded.avatar_url, updated_at=excluded.updated_at
    """, (
        author_data["pixiv_user_id"], author_data.get("username", ""),
        author_data.get("bio"), author_data.get("website_url"), author_data.get("twitter_url"),
        author_data.get("avatar_url"), _now_iso(),
    ))


async def _download_author_avatar(pixiv_user_id: int, avatar_url: str) -> None:
    if not avatar_url:
        return
    ext = "." + avatar_url.rsplit(".", 1)[-1].split("?")[0]
    if ext not in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
        ext = ".jpg"
    dest = settings.IMAGES_DIR / "avatars" / f"{pixiv_user_id}{ext}"
    if dest.exists():
        return
    ok = await download_image(avatar_url, dest)
    if ok:
        async with await get_db() as db:
            await db.execute(
                "UPDATE authors SET avatar_local_path=? WHERE pixiv_user_id=?",
                (str(dest), pixiv_user_id),
            )
            await db.commit()


async def _process_illust(db, illust: dict) -> bool:
    """处理单个作品：入库元数据 + 触发图片下载。返回是否为新作品。"""
    parsed = parse_illust(illust)
    pixiv_id = parsed["pixiv_id"]

    existing = await db.execute_fetchone(
        "SELECT pixiv_id FROM artworks WHERE pixiv_id=?", (pixiv_id,)
    )
    is_new = existing is None

    await db.execute("""
        INSERT INTO artworks (pixiv_id, pixiv_user_id, title, description, tags_json,
            rating, is_ai, artwork_type, page_count, source_url, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(pixiv_id) DO UPDATE SET
            title=excluded.title, description=excluded.description, tags_json=excluded.tags_json,
            rating=excluded.rating, is_ai=excluded.is_ai, page_count=excluded.page_count, fetched_at=excluded.fetched_at
    """, (
        pixiv_id, parsed["pixiv_user_id"], parsed["title"],
        parsed["description"], json.dumps(parsed["tags"], ensure_ascii=False),
        parsed["rating"], int(parsed["is_ai"]), parsed["artwork_type"],
        parsed["page_count"], parsed["source_url"], _now_iso(),
    ))

    for idx, url in enumerate(parsed["image_urls"]):
        await db.execute("""
            INSERT INTO images (pixiv_id, page_index, original_url)
            VALUES (?, ?, ?)
            ON CONFLICT(pixiv_id, page_index) DO NOTHING
        """, (pixiv_id, idx, url))

    return is_new


_MAX_IMAGE_RETRIES = 3    # 单张图片下载最大重试次数
_MAX_AUTHOR_RETRIES = 3   # 单个作者同步任务最大重试次数


async def _download_pending_images(pixiv_id: int) -> None:
    """
    下载某作品所有未下载图片（并发受 DOWNLOAD_CONCURRENCY 限制）。
    失败时递增 retry_count；超过上限后设置 retry_after（稍后再试）。
    """
    async with await get_db() as db:
        rows = await db.execute_fetchall(
            """
            SELECT id, page_index, original_url, retry_count
            FROM images
            WHERE pixiv_id=? AND downloaded=0 AND failed=0
              AND (retry_after IS NULL OR retry_after <= datetime('now'))
            """,
            (pixiv_id,),
        )

    if not rows:
        return

    sem = asyncio.Semaphore(settings.DOWNLOAD_CONCURRENCY)

    async def _dl(row):
        async with sem:
            url = row["original_url"]
            idx = row["page_index"]
            retry_count = row["retry_count"] or 0
            ext = "." + url.rsplit(".", 1)[-1].split("?")[0]
            if ext not in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
                ext = ".jpg"
            dest = settings.IMAGES_DIR / str(pixiv_id) / f"p{idx}{ext}"
            ok = await download_image(url, dest)
            async with await get_db() as db2:
                if ok:
                    await db2.execute(
                        "UPDATE images SET downloaded=1, local_path=?, retry_count=? WHERE id=?",
                        (str(dest), retry_count, row["id"]),
                    )
                else:
                    new_retry = retry_count + 1
                    if new_retry >= _MAX_IMAGE_RETRIES:
                        # 暂时搁置：10 分钟后再试
                        await db2.execute(
                            "UPDATE images SET retry_count=?, retry_after=datetime('now', '+10 minutes') WHERE id=?",
                            (new_retry, row["id"]),
                        )
                        logger.warning(
                            f"[worker] 图片 {pixiv_id}[{idx}] 连续失败 {new_retry} 次，暂停 10 分钟"
                        )
                    else:
                        await db2.execute(
                            "UPDATE images SET retry_count=? WHERE id=?",
                            (new_retry, row["id"]),
                        )
                await db2.commit()

    await asyncio.gather(*[_dl(r) for r in rows])


async def _retry_stale_images() -> None:
    """
    重置 retry_after 到期的图片（重新允许下载）。
    由 worker_loop 定期调用。
    """
    async with await get_db() as db:
        result = await db.execute(
            """
            UPDATE images
            SET retry_after=NULL, retry_count=0
            WHERE downloaded=0 AND failed=0
              AND retry_count >= ? AND retry_after IS NOT NULL
              AND retry_after <= datetime('now')
            """,
            (_MAX_IMAGE_RETRIES,),
        )
        if result.rowcount:
            logger.info(f"[worker] 已重置 {result.rowcount} 张过期重试图片")
        await db.commit()


async def _sync_author(job_id: int, pixiv_user_id: int) -> None:
    """拉取指定作者的所有作品，下载图片并写入 SQLite。"""
    logger.info(f"[worker] 开始同步作者 {pixiv_user_id} (job={job_id})")

    async with await get_db() as db:
        await db.execute(
            "UPDATE sync_jobs SET status='running', started_at=? WHERE id=?",
            (_now_iso(), job_id),
        )
        await db.commit()

    try:
        author_data = await rate_limited_call(lambda: fetch_user_detail(pixiv_user_id))
        if author_data:
            async with await get_db() as db:
                await _upsert_author(db, author_data)
                await db.commit()
            await _download_author_avatar(pixiv_user_id, author_data.get("avatar_url", ""))

        offset = 0
        total_found = 0
        total_new = 0
        max_artworks = settings.MAX_ARTWORKS_PER_AUTHOR

        while True:
            result = await rate_limited_call(
                lambda: fetch_user_illusts_page(pixiv_user_id, offset)
            )
            if not result or result.get("error"):
                break

            illusts = result.get("illusts", [])
            if not illusts:
                break

            async with await get_db() as db:
                for illust in illusts:
                    is_new = await _process_illust(db, illust)
                    total_found += 1
                    if is_new:
                        total_new += 1
                await db.commit()

            for illust in illusts:
                asyncio.create_task(_download_pending_images(illust["id"]))

            if max_artworks and total_found >= max_artworks:
                logger.info(f"  达到 MAX_ARTWORKS_PER_AUTHOR={max_artworks}，停止")
                break

            next_url = result.get("next_url")
            if not next_url:
                break

            params = _parse_next_url_offset(next_url)
            offset = params.get("offset", offset + 30)

        async with await get_db() as db:
            await db.execute("""
                UPDATE sync_jobs SET status='done', finished_at=?, artworks_found=?, artworks_new=?
                WHERE id=?
            """, (_now_iso(), total_found, total_new, job_id))
            await db.execute(
                "UPDATE authors SET status='done', last_synced_at=?, artwork_count=artwork_count+? WHERE pixiv_user_id=?",
                (_now_iso(), total_new, pixiv_user_id),
            )
            await db.commit()

        logger.info(f"[worker] 作者 {pixiv_user_id} 同步完成: found={total_found} new={total_new}")

    except PixivRateLimitError as e:
        # 429：不标记为失败，直接重新入队。
        # 下次出队时 rate_limited_call 会自动等待冷却，无需在此阻塞 worker。
        logger.warning(
            f"[worker] 作者 {pixiv_user_id} 遭遇 429 限速，已重新入队 (job={job_id})"
        )
        async with await get_db() as db:
            await db.execute(
                "UPDATE sync_jobs SET status='rate_limited', error=? WHERE id=?",
                (str(e), job_id),
            )
            await db.execute(
                "UPDATE authors SET status='pending' WHERE pixiv_user_id=?",
                (pixiv_user_id,),
            )
            await db.commit()
        await sync_queue.enqueue(pixiv_user_id)

    except Exception as e:
        logger.error(f"[worker] 作者 {pixiv_user_id} 同步失败: {e}", exc_info=True)
        async with await get_db() as db:
            cur = await db.execute(
                "SELECT retry_count FROM sync_jobs WHERE id=?", (job_id,)
            )
            row = await cur.fetchone()
            retry_count = (row[0] if row else 0) or 0
            new_retry = retry_count + 1
            if new_retry < _MAX_AUTHOR_RETRIES:
                # 未到上限：更新状态，延迟入队（非阻塞）
                await db.execute(
                    "UPDATE sync_jobs SET status='retry', retry_count=?, error=? WHERE id=?",
                    (new_retry, str(e), job_id),
                )
                await db.execute(
                    "UPDATE authors SET status='pending' WHERE pixiv_user_id=?",
                    (pixiv_user_id,),
                )
                await db.commit()
                delay = 60 * new_retry  # 60s / 120s / ...
                logger.warning(
                    f"[worker] 作者 {pixiv_user_id} 失败 (retry {new_retry}/{_MAX_AUTHOR_RETRIES})，"
                    f"{delay}s 后重新入队"
                )

                async def _delayed_enqueue(uid=pixiv_user_id, d=delay):
                    await asyncio.sleep(d)
                    await sync_queue.enqueue(uid)

                asyncio.create_task(_delayed_enqueue())
            else:
                await db.execute(
                    "UPDATE sync_jobs SET status='failed', finished_at=?, retry_count=?, error=? WHERE id=?",
                    (_now_iso(), new_retry, str(e), job_id),
                )
                await db.execute(
                    "UPDATE authors SET status='failed' WHERE pixiv_user_id=?",
                    (pixiv_user_id,),
                )
                await db.commit()
                logger.error(f"[worker] 作者 {pixiv_user_id} 超过最大重试次数，停止")


def _parse_next_url_offset(next_url: str) -> dict:
    """从 next_url 解析 offset 参数。"""
    from urllib.parse import urlparse, parse_qs
    params = parse_qs(urlparse(next_url).query)
    return {k: int(v[0]) for k, v in params.items() if v}


_stale_image_check_interval = 300  # 每 5 分钟检查一次过期重试图片


async def worker_loop() -> None:
    """主循环：持续从队列取作者 ID 并处理，定期清理过期重试图片。"""
    logger.info("[worker] 后台 worker 已启动")
    last_stale_check = 0.0
    while True:
        try:
            # 定期清理过期重试图片
            import time as _time
            now = _time.monotonic()
            if now - last_stale_check > _stale_image_check_interval:
                asyncio.create_task(_retry_stale_images())
                last_stale_check = now

            pixiv_user_id = await asyncio.wait_for(
                sync_queue.get(), timeout=_stale_image_check_interval
            )
            async with await get_db() as db:
                cursor = await db.execute(
                    "INSERT INTO sync_jobs (pixiv_user_id, status) VALUES (?, 'running') RETURNING id",
                    (pixiv_user_id,),
                )
                row = await cursor.fetchone()
                job_id = row[0]
                await db.commit()

            try:
                await _sync_author(job_id, pixiv_user_id)
            finally:
                await sync_queue.done(pixiv_user_id)
        except asyncio.TimeoutError:
            pass  # 队列空时循环，让 stale 检查有机会执行
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[worker] worker_loop 意外错误: {e}", exc_info=True)
            await asyncio.sleep(5)
