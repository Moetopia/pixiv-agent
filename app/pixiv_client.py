"""
pixivpy3 封装：token 自动刷新 + 429 兜底 + 重试逻辑。
"""
import asyncio
import logging
import time
from pathlib import Path
from typing import Optional

from app.config import settings

logger = logging.getLogger(__name__)

_api = None
_token_refreshed_at: float = 0
_TOKEN_EXPIRE_SECONDS = 3000

# 重试配置
_MAX_API_RETRIES = 3
_RETRY_DELAYS = [15, 45, 120]   # 每次重试前等待秒数
_MAX_IMG_RETRIES = 3
_IMG_RETRY_DELAYS = [10, 30, 90]


class PixivRateLimitError(Exception):
    """Pixiv 返回 429 或检测到速率限制时抛出。"""
    def __init__(self, retry_after: int = 120):
        self.retry_after = retry_after
        super().__init__(f"Pixiv rate limit, retry_after={retry_after}s")


def get_api():
    global _api, _token_refreshed_at
    from pixivpy3 import AppPixivAPI
    now = time.time()
    if _api is None or (now - _token_refreshed_at) > _TOKEN_EXPIRE_SECONDS:
        if not settings.PIXIV_REFRESH_TOKEN:
            raise RuntimeError("PIXIV_REFRESH_TOKEN 未配置")
        if _api is None:
            _api = AppPixivAPI()
            from requests.adapters import HTTPAdapter
            adapter = HTTPAdapter(pool_connections=20, pool_maxsize=50)
            _api.requests.mount('http://', adapter)
            _api.requests.mount('https://', adapter)

            if settings.HTTP_PROXY:
                proxies = {
                    "http": settings.HTTP_PROXY,
                    "https": settings.HTTP_PROXY,
                }
                _api.requests.proxies.update(proxies)
                logger.info(f"🔧 Pixiv API 已启用网络代理: {settings.HTTP_PROXY}")
        _api.auth(refresh_token=settings.PIXIV_REFRESH_TOKEN)
        _token_refreshed_at = now
        logger.info("✅ Pixiv token 已刷新")
    return _api


def _check_rate_limit_in_exception(exc: Exception) -> Optional[int]:
    """
    从异常中提取 429 信息。
    返回建议等待秒数，若不是 429 返回 None。
    """
    try:
        import requests
        if isinstance(exc, requests.HTTPError) and exc.response is not None:
            if exc.response.status_code == 429:
                try:
                    return int(exc.response.headers.get("Retry-After", 120))
                except (ValueError, TypeError):
                    return 120
    except ImportError:
        pass
    msg = str(exc).lower()
    if "429" in msg or "too many requests" in msg or "rate limit" in msg:
        return 120
    return None


def _check_rate_limit_in_result(result: dict) -> Optional[int]:
    """检查 Pixiv API JSON 响应中的错误字段是否包含速率限制。"""
    if not isinstance(result, dict):
        return None
    err = result.get("error")
    if not err:
        return None
    msg = (
        (err.get("message") or "")
        + " "
        + (err.get("user_message") or "")
    ).lower()
    if "rate" in msg or "429" in msg or "too many" in msg:
        return 120
    return None


async def _api_call_with_retry(sync_fn, context: str = ""):
    """
    在 asyncio.to_thread 中运行 sync_fn，带重试和 429 兜底。
    - 429 → 通知全局冷却，立即抛 PixivRateLimitError（不重试）
    - 网络/服务器错误 → 最多重试 _MAX_API_RETRIES 次（指数等待）
    """
    from app.queue import cooldown_state

    for attempt in range(_MAX_API_RETRIES):
        try:
            result = await asyncio.to_thread(sync_fn)

            ra = _check_rate_limit_in_result(result)
            if ra is not None:
                await cooldown_state.notify_ratelimit(ra)
                raise PixivRateLimitError(ra)

            await cooldown_state.notify_success()
            return result

        except PixivRateLimitError:
            raise  # 不重试 429

        except Exception as exc:
            ra = _check_rate_limit_in_exception(exc)
            if ra is not None:
                await cooldown_state.notify_ratelimit(ra)
                raise PixivRateLimitError(ra)

            if attempt < _MAX_API_RETRIES - 1:
                delay = _RETRY_DELAYS[attempt]
                logger.warning(
                    f"[pixiv_client] {context} attempt {attempt + 1}/{_MAX_API_RETRIES} "
                    f"失败: {exc}。{delay}s 后重试..."
                )
                await asyncio.sleep(delay)
            else:
                logger.error(f"[pixiv_client] {context} 全部重试失败: {exc}")
                raise


async def fetch_user_detail(pixiv_user_id: int) -> Optional[dict]:
    """拉取 Pixiv 用户详情，含重试和 429 兜底。"""
    def _call():
        api = get_api()
        return api.user_detail(pixiv_user_id)

    result = await _api_call_with_retry(_call, f"user_detail({pixiv_user_id})")
    if not result or result.get("error"):
        return None
    user = result.get("user", {})
    profile = result.get("profile", {})
    return {
        "pixiv_user_id": pixiv_user_id,
        "username": user.get("name", ""),
        "bio": (user.get("comment") or "").strip() or None,
        "avatar_url": user.get("profile_image_urls", {}).get("medium"),
        "website_url": profile.get("webpage") or None,
        "twitter_url": profile.get("twitter_url") or None,
        "background_url": profile.get("background_image_url") or None,
    }


async def fetch_user_illusts_page(pixiv_user_id: int, offset: int = 0, type: str = "illust") -> Optional[dict]:
    """拉取用户作品列表的一页，含重试和 429 兜底。"""
    def _call():
        api = get_api()
        return api.user_illusts(pixiv_user_id, type=type, offset=offset)
    return await _api_call_with_retry(_call, f"user_illusts({pixiv_user_id}, type={type}, offset={offset})")


async def fetch_illust_detail(pixiv_id: int) -> Optional[dict]:
    """拉取单个作品详情，含重试。"""
    def _call():
        api = get_api()
        result = api.illust_detail(pixiv_id)
        return result.get("illust") if not result.get("error") else None
    return await _api_call_with_retry(_call, f"illust_detail({pixiv_id})")


async def download_image(url: str, dest_path: Path) -> bool:
    """
    用 pixivpy3 session 下载图片到本地，含重试和 429 兜底。
    返回是否成功。
    """
    from app.queue import cooldown_state

    def _call():
        api = get_api()
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        response = api.requests.get(
            url,
            headers={"Referer": "https://www.pixiv.net/"},
            timeout=30,
            stream=True,
        )
        response.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=65536):
                f.write(chunk)
        return True

    for attempt in range(_MAX_IMG_RETRIES):
        try:
            result = await asyncio.to_thread(_call)
            await cooldown_state.notify_success()
            return True
        except Exception as exc:
            ra = _check_rate_limit_in_exception(exc)
            if ra is not None:
                await cooldown_state.notify_ratelimit(ra)
                logger.warning(f"[pixiv_client] 图片下载 429: {url}")
                # 下载 429 时等冷却后让调用方决定是否重试
                return False
            if attempt < _MAX_IMG_RETRIES - 1:
                delay = _IMG_RETRY_DELAYS[attempt]
                logger.warning(
                    f"[pixiv_client] 图片下载失败 attempt {attempt + 1}/{_MAX_IMG_RETRIES}: "
                    f"{url} → {exc}。{delay}s 后重试..."
                )
                await asyncio.sleep(delay)
            else:
                logger.error(f"[pixiv_client] 图片下载全部重试失败: {url} → {exc}")
                return False
    return False


RATING_MAP = {0: "safe", 1: "r18", 2: "r18g"}


def parse_illust(illust: dict) -> dict:
    """将 Pixiv API illust 对象标准化为内部字典。"""
    tags = []
    tag_bilingual = []
    seen = set()
    for t in illust.get("tags", []):
        for val in (t.get("name"), t.get("translated_name")):
            if val and val not in seen:
                tags.append(val)
                seen.add(val)
        name = t.get("name", "")
        if name:
            tag_bilingual.append({"ja": name, "translated": t.get("translated_name")})

    upload_tags = [t["ja"].strip().lower() for t in tag_bilingual if t["ja"]][:20]

    meta_page_urls = []
    if illust.get("meta_single_page", {}).get("original_image_url"):
        meta_page_urls = [illust["meta_single_page"]["original_image_url"]]
    elif illust.get("meta_pages"):
        meta_page_urls = [p["image_urls"]["original"] for p in illust["meta_pages"] if p.get("image_urls", {}).get("original")]

    return {
        "pixiv_id": illust["id"],
        "pixiv_user_id": illust.get("user", {}).get("id"),
        "title": (illust.get("title") or "")[:100],
        "description": illust.get("caption", "") or "",
        "tags": upload_tags,
        "tag_bilingual": tag_bilingual,
        "rating": RATING_MAP.get(illust.get("x_restrict", 0), "safe"),
        "is_ai": illust.get("illust_ai_type", 0) >= 2,
        "artwork_type": "manga" if illust.get("type") == "manga" else "illustration",
        "page_count": illust.get("page_count", 1),
        "source_url": f"https://www.pixiv.net/artworks/{illust['id']}",
        "image_urls": meta_page_urls,
        "series": illust.get("series"),
        "create_date": illust.get("create_date"),
    }
