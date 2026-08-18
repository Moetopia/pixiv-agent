import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


class Settings:
    PIXIV_REFRESH_TOKEN: str = os.environ.get("PIXIV_REFRESH_TOKEN", "")
    AGENT_API_KEY: str = os.environ.get("AGENT_API_KEY", "changeme")
    PORT: int = int(os.environ.get("PORT", 8100))
    DATA_DIR: Path = Path(os.environ.get("DATA_DIR", "./data"))
    IMAGES_DIR: Path = DATA_DIR / "images"
    DB_PATH: Path = DATA_DIR / "agent.db"
    RATE_LIMIT: float = float(os.environ.get("RATE_LIMIT", 0.5))
    MAX_ARTWORKS_PER_AUTHOR: int = int(os.environ.get("MAX_ARTWORKS_PER_AUTHOR", 0))
    DOWNLOAD_CONCURRENCY: int = int(os.environ.get("DOWNLOAD_CONCURRENCY", 2))
    NODE_NAME: str = os.environ.get("NODE_NAME", "agent-01")
    HTTP_PROXY: str = os.environ.get("HTTP_PROXY", "")
    # 滚动清理图片缓存：默认只清理已被主服务器取过的旧图片。
    CACHE_CLEANUP_ENABLED: bool = _env_bool("CACHE_CLEANUP_ENABLED", True)
    CACHE_CLEANUP_THRESHOLD_PERCENT: float = float(
        os.environ.get("CACHE_CLEANUP_THRESHOLD_PERCENT", 15.0)
    )
    CACHE_CLEANUP_TARGET_PERCENT: float = float(
        os.environ.get("CACHE_CLEANUP_TARGET_PERCENT", 20.0)
    )
    CACHE_CLEANUP_MIN_AGE_DAYS: int = int(
        os.environ.get("CACHE_CLEANUP_MIN_AGE_DAYS", 7)
    )
    CACHE_CLEANUP_INTERVAL_SECONDS: int = int(
        os.environ.get("CACHE_CLEANUP_INTERVAL_SECONDS", 300)
    )
    CACHE_CLEANUP_BATCH_SIZE: int = int(
        os.environ.get("CACHE_CLEANUP_BATCH_SIZE", 100)
    )
    CACHE_CLEANUP_ONLY_SERVED: bool = _env_bool(
        "CACHE_CLEANUP_ONLY_SERVED", True
    )
    CACHE_PAUSE_SYNC_PERCENT: float = float(
        os.environ.get("CACHE_PAUSE_SYNC_PERCENT", 5.0)
    )
    VERSION: str = "1.0.0"


settings = Settings()
settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
settings.IMAGES_DIR.mkdir(parents=True, exist_ok=True)

if settings.HTTP_PROXY:
    os.environ["http_proxy"] = settings.HTTP_PROXY
    os.environ["https_proxy"] = settings.HTTP_PROXY
    os.environ["HTTP_PROXY"] = settings.HTTP_PROXY
    os.environ["HTTPS_PROXY"] = settings.HTTP_PROXY
