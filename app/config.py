import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


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
    VERSION: str = "1.0.0"


settings = Settings()
settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
settings.IMAGES_DIR.mkdir(parents=True, exist_ok=True)

if settings.HTTP_PROXY:
    os.environ["http_proxy"] = settings.HTTP_PROXY
    os.environ["https_proxy"] = settings.HTTP_PROXY
    os.environ["HTTP_PROXY"] = settings.HTTP_PROXY
    os.environ["HTTPS_PROXY"] = settings.HTTP_PROXY
