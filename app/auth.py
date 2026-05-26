from fastapi import Header, HTTPException
from app.config import settings


async def require_api_key(x_api_key: str = Header(..., alias="X-API-Key")) -> None:
    """验证请求头中的 X-API-Key。"""
    if x_api_key != settings.AGENT_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
