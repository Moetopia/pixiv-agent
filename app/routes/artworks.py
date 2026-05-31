import json
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from app.auth import require_api_key
from app.database import get_db

router = APIRouter()


def _row_to_artwork(row) -> dict:
    d = dict(row)
    try:
        d["tags"] = json.loads(d.get("tags_json") or "[]")
    except Exception:
        d["tags"] = []
    d.pop("tags_json", None)
    d["is_ai"] = bool(d.get("is_ai"))
    return d


@router.get("/artworks")
async def list_artworks(
    since: str = Query(None, description="ISO8601 时间戳，只返回此时间之后拉取的作品"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    pixiv_user_id: int = Query(None),
    _: None = Depends(require_api_key),
):
    """分页返回已缓存的作品元数据列表。"""
    async with get_db() as db:
        conditions = []
        params = []

        if since:
            conditions.append("fetched_at > ?")
            params.append(since)
        if pixiv_user_id:
            conditions.append("pixiv_user_id = ?")
            params.append(pixiv_user_id)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        rows = await db.execute_fetchall(
            f"SELECT * FROM artworks {where} ORDER BY fetched_at DESC LIMIT ? OFFSET ?",
            (*params, limit, offset),
        )
    return [_row_to_artwork(r) for r in rows]


@router.get("/artworks/{pixiv_id}")
async def get_artwork(
    pixiv_id: int,
    _: None = Depends(require_api_key),
):
    """获取单个作品元数据（含 images 列表 + 作者用户名）。"""
    async with get_db() as db:
        row = await db.execute_fetchone(
            """
            SELECT a.*, au.username AS author_username, au.avatar_url AS author_avatar_url
            FROM artworks a
            LEFT JOIN authors au ON a.pixiv_user_id = au.pixiv_user_id
            WHERE a.pixiv_id=?
            """,
            (pixiv_id,),
        )
        if not row:
            raise HTTPException(status_code=404, detail="作品不在本节点缓存中")

        img_rows = await db.execute_fetchall(
            "SELECT page_index, original_url, local_path, downloaded, failed FROM images WHERE pixiv_id=? ORDER BY page_index",
            (pixiv_id,),
        )

    artwork = _row_to_artwork(row)
    artwork["images"] = [dict(r) for r in img_rows]
    return artwork


@router.get("/artworks/{pixiv_id}/images/{page_index}")
async def get_image_file(
    pixiv_id: int,
    page_index: int,
    _: None = Depends(require_api_key),
):
    """流式返回本地缓存的图片文件。"""
    async with get_db() as db:
        row = await db.execute_fetchone(
            "SELECT local_path, downloaded FROM images WHERE pixiv_id=? AND page_index=?",
            (pixiv_id, page_index),
        )

    if not row:
        raise HTTPException(status_code=404, detail="图片记录不存在")
    if not row["downloaded"] or not row["local_path"]:
        raise HTTPException(status_code=202, detail="图片尚未下载完成")

    path = Path(row["local_path"])
    if not path.exists():
        raise HTTPException(status_code=404, detail="图片文件不存在")

    media_type_map = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png", ".webp": "image/webp", ".gif": "image/gif",
    }
    media_type = media_type_map.get(path.suffix.lower(), "application/octet-stream")
    return FileResponse(path, media_type=media_type)


@router.get("/authors/{pixiv_user_id}/avatar")
async def get_author_avatar(
    pixiv_user_id: int,
    _: None = Depends(require_api_key),
):
    """返回作者本地头像。"""
    async with get_db() as db:
        row = await db.execute_fetchone(
            "SELECT avatar_local_path FROM authors WHERE pixiv_user_id=?",
            (pixiv_user_id,)
        )
    if not row or not row["avatar_local_path"]:
        raise HTTPException(status_code=404, detail="作者未找到或头像未下载")
    
    path = Path(row["avatar_local_path"])
    if not path.exists():
        raise HTTPException(status_code=404, detail="头像文件不存在")
        
    media_type_map = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png", ".webp": "image/webp", ".gif": "image/gif",
    }
    media_type = media_type_map.get(path.suffix.lower(), "image/jpeg")
    return FileResponse(path, media_type=media_type)
