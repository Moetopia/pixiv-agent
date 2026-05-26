"""
SQLite 数据库初始化与连接管理（aiosqlite）。
所有表结构在 init_db() 中创建。
"""
import aiosqlite
from app.config import settings

_db_path = str(settings.DB_PATH)


async def get_db() -> aiosqlite.Connection:
    """返回一个新的 aiosqlite 连接（调用方负责关闭）。"""
    conn = await aiosqlite.connect(_db_path)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA foreign_keys=ON")
    return conn


async def init_db() -> None:
    """建表（如不存在则创建）。"""
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA foreign_keys=ON")

        await db.executescript("""
            CREATE TABLE IF NOT EXISTS authors (
                pixiv_user_id   INTEGER PRIMARY KEY,
                username        TEXT    NOT NULL DEFAULT '',
                bio             TEXT,
                website_url     TEXT,
                twitter_url     TEXT,
                avatar_url      TEXT,
                avatar_local_path TEXT,
                status          TEXT    NOT NULL DEFAULT 'pending',
                last_synced_at  TEXT,
                artwork_count   INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
                updated_at      TEXT    NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS artworks (
                pixiv_id        INTEGER PRIMARY KEY,
                pixiv_user_id   INTEGER NOT NULL,
                title           TEXT    NOT NULL DEFAULT '',
                description     TEXT,
                tags_json       TEXT    NOT NULL DEFAULT '[]',
                rating          TEXT    NOT NULL DEFAULT 'safe',
                is_ai           INTEGER NOT NULL DEFAULT 0,
                artwork_type    TEXT    NOT NULL DEFAULT 'illustration',
                page_count      INTEGER NOT NULL DEFAULT 1,
                source_url      TEXT,
                fetched_at      TEXT    NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (pixiv_user_id) REFERENCES authors(pixiv_user_id)
            );

            CREATE INDEX IF NOT EXISTS idx_artworks_user
                ON artworks(pixiv_user_id);
            CREATE INDEX IF NOT EXISTS idx_artworks_fetched
                ON artworks(fetched_at);

            CREATE TABLE IF NOT EXISTS images (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                pixiv_id        INTEGER NOT NULL,
                page_index      INTEGER NOT NULL DEFAULT 0,
                original_url    TEXT    NOT NULL,
                local_path      TEXT,
                downloaded      INTEGER NOT NULL DEFAULT 0,
                failed          INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
                UNIQUE(pixiv_id, page_index),
                FOREIGN KEY (pixiv_id) REFERENCES artworks(pixiv_id)
            );

            CREATE TABLE IF NOT EXISTS sync_jobs (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                pixiv_user_id   INTEGER NOT NULL,
                status          TEXT    NOT NULL DEFAULT 'pending',
                error           TEXT,
                artworks_found  INTEGER NOT NULL DEFAULT 0,
                artworks_new    INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
                started_at      TEXT,
                finished_at     TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_sync_jobs_user
                ON sync_jobs(pixiv_user_id, status);

            -- 节点日志表（供主服务器轮询）
            CREATE TABLE IF NOT EXISTS node_logs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                level       TEXT    NOT NULL DEFAULT 'INFO',
                logger      TEXT    NOT NULL DEFAULT '',
                message     TEXT    NOT NULL DEFAULT '',
                created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_node_logs_created
                ON node_logs(created_at DESC);
        """)

        # 渐进式 ALTER — 已存在时静默跳过（SQLite 3.37+）
        for sql in [
            "ALTER TABLE sync_jobs ADD COLUMN retry_count INT NOT NULL DEFAULT 0",
            "ALTER TABLE sync_jobs ADD COLUMN retry_after TEXT",
            "ALTER TABLE images    ADD COLUMN retry_count INT NOT NULL DEFAULT 0",
            "ALTER TABLE images    ADD COLUMN retry_after TEXT",
        ]:
            try:
                await db.execute(sql)
            except Exception:
                pass  # 列已存在

        await db.commit()
