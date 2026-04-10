"""
SQLite 数据库模块 — 替代 JSON 文件存储
表: settings, models, sessions
支持从 JSON 自动迁移
"""

import json
import logging
import sqlite3
from pathlib import Path
from typing import Optional

logger = logging.getLogger("database")

DB_PATH = "streamvideo.db"


def get_db(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str = DB_PATH):
    """创建表结构"""
    conn = get_db(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS models (
            username TEXT PRIMARY KEY,
            url TEXT NOT NULL,
            platform TEXT DEFAULT '',
            identifier TEXT DEFAULT '',
            display_name TEXT DEFAULT '',
            live_url TEXT DEFAULT '',
            schedule TEXT DEFAULT '',
            quality TEXT DEFAULT 'best',
            auto_merge INTEGER DEFAULT 1,
            enabled INTEGER DEFAULT 1,
            last_online REAL DEFAULT 0,
            total_recordings INTEGER DEFAULT 0,
            created_at REAL DEFAULT (strftime('%s','now'))
        );

        CREATE TABLE IF NOT EXISTS sessions (
            session_id TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            started_at REAL DEFAULT 0,
            ended_at REAL DEFAULT 0,
            segments TEXT DEFAULT '[]',
            status TEXT DEFAULT 'active',
            merged_file TEXT DEFAULT '',
            merge_error TEXT DEFAULT '',
            FOREIGN KEY (username) REFERENCES models(username) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_sessions_username ON sessions(username);
        CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);
    """)
    conn.commit()
    conn.close()
    logger.info(f"Database initialized: {db_path}")


class Database:
    """同步 SQLite 数据库操作封装"""

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        init_db(db_path)
        self._migrate_from_json()

    def _conn(self) -> sqlite3.Connection:
        return get_db(self.db_path)

    # ========== Settings ==========

    def get_settings(self) -> dict:
        conn = self._conn()
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        conn.close()
        result = {}
        for r in rows:
            try:
                result[r["key"]] = json.loads(r["value"])
            except (json.JSONDecodeError, TypeError):
                result[r["key"]] = r["value"]
        return result

    def set_setting(self, key: str, value):
        conn = self._conn()
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (key, json.dumps(value))
        )
        conn.commit()
        conn.close()

    def set_settings(self, settings: dict):
        conn = self._conn()
        for k, v in settings.items():
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (k, json.dumps(v))
            )
        conn.commit()
        conn.close()

    # ========== Models ==========

    def get_models(self) -> list[dict]:
        conn = self._conn()
        rows = conn.execute("SELECT * FROM models ORDER BY created_at").fetchall()
        conn.close()
        result = []
        for r in rows:
            d = dict(r)
            d["schedule"] = json.loads(d["schedule"]) if d["schedule"] else None
            d["auto_merge"] = bool(d["auto_merge"])
            d["enabled"] = bool(d["enabled"])
            result.append(d)
        return result

    def upsert_model(self, username: str, url: str, **kwargs):
        conn = self._conn()
        schedule = kwargs.get("schedule")
        conn.execute("""
            INSERT INTO models (username, url, platform, display_name, quality, auto_merge, schedule)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(username) DO UPDATE SET
                url=excluded.url,
                platform=COALESCE(excluded.platform, platform),
                display_name=COALESCE(excluded.display_name, display_name),
                quality=COALESCE(excluded.quality, quality),
                auto_merge=COALESCE(excluded.auto_merge, auto_merge),
                schedule=COALESCE(excluded.schedule, schedule)
        """, (
            username, url,
            kwargs.get("platform", ""),
            kwargs.get("display_name", username),
            kwargs.get("quality", "best"),
            1 if kwargs.get("auto_merge", True) else 0,
            json.dumps(schedule) if schedule else "",
        ))
        conn.commit()
        conn.close()

    def delete_model(self, username: str):
        conn = self._conn()
        conn.execute("DELETE FROM models WHERE username = ?", (username,))
        conn.commit()
        conn.close()

    def update_model(self, username: str, **kwargs):
        conn = self._conn()
        sets = []
        vals = []
        for k, v in kwargs.items():
            if k == "schedule":
                sets.append("schedule = ?")
                vals.append(json.dumps(v) if v else "")
            elif k == "auto_merge":
                sets.append("auto_merge = ?")
                vals.append(1 if v else 0)
            elif k in ("quality", "platform", "display_name", "live_url", "url"):
                sets.append(f"{k} = ?")
                vals.append(v)
            elif k in ("last_online", "total_recordings"):
                sets.append(f"{k} = ?")
                vals.append(v)
        if sets:
            vals.append(username)
            conn.execute(f"UPDATE models SET {', '.join(sets)} WHERE username = ?", vals)
            conn.commit()
        conn.close()

    # ========== Sessions ==========

    def get_sessions(self, username: str) -> list[dict]:
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM sessions WHERE username = ? ORDER BY started_at DESC", (username,)
        ).fetchall()
        conn.close()
        result = []
        for r in rows:
            d = dict(r)
            d["segments"] = json.loads(d["segments"]) if d["segments"] else []
            result.append(d)
        return result

    def upsert_session(self, session: dict):
        conn = self._conn()
        conn.execute("""
            INSERT INTO sessions (session_id, username, started_at, ended_at, segments, status, merged_file, merge_error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                ended_at=excluded.ended_at,
                segments=excluded.segments,
                status=excluded.status,
                merged_file=excluded.merged_file,
                merge_error=excluded.merge_error
        """, (
            session["session_id"], session["username"],
            session.get("started_at", 0), session.get("ended_at", 0),
            json.dumps(session.get("segments", [])),
            session.get("status", "active"),
            session.get("merged_file", ""),
            session.get("merge_error", ""),
        ))
        conn.commit()
        conn.close()

    def update_session_status(self, session_id: str, status: str, **kwargs):
        conn = self._conn()
        sets = ["status = ?"]
        vals = [status]
        for k in ("merged_file", "merge_error", "ended_at"):
            if k in kwargs:
                sets.append(f"{k} = ?")
                vals.append(kwargs[k])
        vals.append(session_id)
        conn.execute(f"UPDATE sessions SET {', '.join(sets)} WHERE session_id = ?", vals)
        conn.commit()
        conn.close()

    def get_all_sessions_by_status(self, status: str) -> list[dict]:
        conn = self._conn()
        rows = conn.execute("SELECT * FROM sessions WHERE status = ?", (status,)).fetchall()
        conn.close()
        return [dict(r) | {"segments": json.loads(r["segments"])} for r in rows]

    # ========== Stats ==========

    def get_stats(self, username: Optional[str] = None) -> dict:
        conn = self._conn()
        if username:
            row = conn.execute("""
                SELECT COUNT(*) as session_count,
                       SUM(CASE WHEN status='merged' THEN 1 ELSE 0 END) as merged_sessions,
                       SUM(ended_at - started_at) as total_duration
                FROM sessions WHERE username = ? AND ended_at > 0
            """, (username,)).fetchone()
        else:
            row = conn.execute("""
                SELECT COUNT(*) as session_count,
                       SUM(CASE WHEN status='merged' THEN 1 ELSE 0 END) as merged_sessions,
                       SUM(ended_at - started_at) as total_duration
                FROM sessions WHERE ended_at > 0
            """).fetchone()
        conn.close()
        return dict(row) if row else {}

    # ========== Migration ==========

    def _migrate_from_json(self):
        """从 JSON 文件迁移数据到 SQLite（仅首次运行）"""
        config_path = Path("config.json")
        if not config_path.exists():
            return

        conn = self._conn()
        # 检查是否已迁移
        count = conn.execute("SELECT COUNT(*) FROM settings").fetchone()[0]
        if count > 0:
            conn.close()
            return

        logger.info("Migrating from JSON to SQLite...")
        try:
            with open(config_path) as f:
                config = json.load(f)

            # 迁移设置
            for key in ("auto_merge", "merge_gap_minutes", "auto_delete_originals",
                         "min_segment_size_kb", "smart_rename", "webhooks"):
                if key in config:
                    conn.execute(
                        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                        (key, json.dumps(config[key]))
                    )

            # 迁移主播
            for item in config.get("models", []):
                if isinstance(item, dict):
                    url = item.get("url", "")
                    name = item.get("name", "")
                    schedule = item.get("schedule")
                    quality = item.get("quality", "best")
                else:
                    url = item
                    name = ""
                    schedule = None
                    quality = "best"
                if url:
                    conn.execute("""
                        INSERT OR IGNORE INTO models (username, url, display_name, quality, schedule)
                        VALUES (?, ?, ?, ?, ?)
                    """, (name or url, url, name, quality, json.dumps(schedule) if schedule else ""))

            conn.commit()

            # 迁移 sessions.json
            recordings_dir = Path("recordings")
            if recordings_dir.exists():
                for d in recordings_dir.iterdir():
                    if not d.is_dir() or d.name in ("thumbs", "logs"):
                        continue
                    sessions_path = d / "sessions.json"
                    if sessions_path.exists():
                        try:
                            with open(sessions_path) as f:
                                sessions = json.load(f)
                            for s in sessions:
                                conn.execute("""
                                    INSERT OR IGNORE INTO sessions
                                    (session_id, username, started_at, ended_at, segments, status, merged_file, merge_error)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                """, (
                                    s.get("session_id", ""), s.get("username", d.name),
                                    s.get("started_at", 0), s.get("ended_at", 0),
                                    json.dumps(s.get("segments", [])),
                                    s.get("status", ""), s.get("merged_file", ""),
                                    s.get("merge_error", ""),
                                ))
                            conn.commit()
                        except Exception as e:
                            logger.warning(f"Failed to migrate sessions for {d.name}: {e}")

            logger.info("Migration complete")
        except Exception as e:
            logger.error(f"Migration failed: {e}")
        finally:
            conn.close()
