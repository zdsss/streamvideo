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
            retry_count INTEGER DEFAULT 0,
            merge_started_at REAL DEFAULT 0,
            stream_end_reason TEXT DEFAULT '',
            FOREIGN KEY (username) REFERENCES models(username) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_sessions_username ON sessions(username);
        CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);

        CREATE TABLE IF NOT EXISTS merge_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            session_id TEXT DEFAULT '',
            input_files TEXT DEFAULT '[]',
            input_size INTEGER DEFAULT 0,
            output_file TEXT DEFAULT '',
            output_size INTEGER DEFAULT 0,
            savings_bytes INTEGER DEFAULT 0,
            completed_at REAL DEFAULT 0,
            status TEXT DEFAULT 'done',
            error TEXT DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_merge_history_username ON merge_history(username);
        CREATE INDEX IF NOT EXISTS idx_merge_history_completed_at ON merge_history(completed_at DESC);

        CREATE TABLE IF NOT EXISTS danmaku (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            username TEXT NOT NULL,
            file_path TEXT DEFAULT '',
            message_count INTEGER DEFAULT 0,
            peak_density REAL DEFAULT 0,
            keywords_found TEXT DEFAULT '{}',
            created_at REAL DEFAULT (strftime('%s','now'))
        );
        CREATE INDEX IF NOT EXISTS idx_danmaku_session ON danmaku(session_id);
        CREATE INDEX IF NOT EXISTS idx_danmaku_username ON danmaku(username);

        CREATE TABLE IF NOT EXISTS highlights (
            highlight_id TEXT PRIMARY KEY,
            session_id TEXT DEFAULT '',
            username TEXT NOT NULL,
            video_file TEXT NOT NULL,
            start_time REAL NOT NULL,
            end_time REAL NOT NULL,
            score REAL DEFAULT 0,
            category TEXT DEFAULT '',
            signals TEXT DEFAULT '[]',
            title TEXT DEFAULT '',
            status TEXT DEFAULT 'detected',
            created_at REAL DEFAULT (strftime('%s','now'))
        );
        CREATE INDEX IF NOT EXISTS idx_highlights_username ON highlights(username);
        CREATE INDEX IF NOT EXISTS idx_highlights_session ON highlights(session_id);
        CREATE INDEX IF NOT EXISTS idx_highlights_score ON highlights(score DESC);

        CREATE TABLE IF NOT EXISTS clips (
            clip_id TEXT PRIMARY KEY,
            highlight_id TEXT DEFAULT '',
            username TEXT NOT NULL,
            output_file TEXT DEFAULT '',
            resolution TEXT DEFAULT '',
            duration REAL DEFAULT 0,
            format TEXT DEFAULT '',
            size INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending',
            export_url TEXT DEFAULT '',
            title TEXT DEFAULT '',
            description TEXT DEFAULT '',
            tags TEXT DEFAULT '[]',
            created_at REAL DEFAULT (strftime('%s','now'))
        );
        CREATE INDEX IF NOT EXISTS idx_clips_username ON clips(username);
        CREATE INDEX IF NOT EXISTS idx_clips_highlight ON clips(highlight_id);

        CREATE TABLE IF NOT EXISTS highlight_rules (
            rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT DEFAULT '',
            keywords TEXT DEFAULT '[]',
            min_score REAL DEFAULT 0.6,
            min_duration INTEGER DEFAULT 15,
            max_duration INTEGER DEFAULT 60,
            weights TEXT DEFAULT '{}',
            enabled INTEGER DEFAULT 1,
            created_at REAL DEFAULT (strftime('%s','now'))
        );

        CREATE TABLE IF NOT EXISTS user_quotas (
            username TEXT NOT NULL,
            date TEXT NOT NULL,
            clips_generated INTEGER DEFAULT 0,
            PRIMARY KEY (username, date)
        );

        CREATE TABLE IF NOT EXISTS user_tiers (
            username TEXT PRIMARY KEY,
            tier TEXT DEFAULT 'free',
            expires_at REAL DEFAULT 0
        );
    """)
    conn.commit()
    conn.close()
    logger.info(f"Database initialized: {db_path}")


class Database:
    """同步 SQLite 数据库操作封装"""

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        init_db(db_path)
        self._migrate_schema()
        self._migrate_from_json()

    def _conn(self) -> sqlite3.Connection:
        return get_db(self.db_path)

    def _migrate_schema(self):
        """增量 schema 迁移：为已有数据库添加新列"""
        conn = self._conn()
        try:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()}
            migrations = [
                ("retry_count", "INTEGER DEFAULT 0"),
                ("merge_started_at", "REAL DEFAULT 0"),
                ("stream_end_reason", "TEXT DEFAULT ''"),
            ]
            for col, typedef in migrations:
                if col not in cols:
                    conn.execute(f"ALTER TABLE sessions ADD COLUMN {col} {typedef}")
                    logger.info(f"Schema migration: added sessions.{col}")
            # clips 表迁移
            clip_cols = {r[1] for r in conn.execute("PRAGMA table_info(clips)").fetchall()}
            if clip_cols:  # 表存在
                clip_migrations = [
                    ("title", "TEXT DEFAULT ''"),
                    ("description", "TEXT DEFAULT ''"),
                    ("tags", "TEXT DEFAULT '[]'"),
                ]
                for col, typedef in clip_migrations:
                    if col not in clip_cols:
                        conn.execute(f"ALTER TABLE clips ADD COLUMN {col} {typedef}")
                        logger.info(f"Schema migration: added clips.{col}")
            conn.commit()
        except Exception as e:
            logger.warning(f"Schema migration error: {e}")
        finally:
            conn.close()

    # ========== Settings ==========

    def get_settings(self) -> dict:
        conn = self._conn()
        try:
            rows = conn.execute("SELECT key, value FROM settings").fetchall()
            result = {}
            for r in rows:
                try:
                    result[r["key"]] = json.loads(r["value"])
                except (json.JSONDecodeError, TypeError):
                    result[r["key"]] = r["value"]
            return result
        finally:
            conn.close()

    def set_setting(self, key: str, value):
        conn = self._conn()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (key, json.dumps(value))
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def set_settings(self, settings: dict):
        conn = self._conn()
        try:
            for k, v in settings.items():
                conn.execute(
                    "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                    (k, json.dumps(v))
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ========== Models ==========

    def get_models(self) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute("SELECT * FROM models ORDER BY created_at").fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["schedule"] = json.loads(d["schedule"]) if d["schedule"] else None
                d["auto_merge"] = bool(d["auto_merge"])
                d["enabled"] = bool(d["enabled"])
                result.append(d)
            return result
        finally:
            conn.close()

    def upsert_model(self, username: str, url: str, **kwargs):
        conn = self._conn()
        try:
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
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def delete_model(self, username: str):
        """删除主播及其所有关联数据"""
        conn = self._conn()
        try:
            conn.execute("DELETE FROM models WHERE username = ?", (username,))
            # sessions 有 CASCADE，但显式清理更安全
            conn.execute("DELETE FROM sessions WHERE username = ?", (username,))
            conn.execute("DELETE FROM highlights WHERE username = ?", (username,))
            conn.execute("DELETE FROM clips WHERE username = ?", (username,))
            conn.execute("DELETE FROM danmaku WHERE username = ?", (username,))
            conn.execute("DELETE FROM merge_history WHERE username = ?", (username,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def update_model(self, username: str, **kwargs):
        _ALLOWED_COLUMNS = {"quality", "platform", "display_name", "live_url", "url", "last_online", "total_recordings"}
        conn = self._conn()
        try:
            sets = []
            vals = []
            for k, v in kwargs.items():
                if k == "schedule":
                    sets.append("schedule = ?")
                    vals.append(json.dumps(v) if v else "")
                elif k == "auto_merge":
                    sets.append("auto_merge = ?")
                    vals.append(1 if v else 0)
                elif k in _ALLOWED_COLUMNS:
                    sets.append(f"{k} = ?")
                    vals.append(v)
            if sets:
                vals.append(username)
                conn.execute(f"UPDATE models SET {', '.join(sets)} WHERE username = ?", vals)
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ========== Sessions ==========

    def get_sessions(self, username: str) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM sessions WHERE username = ? ORDER BY started_at DESC", (username,)
            ).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["segments"] = json.loads(d["segments"]) if d["segments"] else []
                d.setdefault("retry_count", 0)
                d.setdefault("merge_started_at", 0)
                d.setdefault("stream_end_reason", "")
                result.append(d)
            return result
        finally:
            conn.close()

    def upsert_session(self, session: dict):
        conn = self._conn()
        try:
            conn.execute("""
                INSERT INTO sessions (session_id, username, started_at, ended_at, segments, status,
                    merged_file, merge_error, retry_count, merge_started_at, stream_end_reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    ended_at=excluded.ended_at,
                    segments=excluded.segments,
                    status=excluded.status,
                    merged_file=excluded.merged_file,
                    merge_error=excluded.merge_error,
                    retry_count=excluded.retry_count,
                    merge_started_at=excluded.merge_started_at,
                    stream_end_reason=excluded.stream_end_reason
            """, (
                session["session_id"], session["username"],
                session.get("started_at", 0), session.get("ended_at", 0),
                json.dumps(session.get("segments", [])),
                session.get("status", "active"),
                session.get("merged_file", ""),
                session.get("merge_error", ""),
                session.get("retry_count", 0),
                session.get("merge_started_at", 0),
                session.get("stream_end_reason", ""),
            ))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def update_session_status(self, session_id: str, status: str, **kwargs):
        conn = self._conn()
        try:
            sets = ["status = ?"]
            vals = [status]
            for k in ("merged_file", "merge_error", "ended_at", "retry_count",
                      "merge_started_at", "stream_end_reason", "segments"):
                if k in kwargs:
                    sets.append(f"{k} = ?")
                    v = kwargs[k]
                    vals.append(json.dumps(v) if k == "segments" else v)
            vals.append(session_id)
            conn.execute(f"UPDATE sessions SET {', '.join(sets)} WHERE session_id = ?", vals)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_all_sessions_by_status(self, status: str) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute("SELECT * FROM sessions WHERE status = ?", (status,)).fetchall()
            return [dict(r) | {"segments": json.loads(r["segments"])} for r in rows]
        finally:
            conn.close()

    # ========== Merge History ==========

    def insert_merge_history(self, username: str, session_id: str = "",
                             input_files: list[str] = None, input_size: int = 0,
                             output_file: str = "", output_size: int = 0,
                             savings_bytes: int = 0, status: str = "done", error: str = ""):
        conn = self._conn()
        try:
            conn.execute("""
                INSERT INTO merge_history (username, session_id, input_files, input_size,
                    output_file, output_size, savings_bytes, completed_at, status, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, strftime('%s','now'), ?, ?)
            """, (
                username, session_id, json.dumps(input_files or []), input_size,
                output_file, output_size, savings_bytes, status, error,
            ))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_merge_history(self, username: str, limit: int = 50) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM merge_history WHERE username = ? ORDER BY completed_at DESC LIMIT ?",
                (username, limit)
            ).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["input_files"] = json.loads(d["input_files"]) if d["input_files"] else []
                result.append(d)
            return result
        finally:
            conn.close()

    def get_all_merge_history(self, limit: int = 100) -> list[dict]:
        """获取全局合并历史（不限主播）"""
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM merge_history ORDER BY completed_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["input_files"] = json.loads(d["input_files"]) if d["input_files"] else []
                result.append(d)
            return result
        finally:
            conn.close()

    # ========== Daily Stats ==========

    def get_daily_stats(self, days: int = 30) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute("""
                SELECT date(started_at, 'unixepoch', 'localtime') as day,
                       COUNT(*) as sessions,
                       COALESCE(SUM(ended_at - started_at), 0) as total_duration
                FROM sessions
                WHERE started_at > strftime('%s','now') - ? * 86400
                  AND ended_at > 0
                GROUP BY day ORDER BY day
            """, (days,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # ========== Stats ==========

    def get_stats(self, username: Optional[str] = None) -> dict:
        conn = self._conn()
        try:
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
            return dict(row) if row else {}
        finally:
            conn.close()

    # ========== Danmaku ==========

    def upsert_danmaku(self, session_id: str, username: str, file_path: str,
                       message_count: int = 0, peak_density: float = 0, keywords_found: dict = None):
        conn = self._conn()
        try:
            conn.execute("""
                INSERT INTO danmaku (session_id, username, file_path, message_count, peak_density, keywords_found)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    file_path=excluded.file_path, message_count=excluded.message_count,
                    peak_density=excluded.peak_density, keywords_found=excluded.keywords_found
            """, (session_id, username, file_path, message_count, peak_density,
                  json.dumps(keywords_found or {})))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_danmaku(self, session_id: str) -> Optional[dict]:
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM danmaku WHERE session_id = ?", (session_id,)).fetchone()
            if row:
                d = dict(row)
                d["keywords_found"] = json.loads(d["keywords_found"]) if d["keywords_found"] else {}
                return d
            return None
        finally:
            conn.close()

    def get_danmaku_by_username(self, username: str, limit: int = 20) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM danmaku WHERE username = ? ORDER BY created_at DESC LIMIT ?",
                (username, limit)).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["keywords_found"] = json.loads(d["keywords_found"]) if d["keywords_found"] else {}
                result.append(d)
            return result
        finally:
            conn.close()

    # ========== Highlights ==========

    def insert_highlight(self, highlight_id: str, session_id: str, username: str,
                         video_file: str, start_time: float, end_time: float,
                         score: float = 0, category: str = "", signals: list = None, title: str = ""):
        conn = self._conn()
        try:
            conn.execute("""
                INSERT OR REPLACE INTO highlights
                (highlight_id, session_id, username, video_file, start_time, end_time, score, category, signals, title)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (highlight_id, session_id, username, video_file, start_time, end_time,
                  score, category, json.dumps(signals or []), title))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_highlights(self, username: str, limit: int = 50) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM highlights WHERE username = ? ORDER BY created_at DESC LIMIT ?",
                (username, limit)).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["signals"] = json.loads(d["signals"]) if d["signals"] else []
                result.append(d)
            return result
        finally:
            conn.close()

    def get_highlight(self, highlight_id: str) -> Optional[dict]:
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM highlights WHERE highlight_id = ?", (highlight_id,)).fetchone()
            if row:
                d = dict(row)
                d["signals"] = json.loads(d["signals"]) if d["signals"] else []
                return d
            return None
        finally:
            conn.close()

    def update_highlight_status(self, highlight_id: str, status: str):
        conn = self._conn()
        try:
            conn.execute("UPDATE highlights SET status = ? WHERE highlight_id = ?", (status, highlight_id))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def delete_highlight(self, highlight_id: str):
        conn = self._conn()
        try:
            conn.execute("DELETE FROM highlights WHERE highlight_id = ?", (highlight_id,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_all_highlights(self, limit: int = 100) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM highlights ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["signals"] = json.loads(d["signals"]) if d["signals"] else []
                result.append(d)
            return result
        finally:
            conn.close()

    # ========== Clips ==========

    def insert_clip(self, clip_id: str, highlight_id: str, username: str,
                    output_file: str = "", resolution: str = "", duration: float = 0,
                    format: str = "", size: int = 0, status: str = "done"):
        conn = self._conn()
        try:
            conn.execute("""
                INSERT OR REPLACE INTO clips
                (clip_id, highlight_id, username, output_file, resolution, duration, format, size, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (clip_id, highlight_id, username, output_file, resolution, duration, format, size, status))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_clips(self, username: str, limit: int = 50) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM clips WHERE username = ? ORDER BY created_at DESC LIMIT ?",
                (username, limit)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_clip(self, clip_id: str) -> Optional[dict]:
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM clips WHERE clip_id = ?", (clip_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def update_clip_status(self, clip_id: str, status: str, **kwargs):
        conn = self._conn()
        try:
            sets = ["status = ?"]
            vals = [status]
            for k in ("export_url", "output_file", "size", "title", "description"):
                if k in kwargs:
                    sets.append(f"{k} = ?")
                    vals.append(kwargs[k])
            if "tags" in kwargs:
                sets.append("tags = ?")
                vals.append(json.dumps(kwargs["tags"]) if isinstance(kwargs["tags"], list) else kwargs["tags"])
            vals.append(clip_id)
            conn.execute(f"UPDATE clips SET {', '.join(sets)} WHERE clip_id = ?", vals)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def delete_clip(self, clip_id: str):
        conn = self._conn()
        try:
            conn.execute("DELETE FROM clips WHERE clip_id = ?", (clip_id,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_all_clips(self, limit: int = 100) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM clips ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_clip_stats(self, username: str = "") -> dict:
        conn = self._conn()
        try:
            if username:
                row = conn.execute("""
                    SELECT COUNT(*) as total, SUM(size) as total_size, SUM(duration) as total_duration
                    FROM clips WHERE username = ? AND status = 'done'
                """, (username,)).fetchone()
            else:
                row = conn.execute("""
                    SELECT COUNT(*) as total, SUM(size) as total_size, SUM(duration) as total_duration
                    FROM clips WHERE status = 'done'
                """).fetchone()
            return dict(row) if row else {}
        finally:
            conn.close()

    # ========== Highlight Rules ==========

    def get_highlight_rules(self, username: str = "") -> list[dict]:
        conn = self._conn()
        try:
            if username:
                rows = conn.execute(
                    "SELECT * FROM highlight_rules WHERE username = ? OR username = '' ORDER BY rule_id",
                    (username,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM highlight_rules ORDER BY rule_id").fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["keywords"] = json.loads(d["keywords"]) if d["keywords"] else []
                d["weights"] = json.loads(d["weights"]) if d["weights"] else {}
                d["enabled"] = bool(d["enabled"])
                result.append(d)
            return result
        finally:
            conn.close()

    def upsert_highlight_rule(self, rule_id: Optional[int] = None, **kwargs):
        conn = self._conn()
        try:
            if rule_id:
                sets, vals = [], []
                for k in ("username", "min_score", "min_duration", "max_duration", "enabled"):
                    if k in kwargs:
                        sets.append(f"{k} = ?")
                        vals.append(1 if k == "enabled" and kwargs[k] else kwargs[k])
                for k in ("keywords", "weights"):
                    if k in kwargs:
                        sets.append(f"{k} = ?")
                        vals.append(json.dumps(kwargs[k]))
                if sets:
                    vals.append(rule_id)
                    conn.execute(f"UPDATE highlight_rules SET {', '.join(sets)} WHERE rule_id = ?", vals)
            else:
                conn.execute("""
                    INSERT INTO highlight_rules (username, keywords, min_score, min_duration, max_duration, weights, enabled)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    kwargs.get("username", ""),
                    json.dumps(kwargs.get("keywords", [])),
                    kwargs.get("min_score", 0.6),
                    kwargs.get("min_duration", 15),
                    kwargs.get("max_duration", 60),
                    json.dumps(kwargs.get("weights", {})),
                    1 if kwargs.get("enabled", True) else 0,
                ))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def delete_highlight_rule(self, rule_id: int):
        conn = self._conn()
        try:
            conn.execute("DELETE FROM highlight_rules WHERE rule_id = ?", (rule_id,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

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
