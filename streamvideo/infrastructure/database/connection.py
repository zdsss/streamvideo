import json
import logging
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Optional

logger = logging.getLogger("database")

DB_PATH = os.environ.get("SV_DB_PATH", str(Path(__file__).parent / "streamvideo.db"))

_lock = threading.Lock()
_persistent_conn: Optional[sqlite3.Connection] = None


def get_db(db_path: str = DB_PATH) -> sqlite3.Connection:
    global _persistent_conn
    with _lock:
        if _persistent_conn is None:
            _persistent_conn = sqlite3.connect(db_path, timeout=10, check_same_thread=False)
            _persistent_conn.row_factory = sqlite3.Row
            _persistent_conn.execute("PRAGMA journal_mode=WAL")
            _persistent_conn.execute("PRAGMA foreign_keys=ON")
            _persistent_conn.execute("PRAGMA busy_timeout=5000")
            _persistent_conn.execute("PRAGMA synchronous=NORMAL")
        return _persistent_conn


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
            priority INTEGER DEFAULT 5,
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
            merge_type TEXT DEFAULT '',
            rollback_deadline REAL DEFAULT 0,
            original_segments TEXT DEFAULT '[]',
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
        CREATE UNIQUE INDEX IF NOT EXISTS idx_danmaku_session_username ON danmaku(session_id, username);

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

        CREATE TABLE IF NOT EXISTS distribute_tasks (
            task_id TEXT PRIMARY KEY,
            clip_id TEXT,
            username TEXT,
            platform TEXT,
            file_path TEXT,
            title TEXT DEFAULT '',
            description TEXT DEFAULT '',
            tags TEXT DEFAULT '[]',
            status TEXT DEFAULT 'pending',
            remote_id TEXT DEFAULT '',
            remote_url TEXT DEFAULT '',
            error TEXT DEFAULT '',
            retry_count INTEGER DEFAULT 0,
            created_at REAL DEFAULT 0,
            updated_at REAL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            email TEXT UNIQUE,
            display_name TEXT DEFAULT '',
            password_hash TEXT,
            role TEXT DEFAULT 'user',
            created_at REAL DEFAULT 0,
            last_login REAL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS user_sessions (
            session_token TEXT PRIMARY KEY,
            user_id TEXT,
            created_at REAL DEFAULT 0,
            expires_at REAL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS merge_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL UNIQUE,
            username TEXT NOT NULL,
            segments TEXT DEFAULT '[]',
            confidence REAL DEFAULT 0,
            reasons TEXT DEFAULT '[]',
            status TEXT DEFAULT 'pending',
            created_at REAL DEFAULT (strftime('%s','now'))
        );
        CREATE INDEX IF NOT EXISTS idx_merge_queue_username ON merge_queue(username);
        CREATE INDEX IF NOT EXISTS idx_merge_queue_status ON merge_queue(status);

        CREATE TABLE IF NOT EXISTS translation_cache (
            text_hash   TEXT NOT NULL,
            source_lang TEXT NOT NULL,
            target_lang TEXT NOT NULL,
            translated  TEXT NOT NULL,
            model       TEXT DEFAULT '',
            hit_count   INTEGER DEFAULT 0,
            created_at  REAL DEFAULT (strftime('%s','now')),
            PRIMARY KEY (text_hash, source_lang, target_lang)
        );
        CREATE INDEX IF NOT EXISTS idx_translation_cache ON translation_cache(text_hash, source_lang, target_lang);

        CREATE TABLE IF NOT EXISTS platform_credentials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            access_token TEXT DEFAULT '',
            refresh_token TEXT DEFAULT '',
            openid TEXT DEFAULT '',
            display_name TEXT DEFAULT '',
            expires_at REAL DEFAULT 0,
            created_at REAL DEFAULT (strftime('%s','now')),
            updated_at REAL DEFAULT (strftime('%s','now')),
            UNIQUE(user_id, platform)
        );
        CREATE INDEX IF NOT EXISTS idx_credentials_user_platform ON platform_credentials(user_id, platform);
    """)
    conn.commit()
    logger.info(f"Database initialized: {db_path}")


