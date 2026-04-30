"""Database — 主数据库类，继承所有 Mixin"""
import sqlite3
import logging
import os
from pathlib import Path
from typing import Optional

from streamvideo.infrastructure.database.connection import get_db, init_db, DB_PATH
from streamvideo.infrastructure.database.repositories.settings_mixin import SettingsMixin
from streamvideo.infrastructure.database.repositories.model_mixin import ModelMixin
from streamvideo.infrastructure.database.repositories.session_mixin import SessionMixin
from streamvideo.infrastructure.database.repositories.media_mixin import MediaMixin
from streamvideo.infrastructure.database.repositories.distribute_mixin import DistributeMixin
from streamvideo.infrastructure.database.repositories.user_mixin import UserMixin

logger = logging.getLogger("database")


class Database(SettingsMixin, ModelMixin, SessionMixin, MediaMixin, DistributeMixin, UserMixin):
    """主数据库访问对象，组合所有功能 Mixin"""

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
                ("merge_type", "TEXT DEFAULT ''"),
                ("rollback_deadline", "REAL DEFAULT 0"),
                ("original_segments", "TEXT DEFAULT '[]'"),
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
            # users 表迁移（Stripe 支付字段）
            user_cols = {r[1] for r in conn.execute("PRAGMA table_info(users)").fetchall()}
            if user_cols:
                user_migrations = [
                    ("stripe_customer_id", "TEXT DEFAULT ''"),
                    ("stripe_subscription_id", "TEXT DEFAULT ''"),
                    ("subscription_status", "TEXT DEFAULT 'free'"),
                    ("subscription_expires_at", "REAL DEFAULT 0"),
                ]
                for col, typedef in user_migrations:
                    if col not in user_cols:
                        conn.execute(f"ALTER TABLE users ADD COLUMN {col} {typedef}")
                        logger.info(f"Schema migration: added users.{col}")
            conn.commit()
        except Exception as e:
            logger.warning(f"Schema migration error: {e}")
        finally:
            conn.close()

    # ========== Settings ==========

