"""数据库连接管理

封装 SQLite 连接获取、初始化、迁移逻辑。
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from streamvideo.shared.config import get_settings
from streamvideo.shared.errors import DatabaseError, ErrorCode
from streamvideo.shared.logger import get_logger

logger = get_logger(__name__)


class ConnectionManager:
    """SQLite 连接管理器

    使用 SQLite 内置连接池（每个调用新建连接，依靠 SQLite 自身锁机制）。
    所有连接启用 WAL 模式 + 外键约束。
    """

    def __init__(self, db_path: str | Path | None = None):
        self.db_path = str(db_path) if db_path else str(get_settings().storage.db_path)

    def get_connection(self) -> sqlite3.Connection:
        """获取数据库连接（含 PRAGMA 初始化）"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("PRAGMA synchronous=NORMAL")
            return conn
        except sqlite3.Error as e:
            logger.error(f"Failed to connect to database: {e}", extra={"db_path": self.db_path})
            raise DatabaseError(
                f"Database connection failed: {e}",
                code=ErrorCode.DB_CONNECTION_FAILED,
                details={"db_path": self.db_path},
            )

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """事务上下文管理器

        使用方式：
            with conn_mgr.transaction() as conn:
                conn.execute(...)
        """
        conn = self.get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


# 全局单例
_default_manager: ConnectionManager | None = None


def get_connection_manager() -> ConnectionManager:
    """获取默认连接管理器"""
    global _default_manager
    if _default_manager is None:
        _default_manager = ConnectionManager()
    return _default_manager
