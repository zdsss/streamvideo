"""仓储模式基类

所有 Repository 继承本类，统一连接管理和异常处理。
"""
from __future__ import annotations

import sqlite3
from typing import Any, Optional

from streamvideo.infrastructure.database.connection import (
    ConnectionManager,
    get_connection_manager,
)
from streamvideo.shared.errors import DatabaseError, ErrorCode
from streamvideo.shared.logger import get_logger

logger = get_logger(__name__)


class BaseRepository:
    """仓储基类"""

    def __init__(self, conn_manager: Optional[ConnectionManager] = None):
        self._conn_manager = conn_manager or get_connection_manager()

    def _execute(
        self,
        sql: str,
        params: tuple = (),
        fetch: str = "none",
    ) -> Any:
        """执行 SQL（封装连接生命周期）

        Args:
            sql: SQL 语句
            params: 参数元组
            fetch: none/one/all
        """
        try:
            with self._conn_manager.transaction() as conn:
                cur = conn.execute(sql, params)
                if fetch == "one":
                    row = cur.fetchone()
                    return dict(row) if row else None
                if fetch == "all":
                    return [dict(r) for r in cur.fetchall()]
                return cur.lastrowid
        except sqlite3.IntegrityError as e:
            logger.error(f"DB integrity violation: {e}", extra={"sql": sql})
            raise DatabaseError(
                f"Database integrity violation: {e}",
                code=ErrorCode.DB_CONSTRAINT_VIOLATION,
            )
        except sqlite3.Error as e:
            logger.error(f"DB query failed: {e}", extra={"sql": sql})
            raise DatabaseError(
                f"Database query failed: {e}",
                code=ErrorCode.DB_QUERY_FAILED,
            )

    def fetch_one(self, sql: str, params: tuple = ()) -> Optional[dict]:
        """查询单条"""
        return self._execute(sql, params, fetch="one")

    def fetch_all(self, sql: str, params: tuple = ()) -> list[dict]:
        """查询多条"""
        return self._execute(sql, params, fetch="all")

    def execute(self, sql: str, params: tuple = ()) -> Optional[int]:
        """执行写操作，返回 lastrowid"""
        return self._execute(sql, params, fetch="none")
