"""数据库基础设施层

提供：
- ConnectionManager: SQLite 连接管理
- Repository: 仓储模式数据访问层
"""
from streamvideo.infrastructure.database.connection import (
    ConnectionManager,
    get_connection_manager,
)
from streamvideo.infrastructure.database.repositories.base import BaseRepository
from streamvideo.infrastructure.database.repositories.model import ModelRepository
from streamvideo.infrastructure.database.repositories.session import SessionRepository
from streamvideo.infrastructure.database.repositories.user import UserRepository

__all__ = [
    "ConnectionManager",
    "get_connection_manager",
    "BaseRepository",
    "ModelRepository",
    "SessionRepository",
    "UserRepository",
]
