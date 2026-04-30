"""共享基础设施

- config: 统一配置
- logger: 结构化日志
- errors: 错误码与异常
- constants: 常量定义
"""
from streamvideo.shared.config import Settings, get_settings
from streamvideo.shared.errors import (
    AuthenticationError,
    DatabaseError,
    ErrorCode,
    QuotaError,
    RecordingError,
    StorageError,
    StreamVideoError,
    ValidationError,
)
from streamvideo.shared.logger import get_logger, setup_logging

__all__ = [
    "Settings",
    "get_settings",
    "get_logger",
    "setup_logging",
    "ErrorCode",
    "StreamVideoError",
    "ValidationError",
    "AuthenticationError",
    "RecordingError",
    "StorageError",
    "QuotaError",
    "DatabaseError",
]
