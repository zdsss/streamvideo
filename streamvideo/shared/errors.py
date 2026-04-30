"""错误定义与异常类"""
from enum import Enum
from typing import Any, Optional


class ErrorCode(str, Enum):
    """错误码枚举"""
    # 通用错误 (1xxx)
    INTERNAL_ERROR = "1000"
    INVALID_PARAMETER = "1001"
    RESOURCE_NOT_FOUND = "1002"
    PERMISSION_DENIED = "1003"

    # 认证错误 (2xxx)
    AUTH_INVALID_TOKEN = "2000"
    AUTH_TOKEN_EXPIRED = "2001"
    AUTH_INVALID_CREDENTIALS = "2002"
    AUTH_USER_NOT_FOUND = "2003"

    # 录制错误 (3xxx)
    RECORD_ALREADY_RUNNING = "3000"
    RECORD_START_FAILED = "3001"
    RECORD_STOP_FAILED = "3002"
    RECORD_PLATFORM_UNSUPPORTED = "3003"
    RECORD_STREAM_OFFLINE = "3004"

    # 存储错误 (4xxx)
    STORAGE_FILE_NOT_FOUND = "4000"
    STORAGE_WRITE_FAILED = "4001"
    STORAGE_DELETE_FAILED = "4002"
    STORAGE_INSUFFICIENT_SPACE = "4003"

    # 配额错误 (5xxx)
    QUOTA_EXCEEDED = "5000"
    QUOTA_MODEL_LIMIT = "5001"
    QUOTA_CLIP_LIMIT = "5002"

    # 数据库错误 (6xxx)
    DB_CONNECTION_FAILED = "6000"
    DB_QUERY_FAILED = "6001"
    DB_CONSTRAINT_VIOLATION = "6002"


class StreamVideoError(Exception):
    """基础异常类"""
    def __init__(
        self,
        message: str,
        code: ErrorCode = ErrorCode.INTERNAL_ERROR,
        details: Optional[dict[str, Any]] = None,
    ):
        self.message = message
        self.code = code
        self.details = details or {}
        super().__init__(message)


class ValidationError(StreamVideoError):
    """参数验证错误"""
    def __init__(self, message: str, details: Optional[dict[str, Any]] = None):
        super().__init__(message, ErrorCode.INVALID_PARAMETER, details)


class AuthenticationError(StreamVideoError):
    """认证错误"""
    def __init__(self, message: str, code: ErrorCode = ErrorCode.AUTH_INVALID_TOKEN, details: Optional[dict[str, Any]] = None):
        super().__init__(message, code, details)


class RecordingError(StreamVideoError):
    """录制错误"""
    def __init__(self, message: str, code: ErrorCode = ErrorCode.RECORD_START_FAILED, details: Optional[dict[str, Any]] = None):
        super().__init__(message, code, details)


class StorageError(StreamVideoError):
    """存储错误"""
    def __init__(self, message: str, code: ErrorCode = ErrorCode.STORAGE_FILE_NOT_FOUND, details: Optional[dict[str, Any]] = None):
        super().__init__(message, code, details)


class QuotaError(StreamVideoError):
    """配额错误"""
    def __init__(self, message: str, code: ErrorCode = ErrorCode.QUOTA_EXCEEDED, details: Optional[dict[str, Any]] = None):
        super().__init__(message, code, details)


class DatabaseError(StreamVideoError):
    """数据库错误"""
    def __init__(self, message: str, code: ErrorCode = ErrorCode.DB_QUERY_FAILED, details: Optional[dict[str, Any]] = None):
        super().__init__(message, code, details)
