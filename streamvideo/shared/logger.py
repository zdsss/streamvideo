"""统一日志系统

支持 structlog（如可用）/标准 logging 双轨模式：
- 结构化 JSON 输出（便于日志聚合）
- 请求 ID 追踪（contextvars）
- 动态日志级别
"""
from __future__ import annotations

import json
import logging
import sys
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

try:
    import structlog
    _HAS_STRUCTLOG = True
except ImportError:
    _HAS_STRUCTLOG = False


# 请求上下文变量
_request_id_var: ContextVar[str] = ContextVar("request_id", default="")
_user_id_var: ContextVar[str] = ContextVar("user_id", default="")


def set_request_id(request_id: Optional[str] = None) -> str:
    """设置当前请求 ID（默认随机生成）"""
    rid = request_id or uuid.uuid4().hex[:12]
    _request_id_var.set(rid)
    return rid


def get_request_id() -> str:
    """获取当前请求 ID"""
    return _request_id_var.get()


def set_user_id(user_id: str) -> None:
    """设置当前用户 ID"""
    _user_id_var.set(user_id)


def get_user_id() -> str:
    """获取当前用户 ID"""
    return _user_id_var.get()


class JsonFormatter(logging.Formatter):
    """JSON 结构化日志格式化器"""

    def format(self, record: logging.LogRecord) -> str:
        log_data: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # 注入上下文
        rid = get_request_id()
        uid = get_user_id()
        if rid:
            log_data["request_id"] = rid
        if uid:
            log_data["user_id"] = uid

        # 异常信息
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # 自定义字段
        for key, val in record.__dict__.items():
            if key not in (
                "name", "msg", "args", "levelname", "levelno", "pathname",
                "filename", "module", "exc_info", "exc_text", "stack_info",
                "lineno", "funcName", "created", "msecs", "relativeCreated",
                "thread", "threadName", "processName", "process", "getMessage",
                "message",
            ):
                try:
                    json.dumps(val)
                    log_data[key] = val
                except (TypeError, ValueError):
                    log_data[key] = str(val)

        return json.dumps(log_data, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    """人类友好的文本格式"""

    def format(self, record: logging.LogRecord) -> str:
        rid = get_request_id()
        prefix = f"[{rid}] " if rid else ""
        record.msg = f"{prefix}{record.getMessage()}"
        fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        formatter = logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)


_configured = False


def setup_logging(
    level: str = "INFO",
    format: str = "text",
    log_file: Optional[Path] = None,
) -> None:
    """初始化日志系统（应用启动时调用一次）"""
    global _configured
    if _configured:
        return

    root = logging.getLogger()
    root.setLevel(level.upper())

    # 清除已有 handler
    for h in list(root.handlers):
        root.removeHandler(h)

    formatter: logging.Formatter
    if format.lower() == "json":
        formatter = JsonFormatter()
    else:
        formatter = TextFormatter()

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    root.addHandler(console)

    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(str(log_file), encoding="utf-8")
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    # 降低嘈杂的第三方日志
    for noisy in ("urllib3", "asyncio", "httpx", "websockets"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _configured = True


def get_logger(name: str) -> logging.Logger:
    """获取命名 logger"""
    return logging.getLogger(name)
