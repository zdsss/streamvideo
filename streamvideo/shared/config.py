"""统一配置管理 - 使用 Pydantic Settings

集中所有环境变量与配置项，支持类型校验、默认值与 .env 文件加载。
"""
from __future__ import annotations

import os
import secrets
from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, model_validator

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
    _HAS_PYDANTIC_SETTINGS = True
except ImportError:
    _HAS_PYDANTIC_SETTINGS = False
    BaseSettings = BaseModel  # type: ignore


BASE_DIR = Path(__file__).resolve().parent.parent.parent


def _get_or_generate_secret() -> str:
    secret_file = BASE_DIR / ".session_key"
    if secret_file.exists():
        return secret_file.read_text().strip()
    secret = secrets.token_hex(32)
    secret_file.write_text(secret)
    return secret


class _SettingsBase(BaseSettings if _HAS_PYDANTIC_SETTINGS else BaseModel):
    """基础设置类（兼容无 pydantic-settings 环境）"""
    if _HAS_PYDANTIC_SETTINGS:
        model_config = SettingsConfigDict(
            env_file=".env",
            env_file_encoding="utf-8",
            extra="ignore",
            case_sensitive=False,
        )

    @classmethod
    def from_env(cls):
        """无 pydantic-settings 时从环境变量加载"""
        if _HAS_PYDANTIC_SETTINGS:
            return cls()
        # 手动读取环境变量（snake_case → UPPER_CASE）
        kwargs = {}
        for field_name, field in cls.model_fields.items():
            env_name = field_name.upper()
            if env_name in os.environ:
                kwargs[field_name] = os.environ[env_name]
        return cls(**kwargs)


class ServerConfig(_SettingsBase):
    """服务器配置"""
    host: str = Field(default="0.0.0.0", description="监听地址")
    port: int = Field(default=8080, description="监听端口")
    workers: int = Field(default=1, description="Worker 进程数")
    reload: bool = Field(default=False, description="开发模式自动重载")


class StorageConfig(_SettingsBase):
    """存储配置"""
    base_dir: Path = Field(default=BASE_DIR, description="项目根目录")
    recordings_dir: Path = Field(
        default=BASE_DIR / "recordings",
        description="录像存储目录",
    )
    db_path: Path = Field(
        default=BASE_DIR / "streamvideo.db",
        description="SQLite 数据库路径",
    )
    config_json_path: Path = Field(
        default=BASE_DIR / "config.json",
        description="JSON 配置备份路径",
    )


class AuthConfig(_SettingsBase):
    """认证配置"""
    sv_token: str = Field(default="", description="API 访问 Token（可选）")
    session_secret: str = Field(default_factory=_get_or_generate_secret, description="会话密钥")
    session_ttl_hours: int = Field(default=24 * 7, description="会话有效期（小时）")


def _detect_system_proxy() -> str:
    """Auto-detect proxy from environment variables"""
    for key in ("SV_PROXY", "HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy"):
        val = os.environ.get(key, "").strip()
        if val:
            return val
    return ""


class NetworkConfig(_SettingsBase):
    """网络配置"""
    sv_proxy: str = Field(default="", description="HTTP 代理（自动检测系统代理）")
    request_timeout: int = Field(default=30, description="请求超时（秒）")

    @model_validator(mode="before")
    @classmethod
    def _auto_detect_proxy(cls, data: dict) -> dict:
        if isinstance(data, dict) and not data.get("sv_proxy"):
            detected = _detect_system_proxy()
            if detected:
                data["sv_proxy"] = detected
        return data


class RecorderConfig(_SettingsBase):
    """录制器配置"""
    check_interval_seconds: int = Field(default=60, description="检测主播状态间隔")
    record_timeout_seconds: int = Field(default=3600, description="单段录制超时")
    auto_delete_originals: bool = Field(default=True, description="合并后删除原始片段")
    merge_confidence_high: float = Field(default=0.7, description="合并信心度高阈值")
    merge_confidence_low: float = Field(default=0.4, description="合并信心度低阈值")
    split_size_mb: int = Field(default=2048, description="切分文件大小（MB）")
    split_duration_minutes: int = Field(default=60, description="切分时长（分钟）")
    gpu_encoder: str = Field(default="auto", description="GPU编码器: auto/software/nvenc/videotoolbox/vaapi/qsv")
    preserve_original_on_transcode: bool = Field(default=False, description="转码后保留原始文件（重命名为 .original.mp4）")


class DistributionConfig(_SettingsBase):
    """分发配置"""
    douyin_client_key: str = Field(default="", description="抖音 client_key")
    douyin_client_secret: str = Field(default="", description="抖音 client_secret")
    douyin_redirect_uri: str = Field(default="", description="抖音回调 URI")
    kuaishou_client_key: str = Field(default="", description="快手 client_key")
    kuaishou_client_secret: str = Field(default="", description="快手 client_secret")
    kuaishou_redirect_uri: str = Field(default="", description="快手回调 URI")


class PaymentConfig(_SettingsBase):
    """支付配置"""
    stripe_secret_key: str = Field(default="", description="Stripe 密钥")
    stripe_webhook_secret: str = Field(default="", description="Stripe Webhook 密钥")
    stripe_price_pro: str = Field(default="", description="Pro 套餐价格 ID")
    stripe_price_team: str = Field(default="", description="Team 套餐价格 ID")
    app_url: str = Field(default="http://localhost:8080", description="应用回跳 URL")


class LoggingConfig(_SettingsBase):
    """日志配置"""
    log_level: str = Field(default="INFO", description="日志级别")
    log_format: str = Field(default="json", description="日志格式（json/text）")
    log_file: Optional[Path] = Field(default=None, description="日志文件路径")


class Settings(BaseModel):
    """全局配置聚合"""
    server: ServerConfig = Field(default_factory=lambda: ServerConfig.from_env() if not _HAS_PYDANTIC_SETTINGS else ServerConfig())
    storage: StorageConfig = Field(default_factory=lambda: StorageConfig.from_env() if not _HAS_PYDANTIC_SETTINGS else StorageConfig())
    auth: AuthConfig = Field(default_factory=lambda: AuthConfig.from_env() if not _HAS_PYDANTIC_SETTINGS else AuthConfig())
    network: NetworkConfig = Field(default_factory=lambda: NetworkConfig.from_env() if not _HAS_PYDANTIC_SETTINGS else NetworkConfig())
    recorder: RecorderConfig = Field(default_factory=lambda: RecorderConfig.from_env() if not _HAS_PYDANTIC_SETTINGS else RecorderConfig())
    distribution: DistributionConfig = Field(default_factory=lambda: DistributionConfig.from_env() if not _HAS_PYDANTIC_SETTINGS else DistributionConfig())
    payment: PaymentConfig = Field(default_factory=lambda: PaymentConfig.from_env() if not _HAS_PYDANTIC_SETTINGS else PaymentConfig())
    logging: LoggingConfig = Field(default_factory=lambda: LoggingConfig.from_env() if not _HAS_PYDANTIC_SETTINGS else LoggingConfig())


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """获取全局配置（单例）"""
    return Settings()


def reload_settings() -> Settings:
    """重新加载配置（清除缓存）"""
    get_settings.cache_clear()
    return get_settings()
