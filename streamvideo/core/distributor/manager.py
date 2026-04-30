"""
分发模块 — 管理视频发布到各短视频平台
支持：抖音、快手、小红书、B站（按 API 可用性逐步接入）
"""

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

logger = logging.getLogger("distribute")


class DistributeStatus(str, Enum):
    PENDING = "pending"
    UPLOADING = "uploading"
    PROCESSING = "processing"      # 平台处理中
    PUBLISHED = "published"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Platform(str, Enum):
    DOUYIN = "douyin"
    KUAISHOU = "kuaishou"
    XIAOHONGSHU = "xiaohongshu"
    BILIBILI = "bilibili"
    WEIXINVIDEO = "weixinvideo"


@dataclass
class DistributeTask:
    """分发任务"""
    task_id: str
    clip_id: str
    username: str
    platform: str
    file_path: str
    title: str = ""
    description: str = ""
    tags: list[str] = field(default_factory=list)
    status: str = DistributeStatus.PENDING
    remote_id: str = ""            # 平台返回的视频 ID
    remote_url: str = ""           # 平台视频链接
    error: str = ""
    retry_count: int = 0
    created_at: float = 0
    updated_at: float = 0

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "clip_id": self.clip_id,
            "username": self.username,
            "platform": self.platform,
            "file_path": self.file_path,
            "title": self.title,
            "description": self.description,
            "tags": self.tags,
            "status": self.status,
            "remote_id": self.remote_id,
            "remote_url": self.remote_url,
            "error": self.error,
            "retry_count": self.retry_count,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class BasePlatformPublisher:
    """平台发布器基类"""
    platform: str = ""

    async def authenticate(self, credentials: dict) -> bool:
        """验证凭据"""
        raise NotImplementedError

    async def upload(self, task: DistributeTask, credentials: dict,
                     progress_callback=None) -> dict:
        """上传视频，返回 {"remote_id": ..., "remote_url": ...} 或抛异常"""
        raise NotImplementedError

    async def check_status(self, remote_id: str, credentials: dict) -> dict:
        """查询发布状态，返回 {"status": ..., "url": ...}"""
        raise NotImplementedError


class DistributeManager:
    """分发任务管理器"""

    MAX_RETRIES = 3

    def __init__(self, db=None):
        self.db = db
        self._publishers: dict[str, BasePlatformPublisher] = {}
        self._credentials: dict[str, dict] = {}  # platform -> credentials
        self._tasks: dict[str, DistributeTask] = {}

    def register_publisher(self, platform: str, publisher: BasePlatformPublisher):
        """注册平台发布器"""
        self._publishers[platform] = publisher
        logger.info(f"Registered publisher: {platform}")

    def set_credentials(self, platform: str, credentials: dict):
        """设置平台凭据"""
        self._credentials[platform] = credentials

    def get_available_platforms(self) -> list[str]:
        """获取已注册且有凭据的平台列表"""
        return [p for p in self._publishers if p in self._credentials]

    async def create_task(self, clip_id: str, username: str, platform: str,
                          file_path: str, title: str = "", description: str = "",
                          tags: list[str] = None) -> DistributeTask:
        """创建分发任务"""
        if platform not in self._publishers:
            raise ValueError(f"Platform not supported: {platform}")
        if platform not in self._credentials:
            raise ValueError(f"No credentials for platform: {platform}")

        task = DistributeTask(
            task_id=f"dist_{uuid.uuid4().hex[:12]}",
            clip_id=clip_id,
            username=username,
            platform=platform,
            file_path=file_path,
            title=title or f"{username} 直播精彩片段",
            description=description,
            tags=tags or [],
            created_at=time.time(),
            updated_at=time.time(),
        )
        self._tasks[task.task_id] = task

        if self.db:
            self.db.upsert_distribute_task(task.to_dict())

        logger.info(f"Created distribute task: {task.task_id} → {platform}")
        return task

    async def execute_task(self, task_id: str, progress_callback=None) -> DistributeTask:
        """执行分发任务"""
        task = self._tasks.get(task_id)
        if not task:
            raise ValueError(f"Task not found: {task_id}")

        publisher = self._publishers.get(task.platform)
        credentials = self._credentials.get(task.platform, {})

        if not publisher:
            task.status = DistributeStatus.FAILED
            task.error = f"No publisher for {task.platform}"
            return task

        task.status = DistributeStatus.UPLOADING
        task.updated_at = time.time()

        try:
            result = await publisher.upload(task, credentials, progress_callback)
            task.remote_id = result.get("remote_id", "")
            task.remote_url = result.get("remote_url", "") or result.get("open_url", "")
            if result.get("assist_mode"):
                task.status = DistributeStatus.PUBLISHED
            else:
                task.status = DistributeStatus.PROCESSING
            task.updated_at = time.time()
            logger.info(f"Upload complete: {task.task_id} → {task.remote_id}")
        except ValueError as e:
            # 配置错误或功能未实现 — 直接失败，不重试
            task.status = DistributeStatus.FAILED
            task.error = str(e)
            task.updated_at = time.time()
            logger.error(f"Upload failed (non-retryable): {task.task_id} — {e}")
        except Exception as e:
            task.retry_count += 1
            if task.retry_count < self.MAX_RETRIES:
                task.status = DistributeStatus.PENDING
                task.error = f"Retry {task.retry_count}/{self.MAX_RETRIES}: {e}"
                logger.warning(f"Upload failed (will retry): {task.task_id} — {e}")
            else:
                task.status = DistributeStatus.FAILED
                task.error = str(e)
                logger.error(f"Upload failed (max retries): {task.task_id} — {e}")
            task.updated_at = time.time()

        if self.db:
            self.db.upsert_distribute_task(task.to_dict())

        return task

    async def check_task_status(self, task_id: str) -> DistributeTask:
        """检查分发任务状态（轮询平台）"""
        task = self._tasks.get(task_id)
        if not task or not task.remote_id:
            return task

        publisher = self._publishers.get(task.platform)
        credentials = self._credentials.get(task.platform, {})

        if not publisher:
            return task

        try:
            result = await publisher.check_status(task.remote_id, credentials)
            platform_status = result.get("status", "")
            if platform_status in ("published", "success", "审核通过"):
                task.status = DistributeStatus.PUBLISHED
                task.remote_url = result.get("url", task.remote_url)
            elif platform_status in ("failed", "rejected", "审核不通过"):
                task.status = DistributeStatus.FAILED
                task.error = result.get("reason", "Platform rejected")
            task.updated_at = time.time()
        except Exception as e:
            logger.warning(f"Status check failed: {task.task_id} — {e}")

        if self.db:
            self.db.upsert_distribute_task(task.to_dict())

        return task

    def get_tasks(self, username: str = None, platform: str = None) -> list[dict]:
        """获取分发任务列表"""
        tasks = list(self._tasks.values())
        if username:
            tasks = [t for t in tasks if t.username == username]
        if platform:
            tasks = [t for t in tasks if t.platform == platform]
        return [t.to_dict() for t in sorted(tasks, key=lambda t: t.created_at, reverse=True)]

    def get_task(self, task_id: str) -> Optional[dict]:
        """获取单个任务"""
        task = self._tasks.get(task_id)
        return task.to_dict() if task else None


class MockPublisher(BasePlatformPublisher):
    """Mock 发布器 — 用于测试分发流程"""
    platform = "mock"

    async def authenticate(self, credentials: dict) -> bool:
        return True

    async def upload(self, task: DistributeTask, credentials: dict,
                     progress_callback=None) -> dict:
        """模拟上传（2秒延迟）"""
        if progress_callback:
            await progress_callback(0.3)
        await asyncio.sleep(1)
        if progress_callback:
            await progress_callback(0.7)
        await asyncio.sleep(1)
        if progress_callback:
            await progress_callback(1.0)
        mock_id = f"mock_{uuid.uuid4().hex[:8]}"
        return {
            "remote_id": mock_id,
            "remote_url": f"https://example.com/video/{mock_id}",
        }

    async def check_status(self, remote_id: str, credentials: dict) -> dict:
        return {"status": "published", "url": f"https://example.com/video/{remote_id}"}


class DouyinPublisher(BasePlatformPublisher):
    """抖音发布器 — 需要 OAuth 凭据（待实现）"""
    platform = "douyin"

    async def authenticate(self, credentials: dict) -> bool:
        return bool(credentials.get("access_token"))

    async def upload(self, task: DistributeTask, credentials: dict,
                     progress_callback=None) -> dict:
        if not credentials.get("access_token"):
            raise ValueError("抖音未授权，请先在 System 页面完成 OAuth 授权")
        raise ValueError("抖音 API 对接尚在开发中，请使用辅助投稿模式")

    async def check_status(self, remote_id: str, credentials: dict) -> dict:
        return {"status": "unknown"}


class KuaishouPublisher(BasePlatformPublisher):
    """快手发布器 — 需要 OAuth 凭据（待实现）"""
    platform = "kuaishou"

    async def authenticate(self, credentials: dict) -> bool:
        return bool(credentials.get("access_token"))

    async def upload(self, task: DistributeTask, credentials: dict,
                     progress_callback=None) -> dict:
        if not credentials.get("access_token"):
            raise ValueError("快手未授权，请先在 System 页面完成 OAuth 授权")
        raise ValueError("快手 API 对接尚在开发中，请使用辅助投稿模式")

    async def check_status(self, remote_id: str, credentials: dict) -> dict:
        return {"status": "unknown"}


class BilibiliAssistPublisher(BasePlatformPublisher):
    """B站辅助投稿 — 复制信息后跳转 B站投稿页，无需 API 凭据"""
    platform = "bilibili"

    async def authenticate(self, credentials: dict) -> bool:
        return True

    async def upload(self, task: DistributeTask, credentials: dict,
                     progress_callback=None) -> dict:
        return {
            "assist_mode": True,
            "remote_id": f"bilibili_assist_{uuid.uuid4().hex[:8]}",
            "remote_url": "https://member.bilibili.com/platform/upload/video/frame",
            "title": task.title,
            "description": task.description,
            "tags": task.tags,
            "file_path": task.file_path,
            "open_url": "https://member.bilibili.com/platform/upload/video/frame",
        }

    async def check_status(self, remote_id: str, credentials: dict) -> dict:
        return {"status": "published", "url": ""}


class WeixinVideoPublisher(BasePlatformPublisher):
    """微信视频号辅助投稿 — 复制信息后跳转视频号创作中心，无需 API 凭据"""
    platform = "weixinvideo"

    async def authenticate(self, credentials: dict) -> bool:
        return True

    async def upload(self, task: DistributeTask, credentials: dict,
                     progress_callback=None) -> dict:
        return {
            "assist_mode": True,
            "remote_id": f"weixin_assist_{uuid.uuid4().hex[:8]}",
            "remote_url": "https://channels.weixin.qq.com/platform/post/create",
            "title": task.title,
            "description": task.description,
            "tags": task.tags,
            "file_path": task.file_path,
            "open_url": "https://channels.weixin.qq.com/platform/post/create",
        }

    async def check_status(self, remote_id: str, credentials: dict) -> dict:
        return {"status": "published", "url": ""}
