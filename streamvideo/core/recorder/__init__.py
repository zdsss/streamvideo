"""录制引擎模块

拆分自 recorder.py（3258 行）：
- models: 数据类和枚举
- base: BaseLiveRecorder 基类
- engines/: 各平台录制引擎
- uploader: CloudUploader
- notifier: WebhookNotifier
- manager: RecorderManager
"""
from streamvideo.core.recorder.models import (
    ModelStatus,
    RecordingState,
    RecordingInfo,
    RecordingSession,
    ModelInfo,
)
from streamvideo.core.recorder.base import BaseLiveRecorder
from streamvideo.core.recorder.uploader import CloudUploader
from streamvideo.core.recorder.notifier import WebhookNotifier
from streamvideo.core.recorder.manager import RecorderManager

__all__ = [
    "ModelStatus",
    "RecordingState",
    "RecordingInfo",
    "RecordingSession",
    "ModelInfo",
    "BaseLiveRecorder",
    "CloudUploader",
    "WebhookNotifier",
    "RecorderManager",
]
