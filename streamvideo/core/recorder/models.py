import asyncio
import json
import logging
import os
import re
import shutil
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import urlparse

import aiohttp

logger = logging.getLogger("recorder")


class ModelStatus(str, Enum):
    OFFLINE = "offline"
    PUBLIC = "public"
    PRIVATE = "private"
    GROUP = "group"
    AWAY = "away"
    UNKNOWN = "unknown"



class RecordingState(str, Enum):
    IDLE = "idle"
    MONITORING = "monitoring"
    RECORDING = "recording"
    RECONNECTING = "reconnecting"
    ERROR = "error"


@dataclass

class RecordingInfo:
    file_path: str = ""
    start_time: float = 0
    file_size: int = 0
    duration: float = 0
    bandwidth_kbps: float = 0


@dataclass

class RecordingSession:
    """录制会话：一次直播（可能包含多次断流重连）的所有片段"""
    session_id: str = ""
    username: str = ""
    started_at: float = 0
    ended_at: float = 0
    segments: list[str] = field(default_factory=list)  # 文件名列表
    status: str = "active"  # active | ended | merging | merged | error
    merged_file: str = ""
    merge_error: str = ""
    retry_count: int = 0  # 合并失败重试计数
    merge_started_at: float = 0  # 合并开始时间（用于超时检测）
    stream_end_reason: str = ""  # 录制停止原因: stall_timeout | process_exit_0 | process_exit_error | user_stop
    merge_type: str = ""  # auto_high | auto_smart | manual | ""
    rollback_deadline: float = 0  # unix timestamp: 72h 内可撤回
    original_segments: list[str] = field(default_factory=list)  # 合并前的分片列表（用于撤回）

    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "username": self.username,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "segments": self.segments,
            "status": self.status,
            "merged_file": self.merged_file,
            "merge_error": self.merge_error,
            "retry_count": self.retry_count,
            "merge_started_at": self.merge_started_at,
            "stream_end_reason": self.stream_end_reason,
            "merge_type": self.merge_type,
            "rollback_deadline": self.rollback_deadline,
            "original_segments": self.original_segments,
        }

    @staticmethod
    def from_dict(d: dict) -> "RecordingSession":
        return RecordingSession(
            session_id=d.get("session_id", ""),
            username=d.get("username", ""),
            started_at=d.get("started_at", 0),
            ended_at=d.get("ended_at", 0),
            segments=d.get("segments", []),
            status=d.get("status", "active"),
            merged_file=d.get("merged_file", ""),
            merge_error=d.get("merge_error", ""),
            retry_count=d.get("retry_count", 0),
            merge_started_at=d.get("merge_started_at", 0),
            stream_end_reason=d.get("stream_end_reason", ""),
            merge_type=d.get("merge_type", ""),
            rollback_deadline=d.get("rollback_deadline", 0),
            original_segments=d.get("original_segments", []),
        )


@dataclass

class ModelInfo:
    username: str
    platform: str = "unknown"
    live_url: str = ""
    model_id: Optional[int] = None
    status: ModelStatus = ModelStatus.UNKNOWN
    state: RecordingState = RecordingState.IDLE
    enabled: bool = True
    auto_merge: bool = True
    quality: str = "best"  # best, 1080p, 720p, 480p, audio_only
    current_recording: Optional[RecordingInfo] = None
    recordings: list[RecordingInfo] = field(default_factory=list)
    last_check: float = 0
    last_online: float = 0
    error_msg: str = ""
    consecutive_fails: int = 0
    viewers: int = 0
    thumbnail_url: str = ""

    def to_dict(self):
        return {
            "username": self.username,
            "platform": self.platform,
            "live_url": self.live_url,
            "model_id": self.model_id,
            "status": self.status.value,
            "state": self.state.value,
            "enabled": self.enabled,
            "auto_merge": self.auto_merge,
            "quality": self.quality,
            "current_recording": {
                "file_path": self.current_recording.file_path,
                "start_time": self.current_recording.start_time,
                "file_size": self.current_recording.file_size,
                "duration": self.current_recording.duration,
                "bandwidth_kbps": self.current_recording.bandwidth_kbps,
            } if self.current_recording else None,
            "recording_count": len(self.recordings),
            "last_check": self.last_check,
            "last_online": self.last_online,
            "error_msg": self.error_msg,
            "consecutive_fails": self.consecutive_fails,
            "viewers": self.viewers,
            "thumbnail_url": self.thumbnail_url,
        }


# ========== 平台检测与 URL 解析 ==========

def detect_platform(url_or_id: str) -> tuple[str, str, str]:
    """从 URL 或用户名检测平台，返回 (platform, identifier, display_name)"""
    url = url_or_id.strip()

    # 抖音
    if "douyin.com" in url or "live.douyin.com" in url:
        m = re.search(r'/live/(\d+)', url) or re.search(r'douyin\.com/(\d+)', url)
        room_id = m.group(1) if m else url.split("/")[-1].split("?")[0]
        return "douyin", room_id, f"抖音_{room_id}"

    # B站直播
    if "bilibili.com" in url or "live.bilibili.com" in url:
        m = re.search(r'live\.bilibili\.com/(\d+)', url) or re.search(r'bilibili\.com/(\d+)', url)
        room_id = m.group(1) if m else url.split("/")[-1].split("?")[0]
        return "bilibili", room_id, f"B站_{room_id}"

    # Twitch
    if "twitch.tv" in url:
        m = re.search(r'twitch\.tv/([^/?&#\s]+)', url)
        username = m.group(1) if m else url.split("/")[-1].split("?")[0]
        return "twitch", username, username

    # YouTube
    if "youtube.com" in url or "youtu.be" in url:
        # youtube.com/@channel/live 或 youtube.com/watch?v=xxx
        m = re.search(r'youtube\.com/@([^/?&#\s]+)', url)
        if m:
            return "youtube", f"https://www.youtube.com/@{m.group(1)}/live", m.group(1)
        m = re.search(r'[?&]v=([^&#\s]+)', url)
        if m:
            return "youtube", url, f"YT_{m.group(1)}"
        return "youtube", url, "YouTube"

    # 虎牙
    if "huya.com" in url:
        m = re.search(r'huya\.com/([^/?&#\s]+)', url)
        room = m.group(1) if m else url.split("/")[-1].split("?")[0]
        return "huya", url, f"虎牙_{room}"

    # 斗鱼
    if "douyu.com" in url:
        m = re.search(r'douyu\.com/(\d+)', url)
        room = m.group(1) if m else url.split("/")[-1].split("?")[0]
        return "douyu", url, f"斗鱼_{room}"

    # Kick
    if "kick.com" in url:
        m = re.search(r'kick\.com/([^/?&#\s]+)', url)
        channel = m.group(1) if m else url.split("/")[-1].split("?")[0]
        return "kick", url, f"Kick_{channel}"

    # AfreecaTV
    if "afreecatv.com" in url or "chzzk.naver.com" in url:
        return "generic", url, urlparse(url).netloc.split(".")[0]

    # 未知平台，尝试用 streamlink
    return "generic", url, urlparse(url).netloc.split(".")[0]


# ========== 基类 ==========
