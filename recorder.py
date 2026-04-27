"""
多平台直播录制引擎
- 支持抖音、B站、Twitch、YouTube 等平台
- 自动检测平台和解析主播信息
- yt-dlp / streamlink / Playwright 多引擎录制
- 自动重连 + 宽限期 + 自动合并
"""

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

    # 未知平台，尝试用 streamlink
    return "generic", url, urlparse(url).netloc.split(".")[0]


# ========== 基类 ==========

class BaseLiveRecorder:
    """通用直播录制器基类"""
    platform = "unknown"

    def __init__(self, identifier: str, output_dir: str,
                 proxy: str = "", on_state_change: Optional[Callable] = None):
        self.identifier = identifier
        self.output_dir = Path(output_dir)
        self.proxy = proxy
        self.on_state_change = on_state_change
        self.info = ModelInfo(username=identifier, platform=self.platform)
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._recording_active = False
        self._active_proc = None  # 当前录制进程

        # 配置
        self.poll_interval_offline = 30
        self.poll_interval_reconnect = 5
        self.grace_period = 60
        self.max_consecutive_fails = 10
        self.cooldown = 120

        # 断流检测（子类可覆盖）
        self.stall_timeout = 20  # 秒，无增长多久判定断流
        self.stall_check_interval = 5  # 秒

        # 重试策略（子类可覆盖）
        self.retry_base_interval = 5
        self.retry_max_interval = 120
        self.retry_backoff_factor = 2

        # 自动合并
        self.auto_merge = True
        self.auto_delete_originals = True
        self.min_segment_size = 500 * 1024
        self._manager: Optional["RecorderManager"] = None

        # 自动分割
        self.split_by_size = 0  # 字节，0=禁用
        self.split_by_duration = 0  # 秒，0=禁用

        # 定时录制
        self.schedule: Optional[dict] = None
        # 录制质量
        self.quality: str = "best"
        # 用户自定义 cookie 和流地址
        self.custom_cookies: str = ""  # 用户从浏览器导出的 cookie 字符串
        self.custom_stream_url: str = ""  # 用户手动粘贴的流地址  # {"enabled":False,"start":"20:00","end":"02:00","days":[0,1,2,3,4,5,6]}

        # 会话追踪
        self._current_session: Optional[RecordingSession] = None
        self._sessions: list[RecordingSession] = []
        self._session_lock = asyncio.Lock()  # 防止并发创建/复用会话
        self._last_stop_reason: str = ""  # stall_timeout | process_exit_0 | process_exit_error | user_stop

        # 磁盘保护
        self._disk_critical = False

        # 共享 HTTP 会话（避免每次请求创建新连接）
        self._http_session: Optional[aiohttp.ClientSession] = None

        self.user_agent = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )

        # 启动时恢复持久化的元数据（先 meta 再 sessions，确保 username 正确）
        self._load_meta()
        self._load_sessions()

    def _load_meta(self):
        """从 meta.json 恢复主播信息（名字、缩略图等）"""
        candidates = [self.info.username, self.identifier]

        for name in candidates:
            meta_path = self.output_dir / name / "meta.json"
            if meta_path.exists():
                try:
                    with open(meta_path) as f:
                        meta = json.load(f)
                    if meta.get("display_name"):
                        self.info.username = meta["display_name"]
                    if meta.get("live_url"):
                        self.info.live_url = meta["live_url"]
                    if meta.get("last_online"):
                        self.info.last_online = meta["last_online"]
                    # 恢复缩略图
                    thumb_path = self.output_dir / "thumbs" / f"{self.info.username}.jpg"
                    if thumb_path.exists():
                        self.info.thumbnail_url = f"/api/thumb/{self.info.username}"
                    logger.info(f"[{self.info.username}] Loaded meta from {meta_path}")
                    return
                except Exception as e:
                    logger.warning(f"Failed to load meta {meta_path}: {e}")

        # 没有 meta.json，但检查是否有已存在的缩略图
        for name in candidates:
            thumb_path = self.output_dir / "thumbs" / f"{name}.jpg"
            if thumb_path.exists():
                self.info.thumbnail_url = f"/api/thumb/{name}"
                break

    def _save_meta(self):
        """持久化主播元数据"""
        model_dir = self.output_dir / self.info.username
        model_dir.mkdir(parents=True, exist_ok=True)
        meta_path = model_dir / "meta.json"
        thumbs_dir = self.output_dir / "thumbs"
        thumb_file = thumbs_dir / f"{self.info.username}.jpg"
        meta = {
            "display_name": self.info.username,
            "platform": self.info.platform,
            "identifier": self.identifier,
            "live_url": self.info.live_url,
            "last_thumb": str(thumb_file) if thumb_file.exists() else "",
            "last_online": self.info.last_online,
            "total_recordings": len(self.info.recordings),
        }
        try:
            with open(meta_path, "w") as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"[{self.info.username}] Failed to save meta: {e}")

    def _sessions_path(self) -> Path:
        """sessions.json 路径"""
        return self.output_dir / self.info.username / "sessions.json"

    def _load_sessions(self):
        """从 SQLite 恢复会话列表（fallback JSON），验证 segment 文件存在性"""
        username = self.info.username
        # 优先从 SQLite 加载
        if self._manager and self._manager.db:
            try:
                db_sessions = self._manager.db.get_sessions(username)
                if db_sessions:
                    self._sessions = [RecordingSession.from_dict(s) for s in db_sessions]
                    model_dir = self.output_dir / username
                    changed = False
                    for s in self._sessions:
                        if s.status in ("active", "ended"):
                            before = len(s.segments)
                            s.segments = [fn for fn in s.segments if (model_dir / fn).exists()]
                            removed = before - len(s.segments)
                            if removed > 0:
                                logger.warning(f"[{username}] Session {s.session_id}: removed {removed} missing segment(s)")
                                changed = True
                            if s.status == "ended" and len(s.segments) == 0:
                                s.status = "merged"
                                s.merged_file = ""
                                changed = True
                    if changed:
                        self._save_sessions()
                    logger.info(f"[{username}] Loaded {len(self._sessions)} sessions from SQLite")
                    return
            except Exception as e:
                logger.warning(f"[{username}] Failed to load sessions from SQLite: {e}")

        # Fallback: JSON 文件
        candidates = [username, self.identifier]
        for name in candidates:
            path = self.output_dir / name / "sessions.json"
            if path.exists():
                try:
                    with open(path) as f:
                        data = json.load(f)
                    self._sessions = [RecordingSession.from_dict(s) for s in data]
                    model_dir = self.output_dir / name
                    changed = False
                    for s in self._sessions:
                        if s.status in ("active", "ended"):
                            before = len(s.segments)
                            s.segments = [fn for fn in s.segments if (model_dir / fn).exists()]
                            removed = before - len(s.segments)
                            if removed > 0:
                                logger.warning(f"[{username}] Session {s.session_id}: removed {removed} missing segment(s)")
                                changed = True
                            if s.status == "ended" and len(s.segments) == 0:
                                s.status = "merged"
                                s.merged_file = ""
                                changed = True
                    if changed:
                        self._save_sessions()
                    logger.info(f"[{username}] Loaded {len(self._sessions)} sessions from JSON (fallback)")
                    return
                except Exception as e:
                    logger.warning(f"Failed to load sessions from {path}: {e}")
        self._sessions = []

    def _save_sessions(self):
        """持久化会话列表：SQLite 为主，JSON 为备份"""
        # SQLite 主存储
        if self._manager and self._manager.db:
            try:
                for s in self._sessions:
                    self._manager.db.upsert_session(s.to_dict())
            except Exception as e:
                logger.warning(f"[{self.info.username}] Failed to save sessions to SQLite: {e}")
        # JSON 备份
        model_dir = self.output_dir / self.info.username
        model_dir.mkdir(parents=True, exist_ok=True)
        path = model_dir / "sessions.json"
        try:
            data = [s.to_dict() for s in self._sessions]
            with open(path, "w") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"[{self.info.username}] Failed to save sessions JSON backup: {e}")

    def _create_session(self) -> RecordingSession:
        """创建新的录制会话"""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        short_id = uuid.uuid4().hex[:6]
        session = RecordingSession(
            session_id=f"s_{ts}_{short_id}",
            username=self.info.username,
            started_at=time.time(),
            status="active",
        )
        self._sessions.append(session)
        self._current_session = session
        self._save_sessions()
        logger.info(f"[{self.info.username}] New session: {session.session_id}")
        return session

    def _end_session(self):
        """结束当前会话（调用方应持有 _session_lock 或在单协程上下文中）"""
        if not self._current_session:
            return
        self._current_session.ended_at = time.time()
        self._current_session.status = "ended"
        self._current_session.stream_end_reason = self._last_stop_reason or "unknown"
        self._save_sessions()
        seg_count = len(self._current_session.segments)
        logger.info(f"[{self.info.username}] Session ended: {self._current_session.session_id} "
                     f"({seg_count} segments, reason={self._last_stop_reason})")
        # Webhook: 录制结束
        if self._manager and self._manager.webhook:
            asyncio.ensure_future(self._manager.webhook.notify("recording_end", {
                "username": self.info.username, "segments": seg_count,
                "reason": self._last_stop_reason}))
        self._current_session = None
        self._last_stop_reason = ""

    async def check_status(self) -> tuple[ModelStatus, Optional[int], int]:
        """子类实现：检测在线状态，返回 (status, model_id, viewers)"""
        raise NotImplementedError

    async def _do_record(self, output_path: str) -> bool:
        """子类实现：执行录制，返回是否成功"""
        raise NotImplementedError

    def _get_stream_url(self) -> str:
        """子类实现：返回直播页面 URL"""
        raise NotImplementedError

    # ========== 通用方法 ==========

    def _check_disk_during_recording(self) -> str:
        """录制中磁盘检查。返回: 'ok' | 'warning' | 'critical'"""
        try:
            free = shutil.disk_usage(str(self.output_dir)).free
            if free < 500 * 1024 * 1024:
                logger.warning(f"[{self.info.username}] Disk critically low ({free/1024/1024:.0f}MB)")
                self._disk_critical = True
                if self._manager and self._manager.webhook:
                    asyncio.ensure_future(self._manager.webhook.notify("disk_low", {
                        "username": self.info.username, "free_mb": int(free/1024/1024), "critical": True}))
                if self._manager and hasattr(self._manager, '_disk_warning_callback') and self._manager._disk_warning_callback:
                    asyncio.ensure_future(self._manager._disk_warning_callback(
                        self.info.username, int(free/1024/1024), True))
                return "critical"
            elif free < 2 * 1024 * 1024 * 1024:
                logger.warning(f"[{self.info.username}] Disk space low: {free/1024/1024:.0f}MB remaining")
                if self._manager and self._manager.webhook:
                    asyncio.ensure_future(self._manager.webhook.notify("disk_low", {
                        "username": self.info.username, "free_mb": int(free/1024/1024)}))
                if self._manager and hasattr(self._manager, '_disk_warning_callback') and self._manager._disk_warning_callback:
                    asyncio.ensure_future(self._manager._disk_warning_callback(
                        self.info.username, int(free/1024/1024), False))
                return "warning"
        except Exception:
            pass
        return "ok"

    async def _notify(self):
        if self.on_state_change:
            try:
                await self.on_state_change(self.info)
            except Exception:
                pass

    def _get_http_session(self) -> aiohttp.ClientSession:
        """获取共享 HTTP 会话，避免每次请求创建新连接"""
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15),
                headers={"User-Agent": self.user_agent},
            )
        return self._http_session

    async def _close_http_session(self):
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
            self._http_session = None

    def _set_state(self, state: RecordingState, error: str = ""):
        self.info.state = state
        self.info.error_msg = error
        if self.on_state_change:
            asyncio.ensure_future(self._notify())

    async def start(self):
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop())
        logger.info(f"[{self.info.username}] Monitor started ({self.platform})")

    async def stop(self):
        self._stop_event.set()
        self._recording_active = False
        if self._active_proc and self._active_proc.returncode is None:
            self._active_proc.terminate()
            try:
                await asyncio.wait_for(self._active_proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                self._active_proc.kill()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        # 结束当前会话
        if self._current_session:
            self._end_session()
        await self._close_http_session()
        self._set_state(RecordingState.IDLE)
        logger.info(f"[{self.info.username}] Monitor stopped")

    async def _run_loop(self):
        self._set_state(RecordingState.MONITORING)

        while not self._stop_event.is_set():
            try:
                # 定时录制检查：不在计划时间内则跳过
                if not check_schedule(self.schedule):
                    await self._sleep(60)
                    continue

                status, model_id, viewers = await self.check_status()
                self.info.status = status
                self.info.model_id = model_id
                self.info.viewers = viewers
                self.info.last_check = time.time()
                await self._notify()

                # 非 UNKNOWN 状态说明 API 正常，重置失败计数
                if status != ModelStatus.UNKNOWN:
                    self.info.consecutive_fails = 0

                if status == ModelStatus.PUBLIC:
                    self.info.last_online = time.time()

                    # 磁盘保护：critical 时暂停录制
                    if self._disk_critical:
                        try:
                            free = shutil.disk_usage(str(self.output_dir)).free
                            if free > 1024 * 1024 * 1024:  # > 1GB 恢复
                                self._disk_critical = False
                                logger.info(f"[{self.info.username}] Disk space recovered ({free/1024/1024:.0f}MB), resuming")
                            else:
                                logger.warning(f"[{self.info.username}] Disk critical, skipping recording ({free/1024/1024:.0f}MB free)")
                                await self._sleep(60)
                                continue
                        except Exception:
                            self._disk_critical = False

                    # 创建或复用会话（加锁防止并发竞态）
                    async with self._session_lock:
                        if not self._current_session:
                            reuse = False
                            reuse_window = getattr(self, 'session_reuse_window', 30)
                            if self._sessions:
                                last = self._sessions[-1]
                                if last.status == "ended" and last.ended_at and (time.time() - last.ended_at) < reuse_window:
                                    last.status = "active"
                                    last.ended_at = 0
                                    self._current_session = last
                                    self._save_sessions()
                                    reuse = True
                                    logger.info(f"[{self.info.username}] Reusing recent session: {last.session_id}")
                            if not reuse:
                                self._create_session()

                    self._set_state(RecordingState.RECORDING)
                    # Webhook: 录制开始
                    if self._manager and self._manager.webhook:
                        asyncio.ensure_future(self._manager.webhook.notify("recording_start", {
                            "username": self.info.username, "platform": self.info.platform}))
                    _, mp4_path = self._make_output_paths()

                    rec_info = RecordingInfo(file_path=mp4_path, start_time=time.time())
                    self.info.current_recording = rec_info
                    self._recording_active = True
                    self._last_stop_reason = ""  # 重置停止原因
                    await self._notify()

                    # 启动弹幕抓取（仅抖音）
                    if hasattr(self, '_danmaku_enabled') and self._danmaku_enabled and self.platform == "douyin":
                        try:
                            from danmaku import DanmakuCapture
                            session_id = self._current_session.session_id if self._current_session else ""
                            self._danmaku = DanmakuCapture(
                                room_id=getattr(self, 'room_id', self.identifier),
                                username=self.info.username,
                                output_dir=self.output_dir,
                                ttwid=getattr(self, '_ttwid', ''),
                                session_id=session_id,
                            )
                            await self._danmaku.start(time.time())
                        except Exception as e:
                            logger.warning(f"[{self.info.username}] Danmaku capture start failed: {e}")
                            self._danmaku = None

                    success = await self._do_record(mp4_path)
                    self._recording_active = False

                    # 停止弹幕抓取
                    danmaku_path = None
                    if hasattr(self, '_danmaku') and self._danmaku:
                        try:
                            danmaku_path = await self._danmaku.stop()
                            if danmaku_path and self._manager and self._manager.db:
                                stats = self._danmaku.get_stats()
                                self._manager.db.upsert_danmaku(
                                    session_id=self._current_session.session_id if self._current_session else "",
                                    username=self.info.username,
                                    file_path=str(danmaku_path),
                                    message_count=stats.get("total", 0),
                                    peak_density=stats.get("peak_density", 0),
                                )
                        except Exception as e:
                            logger.warning(f"[{self.info.username}] Danmaku capture stop failed: {e}")
                        self._danmaku = None

                    if success and os.path.exists(mp4_path):
                        file_size = os.path.getsize(mp4_path)
                        if file_size > 100_000:
                            rec_info.file_size = file_size
                            rec_info.duration = time.time() - rec_info.start_time
                            self.info.recordings.append(rec_info)
                            logger.info(f"[{self.info.username}] Saved: {mp4_path} ({file_size/1024/1024:.1f} MB)")
                            self._save_meta()

                        # 磁盘空间预警
                        try:
                            free = shutil.disk_usage(str(self.output_dir)).free
                            if free < 1024 * 1024 * 1024:  # < 1GB
                                logger.warning(f"[{self.info.username}] Low disk space: {free/1024/1024:.0f}MB remaining")
                                if self._manager and self._manager.webhook:
                                    asyncio.ensure_future(self._manager.webhook.notify("disk_low", {
                                        "username": self.info.username, "free_mb": int(free/1024/1024)}))
                        except Exception:
                            pass

                        # 追加片段到当前会话
                        if self._current_session:
                            self._current_session.segments.append(Path(mp4_path).name)
                            self._save_sessions()

                    self.info.current_recording = None

                    if not self._stop_event.is_set():
                        if self._last_stop_reason == "auto_split":
                            # 自动分割：不进入 grace period，直接继续录制下一段
                            self._last_stop_reason = ""
                            continue
                        await self._grace_period_check()
                else:
                    if self.info.state != RecordingState.MONITORING:
                        self._set_state(RecordingState.MONITORING)
                    await self._sleep(self.poll_interval_offline)

            except asyncio.CancelledError:
                break
            except (aiohttp.ClientError, ConnectionError, OSError) as e:
                # 网络错误：短重试
                logger.warning(f"[{self.info.username}] Network error: {e}")
                self.info.consecutive_fails += 1
                retry_interval = min(
                    self.retry_base_interval * (self.retry_backoff_factor ** min(self.info.consecutive_fails, 6)),
                    self.retry_max_interval
                )
                self._set_state(RecordingState.ERROR, f"网络错误: {e}")
                if self.info.consecutive_fails >= self.max_consecutive_fails:
                    await self._sleep(self.cooldown)
                    self.info.consecutive_fails = 0
                else:
                    await self._sleep(retry_interval)
            except Exception as e:
                logger.error(f"[{self.info.username}] Loop error: {e}", exc_info=True)
                self.info.consecutive_fails += 1
                self._set_state(RecordingState.ERROR, str(e))
                if self.info.consecutive_fails >= self.max_consecutive_fails:
                    await self._sleep(self.cooldown)
                    self.info.consecutive_fails = 0
                else:
                    await self._sleep(self.poll_interval_offline)

        self._set_state(RecordingState.IDLE)

    async def _grace_period_check(self):
        self._set_state(RecordingState.RECONNECTING)
        grace_start = time.time()

        # 自适应 grace period：进程正常退出（主播下播）用短时间，stall（网络问题）用长时间
        stop_reason = self._last_stop_reason
        if stop_reason == "process_exit_0":
            effective_grace = min(self.grace_period, 30)  # 主播正常下播，30s 足够
        elif stop_reason == "stall_timeout":
            effective_grace = self.grace_period  # 网络问题，用完整 grace period
        else:
            effective_grace = self.grace_period

        while not self._stop_event.is_set():
            elapsed = time.time() - grace_start
            if elapsed >= effective_grace:
                logger.info(f"[{self.info.username}] Grace period ended ({stop_reason}, {effective_grace:.0f}s), confirmed offline")
                self._end_session()
                await self._try_auto_merge()
                self._set_state(RecordingState.MONITORING)
                return

            await self._sleep(self.poll_interval_reconnect)
            status, _, viewers = await self.check_status()
            self.info.status = status
            self.info.viewers = viewers
            self.info.last_check = time.time()

            if status == ModelStatus.PUBLIC:
                logger.info(f"[{self.info.username}] Back online during grace period ({elapsed:.0f}s)")
                self.info.last_online = time.time()
                self._last_stop_reason = ""  # 重置
                return  # 会话保持 active，继续录制

        # 被 stop() 中断
        self._end_session()
        await self._try_auto_merge()
        self._set_state(RecordingState.IDLE)

    async def _try_auto_merge(self):
        """会话结束时，扫描文件系统触发自动合并（失败自动重试一次）"""
        if not self.auto_merge or not self._manager:
            return
        # 等待最后一个片段写入完成
        await asyncio.sleep(3)
        try:
            await self._manager.auto_merge_for_model(self.info.username)
        except Exception as e:
            logger.error(f"[{self.info.username}] Auto-merge failed: {e}, retrying in 30s...")
            await self._sleep(30)
            try:
                await self._manager.auto_merge_for_model(self.info.username)
            except Exception as e2:
                logger.error(f"[{self.info.username}] Auto-merge retry failed: {e2}")

    async def _record_with_streamlink(self, output_path: str, stream_url: str, quality: str = "best") -> bool:
        """通用 streamlink 录制"""
        cmd = ["streamlink", "--hls-live-edge", "6", "--stream-segment-attempts", "3"]
        if self.proxy:
            cmd += ["--http-proxy", self.proxy]
        cmd += [stream_url, quality, "-o", output_path]

        self._active_proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        logger.info(f"[{self.info.username}] streamlink started (pid={self._active_proc.pid})")

        last_size = 0
        stall_count = 0
        last_bw_time = time.time()
        last_bw_size = 0
        disk_check_counter = 0
        while not self._stop_event.is_set() and self._recording_active:
            await self._sleep(5)
            if self._active_proc.returncode is not None:
                break
            if os.path.exists(output_path) and self.info.current_recording:
                current_size = os.path.getsize(output_path)
                self.info.current_recording.file_size = current_size
                self.info.current_recording.duration = time.time() - self.info.current_recording.start_time
                # 计算瞬时码率
                now = time.time()
                dt = now - last_bw_time
                if dt > 0:
                    bw = (current_size - last_bw_size) * 8 / dt / 1000  # kbps
                    self.info.current_recording.bandwidth_kbps = max(0, bw)
                    last_bw_time = now
                    last_bw_size = current_size
                await self._notify()
                # 磁盘空间监控（每 60 秒检查一次）
                disk_check_counter += 1
                if disk_check_counter % 12 == 0:
                    disk_status = self._check_disk_during_recording()
                    if disk_status == "critical":
                        break
                # 断流检测（使用可配置阈值）
                if current_size > 0 and current_size == last_size:
                    stall_count += 1
                    stall_seconds = stall_count * self.stall_check_interval
                    if stall_seconds >= self.stall_timeout:
                        logger.warning(f"[{self.info.username}] streamlink stalled ({stall_seconds}s)")
                        self._last_stop_reason = "stall_timeout"
                        break
                else:
                    stall_count = 0
                last_size = current_size
                # 自动分割检测
                if self._should_split(current_size, self.info.current_recording.start_time if self.info.current_recording else 0):
                    logger.info(f"[{self.info.username}] Auto-split triggered (size={current_size/1024/1024:.0f}MB)")
                    self._last_stop_reason = "auto_split"
                    break

        if self._active_proc and self._active_proc.returncode is None:
            self._active_proc.terminate()
            try:
                await asyncio.wait_for(self._active_proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                self._active_proc.kill()

        # 记录停止原因
        if not self._last_stop_reason:
            if self._stop_event.is_set():
                self._last_stop_reason = "user_stop"
            elif self._active_proc and self._active_proc.returncode == 0:
                self._last_stop_reason = "process_exit_0"
            elif self._active_proc and self._active_proc.returncode is not None:
                self._last_stop_reason = "process_exit_error"

        self._active_proc = None
        return os.path.exists(output_path) and os.path.getsize(output_path) > 100_000

    async def _remux_to_mp4(self, raw_path: str, mp4_path: str) -> bool:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
            "-i", raw_path, "-c", "copy", "-movflags", "+faststart", mp4_path,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.warning(f"[{self.info.username}] Remux failed: {stderr.decode()[:200]}")
            if os.path.exists(raw_path) and os.path.getsize(raw_path) > 100_000:
                os.rename(raw_path, mp4_path)
                return True
            return False
        if os.path.exists(raw_path):
            os.remove(raw_path)
        return True

    def _make_output_paths(self) -> tuple[str, str]:
        model_dir = self.output_dir / self.info.username
        model_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return str(model_dir / f"{ts}.raw.mp4"), str(model_dir / f"{ts}.mp4")

    async def _sleep(self, seconds: float):
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    def _should_split(self, current_size: int, start_time: float) -> bool:
        """检查是否需要自动分割"""
        if self.split_by_size > 0 and current_size >= self.split_by_size:
            return True
        if self.split_by_duration > 0 and start_time > 0:
            elapsed = time.time() - start_time
            if elapsed >= self.split_by_duration:
                return True
        return False



# ========== 抖音 ==========

class DouyinRecorder(BaseLiveRecorder):
    platform = "douyin"

    def __init__(self, identifier: str, output_dir: str,
                 proxy: str = "", on_state_change=None):
        # 抖音不需要代理
        super().__init__(identifier, output_dir, proxy="", on_state_change=on_state_change)
        self.room_id = identifier
        self.info.platform = "douyin"
        self.info.live_url = f"https://live.douyin.com/{identifier}"
        self._streamer_name = None
        self._ttwid = ""
        self._ttwid_time = 0
        # 抖音 CDN 较慢，放宽断流检测
        self.stall_timeout = 30
        self.grace_period = 90
        # 弹幕抓取
        self._danmaku = None
        self._danmaku_enabled = True
        self._cached_stream_url = ""  # API 返回的流地址缓存

    def _get_stream_url(self) -> str:
        return f"https://live.douyin.com/{self.room_id}"

    async def _get_ttwid(self) -> str:
        """获取抖音 ttwid cookie（缓存 1 小时）"""
        if self._ttwid and (time.time() - self._ttwid_time) < 3600:
            return self._ttwid
        try:
            session = self._get_http_session()
            async with session.get(
                "https://live.douyin.com/",
                headers={"Accept-Encoding": "gzip, deflate"},
                timeout=aiohttp.ClientTimeout(total=10),
                allow_redirects=True,
            ) as resp:
                    ttwid = ""
                    # 从 Set-Cookie header 提取
                    for h in resp.headers.getall("Set-Cookie", []):
                        if "ttwid=" in h:
                            ttwid = h.split("ttwid=")[1].split(";")[0]
                            break
                    if not ttwid:
                        cookies = resp.cookies
                        for cookie in cookies.values():
                            if cookie.key == "ttwid":
                                ttwid = cookie.value
                                break
                    if ttwid:
                        first_time = not self._ttwid
                        self._ttwid = ttwid
                        self._ttwid_time = time.time()
                        if first_time:
                            logger.info(f"[{self.info.username}] ttwid obtained successfully")
                        return ttwid
        except Exception as e:
            logger.warning(f"[{self.info.username}] Failed to get ttwid: {e}")
        return self._ttwid

    async def check_status(self) -> tuple[ModelStatus, Optional[int], int]:
        """用抖音 webcast API 检测直播状态"""
        try:
            ttwid = await self._get_ttwid()
            url = (
                f"https://live.douyin.com/webcast/room/web/enter/"
                f"?aid=6383&app_name=douyin_web&live_id=1"
                f"&device_platform=web&language=zh-CN"
                f"&browser_language=zh-CN&browser_platform=MacIntel"
                f"&browser_name=Chrome&browser_version=120"
                f"&web_rid={self.room_id}"
            )
            headers = {
                "User-Agent": self.user_agent,
                "Referer": f"https://live.douyin.com/{self.room_id}",
                "Accept-Encoding": "gzip, deflate",
            }
            cookies = {"ttwid": ttwid} if ttwid else {}

            session = self._get_http_session()
            async with session.get(
                url, headers=headers, cookies=cookies,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"[{self.info.username}] Douyin API returned {resp.status}")
                    return await self._check_status_fallback()
                data = await resp.json()

            if data.get("status_code") != 0:
                logger.warning(f"[{self.info.username}] Douyin API error: {data.get('status_code')}")
                return await self._check_status_fallback()

            try:
                room_data = data.get("data", {})
                if not isinstance(room_data, dict):
                    return await self._check_status_fallback()

                # 提取主播信息（顶层 user）
                user = room_data.get("user", {})
                if isinstance(user, dict):
                    nickname = user.get("nickname", "")
                    if nickname and not self._streamer_name:
                        self._streamer_name = nickname
                        self.info.username = nickname
                        logger.info(f"[{self.info.username}] Douyin streamer: {nickname}")
                        self._save_meta()
                    # 提取头像
                    avatar = user.get("avatar_thumb", {})
                    if isinstance(avatar, dict):
                        url_list = avatar.get("url_list", [])
                        if url_list and not self.info.thumbnail_url:
                            self.info.thumbnail_url = url_list[0]

                # 检测在线状态 — 多种方式兼容不同 API 版本
                is_live = False
                viewers = 0

                # 方式1: data.data[0].status == 2 (新版 API)
                inner_rooms = room_data.get("data", [])
                if isinstance(inner_rooms, list) and inner_rooms:
                    room = inner_rooms[0] if isinstance(inner_rooms[0], dict) else {}
                    room_status = room.get("status", 0)
                    is_live = room_status == 2
                    # 提取观众数
                    viewers_str = room.get("user_count_str", "0")
                    try:
                        viewers = int(str(viewers_str).replace("万", "0000").replace("+", "").replace("w", "0000"))
                    except (ValueError, TypeError):
                        pass
                    # 缓存流地址（如果 API 返回了）
                    stream_url_data = room.get("stream_url", {})
                    if isinstance(stream_url_data, dict):
                        flv_url = stream_url_data.get("flv_pull_url", {})
                        hls_url = stream_url_data.get("hls_pull_url_map", {})
                        if isinstance(flv_url, dict) and flv_url:
                            # 取最高画质
                            self._cached_stream_url = list(flv_url.values())[-1]
                        elif isinstance(hls_url, dict) and hls_url:
                            self._cached_stream_url = list(hls_url.values())[-1]

                # 方式2: 顶层 room_status (旧版 API)
                if not is_live:
                    top_status = room_data.get("room_status", 0)
                    if top_status == 1:
                        is_live = True

                # 方式3: web_stream_url 存在说明在线
                if not is_live:
                    web_stream = room_data.get("web_stream_url")
                    if web_stream:
                        is_live = True
                        self._cached_stream_url = web_stream

                if is_live:
                    return ModelStatus.PUBLIC, int(self.room_id), viewers
                return ModelStatus.OFFLINE, int(self.room_id), 0
            except (KeyError, TypeError, AttributeError) as e:
                logger.warning(f"[{self.info.username}] Douyin API response parse error: {e}")
                return await self._check_status_fallback()

        except Exception as e:
            logger.warning(f"[{self.info.username}] Douyin API check error: {e}")
            return await self._check_status_fallback()

    async def _check_status_fallback(self) -> tuple[ModelStatus, Optional[int], int]:
        """Fallback: 用 streamlink 检测"""
        try:
            proc = await asyncio.create_subprocess_exec(
                "streamlink", "--json", self._get_stream_url(),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
            data = json.loads(stdout.decode())
            if data.get("streams"):
                author = data.get("metadata", {}).get("author", "")
                if author and not self._streamer_name:
                    self._streamer_name = author
                    self.info.username = author
                    self._save_meta()
                return ModelStatus.PUBLIC, int(self.room_id), 0
            return ModelStatus.OFFLINE, int(self.room_id), 0
        except Exception as e:
            logger.debug(f"[{self.info.username}] Douyin streamlink fallback error: {e}")
            return ModelStatus.UNKNOWN, None, 0

    async def _do_record(self, output_path: str) -> bool:
        """录制抖音直播：API流地址 > 自定义流地址 > streamlink > Playwright+ffmpeg"""
        # 方案0: 用户提供了自定义流地址
        if self.custom_stream_url:
            logger.info(f"[{self.info.username}] Using custom stream URL")
            return await self._record_with_ffmpeg(output_path, self.custom_stream_url)

        # 方案1: API 返回的流地址（最可靠）
        if self._cached_stream_url:
            logger.info(f"[{self.info.username}] Using API stream URL: {self._cached_stream_url[:80]}...")
            result = await self._record_with_ffmpeg(output_path, self._cached_stream_url)
            if result:
                return True
            logger.info(f"[{self.info.username}] API stream URL failed, trying streamlink")
            self._cached_stream_url = ""  # 清除失效的缓存

        q = self.quality if self.quality != "best" else "origin"

        # 方案2: streamlink（传入 ttwid + 用户 cookie）
        ttwid = await self._get_ttwid()
        extra_args = []
        cookie_str = f"ttwid={ttwid}" if ttwid else ""
        if self.custom_cookies:
            cookie_str = self.custom_cookies if not cookie_str else f"{cookie_str}; {self.custom_cookies}"
        if cookie_str:
            extra_args = ["--http-cookie", cookie_str]

        cmd = ["streamlink", "--hls-live-edge", "6", "--stream-segment-attempts", "3"]
        cmd += extra_args
        cmd += [self._get_stream_url(), q, "-o", output_path]

        self._active_proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        logger.info(f"[{self.info.username}] streamlink started (pid={self._active_proc.pid})")
        await asyncio.sleep(8)
        if self._active_proc.returncode is not None:
            self._active_proc = None
            logger.info(f"[{self.info.username}] streamlink failed, trying Playwright + ffmpeg")

            # 方案3: Playwright 提取流地址 + ffmpeg
            stream_url = await self._get_stream_url_via_playwright()
            if stream_url:
                return await self._record_with_ffmpeg(output_path, stream_url)

            logger.warning(f"[{self.info.username}] All recording methods failed, cooling down 60s")
            self._last_stop_reason = "process_exit_error"
            await self._sleep(60)
            return False

        return await self._monitor_streamlink(output_path)

    async def _monitor_streamlink(self, output_path: str) -> bool:
        """监控 streamlink 录制进程的文件增长"""
        last_size = 0
        stall_count = 0
        last_bw_time = time.time()
        last_bw_size = 0
        while not self._stop_event.is_set() and self._recording_active:
            await self._sleep(self.stall_check_interval)
            if self._active_proc.returncode is not None:
                break
            if os.path.exists(output_path) and self.info.current_recording:
                current_size = os.path.getsize(output_path)
                self.info.current_recording.file_size = current_size
                self.info.current_recording.duration = time.time() - self.info.current_recording.start_time
                now = time.time()
                dt = now - last_bw_time
                if dt > 0:
                    self.info.current_recording.bandwidth_kbps = max(0, (current_size - last_bw_size) * 8 / dt / 1000)
                    last_bw_time = now
                    last_bw_size = current_size
                await self._notify()
                if current_size > 0 and current_size == last_size:
                    stall_count += 1
                    if stall_count * self.stall_check_interval >= self.stall_timeout:
                        logger.warning(f"[{self.info.username}] streamlink stalled")
                        self._last_stop_reason = "stall_timeout"
                        break
                else:
                    stall_count = 0
                last_size = current_size

        if self._active_proc and self._active_proc.returncode is None:
            self._active_proc.terminate()
            try:
                await asyncio.wait_for(self._active_proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                self._active_proc.kill()
        # 记录停止原因
        if not self._last_stop_reason:
            if self._stop_event.is_set():
                self._last_stop_reason = "user_stop"
            elif self._active_proc and getattr(self._active_proc, 'returncode', None) == 0:
                self._last_stop_reason = "process_exit_0"
            else:
                self._last_stop_reason = "process_exit_error"
        self._active_proc = None
        return os.path.exists(output_path) and os.path.getsize(output_path) > 100_000

    async def _get_stream_url_via_playwright(self) -> Optional[str]:
        """用 Playwright 打开抖音页面，拦截流地址"""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.warning(f"[{self.info.username}] Playwright not installed")
            return None

        async def _run() -> Optional[str]:
            stream_url = None
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                try:
                    context = await browser.new_context(user_agent=self.user_agent)
                    page = await context.new_page()

                    async def on_response(response):
                        nonlocal stream_url
                        url = response.url
                        if stream_url:
                            return
                        if ".flv" in url and "pull" in url:
                            stream_url = url
                            logger.info(f"[{self.info.username}] Found FLV stream: {url[:80]}...")
                        elif ".m3u8" in url and "pull" in url:
                            stream_url = url
                            logger.info(f"[{self.info.username}] Found HLS stream: {url[:80]}...")

                    page.on("response", on_response)

                    try:
                        await page.goto(self._get_stream_url(), timeout=15000)
                    except Exception:
                        pass

                    # 等待流地址出现（最多 15 秒）
                    for _ in range(30):
                        if stream_url or self._stop_event.is_set():
                            break
                        await asyncio.sleep(0.5)
                finally:
                    await browser.close()
            return stream_url

        try:
            return await asyncio.wait_for(_run(), timeout=45)
        except asyncio.TimeoutError:
            logger.warning(f"[{self.info.username}] Playwright timed out after 45s")
            return None
        except Exception as e:
            logger.warning(f"[{self.info.username}] Playwright error: {e}")
            return None

    async def _record_with_ffmpeg(self, output_path: str, stream_url: str) -> bool:
        """用 ffmpeg 直接录制流"""
        cmd = [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
            "-headers", f"User-Agent: {self.user_agent}\r\nReferer: https://live.douyin.com/\r\n",
            "-i", stream_url,
            "-c", "copy", "-movflags", "+faststart",
            output_path,
        ]
        self._active_proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        logger.info(f"[{self.info.username}] ffmpeg recording started (pid={self._active_proc.pid})")

        last_size = 0
        stall_count = 0
        last_bw_time = time.time()
        last_bw_size = 0
        while not self._stop_event.is_set() and self._recording_active:
            await self._sleep(self.stall_check_interval)
            if self._active_proc.returncode is not None:
                break
            if os.path.exists(output_path) and self.info.current_recording:
                current_size = os.path.getsize(output_path)
                self.info.current_recording.file_size = current_size
                self.info.current_recording.duration = time.time() - self.info.current_recording.start_time
                now = time.time()
                dt = now - last_bw_time
                if dt > 0:
                    self.info.current_recording.bandwidth_kbps = max(0, (current_size - last_bw_size) * 8 / dt / 1000)
                    last_bw_time = now
                    last_bw_size = current_size
                await self._notify()
                if current_size > 0 and current_size == last_size:
                    stall_count += 1
                    if stall_count * self.stall_check_interval >= self.stall_timeout:
                        logger.warning(f"[{self.info.username}] ffmpeg stalled")
                        self._last_stop_reason = "stall_timeout"
                        break
                else:
                    stall_count = 0
                last_size = current_size
                # 自动分割检测
                if current_size > 0 and self._should_split(current_size, self.info.current_recording.start_time):
                    logger.info(f"[{self.info.username}] ffmpeg auto-split triggered (size={current_size/1024/1024:.0f}MB)")
                    self._last_stop_reason = "auto_split"
                    break

        if self._active_proc and self._active_proc.returncode is None:
            self._active_proc.terminate()
            try:
                await asyncio.wait_for(self._active_proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                self._active_proc.kill()
        self._active_proc = None
        return os.path.exists(output_path) and os.path.getsize(output_path) > 100_000


# ========== B站直播 ==========

class BilibiliRecorder(BaseLiveRecorder):
    platform = "bilibili"

    def __init__(self, identifier: str, output_dir: str,
                 proxy: str = "", on_state_change=None):
        super().__init__(identifier, output_dir, proxy="", on_state_change=on_state_change)
        self.room_id = identifier
        self.info.platform = "bilibili"
        self.info.live_url = f"https://live.bilibili.com/{identifier}"
        self._streamer_name = None

    def _get_stream_url(self) -> str:
        return f"https://live.bilibili.com/{self.room_id}"

    async def check_status(self) -> tuple[ModelStatus, Optional[int], int]:
        try:
            url = f"https://api.live.bilibili.com/room/v1/Room/get_info?room_id={self.room_id}"
            session = self._get_http_session()
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return ModelStatus.UNKNOWN, None, 0
                data = await resp.json()

            info = data.get("data", {})
            live_status = info.get("live_status", 0)
            viewers = info.get("online", 0)

            if not self._streamer_name:
                uid = info.get("uid")
                title = info.get("title", "")
                if title:
                    self._streamer_name = title
                # 获取主播名
                try:
                    uinfo_url = f"https://api.live.bilibili.com/live_user/v1/Master/info?uid={uid}"
                    async with session.get(uinfo_url, timeout=aiohttp.ClientTimeout(total=5)) as r2:
                        if r2.status == 200:
                            udata = await r2.json()
                            name = udata.get("data", {}).get("info", {}).get("uname", "")
                            if name:
                                self._streamer_name = name
                                self.info.username = name
                                logger.info(f"[{self.info.username}] Bilibili streamer: {name}")
                                self._save_meta()
                except Exception:
                    pass

            if live_status == 1:
                return ModelStatus.PUBLIC, int(self.room_id), viewers
            return ModelStatus.OFFLINE, int(self.room_id), 0
        except Exception as e:
            logger.warning(f"[{self.info.username}] Bilibili check error: {e}")
            return ModelStatus.UNKNOWN, None, 0

    async def _do_record(self, output_path: str) -> bool:
        return await self._record_with_streamlink(
            output_path, self._get_stream_url(), quality=self.quality
        )


# ========== Twitch ==========

class TwitchRecorder(BaseLiveRecorder):
    platform = "twitch"

    def __init__(self, identifier: str, output_dir: str,
                 proxy: str = "", on_state_change=None):
        super().__init__(identifier, output_dir, proxy, on_state_change)
        self.info.platform = "twitch"
        self.info.live_url = f"https://www.twitch.tv/{identifier}"

    def _get_stream_url(self) -> str:
        return f"https://www.twitch.tv/{self.identifier}"

    async def check_status(self) -> tuple[ModelStatus, Optional[int], int]:
        try:
            proc = await asyncio.create_subprocess_exec(
                "streamlink", "--json", self._get_stream_url(),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
            data = json.loads(stdout.decode())
            if data.get("streams"):
                return ModelStatus.PUBLIC, None, 0
            return ModelStatus.OFFLINE, None, 0
        except Exception as e:
            logger.debug(f"[{self.info.username}] Twitch check error: {e}")
            return ModelStatus.UNKNOWN, None, 0

    async def _do_record(self, output_path: str) -> bool:
        return await self._record_with_streamlink(
            output_path, self._get_stream_url(), quality=self.quality
        )


# ========== YouTube ==========

class YouTubeRecorder(BaseLiveRecorder):
    platform = "youtube"

    def __init__(self, identifier: str, output_dir: str,
                 proxy: str = "", on_state_change=None):
        super().__init__(identifier, output_dir, proxy, on_state_change)
        self.info.platform = "youtube"
        self.info.live_url = identifier

    def _get_stream_url(self) -> str:
        return self.identifier

    async def check_status(self) -> tuple[ModelStatus, Optional[int], int]:
        try:
            cmd = ["streamlink", "--json"]
            if self.proxy:
                cmd += ["--http-proxy", self.proxy]
            cmd.append(self._get_stream_url())
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
            data = json.loads(stdout.decode())
            if data.get("streams"):
                author = data.get("metadata", {}).get("author", "")
                if author and self.info.username.startswith("YT_"):
                    self.info.username = author
                return ModelStatus.PUBLIC, None, 0
            return ModelStatus.OFFLINE, None, 0
        except Exception as e:
            logger.debug(f"[{self.info.username}] YouTube check error: {e}")
            return ModelStatus.UNKNOWN, None, 0

    async def _do_record(self, output_path: str) -> bool:
        return await self._record_with_streamlink(
            output_path, self._get_stream_url(), quality=self.quality
        )


# ========== 通用平台（streamlink） ==========

class GenericRecorder(BaseLiveRecorder):
    """通用录制器 — 支持任意 streamlink/yt-dlp 可识别的直播/视频（2 级降级策略）"""
    platform = "generic"

    def __init__(self, identifier: str, output_dir: str,
                 proxy: str = "", on_state_change=None):
        super().__init__(identifier, output_dir, proxy, on_state_change)
        self.info.platform = "generic"
        self.info.live_url = identifier

    def _get_stream_url(self) -> str:
        return self.identifier

    async def check_status(self) -> tuple[ModelStatus, Optional[int], int]:
        """2 级检测：streamlink → yt-dlp"""

        # 策略1: streamlink（短超时 8s）
        try:
            proc = await asyncio.create_subprocess_exec(
                "streamlink", "--json", self._get_stream_url(),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=8)
            data = json.loads(stdout.decode())
            if data.get("streams"):
                return ModelStatus.PUBLIC, None, 0
        except Exception:
            pass

        # 策略2: yt-dlp（短超时 8s）
        if self._manager and self._manager._ytdlp_available:
            try:
                proc = await asyncio.create_subprocess_exec(
                    "yt-dlp", "--dump-json", "--no-download", self._get_stream_url(),
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
                )
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=8)
                if proc.returncode == 0 and stdout.strip():
                    return ModelStatus.PUBLIC, None, 0
            except Exception:
                pass

        return ModelStatus.OFFLINE, None, 0

    async def _do_record(self, output_path: str) -> bool:
        """2 级降级录制：streamlink → yt-dlp"""

        # 策略1: streamlink
        result = await self._record_with_streamlink(
            output_path, self._get_stream_url(), quality=self.quality
        )
        if result:
            return True
        logger.info(f"[{self.info.username}] streamlink failed, trying yt-dlp")

        # 策略2: yt-dlp
        if self._manager and self._manager._ytdlp_available:
            rc = await self._try_ytdlp_record(output_path)
            if rc:
                return True

        logger.warning(f"[{self.info.username}] All recording methods failed")
        self._last_stop_reason = "process_exit_error"
        return False

    async def _try_ytdlp_record(self, output_path: str) -> bool:
        """yt-dlp 录制"""
        try:
            cmd = ["yt-dlp", "--no-part", "--hls-use-mpegts", "--no-overwrites",
                   "-o", output_path, self._get_stream_url()]
            if self.proxy:
                cmd = ["yt-dlp", "--proxy", self.proxy] + cmd[1:]
            self._active_proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            )
            logger.info(f"[{self.info.username}] yt-dlp started (pid={self._active_proc.pid})")

            while not self._stop_event.is_set() and self._recording_active:
                await self._sleep(5)
                if self._active_proc.returncode is not None:
                    break
                if os.path.exists(output_path) and self.info.current_recording:
                    self.info.current_recording.file_size = os.path.getsize(output_path)
                    self.info.current_recording.duration = time.time() - self.info.current_recording.start_time
                    await self._notify()

            if self._active_proc and self._active_proc.returncode is None:
                self._active_proc.terminate()
                try:
                    await asyncio.wait_for(self._active_proc.wait(), timeout=10)
                except asyncio.TimeoutError:
                    self._active_proc.kill()
            self._active_proc = None
            return os.path.exists(output_path) and os.path.getsize(output_path) > 100_000
        except Exception as e:
            logger.warning(f"[{self.info.username}] yt-dlp error: {e}")
            self._active_proc = None
            return False


# ========== 平台注册表 ==========

# ========== 云存储上传 ==========

class CloudUploader:
    """可选的云存储上传（S3 兼容 / 阿里云 OSS）"""

    def __init__(self):
        self.config: Optional[dict] = None  # {"type":"s3","bucket":"...","prefix":"...","access_key":"...","secret_key":"...","endpoint":"...","region":"..."}

    async def upload(self, file_path: Path, username: str) -> Optional[str]:
        """上传文件到云存储，返回远程 URL 或 None"""
        if not self.config or not self.config.get("type"):
            return None
        try:
            import subprocess
            cloud_type = self.config["type"]
            bucket = self.config.get("bucket", "")
            prefix = self.config.get("prefix", "streamvideo")
            remote_key = f"{prefix}/{username}/{file_path.name}"

            if cloud_type in ("s3", "oss"):
                # 使用 AWS CLI 或 ossutil（需要预先配置）
                endpoint = self.config.get("endpoint", "")
                access_key = self.config.get("access_key", "")
                secret_key = self.config.get("secret_key", "")
                region = self.config.get("region", "us-east-1")

                env = os.environ.copy()
                env["AWS_ACCESS_KEY_ID"] = access_key
                env["AWS_SECRET_ACCESS_KEY"] = secret_key
                env["AWS_DEFAULT_REGION"] = region

                cmd = ["aws", "s3", "cp", str(file_path), f"s3://{bucket}/{remote_key}"]
                if endpoint:
                    cmd += ["--endpoint-url", endpoint]

                proc = await asyncio.create_subprocess_exec(
                    *cmd, env=env,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                )
                _, stderr = await asyncio.wait_for(proc.communicate(), timeout=600)
                if proc.returncode == 0:
                    url = f"s3://{bucket}/{remote_key}"
                    logger.info(f"[{username}] Uploaded to cloud: {url}")
                    return url
                else:
                    logger.warning(f"[{username}] Cloud upload failed: {(stderr.decode() if stderr else '')[:200]}")
            elif cloud_type == "rclone":
                # 使用 rclone（通用方案，支持所有云存储）
                remote = self.config.get("remote", "")
                cmd = ["rclone", "copy", str(file_path), f"{remote}:{bucket}/{prefix}/{username}/"]
                proc = await asyncio.create_subprocess_exec(
                    *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                )
                _, stderr = await asyncio.wait_for(proc.communicate(), timeout=600)
                if proc.returncode == 0:
                    url = f"{remote}:{bucket}/{prefix}/{username}/{file_path.name}"
                    logger.info(f"[{username}] Uploaded via rclone: {url}")
                    return url
                else:
                    logger.warning(f"[{username}] rclone upload failed: {(stderr.decode() if stderr else '')[:200]}")
            else:
                logger.warning(f"[{username}] Unknown cloud type: {cloud_type}")
        except Exception as e:
            logger.warning(f"[{username}] Cloud upload error: {e}")
        return None


# ========== Webhook 通知 ==========

class WebhookNotifier:
    """异步 Webhook 通知引擎"""

    def __init__(self):
        self.webhooks: list[dict] = []  # [{"type":"generic|discord|telegram", "url":"...", "events":[...]}]
        self._http_session: Optional[aiohttp.ClientSession] = None

    def _get_http_session(self) -> aiohttp.ClientSession:
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
            )
        return self._http_session

    async def notify(self, event: str, data: dict):
        """发送通知到所有匹配的 webhook（带重试）"""
        if not self.webhooks:
            return
        for wh in self.webhooks:
            if event not in wh.get("events", []):
                continue
            for attempt in range(3):
                try:
                    await self._send(wh, event, data)
                    break
                except Exception as e:
                    if attempt < 2:
                        await asyncio.sleep(2 ** (attempt + 1))
                    else:
                        logger.warning(f"Webhook failed after 3 attempts ({wh.get('type')}): {e}")

    async def _send(self, wh: dict, event: str, data: dict):
        wh_type = wh.get("type", "generic")
        url = wh.get("url", "")

        session = self._get_http_session()
        if wh_type == "discord":
            if not url:
                return
            payload = self._format_discord(event, data)
            async with session.post(url, json=payload) as resp:
                if resp.status >= 400:
                    logger.warning(f"Webhook discord returned {resp.status}")
        elif wh_type == "telegram":
            await self._send_telegram(session, wh, event, data)
        else:
            logger.warning(f"Unsupported webhook type: {wh_type}")

    def _format_discord(self, event: str, data: dict) -> dict:
        titles = {
            "recording_start": "🔴 开始录制",
            "recording_end": "⏹ 录制结束",
            "merge_done": "✅ 合并完成",
            "error": "❌ 错误",
            "disk_low": "⚠️ 磁盘空间不足",
        }
        desc_parts = []
        if data.get("username"):
            desc_parts.append(f"**主播**: {data['username']}")
        if data.get("filename"):
            desc_parts.append(f"**文件**: {data['filename']}")
        if data.get("size"):
            desc_parts.append(f"**大小**: {data['size']}")
        if data.get("message"):
            desc_parts.append(data["message"])

        return {"embeds": [{
            "title": titles.get(event, event),
            "description": "\n".join(desc_parts) or event,
            "color": {"recording_start": 0xe17055, "recording_end": 0x636e72,
                       "merge_done": 0x00b894, "error": 0xe17055, "disk_low": 0xfdcb6e}.get(event, 0x6c5ce7),
        }]}

    async def _send_telegram(self, session, wh: dict, event: str, data: dict):
        bot_token = wh.get("bot_token", "")
        chat_id = wh.get("chat_id", "")
        if not bot_token or not chat_id:
            return
        titles = {"recording_start": "🔴 开始录制", "recording_end": "⏹ 录制结束",
                  "merge_done": "✅ 合并完成", "error": "❌ 错误", "disk_low": "⚠️ 磁盘不足"}
        text = f"*{titles.get(event, event)}*"
        if data.get("username"):
            text += f"\n主播: {data['username']}"
        if data.get("filename"):
            text += f"\n文件: {data['filename']}"
        if data.get("message"):
            text += f"\n{data['message']}"

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        async with session.post(url, json={
            "chat_id": chat_id, "text": text, "parse_mode": "Markdown",
        }, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status >= 400:
                logger.warning(f"Telegram webhook returned {resp.status}")

    async def test(self, wh: dict) -> bool:
        """测试 webhook 连通性（含类型字段验证）"""
        wh_type = wh.get("type", "discord")
        if wh_type == "telegram" and (not wh.get("bot_token") or not wh.get("chat_id")):
            raise ValueError("Telegram 需要 bot_token 和 chat_id")
        if wh_type == "discord" and not wh.get("url"):
            raise ValueError("Discord 需要 URL")
        try:
            await self._send(wh, "test", {"message": "StreamVideo webhook test"})
            return True
        except Exception:
            return False


# ========== 定时录制 ==========

def check_schedule(schedule: Optional[dict]) -> bool:
    """检查当前时间是否在定时计划内"""
    if not schedule or not schedule.get("enabled"):
        return True  # 无计划 = 始终允许
    now = datetime.now()
    weekday = now.weekday()  # 0=Monday
    if weekday not in schedule.get("days", [0, 1, 2, 3, 4, 5, 6]):
        return False
    start_str = schedule.get("start", "00:00")
    end_str = schedule.get("end", "23:59")
    start_h, start_m = map(int, start_str.split(":"))
    end_h, end_m = map(int, end_str.split(":"))
    start_min = start_h * 60 + start_m
    end_min = end_h * 60 + end_m
    now_min = now.hour * 60 + now.minute
    if start_min <= end_min:
        return start_min <= now_min <= end_min
    else:  # 跨午夜，如 20:00 - 02:00
        return now_min >= start_min or now_min <= end_min


PLATFORM_CLASSES = {
    "douyin": DouyinRecorder,
    "bilibili": BilibiliRecorder,
    "twitch": TwitchRecorder,
    "youtube": YouTubeRecorder,
    "generic": GenericRecorder,
}


# ========== RecorderManager ==========

class RecorderManager:
    """管理多个主播的录制器"""

    def __init__(self, output_dir: str = "recordings",
                 proxy: str = "",
                 on_state_change: Optional[Callable] = None,
                 db=None):
        self.output_dir = output_dir
        self.proxy = proxy or os.environ.get("SV_PROXY", "http://127.0.0.1:7890")
        self.on_state_change = on_state_change
        self.db = db  # Database instance for merge history
        self.recorders: dict[str, BaseLiveRecorder] = {}
        self._thumb_task: Optional[asyncio.Task] = None
        self._active_merges: dict[str, dict] = {}
        self._merge_timeout = 14400  # 默认 4 小时（秒），可通过设置覆盖
        self._merge_gap_minutes = 15  # 合并分组时间间隔（分钟）
        self._post_process_rename = False  # 智能重命名开关
        self._post_process_h265 = False  # H.265 转码开关
        self._post_process_script = ""  # 录后脚本路径
        self._filename_template = "{username}_{date}_{duration}_merged"  # 文件名模板
        self._highlight_auto_detect = False  # 合并后自动检测高光
        self._highlight_config = {}  # 高光检测配置
        self._disk_warning_callback = None  # 磁盘警告回调
        self.cloud = CloudUploader()
        self.webhook = WebhookNotifier()
        self._ytdlp_available = shutil.which("yt-dlp") is not None
        self._streamlink_available = shutil.which("streamlink") is not None
        logger.info(f"Recording engines: yt-dlp={'yes' if self._ytdlp_available else 'no'}, streamlink={'yes' if self._streamlink_available else 'no'}")

    def _persist_sessions(self, username: str, sessions: list):
        """统一持久化 sessions：SQLite 为主，JSON 为备份"""
        # SQLite
        if self.db:
            try:
                for s in sessions:
                    self.db.upsert_session(s.to_dict() if hasattr(s, 'to_dict') else s)
            except Exception as e:
                logger.warning(f"[{username}] Failed to persist sessions to SQLite: {e}")
        # JSON 备份
        model_dir = Path(self.output_dir) / username
        model_dir.mkdir(parents=True, exist_ok=True)
        try:
            data = [s.to_dict() if hasattr(s, 'to_dict') else s for s in sessions]
            with open(model_dir / "sessions.json", "w") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"[{username}] Failed to persist sessions JSON: {e}")

    def add_model(self, url_or_id: str) -> ModelInfo:
        """添加主播，自动检测平台"""
        platform, identifier, display_name = detect_platform(url_or_id)

        # 用 display_name 作为 key（避免重复）
        key = display_name
        if key in self.recorders:
            return self.recorders[key].info

        cls = PLATFORM_CLASSES.get(platform, GenericRecorder)
        rec = cls(
            identifier=identifier, output_dir=self.output_dir,
            proxy=self.proxy, on_state_change=self.on_state_change,
        )
        # 只在 meta 未恢复名字时使用默认 display_name
        if rec.info.username == identifier:
            rec.info.username = display_name
        key = rec.info.username  # 用恢复后的名字作为 key
        if key in self.recorders:
            return self.recorders[key].info
        rec._manager = self
        rec._load_sessions()  # 重新加载，此时 _manager 已设置，可从 SQLite 读取
        self.recorders[key] = rec
        logger.info(f"Added [{platform}] {display_name} (id={identifier})")
        return rec.info

    def remove_model(self, username: str):
        if username in self.recorders:
            rec = self.recorders.pop(username)
            asyncio.ensure_future(rec.stop())

    async def start_model(self, username: str):
        if username in self.recorders:
            self.recorders[username].info.enabled = True
            await self.recorders[username].start()

    async def stop_model(self, username: str):
        if username in self.recorders:
            self.recorders[username].info.enabled = False
            await self.recorders[username].stop()

    async def start_all(self):
        for rec in self.recorders.values():
            if rec.info.enabled:
                await rec.start()
        if not self._thumb_task or self._thumb_task.done():
            self._thumb_task = asyncio.create_task(self._thumbnail_loop())

    async def stop_all(self):
        for rec in self.recorders.values():
            await rec.stop()
        if self._thumb_task:
            self._thumb_task.cancel()

    def _rec_to_dict(self, rec: BaseLiveRecorder) -> dict:
        d = rec.info.to_dict()
        d["custom_cookies"] = rec.custom_cookies or ""
        d["custom_stream_url"] = rec.custom_stream_url or ""
        d["schedule"] = rec.schedule
        return d

    def get_all_info(self) -> list[dict]:
        return [self._rec_to_dict(rec) for rec in self.recorders.values()]

    def get_model_info(self, username: str) -> Optional[dict]:
        if username in self.recorders:
            return self._rec_to_dict(self.recorders[username])
        return None

    def get_recordings(self, username: str) -> list[dict]:
        model_dir = Path(self.output_dir) / username
        if not model_dir.exists():
            return []
        files = []
        for f in sorted(model_dir.glob("*.mp4"), reverse=True):
            if ".raw." in f.name:
                continue
            stat = f.stat()
            files.append({"filename": f.name, "path": str(f), "size": stat.st_size, "created": stat.st_mtime})
        return files

    def get_grouped_recordings(self, username: str, gap_minutes: int = 15) -> list[dict]:
        files = self.get_recordings(username)
        if not files:
            return []
        files = [f for f in files if "_merged" not in f["filename"]]
        rec = self.recorders.get(username)
        if rec and rec.info.current_recording:
            active_name = Path(rec.info.current_recording.file_path).name
            files = [f for f in files if f["filename"] != active_name]
        files = list(reversed(files))

        groups, current_group, prev_time = [], [], None
        for f in files:
            name = f["filename"].replace(".mp4", "")
            try:
                ts = datetime.strptime(name, "%Y%m%d_%H%M%S")
            except ValueError:
                continue
            if prev_time and (ts - prev_time).total_seconds() > gap_minutes * 60:
                if len(current_group) >= 2:
                    groups.append(current_group)
                current_group = []
            current_group.append(f)
            prev_time = ts
        if len(current_group) >= 2:
            groups.append(current_group)

        return [{"id": g[0]["filename"].replace(".mp4", ""), "files": g,
                 "total_size": sum(f["size"] for f in g), "count": len(g)} for g in groups]

    def _calc_merge_confidence(self, username: str, session: "RecordingSession",
                                valid_files: list[str], codec_consistent: bool,
                                gap_minutes: float = 15) -> tuple[float, list[str]]:
        """计算合并信心度评分，返回 (score, reasons)"""
        score = 0.0
        reasons = []

        # 精确 session 匹配加分最高
        if session.session_id:
            score += 0.4
            reasons.append("同一会话")

        # 文件名前缀同一用户
        if valid_files and all(f.startswith(username) or True for f in valid_files):
            score += 0.2
            reasons.append("同一主播")

        # 编码一致不需重编码
        if codec_consistent:
            score += 0.1
            reasons.append("编码一致")

        # 时间间隔检查（从文件名解析时间戳）
        if len(valid_files) >= 2:
            max_gap_min = 0.0
            try:
                import re as _re
                timestamps = []
                for fn in valid_files:
                    m = _re.search(r'(\d{8}_\d{6})', fn)
                    if m:
                        from datetime import datetime as _dt
                        ts = _dt.strptime(m.group(1), "%Y%m%d_%H%M%S").timestamp()
                        timestamps.append(ts)
                if len(timestamps) >= 2:
                    timestamps.sort()
                    gaps = [(timestamps[i+1] - timestamps[i]) / 60 for i in range(len(timestamps)-1)]
                    max_gap_min = max(gaps)
                    if max_gap_min < gap_minutes:
                        score += 0.3
                        reasons.append(f"时间间隔正常({max_gap_min:.0f}min)")
                    elif max_gap_min > 60:
                        score -= 0.3
                        reasons.append(f"间隔过大({max_gap_min:.0f}min,可能跨直播)")
            except Exception:
                pass

        # 编码不一致降低信心
        if not codec_consistent:
            score -= 0.2
            reasons.append("编码不一致需重编")

        return max(0.0, min(1.0, score)), reasons

    def _get_per_model_config(self, username: str) -> dict:
        """获取主播的 per-model 配置（覆盖全局配置）"""
        rec = self.recorders.get(username)
        if not rec:
            return {}
        return {
            "h265_transcode": getattr(rec, "_per_model_h265", None),
            "filename_template": getattr(rec, "_per_model_filename_template", None),
        }

    async def auto_merge_for_model(self, username: str):
        """自动合并：优先使用 session 数据，fallback 到文件名分组"""
        model_dir = Path(self.output_dir) / username
        min_size = 500 * 1024

        # 读取 per-model 配置（覆盖全局）
        per_model = self._get_per_model_config(username)
        pm_h265 = per_model.get("h265_transcode")  # None = 继承全局
        pm_template = per_model.get("filename_template")  # None = 继承全局

        # 1. 基于 session 的精确合并（从内存或 SQLite 加载）
        session_merged = False
        rec = self.recorders.get(username)
        sessions = list(rec._sessions) if rec else []
        if not sessions and self.db:
            try:
                db_sessions = self.db.get_sessions(username)
                sessions = [RecordingSession.from_dict(s) for s in db_sessions]
            except Exception:
                pass
        if not sessions:
            sessions_path = model_dir / "sessions.json"
            if sessions_path.exists():
                try:
                    with open(sessions_path) as f:
                        sessions = [RecordingSession.from_dict(s) for s in json.load(f)]
                except Exception:
                    pass

        if sessions:
            try:
                for session in sessions:
                    if session.status != "ended":
                        continue
                    # 重试次数限制
                    if session.retry_count >= 3:
                        logger.warning(f"[{username}] Session {session.session_id}: max retries reached, skipping")
                        # P0: 合并失败超过重试次数时发送 webhook 警告
                        try:
                            await self.webhook.notify("merge_failed", {
                                "username": username,
                                "session_id": session.session_id,
                                "error": session.merge_error or "超过最大重试次数",
                                "retry_count": session.retry_count,
                                "segments": len(session.segments),
                            })
                        except Exception:
                            pass
                        if hasattr(self, '_merge_callback') and self._merge_callback:
                            try:
                                await self._merge_callback(username, session.session_id,
                                                           "merge_failed_permanent",
                                                           error=session.merge_error or "超过最大重试次数",
                                                           retry_count=session.retry_count)
                            except Exception:
                                pass
                        continue
                    # 过滤：只保留存在且足够大的片段
                    valid = []
                    for fn in session.segments:
                        fp = model_dir / fn
                        if fp.exists() and fp.stat().st_size >= min_size:
                            valid.append(fn)
                        elif fp.exists() and fp.stat().st_size < min_size:
                            logger.info(f"[{username}] Auto-cleanup small segment: {fn} ({fp.stat().st_size/1024:.0f} KB)")
                            fp.unlink()

                    if len(valid) == 0:
                        session.status = "merged"
                        session.merged_file = ""
                        continue

                    # 单片段：跳过合并，但执行后处理
                    if len(valid) == 1:
                        session.status = "merged"
                        single_file = model_dir / valid[0]
                        await self._post_process_fix_timestamps(single_file)
                        await self._post_process_transcode(single_file, username, h265_override=pm_h265)
                        final_name = self._generate_smart_name(username, valid, valid[0], template_override=pm_template)
                        if final_name != valid[0]:
                            final_path = model_dir / final_name
                            if not final_path.exists():
                                single_file.rename(final_path)
                                session.merged_file = final_name
                            else:
                                session.merged_file = valid[0]
                        else:
                            session.merged_file = valid[0]
                        session_merged = True
                        continue

                    # ffprobe 校验编码一致性（自动跳过损坏片段）
                    consistent, probe_valid, skipped, codec_error, codec_map = await self._check_codec_consistency(model_dir, valid)
                    if skipped:
                        logger.info(f"[{username}] Skipped {len(skipped)} corrupted segments: {skipped}")
                        # 通知前端损坏片段
                        if hasattr(self, '_merge_callback') and self._merge_callback:
                            await self._merge_callback(username, "", "segment_warning",
                                                       skipped_files=skipped, reason="corrupted")
                    if len(probe_valid) < 2:
                        if len(probe_valid) == 1:
                            session.status = "merged"
                            single_file = model_dir / probe_valid[0]
                            await self._post_process_fix_timestamps(single_file)
                            await self._post_process_transcode(single_file, username)
                            session.merged_file = probe_valid[0]
                        else:
                            session.status = "merged"
                            session.merged_file = ""
                        continue

                    # Codec 不一致时自动重编码
                    if not consistent and len(probe_valid) >= 2:
                        logger.info(f"[{username}] Session {session.session_id}: codec mismatch, attempting auto re-encode")
                        try:
                            probe_valid = await self._reencode_segments_to_dominant(
                                model_dir, probe_valid, codec_map, username)
                        except Exception as e:
                            logger.warning(f"[{username}] Re-encode failed: {e}, skipping auto-merge")
                            session.status = "error"
                            session.merge_error = f"重编码失败: {e}"
                            session.retry_count += 1
                            continue
                    valid = probe_valid

                    # P0: 合并信心度评分
                    merge_gap = getattr(self, '_merge_gap_minutes', 15)
                    confidence, confidence_reasons = self._calc_merge_confidence(
                        username, session, valid, consistent, gap_minutes=merge_gap)
                    logger.info(f"[{username}] Session {session.session_id}: merge confidence={confidence:.2f} ({', '.join(confidence_reasons)})")

                    if confidence < 0.4:
                        logger.warning(f"[{username}] Session {session.session_id}: low confidence ({confidence:.2f}), skipping auto-merge")
                        if hasattr(self, '_merge_callback') and self._merge_callback:
                            try:
                                await self._merge_callback(username, session.session_id,
                                                           "merge_low_confidence",
                                                           confidence=confidence,
                                                           reasons=confidence_reasons,
                                                           files=valid)
                            except Exception:
                                pass
                        continue
                    elif confidence < 0.7:
                        logger.info(f"[{username}] Session {session.session_id}: medium confidence ({confidence:.2f}), notifying user")
                        if hasattr(self, '_merge_callback') and self._merge_callback:
                            try:
                                await self._merge_callback(username, session.session_id,
                                                           "merge_confirm_required",
                                                           confidence=confidence,
                                                           reasons=confidence_reasons,
                                                           files=valid)
                            except Exception:
                                pass
                        # 中等信心度：仍然自动合并，但通知前端让用户知情

                    total = sum((model_dir / fn).stat().st_size for fn in valid)
                    logger.info(f"[{username}] Session {session.session_id}: merging {len(valid)} segments ({total/1024/1024:.1f} MB)...")
                    session.status = "merging"
                    session.merge_started_at = time.time()
                    # 持久化 merging 状态
                    self._persist_sessions(username, sessions)

                    try:
                        merge_id = await self.merge_segments(username, valid, delete_originals=True)
                        merge_info = self._active_merges.get(merge_id, {})
                        if merge_info.get("status") == "done":
                            session.status = "merged"
                            session.merged_file = merge_info.get("filename", "")
                            # 通知前端自动合并完成
                            await self._notify_merge(username, session.session_id, "auto_merge_done",
                                                     filename=session.merged_file)
                        else:
                            session.status = "error"
                            session.merge_error = merge_info.get("error", "合并失败")
                            session.retry_count += 1
                    except Exception as e:
                        session.status = "error"
                        session.merge_error = str(e)
                        session.retry_count += 1
                        logger.error(f"[{username}] Session merge error: {e}")
                    session_merged = True

                # 保存更新后的 session 状态
                self._persist_sessions(username, sessions)
                # 同步到 recorder 的内存
                rec = self.recorders.get(username)
                if rec:
                    rec._sessions = sessions
            except Exception as e:
                logger.error(f"[{username}] Session-based merge error: {e}")

        # 2. Fallback: 文件名时间戳分组（兼容无 session 的历史文件）
        if not session_merged:
            groups = self.get_grouped_recordings(username, gap_minutes=15)
            if not groups:
                return
            for group in groups:
                valid = []
                for f in group["files"]:
                    fp = model_dir / f["filename"]
                    if fp.exists() and fp.stat().st_size >= min_size:
                        valid.append(f["filename"])
                    elif fp.exists():
                        logger.info(f"[{username}] Auto-cleanup small segment: {f['filename']} ({fp.stat().st_size/1024:.0f} KB)")
                        fp.unlink()
                if len(valid) < 2:
                    continue
                total = sum((model_dir / fn).stat().st_size for fn in valid if (model_dir / fn).exists())
                logger.info(f"[{username}] Fallback auto-merging {len(valid)} segments ({total/1024/1024:.1f} MB)...")
                try:
                    await self.merge_segments(username, valid, delete_originals=True)
                except Exception as e:
                    logger.error(f"[{username}] Auto-merge error: {e}")

    async def _check_codec_consistency(self, model_dir: Path, filenames: list[str]) -> tuple[bool, list[str], list[str], str, dict]:
        """用 ffprobe 检查所有片段的视频编码是否一致，跳过损坏片段
        返回 (consistent, valid_files, skipped_files, error_detail, codec_map)
        codec_map: {filename: codec_key} 用于重编码决策"""
        if not shutil.which("ffprobe"):
            return True, filenames, [], "", {}
        codecs = {}  # fn -> codec_key
        skipped = []
        for fn in filenames:
            fp = model_dir / fn
            try:
                proc = await asyncio.create_subprocess_exec(
                    "ffprobe", "-v", "quiet", "-select_streams", "v:0",
                    "-show_entries", "stream=codec_name,width,height,duration",
                    "-of", "json", str(fp),
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
                )
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
                data = json.loads(stdout.decode())
                streams = data.get("streams", [])
                if not streams:
                    logger.warning(f"[codec check] No video stream in {fn}, skipping")
                    skipped.append(fn)
                    continue
                s = streams[0]
                dur = float(s.get("duration", 0) or 0)
                if dur <= 0:
                    logger.warning(f"[codec check] Zero duration in {fn}, skipping")
                    skipped.append(fn)
                    continue
                codecs[fn] = f"{s.get('codec_name')}_{s.get('width')}x{s.get('height')}"
            except Exception as e:
                logger.warning(f"[codec check] Failed to probe {fn}: {e}, skipping")
                skipped.append(fn)
        valid = [fn for fn in filenames if fn in codecs]
        if not valid:
            return False, [], skipped, f"全部 {len(filenames)} 个片段均无有效视频流", {}
        unique_codecs = set(codecs.values())
        consistent = len(unique_codecs) <= 1
        if not consistent:
            detail = " vs ".join(sorted(unique_codecs))
            logger.warning(f"[codec check] Codec mismatch: {unique_codecs}")
            return False, valid, skipped, f"编码不一致: {detail}", codecs
        return consistent, valid, skipped, "", codecs

    async def _reencode_segments_to_dominant(self, model_dir: Path, filenames: list[str],
                                              codec_map: dict, username: str) -> list[str]:
        """将 codec 不一致的 segments 重编码为多数派 codec，返回统一后的文件名列表"""
        from collections import Counter
        # 找到多数派 codec
        codec_counts = Counter(codec_map[fn] for fn in filenames if fn in codec_map)
        dominant_codec = codec_counts.most_common(1)[0][0]
        codec_name, resolution = dominant_codec.rsplit("_", 1)
        width, height = resolution.split("x")

        result_files = []
        temp_files = []  # 跟踪临时文件以便清理
        for fn in filenames:
            if codec_map.get(fn) == dominant_codec:
                result_files.append(fn)
                continue
            # 需要重编码
            reencode_name = fn.replace(".mp4", f".reenc.mp4")
            reencode_path = model_dir / reencode_name
            src_path = model_dir / fn
            logger.info(f"[{username}] Re-encoding {fn} ({codec_map.get(fn)}) → {dominant_codec}")
            try:
                # 检查磁盘空间
                file_size = src_path.stat().st_size
                disk_free = shutil.disk_usage(str(model_dir)).free
                if disk_free < file_size * 1.5:
                    logger.warning(f"[{username}] Insufficient disk for re-encode, skipping {fn}")
                    result_files.append(fn)  # 保留原文件，让 ffmpeg concat 尝试
                    continue

                # 根据目标 codec 选择编码器
                if codec_name == "hevc" or codec_name == "h265":
                    v_codec = ["-c:v", "libx265", "-crf", "23", "-preset", "fast", "-tag:v", "hvc1"]
                else:
                    v_codec = ["-c:v", "libx264", "-crf", "23", "-preset", "fast"]

                proc = await asyncio.create_subprocess_exec(
                    "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
                    "-i", str(src_path),
                    *v_codec,
                    "-vf", f"scale={width}:{height}",
                    "-c:a", "aac", "-b:a", "128k",
                    "-movflags", "+faststart",
                    str(reencode_path),
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
                )
                _, stderr = await asyncio.wait_for(proc.communicate(), timeout=3600)
                if proc.returncode == 0 and reencode_path.exists() and reencode_path.stat().st_size > 0:
                    logger.info(f"[{username}] Re-encoded {fn} → {reencode_name} "
                               f"({file_size/1024/1024:.1f}MB → {reencode_path.stat().st_size/1024/1024:.1f}MB)")
                    result_files.append(reencode_name)
                    temp_files.append(reencode_name)
                else:
                    error_msg = stderr.decode()[:200] if stderr else "unknown"
                    logger.warning(f"[{username}] Re-encode failed for {fn}: {error_msg}")
                    if reencode_path.exists():
                        reencode_path.unlink()
                    result_files.append(fn)  # fallback to original
            except asyncio.TimeoutError:
                logger.warning(f"[{username}] Re-encode timed out for {fn}")
                if reencode_path.exists():
                    reencode_path.unlink()
                result_files.append(fn)
            except Exception as e:
                logger.warning(f"[{username}] Re-encode error for {fn}: {e}")
                if reencode_path.exists():
                    reencode_path.unlink()
                result_files.append(fn)

        # 存储临时文件列表以便合并后清理
        self._active_merges.setdefault("_temp_reenc_files", {})[username] = [
            str(model_dir / f) for f in temp_files
        ]
        return result_files

    async def merge_segments(self, username: str, filenames: list[str],
                             delete_originals: bool = False) -> str:
        model_dir = Path(self.output_dir) / username
        for fn in filenames:
            if ".." in fn or "/" in fn:
                raise ValueError(f"非法文件名: {fn}")
            if not (model_dir / fn).exists():
                raise FileNotFoundError(f"文件不存在: {fn}")

        rec = self.recorders.get(username)
        if rec and rec.info.current_recording:
            active_name = Path(rec.info.current_recording.file_path).name
            if active_name in filenames:
                raise ValueError("不能合并正在录制的文件")

        base = filenames[0].replace(".mp4", "")
        merge_id = f"{base}_merged"
        output_name = f"{merge_id}.mp4"
        output_path = model_dir / output_name

        if merge_id in self._active_merges and self._active_merges[merge_id].get("status") == "running":
            return merge_id

        # 计算预期总大小（用于进度追踪）
        expected_size = sum((model_dir / fn).stat().st_size for fn in filenames if (model_dir / fn).exists())

        self._active_merges[merge_id] = {"status": "running", "progress": 0, "expected_size": expected_size}
        concat_path = model_dir / f".concat_{int(time.time())}.txt"
        merge_ok = False

        # 获取所有片段总时长（用于精确进度计算）
        total_duration_us = 0
        for fn in filenames:
            try:
                fp = model_dir / fn
                proc = await asyncio.create_subprocess_exec(
                    "ffprobe", "-v", "quiet", "-show_entries", "format=duration",
                    "-of", "json", str(fp),
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
                )
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
                data = json.loads(stdout.decode())
                dur = float(data.get("format", {}).get("duration", 0) or 0)
                total_duration_us += int(dur * 1_000_000)
            except Exception:
                pass

        # 进度监控任务
        progress_stop = asyncio.Event()
        merge_start_time = time.time()
        # 共享进度状态（由 ffmpeg stdout reader 更新）
        progress_state = {"current_time_us": 0}

        async def read_ffmpeg_progress(stdout_pipe):
            """从 ffmpeg -progress pipe:1 输出中解析进度"""
            try:
                while not progress_stop.is_set():
                    line = await stdout_pipe.readline()
                    if not line:
                        break
                    text = line.decode().strip()
                    if text.startswith("out_time_us="):
                        try:
                            progress_state["current_time_us"] = int(text.split("=")[1])
                        except (ValueError, IndexError):
                            pass
            except asyncio.CancelledError:
                return
            except Exception:
                pass

        async def monitor_progress():
            while not progress_stop.is_set():
                try:
                    if total_duration_us > 0:
                        raw_progress = progress_state["current_time_us"] / total_duration_us
                    elif output_path.exists() and expected_size > 0:
                        # fallback: 文件大小估算
                        raw_progress = output_path.stat().st_size / expected_size
                    else:
                        raw_progress = 0
                    progress = min(raw_progress * 0.90, 0.90)
                    self._active_merges[merge_id]["progress"] = progress
                    # ETA
                    elapsed = time.time() - merge_start_time
                    eta_str = ""
                    if raw_progress > 0.01 and elapsed > 5:
                        total_est = elapsed / raw_progress
                        remaining = max(0, total_est - elapsed)
                        if remaining >= 3600:
                            eta_str = f" · 预计剩余 {int(remaining/3600)}h{int(remaining%3600/60)}m"
                        elif remaining >= 60:
                            eta_str = f" · 预计剩余 {int(remaining/60)}:{int(remaining%60):02d}"
                        else:
                            eta_str = f" · 预计剩余 {int(remaining)}s"
                    if hasattr(self, '_merge_progress_callback') and self._merge_progress_callback:
                        await self._merge_progress_callback(
                            username, merge_id, progress,
                            f"合并 {len(filenames)} 个片段{eta_str}"
                        )
                except Exception:
                    pass
                try:
                    await asyncio.wait_for(progress_stop.wait(), timeout=2)
                    break
                except asyncio.TimeoutError:
                    pass

        progress_task = asyncio.create_task(monitor_progress())

        try:
            with open(concat_path, "w") as f:
                for fn in filenames:
                    f.write(f"file '{fn}'\n")
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
                "-f", "concat", "-safe", "0", "-i", str(concat_path),
                "-c", "copy", "-movflags", "+faststart",
                "-progress", "pipe:1",
                str(output_path),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            # 启动 ffmpeg 进度读取协程
            progress_reader = asyncio.create_task(read_ffmpeg_progress(proc.stdout))
            # 存储进程引用和停止事件，支持取消
            self._active_merges[merge_id]["_proc"] = proc
            self._active_merges[merge_id]["_stop"] = progress_stop
            self._active_merges[merge_id]["_output_path"] = str(output_path)
            merge_timeout = getattr(self, '_merge_timeout', 14400)
            try:
                await asyncio.wait_for(proc.wait(), timeout=merge_timeout)
                stderr = await proc.stderr.read()
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                progress_stop.set()
                progress_reader.cancel()
                await progress_task
                if output_path.exists():
                    output_path.unlink()
                timeout_h = merge_timeout / 3600
                total_mb = expected_size / 1024 / 1024
                self._active_merges[merge_id] = {"status": "error", "error": f"合并超时 (>{timeout_h:.0f}h, {total_mb:.0f}MB)"}
                logger.error(f"[{username}] Merge timed out after {timeout_h:.0f}h ({total_mb:.0f}MB)")
                return merge_id

            progress_stop.set()
            progress_reader.cancel()
            await progress_task

            if proc.returncode != 0:
                if output_path.exists():
                    output_path.unlink()
                error_msg = stderr.decode()[:300] if stderr else "未知错误"
                self._active_merges[merge_id] = {"status": "error", "error": error_msg}
                return merge_id

            merge_ok = True
            result_size = output_path.stat().st_size if output_path.exists() else 0

            # 后处理阶段进度: 0.90 → 0.95 → 1.0
            async def _update_progress(progress: float, phase: str):
                self._active_merges[merge_id]["progress"] = progress
                if hasattr(self, '_merge_progress_callback') and self._merge_progress_callback:
                    await self._merge_progress_callback(username, merge_id, progress, phase)

            # 后处理：时间戳修复
            await _update_progress(0.91, "修复时间戳...")
            await self._post_process_fix_timestamps(output_path)
            result_size = output_path.stat().st_size if output_path.exists() else result_size

            # 后处理：H.265 转码（可选，耗时）
            await _update_progress(0.93, "转码处理...")
            await self._post_process_transcode(output_path, username)
            result_size = output_path.stat().st_size if output_path.exists() else result_size

            # 后处理：智能重命名
            await _update_progress(0.96, "智能重命名...")
            final_name = self._generate_smart_name(username, filenames, output_name)
            if final_name != output_name:
                final_path = model_dir / final_name
                if not final_path.exists():
                    output_path.rename(final_path)
                    output_name = final_name
                    output_path = final_path
                    logger.info(f"[{username}] Renamed to: {final_name}")

            # 后处理：云存储上传（可选）
            if output_path.exists():
                cloud_url = await self.cloud.upload(output_path, username)

            # 后处理：录后脚本（可选）
            if output_path.exists():
                await self._run_post_script(output_path, username, "merge_done")

            # 后处理：FlashCut 全自动流水线（检测高光 + 生成片段）
            if getattr(self, '_highlight_auto_detect', False) and output_path.exists():
                await _update_progress(0.98, "FlashCut 自动处理中...")
                await self._auto_flashcut_pipeline(username, output_path)

            self._active_merges[merge_id] = {
                "status": "done", "filename": output_name, "size": result_size,
                "progress": 1.0, "input_count": len(filenames), "input_size": expected_size,
                "savings_bytes": max(0, expected_size - result_size),
                "savings_pct": round(max(0, expected_size - result_size) / expected_size * 100, 1) if expected_size > 0 else 0,
            }
            logger.info(f"[{username}] Merged {len(filenames)} files -> {output_name} ({result_size/1024/1024:.1f} MB)")
            # 同步 session 状态（手动合并也能更新对应 session）
            self._sync_sessions_after_merge(username, filenames, output_name)
            if delete_originals:
                for fn in filenames:
                    fp = model_dir / fn
                    if fp.exists():
                        fp.unlink()
            # 清理重编码临时文件
            temp_reenc = self._active_merges.get("_temp_reenc_files", {}).pop(username, [])
            for tmp_path in temp_reenc:
                try:
                    p = Path(tmp_path)
                    if p.exists():
                        p.unlink()
                except Exception:
                    pass
        except Exception as e:
            progress_stop.set()
            if not merge_ok and output_path.exists():
                output_path.unlink()
            self._active_merges[merge_id] = {"status": "error", "error": str(e)}
            logger.error(f"[{username}] Merge error: {e}")
        finally:
            if concat_path.exists():
                concat_path.unlink()

        try:
            status_info = self._active_merges.get(merge_id, {})
            if status_info.get("status") == "done":
                await self._notify_merge(username, merge_id, "done",
                                         filename=output_name, size=status_info.get("size", 0),
                                         input_count=len(filenames), input_size=expected_size,
                                         savings_bytes=status_info.get("savings_bytes", 0),
                                         savings_pct=status_info.get("savings_pct", 0))
                # Webhook: 合并完成
                await self.webhook.notify("merge_done", {
                    "username": username, "filename": output_name,
                    "size": f"{result_size/1024/1024:.1f}MB", "segments": len(filenames)})
                # 写入合并历史
                if self.db:
                    try:
                        self.db.insert_merge_history(
                            username=username, input_files=filenames,
                            input_size=expected_size, output_file=output_name,
                            output_size=status_info.get("size", 0),
                            savings_bytes=status_info.get("savings_bytes", 0))
                    except Exception as he:
                        logger.warning(f"[{username}] Failed to write merge history: {he}")
            elif status_info.get("status") == "error":
                await self._notify_merge(username, merge_id, "error", error=status_info.get("error", ""))
                await self.webhook.notify("error", {
                    "username": username, "message": f"合并失败: {status_info.get('error','')}"})
                if self.db:
                    try:
                        self.db.insert_merge_history(
                            username=username, input_files=filenames,
                            input_size=expected_size, status="error",
                            error=status_info.get("error", ""))
                    except Exception as he:
                        logger.warning(f"[{username}] Failed to write merge history: {he}")
        except Exception as e:
            logger.warning(f"[{username}] Merge notification failed: {e}")

        # 60 秒后清理 _active_merges 条目，防止内存泄漏
        loop = asyncio.get_event_loop()
        _mid = merge_id
        loop.call_later(60, lambda: self._active_merges.pop(_mid, None))

        return merge_id

    async def cancel_merge(self, merge_id: str) -> bool:
        """取消正在运行的合并任务"""
        info = self._active_merges.get(merge_id)
        if not info or info.get("status") != "running":
            return False
        proc = info.get("_proc")
        stop_event = info.get("_stop")
        output_path = info.get("_output_path")
        # 终止 ffmpeg 进程
        if proc and proc.returncode is None:
            proc.kill()
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass
        # 停止进度监控
        if stop_event:
            stop_event.set()
        # 清理输出文件
        if output_path:
            p = Path(output_path)
            if p.exists():
                try:
                    p.unlink()
                except Exception:
                    pass
        self._active_merges[merge_id] = {"status": "cancelled", "error": "用户取消"}
        logger.info(f"Merge cancelled: {merge_id}")
        return True

    async def _post_process_fix_timestamps(self, file_path: Path):
        """合并后修复时间戳，确保播放器兼容"""
        if not file_path.exists():
            return
        # 检查磁盘空间（需要至少 1.1 倍文件大小）
        try:
            file_size = file_path.stat().st_size
            disk_free = shutil.disk_usage(str(file_path.parent)).free
            if disk_free < file_size * 1.1:
                logger.warning(f"Post-process skipped: insufficient disk space ({disk_free/1024/1024:.0f}MB free, need {file_size*1.1/1024/1024:.0f}MB)")
                return
        except Exception:
            pass
        fixed_path = file_path.with_suffix(".fixed.mp4")
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
                "-fflags", "+genpts", "-i", str(file_path),
                "-c", "copy", "-movflags", "+faststart", str(fixed_path),
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()
            if proc.returncode == 0 and fixed_path.exists() and fixed_path.stat().st_size > 0:
                file_path.unlink()
                fixed_path.rename(file_path)
                logger.info(f"Post-process: timestamps fixed for {file_path.name}")
            else:
                error_msg = stderr.decode()[:200] if stderr else "unknown"
                logger.warning(f"Post-process timestamp fix failed (rc={proc.returncode}): {error_msg}")
                if fixed_path.exists():
                    fixed_path.unlink()
        except Exception as e:
            logger.warning(f"Post-process timestamp fix failed: {e}")
            if fixed_path.exists():
                fixed_path.unlink()

    async def _post_process_transcode(self, file_path: Path, username: str, h265_override=None):
        """合并后转码为 H.265 压缩（可选，耗时较长）"""
        use_h265 = h265_override if h265_override is not None else self._post_process_h265
        if not use_h265:
            return
        if not file_path.exists():
            return
        # 检查磁盘空间
        try:
            file_size = file_path.stat().st_size
            disk_free = shutil.disk_usage(str(file_path.parent)).free
            if disk_free < file_size:
                logger.warning(f"[{username}] Transcode skipped: insufficient disk space")
                return
        except Exception:
            pass

        h265_path = file_path.with_suffix(".h265.mp4")
        logger.info(f"[{username}] Transcoding to H.265: {file_path.name} ({file_size/1024/1024:.0f}MB)...")

        try:
            # 使用 libx265，CRF 28（较好的压缩率），保留音频
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
                "-i", str(file_path),
                "-c:v", "libx265", "-crf", "28", "-preset", "medium",
                "-c:a", "aac", "-b:a", "128k",
                "-movflags", "+faststart",
                "-tag:v", "hvc1",  # Apple 兼容
                str(h265_path),
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()

            if proc.returncode == 0 and h265_path.exists() and h265_path.stat().st_size > 0:
                new_size = h265_path.stat().st_size
                ratio = (1 - new_size / file_size) * 100 if file_size > 0 else 0
                file_path.unlink()
                h265_path.rename(file_path)
                logger.info(f"[{username}] Transcode done: {file_size/1024/1024:.0f}MB → {new_size/1024/1024:.0f}MB ({ratio:.0f}% smaller)")
                # Webhook 通知
                await self.webhook.notify("merge_done", {
                    "username": username, "message": f"H.265 转码完成，压缩 {ratio:.0f}%"})
            else:
                error_msg = stderr.decode()[:200] if stderr else "unknown"
                logger.warning(f"[{username}] Transcode failed: {error_msg}")
                if h265_path.exists():
                    h265_path.unlink()
        except Exception as e:
            logger.warning(f"[{username}] Transcode error: {e}")
            if h265_path.exists():
                h265_path.unlink()

    def _generate_smart_name(self, username: str, input_files: list[str], default_name: str,
                              template_override: str = None) -> str:
        """生成智能文件名，支持模板变量"""
        if not self._post_process_rename:
            return default_name
        try:
            first = input_files[0].replace(".mp4", "")
            ts = datetime.strptime(first, "%Y%m%d_%H%M%S")

            last = input_files[-1].replace(".mp4", "")
            ts_last = datetime.strptime(last, "%Y%m%d_%H%M%S")
            span = (ts_last - ts).total_seconds()
            if span < 3600:
                dur_str = f"{int(span/60)}m"
            else:
                dur_str = f"{int(span/3600)}h{int(span%3600/60)}m"

            safe_name = re.sub(r'[<>:"/\\|?*]', '_', username)

            # 使用模板（per-model 覆盖 > 全局配置）
            template = template_override or getattr(self, '_filename_template', '{username}_{date}_{duration}_merged')
            variables = {
                "username": safe_name,
                "platform": "",
                "date": ts.strftime("%Y-%m-%d"),
                "time": ts.strftime("%H-%M"),
                "datetime": ts.strftime("%Y-%m-%d_%H-%M"),
                "duration": dur_str,
                "segments": str(len(input_files)),
                "quality": "",
                "year": ts.strftime("%Y"),
                "month": ts.strftime("%m"),
                "day": ts.strftime("%d"),
            }
            # 获取平台和质量信息
            rec = self.recorders.get(username)
            if rec:
                variables["platform"] = rec.info.platform
                variables["quality"] = rec.quality

            try:
                result = template.format_map(variables) + ".mp4"
            except (KeyError, ValueError):
                result = f"{safe_name}_{ts.strftime('%Y-%m-%d_%H-%M')}_{dur_str}_merged.mp4"

            # 清理非法字符
            result = re.sub(r'[<>:"/\\|?*]', '_', result)
            logger.info(f"[{username}] Smart rename: {default_name} → {result}")
            return result
        except ValueError as e:
            logger.warning(f"[{username}] Smart rename failed (filename parse error): {e}, using default")
            return default_name
        except Exception as e:
            logger.warning(f"[{username}] Smart rename failed: {e}, using default")
            return default_name

    async def _notify_merge(self, username: str, merge_id: str, status: str, **kwargs):
        if hasattr(self, '_merge_callback') and self._merge_callback:
            await self._merge_callback(username, merge_id, status, **kwargs)

    async def _run_post_script(self, file_path: Path, username: str, event: str, session_id: str = ""):
        """执行录后脚本"""
        script = getattr(self, '_post_process_script', '')
        if not script or not Path(script).exists():
            return
        env = {
            **os.environ,
            "SV_USERNAME": username,
            "SV_FILE_PATH": str(file_path),
            "SV_FILE_SIZE": str(file_path.stat().st_size) if file_path.exists() else "0",
            "SV_PLATFORM": "",
            "SV_SESSION_ID": session_id,
            "SV_EVENT": event,
        }
        rec = self.recorders.get(username)
        if rec:
            env["SV_PLATFORM"] = rec.info.platform
        try:
            proc = await asyncio.create_subprocess_exec(
                script,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                env=env,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
            if proc.returncode == 0:
                logger.info(f"[{username}] Post-script OK: {event}")
            else:
                logger.warning(f"[{username}] Post-script exit {proc.returncode}: {stderr.decode()[:200]}")
        except asyncio.TimeoutError:
            logger.warning(f"[{username}] Post-script timed out (300s)")
        except Exception as e:
            logger.warning(f"[{username}] Post-script error: {e}")

    async def _detect_highlights(self, username: str, video_path: Path, session_id: str = ""):
        """运行高光检测并存入数据库"""
        try:
            from highlight import HighlightDetector
            config = getattr(self, '_highlight_config', {})
            detector = HighlightDetector(config)

            # 查找对应的弹幕文件
            danmaku_path = None
            if session_id:
                dp = video_path.parent / f"{session_id}_danmaku.json"
                if dp.exists():
                    danmaku_path = dp
            if not danmaku_path:
                # 尝试从数据库查找
                if self.db:
                    try:
                        dm = self.db.get_danmaku(session_id) if session_id else None
                        if dm and dm.get("file_path"):
                            dp = Path(dm["file_path"])
                            if dp.exists():
                                danmaku_path = dp
                    except Exception:
                        pass

            highlights = await detector.detect(video_path, danmaku_path)
            if not highlights:
                return

            # 存入数据库
            for h in highlights:
                hid = f"h_{int(time.time())}_{uuid.uuid4().hex[:6]}"
                if self.db:
                    self.db.insert_highlight(
                        highlight_id=hid, session_id=session_id, username=username,
                        video_file=video_path.name, start_time=h.start_time,
                        end_time=h.end_time, score=h.score, category=h.category,
                        signals=h.signals, title=h.title,
                    )

            logger.info(f"[{username}] Auto-detected {len(highlights)} highlights in {video_path.name}")

            # WebSocket 通知
            if hasattr(self, '_merge_callback') and self._merge_callback:
                await self._merge_callback(username, "", "highlight_detected",
                                           count=len(highlights), video_file=video_path.name)
        except Exception as e:
            logger.warning(f"[{username}] Highlight detection error: {e}")

    async def _auto_flashcut_pipeline(self, username: str, video_path: Path, session_id: str = ""):
        """FlashCut 全自动流水线：检测高光 → 生成字幕片段 → 生成封面"""
        try:
            # 广播进度
            async def _broadcast(msg):
                if hasattr(self, '_merge_callback') and self._merge_callback:
                    await self._merge_callback(username, "", "flashcut_progress", message=msg)

            await _broadcast("正在分析高光时刻...")

            # 1. 检测高光
            await self._detect_highlights(username, video_path, session_id)
            if not self.db:
                return

            highlights = self.db.get_highlights(username, limit=20)
            # 只处理刚检测到的（status=detected）
            new_highlights = [h for h in highlights if h.get("status") == "detected"
                             and h.get("video_file") == video_path.name]
            if not new_highlights:
                await _broadcast("未检测到高光片段")
                return

            await _broadcast(f"检测到 {len(new_highlights)} 个高光，正在生成短视频...")

            # 2. 配额检查
            from quota import QuotaManager
            quota_mgr = QuotaManager(self.db)
            allowed, used, limit = quota_mgr.check_quota(username)
            force_watermark = quota_mgr.should_watermark(username)
            remaining = limit - used

            # 3. 查找弹幕文件
            danmaku_path = None
            if session_id:
                dp = video_path.parent / f"{session_id}_danmaku.json"
                if dp.exists():
                    danmaku_path = dp

            # 4. 逐个生成片段
            from clipgen import ClipGenerator, ClipConfig
            config = ClipConfig(
                resolution=self._highlight_config.get("clip_resolution", "1080x1920"),
                format=self._highlight_config.get("clip_format", "vertical"),
                watermark=self._highlight_config.get("clip_watermark", ""),
                danmaku_overlay=self._highlight_config.get("clip_danmaku_overlay", True),
            )
            gen = ClipGenerator(config, self.output_dir)
            auto_subtitle = self._highlight_config.get("auto_subtitle", True)

            generated = 0
            for i, h in enumerate(new_highlights):
                # 配额检查
                if not allowed and remaining <= 0:
                    await _broadcast(f"已达每日配额上限 ({limit} 条)，已生成 {generated} 条")
                    break

                await _broadcast(f"正在生成短视频 ({i+1}/{len(new_highlights)})...")

                result = await gen.generate_clip(
                    video_path, h, danmaku_path,
                    auto_subtitle=auto_subtitle,
                    auto_cover=True,
                    force_watermark=force_watermark,
                )

                if result.get("status") == "done":
                    self.db.insert_clip(
                        clip_id=result["clip_id"], highlight_id=h.get("highlight_id", ""),
                        username=username, output_file=result.get("output_file", ""),
                        resolution=result.get("resolution", ""), duration=result.get("duration", 0),
                        format=result.get("format", ""), size=result.get("size", 0), status="done",
                    )
                    self.db.update_highlight_status(h["highlight_id"], "clipped")
                    quota_mgr.consume_quota(username)
                    generated += 1
                    remaining -= 1
                    allowed = remaining > 0

            await _broadcast(f"完成！生成了 {generated} 条短视频")

            # WebSocket 通知
            if hasattr(self, '_merge_callback') and self._merge_callback:
                await self._merge_callback(username, "", "flashcut_done",
                                           count=generated, video_file=video_path.name)

            logger.info(f"[{username}] FlashCut pipeline: {generated} clips from {video_path.name}")

        except Exception as e:
            logger.error(f"[{username}] FlashCut pipeline error: {e}")

    def _sync_sessions_after_merge(self, username: str, merged_files: list[str], output_file: str):
        """手动合并后，同步更新对应 session 状态为 merged"""
        rec = self.recorders.get(username)
        if not rec:
            return
        merged_set = set(merged_files)
        changed = False
        for s in rec._sessions:
            if s.status not in ("ended", "error"):
                continue
            if set(s.segments).issubset(merged_set) and s.segments:
                s.status = "merged"
                s.merged_file = output_file
                s.merge_error = ""
                changed = True
                logger.info(f"[{username}] Session {s.session_id} synced to merged via manual merge")
        if changed:
            rec._save_sessions()

    def get_sessions(self, username: str) -> list[dict]:
        """获取指定主播的所有会话"""
        # 优先从内存中的 recorder 获取
        rec = self.recorders.get(username)
        if rec:
            return [s.to_dict() for s in rec._sessions]
        # fallback: SQLite
        if self.db:
            try:
                return self.db.get_sessions(username)
            except Exception:
                pass
        # fallback: JSON
        sessions_path = Path(self.output_dir) / username / "sessions.json"
        if sessions_path.exists():
            try:
                with open(sessions_path) as f:
                    return json.load(f)
            except Exception:
                pass
        return []

    def update_session_status(self, username: str, session_id: str, status: str, **kwargs):
        """更新指定会话的状态"""
        rec = self.recorders.get(username)
        sessions = rec._sessions if rec else []
        if not sessions:
            if self.db:
                try:
                    db_sessions = self.db.get_sessions(username)
                    sessions = [RecordingSession.from_dict(s) for s in db_sessions]
                except Exception:
                    pass
            if not sessions:
                sessions_path = Path(self.output_dir) / username / "sessions.json"
                if sessions_path.exists():
                    try:
                        with open(sessions_path) as f:
                            sessions = [RecordingSession.from_dict(s) for s in json.load(f)]
                    except Exception:
                        return
        for s in sessions:
            if s.session_id == session_id:
                s.status = status
                for k, v in kwargs.items():
                    if hasattr(s, k):
                        setattr(s, k, v)
                break
        # 持久化（SQLite 为主，JSON 为备份）
        self._persist_sessions(username, sessions)

    async def _thumbnail_loop(self):
        """定期从录制中的视频生成缩略图，离线时保留已有缩略图"""
        thumbs_dir = Path(self.output_dir) / "thumbs"
        thumbs_dir.mkdir(parents=True, exist_ok=True)

        while True:
            try:
                for rec in self.recorders.values():
                    name = rec.info.username
                    thumb_file = thumbs_dir / f"{name}.jpg"

                    if rec.info.state == RecordingState.RECORDING and rec.info.current_recording:
                        # 录制中：从视频生成新缩略图
                        fp = rec.info.current_recording.file_path
                        for path in [fp, fp.replace(".mp4", ".raw.mp4")]:
                            if os.path.exists(path) and os.path.getsize(path) > 50_000:
                                await self._generate_thumbnail(path, str(thumb_file))
                                rec.info.thumbnail_url = f"/api/thumb/{name}"
                                rec._save_meta()
                                break
                    elif not rec.info.thumbnail_url and thumb_file.exists():
                        # 非录制：恢复已有缩略图
                        rec.info.thumbnail_url = f"/api/thumb/{name}"
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Thumbnail error: {e}")
            await asyncio.sleep(15)

    async def _generate_thumbnail(self, video_path: str, thumb_path: str):
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-sseof", "-1",
            "-i", video_path,
            "-vframes", "1", "-vf", "scale=320:-1",
            thumb_path,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.wait()

