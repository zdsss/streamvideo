"""
多平台直播录制引擎
- 支持 Stripchat、抖音等平台
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

    # Stripchat
    if "stripchat.com" in url:
        m = re.search(r'stripchat\.com/([^/?&#\s]+)', url)
        username = m.group(1) if m else url
        return "stripchat", username, username

    # 纯用户名（默认 Stripchat）
    if not url.startswith("http"):
        return "stripchat", url, url

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
        # 尝试从多个可能的目录加载
        candidates = [self.info.username, self.identifier]

        # 也扫描 recordings 目录下所有 meta.json，匹配 identifier
        try:
            for d in self.output_dir.iterdir():
                if d.is_dir() and (d / "meta.json").exists():
                    try:
                        with open(d / "meta.json") as f:
                            meta = json.load(f)
                        if meta.get("identifier") == self.identifier:
                            candidates.insert(0, d.name)  # 优先匹配
                    except Exception:
                        pass
        except Exception:
            pass

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
        """从 sessions.json 恢复会话列表"""
        # 需要先有 username 目录，尝试多个候选
        candidates = [self.info.username, self.identifier]
        for name in candidates:
            path = self.output_dir / name / "sessions.json"
            if path.exists():
                try:
                    with open(path) as f:
                        data = json.load(f)
                    self._sessions = [RecordingSession.from_dict(s) for s in data]
                    logger.info(f"[{self.info.username}] Loaded {len(self._sessions)} sessions")
                    return
                except Exception as e:
                    logger.warning(f"Failed to load sessions from {path}: {e}")
        self._sessions = []

    def _save_sessions(self):
        """持久化会话列表到 sessions.json"""
        model_dir = self.output_dir / self.info.username
        model_dir.mkdir(parents=True, exist_ok=True)
        path = model_dir / "sessions.json"
        try:
            data = [s.to_dict() for s in self._sessions]
            with open(path, "w") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"[{self.info.username}] Failed to save sessions: {e}")

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
        """结束当前会话"""
        if not self._current_session:
            return
        self._current_session.ended_at = time.time()
        self._current_session.status = "ended"
        self._save_sessions()
        seg_count = len(self._current_session.segments)
        logger.info(f"[{self.info.username}] Session ended: {self._current_session.session_id} "
                     f"({seg_count} segments)")
        # Webhook: 录制结束
        if self._manager and self._manager.webhook:
            asyncio.ensure_future(self._manager.webhook.notify("recording_end", {
                "username": self.info.username, "segments": seg_count}))
        self._current_session = None

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

    async def _notify(self):
        if self.on_state_change:
            try:
                await self.on_state_change(self.info)
            except Exception:
                pass

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
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        # 结束当前会话
        if self._current_session:
            self._end_session()
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

                    # 创建或复用会话（防重复：30秒内刚结束的会话直接复用）
                    if not self._current_session:
                        reuse = False
                        if self._sessions:
                            last = self._sessions[-1]
                            if last.status == "ended" and last.ended_at and (time.time() - last.ended_at) < 30:
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
                    await self._notify()

                    success = await self._do_record(mp4_path)
                    self._recording_active = False

                    if success and os.path.exists(mp4_path) and os.path.getsize(mp4_path) > 100_000:
                        rec_info.file_size = os.path.getsize(mp4_path)
                        rec_info.duration = time.time() - rec_info.start_time
                        self.info.recordings.append(rec_info)
                        logger.info(f"[{self.info.username}] Saved: {mp4_path} ({rec_info.file_size/1024/1024:.1f} MB)")
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

        while not self._stop_event.is_set():
            elapsed = time.time() - grace_start
            if elapsed >= self.grace_period:
                logger.info(f"[{self.info.username}] Grace period ended, confirmed offline")
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
                return  # 会话保持 active，继续录制

        # 被 stop() 中断
        self._end_session()
        await self._try_auto_merge()
        self._set_state(RecordingState.IDLE)

    async def _try_auto_merge(self):
        """会话结束时，扫描文件系统触发自动合并（失败自动重试一次）"""
        if not self.auto_merge or not self._manager:
            return
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
                # 断流检测（使用可配置阈值）
                if current_size > 0 and current_size == last_size:
                    stall_count += 1
                    stall_seconds = stall_count * self.stall_check_interval
                    if stall_seconds >= self.stall_timeout:
                        logger.warning(f"[{self.info.username}] streamlink stalled ({stall_seconds}s)")
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


# ========== Stripchat ==========

class StripchatRecorder(BaseLiveRecorder):
    platform = "stripchat"

    def __init__(self, identifier: str, output_dir: str,
                 proxy: str = "http://127.0.0.1:7890", on_state_change=None):
        super().__init__(identifier, output_dir, proxy, on_state_change)
        self.info.platform = "stripchat"
        self.info.live_url = f"https://stripchat.com/{identifier}"
        # Stripchat HLS 断流检测更灵敏
        self.stall_timeout = 15

    def _get_stream_url(self) -> str:
        return f"https://stripchat.com/{self.identifier}"

    async def check_status(self) -> tuple[ModelStatus, Optional[int], int]:
        url = f"https://stripchat.com/api/front/models/username/{self.identifier}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, proxy=self.proxy,
                    headers={"User-Agent": self.user_agent, "Accept": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        return ModelStatus.UNKNOWN, None, 0
                    data = await resp.json()

            model_id = data.get("id")
            is_live = data.get("isLive", False)
            status_str = data.get("status", "offline")
            viewers = data.get("viewersCount", 0)

            if is_live and status_str == "public":
                return ModelStatus.PUBLIC, model_id, viewers
            elif status_str in ("private", "p2p"):
                return ModelStatus.PRIVATE, model_id, viewers
            elif status_str == "groupShow":
                return ModelStatus.GROUP, model_id, viewers
            elif is_live:
                return ModelStatus.AWAY, model_id, viewers
            else:
                return ModelStatus.OFFLINE, model_id, viewers
        except Exception as e:
            logger.warning(f"[{self.info.username}] API error: {e}")
            return ModelStatus.UNKNOWN, None, 0

    async def _do_record(self, output_path: str) -> bool:
        # 优先 yt-dlp
        if self._manager and self._manager._ytdlp_available:
            rc = await self._record_with_ytdlp(output_path)
            if rc == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 100_000:
                return True
            logger.info(f"[{self.info.username}] yt-dlp failed (rc={rc}), trying Playwright")

        # Fallback: Playwright
        raw_path = output_path.replace(".mp4", ".raw.mp4")
        success = await self._record_with_playwright(raw_path)
        if success and os.path.exists(raw_path) and os.path.getsize(raw_path) > 100_000:
            await self._remux_to_mp4(raw_path, output_path)
            return os.path.exists(output_path) and os.path.getsize(output_path) > 100_000
        return False

    async def _record_with_ytdlp(self, output_path: str) -> int:
        """返回: 0=成功, 1=失败, 2=private/offline"""
        try:
            cmd = [
                "yt-dlp", "--proxy", self.proxy,
                "--no-part", "--hls-use-mpegts", "--live-from-start", "--no-overwrites",
                "-o", output_path, self._get_stream_url(),
            ]
            self._active_proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            )
            logger.info(f"[{self.info.username}] yt-dlp started (pid={self._active_proc.pid})")

            output_lines = []
            async def read_output():
                while True:
                    line = await self._active_proc.stdout.readline()
                    if not line:
                        break
                    output_lines.append(line.decode().strip())

            read_task = asyncio.create_task(read_output())

            while not self._stop_event.is_set() and self._recording_active:
                await self._sleep(5)
                if self._active_proc.returncode is not None:
                    break
                for ext in ["", ".part"]:
                    fp = output_path + ext
                    if os.path.exists(fp) and self.info.current_recording:
                        self.info.current_recording.file_size = os.path.getsize(fp)
                        self.info.current_recording.duration = time.time() - self.info.current_recording.start_time
                        await self._notify()
                        break

            if self._active_proc and self._active_proc.returncode is None:
                self._active_proc.terminate()
                try:
                    await asyncio.wait_for(self._active_proc.wait(), timeout=10)
                except asyncio.TimeoutError:
                    self._active_proc.kill()

            read_task.cancel()
            try:
                await read_task
            except asyncio.CancelledError:
                pass

            rc = self._active_proc.returncode or 0
            all_output = "\n".join(output_lines).lower()
            if "private show" in all_output or "model is offline" in all_output:
                return 2
            return 0 if rc == 0 else 1
        except Exception as e:
            logger.error(f"[{self.info.username}] yt-dlp error: {e}")
            return 1
        finally:
            self._active_proc = None

    async def _record_with_playwright(self, output_raw: str) -> bool:
        from playwright.async_api import async_playwright

        init_data = None
        seg_count = 0
        file_handle = None
        model_pattern = f"/{self.info.model_id}/" if self.info.model_id else f"/{self.identifier}/"

        try:
            async with async_playwright() as p:
                launch_args = {"headless": True}
                if self.proxy:
                    launch_args["proxy"] = {"server": self.proxy}
                browser = await p.chromium.launch(**launch_args)
                context = await browser.new_context(user_agent=self.user_agent)
                page = await context.new_page()

                async def force_best_quality(route):
                    try:
                        response = await route.fetch()
                        body = await response.text()
                        if "#EXT-X-STREAM-INF" in body:
                            lines = body.strip().split("\n")
                            best_bw, best_lines, header_lines = 0, [], []
                            i = 0
                            while i < len(lines):
                                if lines[i].startswith("#EXT-X-STREAM-INF"):
                                    bw_match = re.search(r"BANDWIDTH=(\d+)", lines[i])
                                    bw = int(bw_match.group(1)) if bw_match else 0
                                    uri = lines[i + 1] if i + 1 < len(lines) else ""
                                    if bw > best_bw:
                                        best_bw = bw
                                        best_lines = [lines[i], uri]
                                    i += 2
                                else:
                                    header_lines.append(lines[i])
                                    i += 1
                            if best_lines:
                                logger.info(f"[{self.info.username}] Forced best quality: {best_bw/1000:.0f} kbps")
                                await route.fulfill(status=response.status, headers=dict(response.headers),
                                                    body="\n".join(header_lines + best_lines) + "\n")
                                return
                        await route.fulfill(response=response)
                    except Exception:
                        await route.continue_()

                await page.route("**/*_auto.m3u8*", force_best_quality)
                await page.route("**/master*m3u8*", force_best_quality)

                async def on_response(response):
                    nonlocal init_data, seg_count, file_handle
                    url = response.url
                    if model_pattern not in url or not url.endswith(".mp4"):
                        return
                    if "/cpa/" in url or "media.mp4" in url:
                        return
                    try:
                        body = await response.body()
                        if not body or len(body) < 100:
                            return
                        if "_init_" in url:
                            if not init_data:
                                init_data = body
                                file_handle = open(output_raw, "wb")
                                file_handle.write(body)
                                file_handle.flush()
                                logger.info(f"[{self.info.username}] Init segment: {len(body)} bytes")
                        elif init_data and file_handle:
                            file_handle.write(body)
                            file_handle.flush()
                            seg_count += 1
                            if seg_count % 50 == 0:
                                logger.info(f"[{self.info.username}] {seg_count} segments, {os.path.getsize(output_raw)/1024/1024:.1f} MB")
                    except Exception:
                        pass

                page.on("response", on_response)
                logger.info(f"[{self.info.username}] Playwright: loading page...")
                try:
                    await page.goto(self._get_stream_url(), timeout=20000)
                except Exception:
                    pass

                for _ in range(20):
                    if init_data or self._stop_event.is_set():
                        break
                    await asyncio.sleep(0.5)

                if not init_data:
                    await browser.close()
                    return False

                logger.info(f"[{self.info.username}] Recording started")
                stall_count, last_seg_count = 0, 0
                while not self._stop_event.is_set() and self._recording_active:
                    await self._sleep(5)
                    if os.path.exists(output_raw) and self.info.current_recording:
                        self.info.current_recording.file_size = os.path.getsize(output_raw)
                        self.info.current_recording.duration = time.time() - self.info.current_recording.start_time
                        await self._notify()
                    if seg_count == last_seg_count:
                        stall_count += 1
                        if stall_count >= 6:
                            break
                    else:
                        stall_count = 0
                    last_seg_count = seg_count
                await browser.close()
        except Exception as e:
            logger.error(f"[{self.info.username}] Playwright error: {e}")
            return False
        finally:
            if file_handle:
                file_handle.close()
        return seg_count > 0


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

    def _get_stream_url(self) -> str:
        return f"https://live.douyin.com/{self.room_id}"

    async def _get_ttwid(self) -> str:
        """获取抖音 ttwid cookie（缓存 1 小时）"""
        if self._ttwid and (time.time() - self._ttwid_time) < 3600:
            return self._ttwid
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://live.douyin.com/",
                    headers={"User-Agent": self.user_agent},
                    timeout=aiohttp.ClientTimeout(total=10),
                    allow_redirects=True,
                ) as resp:
                    cookies = resp.cookies
                    ttwid = ""
                    for cookie in cookies.values():
                        if cookie.key == "ttwid":
                            ttwid = cookie.value
                            break
                    if not ttwid:
                        # 从 Set-Cookie header 提取
                        for h in resp.headers.getall("Set-Cookie", []):
                            if "ttwid=" in h:
                                ttwid = h.split("ttwid=")[1].split(";")[0]
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
        """用抖音 webcast API 检测直播状态（比 streamlink 更可靠）"""
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

            async with aiohttp.ClientSession() as session:
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
                    logger.warning(f"[{self.info.username}] Douyin API unexpected data type: {type(room_data)}")
                    return await self._check_status_fallback()
                room_status = room_data.get("room_status", 0)

                # 提取主播信息
                user = room_data.get("user", {})
                nickname = user.get("nickname", "")
                if nickname and not self._streamer_name:
                    self._streamer_name = nickname
                    self.info.username = nickname
                    logger.info(f"[{self.info.username}] Douyin streamer: {nickname}")
                    self._save_meta()

                # 提取头像
                avatar = user.get("avatar_thumb", {}).get("url_list", [])
                if avatar and not self.info.thumbnail_url:
                    self.info.thumbnail_url = avatar[0]

                if room_status == 1:
                    return ModelStatus.PUBLIC, int(self.room_id), 0
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
        except Exception:
            return ModelStatus.UNKNOWN, None, 0

    async def _do_record(self, output_path: str) -> bool:
        """录制抖音直播：自定义流地址 > streamlink > Playwright+ffmpeg"""
        # 方案0: 用户提供了自定义流地址，直接 ffmpeg 录制
        if self.custom_stream_url:
            logger.info(f"[{self.info.username}] Using custom stream URL")
            return await self._record_with_ffmpeg(output_path, self.custom_stream_url)

        q = self.quality if self.quality != "best" else "origin"

        # 方案1: streamlink（传入 ttwid + 用户 cookie）
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

            # 方案2: Playwright 提取流地址 + ffmpeg
            stream_url = await self._get_stream_url_via_playwright()
            if stream_url:
                return await self._record_with_ffmpeg(output_path, stream_url)

            logger.warning(f"[{self.info.username}] All recording methods failed, cooling down 60s")
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
        self._active_proc = None
        return os.path.exists(output_path) and os.path.getsize(output_path) > 100_000

    async def _get_stream_url_via_playwright(self) -> Optional[str]:
        """用 Playwright 打开抖音页面，拦截流地址"""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.warning(f"[{self.info.username}] Playwright not installed")
            return None

        stream_url = None
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(user_agent=self.user_agent)
                page = await context.new_page()

                async def on_response(response):
                    nonlocal stream_url
                    url = response.url
                    # 拦截 FLV 或 M3U8 流地址
                    if stream_url:
                        return
                    if ".flv" in url and "pull" in url:
                        stream_url = url.split("?")[0] + "?" + url.split("?")[1] if "?" in url else url
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

                await browser.close()
        except Exception as e:
            logger.warning(f"[{self.info.username}] Playwright error: {e}")

        return stream_url

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
        while not self._stop_event.is_set() and self._recording_active:
            await self._sleep(self.stall_check_interval)
            if self._active_proc.returncode is not None:
                break
            if os.path.exists(output_path) and self.info.current_recording:
                current_size = os.path.getsize(output_path)
                self.info.current_recording.file_size = current_size
                self.info.current_recording.duration = time.time() - self.info.current_recording.start_time
                now = time.time()
                if hasattr(self, '_last_bw_time'):
                    dt = now - self._last_bw_time
                    if dt > 0:
                        self.info.current_recording.bandwidth_kbps = max(0, (current_size - last_size) * 8 / dt / 1000)
                self._last_bw_time = now
                await self._notify()
                if current_size > 0 and current_size == last_size:
                    stall_count += 1
                    if stall_count * self.stall_check_interval >= self.stall_timeout:
                        logger.warning(f"[{self.info.username}] ffmpeg stalled")
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
            async with aiohttp.ClientSession() as session:
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
                    async with aiohttp.ClientSession() as s2:
                        async with s2.get(uinfo_url, timeout=aiohttp.ClientTimeout(total=5)) as r2:
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
        except Exception:
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
        except Exception:
            return ModelStatus.UNKNOWN, None, 0

    async def _do_record(self, output_path: str) -> bool:
        return await self._record_with_streamlink(
            output_path, self._get_stream_url(), quality=self.quality
        )


# ========== 通用平台（streamlink） ==========

class GenericRecorder(BaseLiveRecorder):
    platform = "generic"

    def __init__(self, identifier: str, output_dir: str,
                 proxy: str = "", on_state_change=None):
        super().__init__(identifier, output_dir, proxy, on_state_change)
        self.info.platform = "generic"
        self.info.live_url = identifier

    def _get_stream_url(self) -> str:
        return self.identifier

    async def check_status(self) -> tuple[ModelStatus, Optional[int], int]:
        """用 streamlink 检测是否有可用流"""
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
        except Exception:
            return ModelStatus.UNKNOWN, None, 0

    async def _do_record(self, output_path: str) -> bool:
        return await self._record_with_streamlink(
            output_path, self._get_stream_url(), quality=self.quality
        )


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
                _, stderr = await asyncio.wait_for(proc.communicate(), timeout=3600)
                if proc.returncode == 0:
                    url = f"s3://{bucket}/{remote_key}"
                    logger.info(f"[{username}] Uploaded to cloud: {url}")
                    return url
                else:
                    logger.warning(f"[{username}] Cloud upload failed: {stderr.decode()[:200]}")
            elif cloud_type == "rclone":
                # 使用 rclone（通用方案，支持所有云存储）
                remote = self.config.get("remote", "")
                cmd = ["rclone", "copy", str(file_path), f"{remote}:{bucket}/{prefix}/{username}/"]
                proc = await asyncio.create_subprocess_exec(
                    *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                )
                _, stderr = await asyncio.wait_for(proc.communicate(), timeout=3600)
                if proc.returncode == 0:
                    url = f"{remote}:{bucket}/{prefix}/{username}/{file_path.name}"
                    logger.info(f"[{username}] Uploaded via rclone: {url}")
                    return url
                else:
                    logger.warning(f"[{username}] rclone upload failed: {stderr.decode()[:200]}")
        except Exception as e:
            logger.warning(f"[{username}] Cloud upload error: {e}")
        return None


# ========== Webhook 通知 ==========

class WebhookNotifier:
    """异步 Webhook 通知引擎"""

    def __init__(self):
        self.webhooks: list[dict] = []  # [{"type":"generic|discord|telegram", "url":"...", "events":[...]}]

    async def notify(self, event: str, data: dict):
        """发送通知到所有匹配的 webhook"""
        if not self.webhooks:
            return
        for wh in self.webhooks:
            if event not in wh.get("events", []):
                continue
            try:
                await self._send(wh, event, data)
            except Exception as e:
                logger.warning(f"Webhook failed ({wh.get('type')}): {e}")

    async def _send(self, wh: dict, event: str, data: dict):
        wh_type = wh.get("type", "generic")
        url = wh.get("url", "")
        if not url:
            return

        async with aiohttp.ClientSession() as session:
            if wh_type == "discord":
                payload = self._format_discord(event, data)
            elif wh_type == "telegram":
                payload = None
                await self._send_telegram(session, wh, event, data)
                return
            else:
                payload = {"event": event, **data}

            async with session.post(
                url, json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status >= 400:
                    logger.warning(f"Webhook {wh_type} returned {resp.status}")

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
        """测试 webhook 连通性"""
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
    "stripchat": StripchatRecorder,
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
                 proxy: str = "http://127.0.0.1:7890",
                 on_state_change: Optional[Callable] = None):
        self.output_dir = output_dir
        self.proxy = proxy
        self.on_state_change = on_state_change
        self.recorders: dict[str, BaseLiveRecorder] = {}
        self._thumb_task: Optional[asyncio.Task] = None
        self._active_merges: dict[str, dict] = {}
        self._post_process_rename = False  # 智能重命名开关
        self._post_process_h265 = False  # H.265 转码开关
        self.cloud = CloudUploader()
        self.webhook = WebhookNotifier()
        self._ytdlp_available = shutil.which("yt-dlp") is not None
        self._streamlink_available = shutil.which("streamlink") is not None
        logger.info(f"Recording engines: yt-dlp={'yes' if self._ytdlp_available else 'no'}, streamlink={'yes' if self._streamlink_available else 'no'}")

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

    async def auto_merge_for_model(self, username: str):
        """自动合并：优先使用 session 数据，fallback 到文件名分组"""
        model_dir = Path(self.output_dir) / username
        min_size = 500 * 1024

        # 1. 基于 session 的精确合并
        sessions_path = model_dir / "sessions.json"
        session_merged = False
        if sessions_path.exists():
            try:
                with open(sessions_path) as f:
                    sessions = [RecordingSession.from_dict(s) for s in json.load(f)]
                for session in sessions:
                    if session.status != "ended" or len(session.segments) < 2:
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
                    if len(valid) < 2:
                        # 不足 2 个有效片段，标记完成
                        if len(valid) <= 1:
                            session.status = "merged"
                            session.merged_file = valid[0] if valid else ""
                        continue

                    # ffprobe 校验编码一致性
                    if not await self._check_codec_consistency(model_dir, valid):
                        logger.warning(f"[{username}] Session {session.session_id}: codec mismatch, skipping auto-merge")
                        session.status = "error"
                        session.merge_error = "编码不一致，无法自动合并"
                        continue

                    total = sum((model_dir / fn).stat().st_size for fn in valid)
                    logger.info(f"[{username}] Session {session.session_id}: merging {len(valid)} segments ({total/1024/1024:.1f} MB)...")
                    session.status = "merging"
                    # 持久化 merging 状态
                    with open(sessions_path, "w") as f:
                        json.dump([s.to_dict() for s in sessions], f, ensure_ascii=False, indent=2)

                    try:
                        merge_id = await self.merge_segments(username, valid, delete_originals=True)
                        merge_info = self._active_merges.get(merge_id, {})
                        if merge_info.get("status") == "done":
                            session.status = "merged"
                            session.merged_file = merge_info.get("filename", "")
                        else:
                            session.status = "error"
                            session.merge_error = merge_info.get("error", "合并失败")
                    except Exception as e:
                        session.status = "error"
                        session.merge_error = str(e)
                        logger.error(f"[{username}] Session merge error: {e}")
                    session_merged = True

                # 保存更新后的 session 状态
                with open(sessions_path, "w") as f:
                    json.dump([s.to_dict() for s in sessions], f, ensure_ascii=False, indent=2)
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

    async def _check_codec_consistency(self, model_dir: Path, filenames: list[str]) -> bool:
        """用 ffprobe 检查所有片段的视频编码是否一致"""
        if not shutil.which("ffprobe"):
            return True  # 没有 ffprobe 就跳过检查
        codecs = []
        for fn in filenames:  # 检查所有文件
            fp = model_dir / fn
            try:
                proc = await asyncio.create_subprocess_exec(
                    "ffprobe", "-v", "quiet", "-select_streams", "v:0",
                    "-show_entries", "stream=codec_name,width,height",
                    "-of", "json", str(fp),
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
                )
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
                data = json.loads(stdout.decode())
                streams = data.get("streams", [])
                if streams:
                    s = streams[0]
                    codecs.append(f"{s.get('codec_name')}_{s.get('width')}x{s.get('height')}")
                else:
                    logger.warning(f"[codec check] No video stream in {fn}")
                    return False  # 保守策略：无法解析则拒绝合并
            except Exception as e:
                logger.warning(f"[codec check] Failed to probe {fn}: {e}")
                return False  # 保守策略：异常则拒绝合并
        if not codecs:
            return True
        return len(set(codecs)) <= 1  # 所有片段编码一致

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

        # 进度监控任务
        progress_stop = asyncio.Event()

        async def monitor_progress():
            while not progress_stop.is_set():
                try:
                    if output_path.exists() and expected_size > 0:
                        current = output_path.stat().st_size
                        progress = min(current / expected_size, 0.99)
                        self._active_merges[merge_id]["progress"] = progress
                        # 通过回调广播进度
                        if hasattr(self, '_merge_progress_callback') and self._merge_progress_callback:
                            await self._merge_progress_callback(
                                username, merge_id, progress,
                                f"{len(filenames)} 个片段"
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
                "-c", "copy", "-movflags", "+faststart", str(output_path),
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()

            progress_stop.set()
            await progress_task

            if proc.returncode != 0:
                if output_path.exists():
                    output_path.unlink()
                error_msg = stderr.decode()[:300] if stderr else "未知错误"
                self._active_merges[merge_id] = {"status": "error", "error": error_msg}
                return merge_id

            merge_ok = True
            result_size = output_path.stat().st_size if output_path.exists() else 0

            # 后处理：时间戳修复
            await self._post_process_fix_timestamps(output_path)
            result_size = output_path.stat().st_size if output_path.exists() else result_size

            # 后处理：H.265 转码（可选，耗时）
            await self._post_process_transcode(output_path, username)
            result_size = output_path.stat().st_size if output_path.exists() else result_size

            # 后处理：智能重命名
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

            self._active_merges[merge_id] = {
                "status": "done", "filename": output_name, "size": result_size,
                "progress": 1.0, "input_count": len(filenames), "input_size": expected_size,
            }
            logger.info(f"[{username}] Merged {len(filenames)} files -> {output_name} ({result_size/1024/1024:.1f} MB)")
            if delete_originals:
                for fn in filenames:
                    fp = model_dir / fn
                    if fp.exists():
                        fp.unlink()
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
                                         input_count=len(filenames), input_size=expected_size)
                # Webhook: 合并完成
                await self.webhook.notify("merge_done", {
                    "username": username, "filename": output_name,
                    "size": f"{result_size/1024/1024:.1f}MB", "segments": len(filenames)})
            elif status_info.get("status") == "error":
                await self._notify_merge(username, merge_id, "error", error=status_info.get("error", ""))
                await self.webhook.notify("error", {
                    "username": username, "message": f"合并失败: {status_info.get('error','')}"})
        except Exception as e:
            logger.warning(f"[{username}] Merge notification failed: {e}")

        return merge_id

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

    async def _post_process_transcode(self, file_path: Path, username: str):
        """合并后转码为 H.265 压缩（可选，耗时较长）"""
        if not self._post_process_h265:
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
                ratio = (1 - new_size / file_size) * 100
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

    def _generate_smart_name(self, username: str, input_files: list[str], default_name: str) -> str:
        """生成智能文件名：{主播名}_{日期}_{时长}_merged.mp4"""
        if not self._post_process_rename:
            return default_name
        try:
            first = input_files[0].replace(".mp4", "")
            ts = datetime.strptime(first, "%Y%m%d_%H%M%S")
            date_str = ts.strftime("%Y-%m-%d_%H-%M")

            last = input_files[-1].replace(".mp4", "")
            ts_last = datetime.strptime(last, "%Y%m%d_%H%M%S")
            span = (ts_last - ts).total_seconds()
            if span < 3600:
                dur_str = f"{int(span/60)}m"
            else:
                dur_str = f"{int(span/3600)}h{int(span%3600/60)}m"

            safe_name = re.sub(r'[<>:"/\\|?*]', '_', username)
            result = f"{safe_name}_{date_str}_{dur_str}_merged.mp4"
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

    def get_sessions(self, username: str) -> list[dict]:
        """获取指定主播的所有会话"""
        # 优先从内存中的 recorder 获取
        rec = self.recorders.get(username)
        if rec:
            return [s.to_dict() for s in rec._sessions]
        # fallback: 从磁盘加载
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
        # 持久化
        model_dir = Path(self.output_dir) / username
        model_dir.mkdir(parents=True, exist_ok=True)
        try:
            with open(model_dir / "sessions.json", "w") as f:
                json.dump([s.to_dict() for s in sessions], f, ensure_ascii=False, indent=2)
        except Exception:
            pass

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

