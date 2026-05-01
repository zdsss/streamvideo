import asyncio
import json
import logging
import os
import re
import shutil
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import urlparse

import aiohttp

logger = logging.getLogger("recorder")

def _safe_task(coro):
    """Create task with error logging instead of silent exception loss"""
    task = asyncio.create_task(coro)
    task.add_done_callback(lambda t: t.exception() and logger.error(f"Background task error: {t.exception()}") if not t.cancelled() else None)
    return task

from streamvideo.core.recorder.models import *
from streamvideo.shared.constants import DISK_CRITICAL_BYTES, DISK_WARNING_BYTES, DISK_RESUME_BYTES

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

        # 崩溃循环检测
        self._restart_timestamps: deque[float] = deque(maxlen=10)

        # 弹幕抓取（默认禁用，各平台子类或 server 可覆盖）
        self._danmaku = None
        self._danmaku_enabled = False

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
                            # Recover stuck merging sessions
                            if s.status == "merging":
                                logger.info(f"[{username}] Recovering stuck merging session {s.session_id}")
                                s.status = "ended"
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
                            # Recover stuck merging sessions
                            if s.status == "merging":
                                logger.info(f"[{username}] Recovering stuck merging session {s.session_id}")
                                s.status = "ended"
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
        if len(self._sessions) > 100:
            self._sessions = self._sessions[-100:]
        self._current_session = session
        self._save_sessions()
        logger.info(f"[{self.info.username}] New session: {session.session_id}")
        return session

    async def _end_session(self):
        """结束当前会话（持有 _session_lock 保护并发访问）"""
        async with self._session_lock:
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
                _safe_task(self._manager.webhook.notify("recording_end", {
                    "username": self.info.username, "segments": seg_count,
                    "reason": self._last_stop_reason}))
            self._current_session = None
            self._last_stop_reason = ""

    async def check_status(self) -> tuple[ModelStatus, Optional[int], int]:
        """子类实现：检测在线状态，返回 (status, model_id, viewers)"""
        raise NotImplementedError

    async def _do_record(self, output_path: str) -> bool:
        """默认录制：使用 streamlink"""
        return await self._record_with_streamlink(output_path, self._get_stream_url(), quality=self.quality)

    def _get_stream_url(self) -> str:
        """默认流地址：直接使用 identifier"""
        return self.identifier

    async def _check_status_streamlink(self, url: str = "") -> tuple[ModelStatus, Optional[int], int]:
        """Common streamlink --json status check (shared by twitch/huya/douyu/kick)"""
        url = url or self._get_stream_url()
        cmd = ["streamlink", "--json", "--retry-open", "2"]
        if self.proxy:
            cmd += ["--http-proxy", self.proxy]
        cmd.append(url)
        rc, stdout, _ = await self._run_cmd(cmd, timeout=15)
        if rc == 0 and stdout.strip():
            try:
                data = json.loads(stdout)
                if data.get("streams"):
                    return ModelStatus.PUBLIC, None, 0
            except (json.JSONDecodeError, ValueError):
                pass
        return ModelStatus.OFFLINE, None, 0

    @staticmethod
    async def _run_cmd(cmd: list[str], timeout: float = 15) -> tuple[int, str, str]:
        """Run subprocess with timeout + guaranteed cleanup. Returns (returncode, stdout, stderr)."""
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            return proc.returncode, stdout.decode(), stderr.decode()
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return -1, "", "timeout"
        except Exception:
            proc.kill()
            await proc.wait()
            raise

    # ========== 通用方法 ==========

    def _check_disk_during_recording(self) -> str:
        """录制中磁盘检查。返回: 'ok' | 'warning' | 'critical'"""
        try:
            free = shutil.disk_usage(str(self.output_dir)).free
            if free < DISK_CRITICAL_BYTES:
                logger.warning(f"[{self.info.username}] Disk critically low ({free/1024/1024:.0f}MB)")
                self._disk_critical = True
                if self._manager and self._manager.webhook:
                    _safe_task(self._manager.webhook.notify("disk_low", {
                        "username": self.info.username, "free_mb": int(free/1024/1024), "critical": True}))
                if self._manager and hasattr(self._manager, '_disk_warning_callback') and self._manager._disk_warning_callback:
                    _safe_task(self._manager._disk_warning_callback(
                        self.info.username, int(free/1024/1024), True))
                return "critical"
            elif free < DISK_WARNING_BYTES:
                logger.warning(f"[{self.info.username}] Disk space low: {free/1024/1024:.0f}MB remaining")
                if self._manager and self._manager.webhook:
                    _safe_task(self._manager.webhook.notify("disk_low", {
                        "username": self.info.username, "free_mb": int(free/1024/1024)}))
                if self._manager and hasattr(self._manager, '_disk_warning_callback') and self._manager._disk_warning_callback:
                    _safe_task(self._manager._disk_warning_callback(
                        self.info.username, int(free/1024/1024), False))
                return "warning"
        except Exception:
            logger.debug("suppressed exception", exc_info=True)
        return "ok"

    async def _notify(self):
        if self.on_state_change:
            try:
                await self.on_state_change(self.info)
            except Exception:
                logger.debug("suppressed exception", exc_info=True)

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
            _safe_task(self._notify())

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
            await self._end_session()
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
                            if free > DISK_RESUME_BYTES:  # > 4GB 恢复
                                self._disk_critical = False
                                logger.info(f"[{self.info.username}] Disk space recovered ({free/1024/1024:.0f}MB), resuming")
                            else:
                                logger.warning(f"[{self.info.username}] Disk critical, skipping recording ({free/1024/1024:.0f}MB free)")
                                await self._sleep(60)
                                continue
                        except Exception:
                            self._disk_critical = False

                    # 崩溃循环检测：5次快速重启（300秒内）进入冷却
                    if len(self._restart_timestamps) >= 5:
                        recent = [t for t in self._restart_timestamps if time.time() - t < 300]
                        if len(recent) >= 5:
                            logger.warning(f"[{self.info.username}] Crash loop detected ({len(recent)} quick restarts in 5min), cooling down 600s")
                            self._set_state(RecordingState.ERROR, "crash loop cooldown")
                            await self._sleep(600)
                            if self._stop_event.is_set():
                                break
                            self._restart_timestamps.clear()
                            continue

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
                        _safe_task(self._manager.webhook.notify("recording_start", {
                            "username": self.info.username, "platform": self.info.platform}))
                    _, mp4_path = self._make_output_paths()

                    rec_info = RecordingInfo(file_path=mp4_path, start_time=time.time())
                    self.info.current_recording = rec_info
                    self._recording_active = True
                    self._last_stop_reason = ""  # 重置停止原因
                    await self._notify()

                    # 启动弹幕抓取（支持 Douyin / Bilibili / Twitch）
                    if hasattr(self, '_danmaku_enabled') and self._danmaku_enabled:
                        try:
                            session_id = self._current_session.session_id if self._current_session else ""
                            if self.platform == "douyin":
                                from streamvideo.core.processor.danmaku import DanmakuCapture
                                self._danmaku = DanmakuCapture(
                                    room_id=getattr(self, 'room_id', self.identifier),
                                    username=self.info.username,
                                    output_dir=self.output_dir,
                                    ttwid=getattr(self, '_ttwid', ''),
                                    session_id=session_id,
                                )
                            elif self.platform == "bilibili":
                                from streamvideo.core.processor.danmaku import BilibiliDanmakuCapture
                                self._danmaku = BilibiliDanmakuCapture(
                                    room_id=getattr(self, 'room_id', self.identifier),
                                    username=self.info.username,
                                    output_dir=self.output_dir,
                                    session_id=session_id,
                                )
                            elif self.platform == "twitch":
                                from streamvideo.core.processor.danmaku import TwitchDanmakuCapture
                                self._danmaku = TwitchDanmakuCapture(
                                    channel=self.identifier,
                                    username=self.info.username,
                                    output_dir=self.output_dir,
                                    session_id=session_id,
                                )
                            else:
                                self._danmaku = None
                            if self._danmaku:
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
                            if len(self.info.recordings) > 200:
                                self.info.recordings = self.info.recordings[-200:]
                            logger.info(f"[{self.info.username}] Saved: {mp4_path} ({file_size/1024/1024:.1f} MB)")
                            self._save_meta()

                        # 磁盘空间预警
                        try:
                            free = shutil.disk_usage(str(self.output_dir)).free
                            if free < DISK_RESUME_BYTES:  # < 4GB
                                logger.warning(f"[{self.info.username}] Low disk space: {free/1024/1024:.0f}MB remaining")
                                if self._manager and self._manager.webhook:
                                    _safe_task(self._manager.webhook.notify("disk_low", {
                                        "username": self.info.username, "free_mb": int(free/1024/1024)}))
                        except Exception:
                            logger.debug("suppressed exception", exc_info=True)

                        # 追加片段到当前会话
                        if self._current_session:
                            self._current_session.segments.append(Path(mp4_path).name)
                            self._save_sessions()

                    self.info.current_recording = None

                    # 崩溃循环追踪：记录快速重启，成功长录制则重置
                    if success and rec_info.duration and rec_info.duration > 60 and (
                            os.path.exists(mp4_path) and os.path.getsize(mp4_path) > 100_000):
                        self._restart_timestamps.clear()
                    else:
                        self._restart_timestamps.append(time.time())

                    if not self._stop_event.is_set():
                        if self._last_stop_reason == "auto_split":
                            # 自动分割：不进入 grace period，直接继续录制下一段
                            self._last_stop_reason = ""
                            continue
                        await self._grace_period_check()
                else:
                    if self.info.state != RecordingState.MONITORING:
                        self._set_state(RecordingState.MONITORING)
                    await self._sleep(self._smart_poll_interval())

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
                await self._end_session()
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
        await self._end_session()
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
        cmd = ["streamlink", "--hls-live-edge", "6", "--stream-segment-attempts", "3",
               "--retry-open", "3", "--ringbuffer-size", "32M"]
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

    async def _try_ytdlp_record(self, output_path: str) -> bool:
        """yt-dlp recording fallback (shared by all subclasses)"""
        try:
            url = self._get_stream_url()
            cmd = ["yt-dlp", "--no-part", "--hls-use-mpegts", "--no-overwrites",
                   "-o", output_path, url]
            if self.proxy:
                cmd = ["yt-dlp", "--proxy", self.proxy] + cmd[1:]
            self._active_proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            )
            logger.info(f"[{self.info.username}] yt-dlp started (pid={self._active_proc.pid})")

            last_size = 0
            stall_count = 0
            while not self._stop_event.is_set() and self._recording_active:
                await self._sleep(5)
                if self._active_proc.returncode is not None:
                    break
                if os.path.exists(output_path) and self.info.current_recording:
                    current_size = os.path.getsize(output_path)
                    self.info.current_recording.file_size = current_size
                    self.info.current_recording.duration = time.time() - self.info.current_recording.start_time
                    await self._notify()
                    # Stall detection
                    if current_size > 0 and current_size == last_size:
                        stall_count += 1
                        if stall_count * self.stall_check_interval >= self.stall_timeout:
                            logger.warning(f"[{self.info.username}] yt-dlp stalled")
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

            if not self._last_stop_reason:
                if self._stop_event.is_set():
                    self._last_stop_reason = "user_stop"
                elif self._active_proc and self._active_proc.returncode == 0:
                    self._last_stop_reason = "process_exit_0"
                elif self._active_proc and self._active_proc.returncode is not None:
                    self._last_stop_reason = "process_exit_error"

            self._active_proc = None
            return os.path.exists(output_path) and os.path.getsize(output_path) > 100_000
        except Exception as e:
            logger.warning(f"[{self.info.username}] yt-dlp error: {e}")
            self._active_proc = None
            return False

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

    def _smart_poll_interval(self) -> float:
        """根据历史开播时间动态调整检测频率。
        高概率开播时段（历史数据 >= 30% 命中）返回 poll_interval_offline（默认30s），
        其他时段返回 300s（5分钟），节省检测开销。
        """
        sessions = self._sessions
        if len(sessions) < 5:
            return self.poll_interval_offline  # 数据不足，保持默认高频

        now_hour = datetime.now().hour
        # 统计最近 30 次 session 的开播小时分布
        hour_counts = [0] * 24
        for s in sessions[-30:]:
            if s.started_at:
                h = datetime.fromtimestamp(s.started_at).hour
                hour_counts[h] += 1

        total = sum(hour_counts)
        if total == 0:
            return self.poll_interval_offline

        # 检查当前时间 ±1 小时窗口内的命中概率
        window = [(now_hour - 1) % 24, now_hour, (now_hour + 1) % 24]
        window_hits = sum(hour_counts[h] for h in window)
        probability = window_hits / total

        if probability >= 0.25:
            return self.poll_interval_offline  # 高概率时段，高频检测
        return 300.0  # 低概率时段，5分钟一次

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
