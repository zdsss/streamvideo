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

from streamvideo.core.recorder.base import BaseLiveRecorder

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
            logger.debug("suppressed exception", exc_info=True)

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
                logger.debug("suppressed exception", exc_info=True)

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
