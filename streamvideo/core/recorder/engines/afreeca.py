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

from streamvideo.core.recorder.models import *
from streamvideo.core.recorder.base import BaseLiveRecorder


class AfreecaTVRecorder(BaseLiveRecorder):
    """AfreecaTV / Soop 直播录制 — streamlink + yt-dlp fallback"""
    platform = "afreeca"

    def __init__(self, identifier: str, output_dir: str, proxy: str = "", on_state_change=None):
        super().__init__(identifier, output_dir, proxy, on_state_change)
        self.info.platform = "afreeca"
        self.info.live_url = identifier

    def _get_stream_url(self) -> str:
        return self.identifier

    async def check_status(self) -> tuple[ModelStatus, Optional[int], int]:
        # Strategy 1: streamlink (native AfreecaTV support)
        cmd = ["streamlink", "--json", "--retry-open", "2"]
        if self.proxy:
            cmd += ["--http-proxy", self.proxy]
        cmd.append(self._get_stream_url())
        rc, stdout, _ = await self._run_cmd(cmd, timeout=15)
        if rc == 0 and stdout.strip():
            try:
                data = json.loads(stdout)
                if data.get("streams"):
                    return ModelStatus.PUBLIC, None, 0
            except (json.JSONDecodeError, ValueError):
                pass

        # Strategy 2: yt-dlp fallback
        if self._manager and self._manager._ytdlp_available:
            cmd = ["yt-dlp", "--dump-json", "--no-download"]
            if self.proxy:
                cmd += ["--proxy", self.proxy]
            cmd.append(self._get_stream_url())
            rc, stdout, _ = await self._run_cmd(cmd, timeout=15)
            if rc == 0 and stdout.strip():
                try:
                    data = json.loads(stdout)
                    viewers = data.get("view_count") or data.get("concurrent_view_count") or 0
                    return ModelStatus.PUBLIC, int(viewers) if viewers else None, 0
                except (json.JSONDecodeError, ValueError):
                    pass

        return ModelStatus.OFFLINE, None, 0

    async def _do_record(self, output_path: str) -> bool:
        # Strategy 1: streamlink
        result = await self._record_with_streamlink(
            output_path, self._get_stream_url(), quality=self.quality
        )
        if result:
            return True
        logger.info(f"[{self.info.username}] streamlink failed, trying yt-dlp")

        # Strategy 2: yt-dlp
        if self._manager and self._manager._ytdlp_available:
            rc = await self._try_ytdlp_record(output_path)
            if rc:
                return True

        logger.warning(f"[{self.info.username}] All recording methods failed")
        self._last_stop_reason = "process_exit_error"
        return False

