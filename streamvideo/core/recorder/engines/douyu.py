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

class DouyuRecorder(BaseLiveRecorder):
    """斗鱼直播录制 — streamlink"""
    platform = "douyu"

    def __init__(self, identifier: str, output_dir: str, proxy: str = "", on_state_change=None):
        super().__init__(identifier, output_dir, proxy, on_state_change)
        self.info.platform = "douyu"
        self.info.live_url = identifier

    def _get_stream_url(self) -> str:
        return self.identifier

    async def check_status(self) -> tuple[ModelStatus, Optional[int], int]:
        try:
            cmd = ["streamlink", "--json", "--retry-open", "2", self._get_stream_url()]
            if self.proxy:
                cmd = ["streamlink", "--json", "--http-proxy", self.proxy, "--retry-open", "2", self._get_stream_url()]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
            data = json.loads(stdout.decode())
            if data.get("streams"):
                return ModelStatus.PUBLIC, None, 0
            return ModelStatus.OFFLINE, None, 0
        except Exception as e:
            logger.debug(f"[{self.info.username}] Douyu check error: {e}")
            return ModelStatus.UNKNOWN, None, 0

    async def _do_record(self, output_path: str) -> bool:
        return await self._record_with_streamlink(output_path, self._get_stream_url(), quality=self.quality)

