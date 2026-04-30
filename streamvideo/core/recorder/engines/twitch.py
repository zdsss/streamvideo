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
