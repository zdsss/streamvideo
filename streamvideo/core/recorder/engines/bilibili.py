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
                    logger.debug("suppressed exception", exc_info=True)

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
