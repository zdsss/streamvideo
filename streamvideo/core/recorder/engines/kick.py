import logging

from streamvideo.core.recorder.models import *
from streamvideo.core.recorder.base import BaseLiveRecorder

logger = logging.getLogger("recorder")


class KickRecorder(BaseLiveRecorder):
    """Kick 直播录制 — streamlink"""
    platform = "kick"

    def __init__(self, identifier: str, output_dir: str, proxy: str = "", on_state_change=None):
        super().__init__(identifier, output_dir, proxy, on_state_change)
        self.info.platform = "kick"
        self.info.live_url = identifier

    async def check_status(self) -> tuple[ModelStatus, Optional[int], int]:
        return await self._check_status_streamlink()
