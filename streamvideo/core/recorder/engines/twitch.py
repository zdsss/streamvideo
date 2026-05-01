import logging

from streamvideo.core.recorder.models import *
from streamvideo.core.recorder.base import BaseLiveRecorder

logger = logging.getLogger("recorder")


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
        return await self._check_status_streamlink()
