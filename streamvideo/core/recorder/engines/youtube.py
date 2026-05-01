import json
import logging

from streamvideo.core.recorder.models import *
from streamvideo.core.recorder.base import BaseLiveRecorder

logger = logging.getLogger("recorder")


class YouTubeRecorder(BaseLiveRecorder):
    platform = "youtube"

    def __init__(self, identifier: str, output_dir: str,
                 proxy: str = "", on_state_change=None):
        super().__init__(identifier, output_dir, proxy, on_state_change)
        self.info.platform = "youtube"
        self.info.live_url = identifier

    async def check_status(self) -> tuple[ModelStatus, Optional[int], int]:
        cmd = ["streamlink", "--json", "--retry-open", "2"]
        if self.proxy:
            cmd += ["--http-proxy", self.proxy]
        cmd.append(self._get_stream_url())
        rc, stdout, _ = await self._run_cmd(cmd, timeout=15)
        if rc == 0 and stdout.strip():
            try:
                data = json.loads(stdout)
                if data.get("streams"):
                    author = data.get("metadata", {}).get("author", "")
                    if author and self.info.username.startswith("YT_"):
                        self.info.username = author
                    return ModelStatus.PUBLIC, None, 0
            except (json.JSONDecodeError, ValueError):
                pass
        return ModelStatus.OFFLINE, None, 0
