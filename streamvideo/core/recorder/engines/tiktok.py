import asyncio
import json
import logging
import os
from typing import Optional

import aiohttp

from streamvideo.core.recorder.models import ModelStatus
from streamvideo.core.recorder.base import BaseLiveRecorder

logger = logging.getLogger("recorder")


class TikTokRecorder(BaseLiveRecorder):
    """TikTok Live — streamlink with yt-dlp fallback"""
    platform = "tiktok"

    def __init__(self, identifier: str, output_dir: str, proxy: str = "", on_state_change=None):
        super().__init__(identifier, output_dir, proxy, on_state_change)
        self.info.platform = "tiktok"
        self.info.live_url = identifier
        self._resolved_url: Optional[str] = None

    def _get_stream_url(self) -> str:
        return self._resolved_url or self.identifier

    async def _resolve_share_url(self) -> str:
        """Resolve short URLs (vm.tiktok.com/xxx, vt.tiktok.com/xxx) to full URL via redirect."""
        if "vm.tiktok.com" not in self.identifier and "vt.tiktok.com" not in self.identifier:
            return self.identifier
        try:
            session = self._get_http_session()
            async with session.get(self.identifier, allow_redirects=True) as resp:
                resolved = str(resp.url)
                logger.info(f"[{self.info.username}] Resolved share URL: {self.identifier} -> {resolved}")
                return resolved
        except Exception as e:
            logger.warning(f"[{self.info.username}] Failed to resolve share URL: {e}")
            return self.identifier

    async def check_status(self) -> tuple[ModelStatus, Optional[int], int]:
        # Resolve share URL on first check
        if not self._resolved_url:
            self._resolved_url = await self._resolve_share_url()
            if self._resolved_url != self.identifier:
                self.info.live_url = self._resolved_url

        url = self._get_stream_url()

        # Strategy 1: streamlink
        try:
            cmd = ["streamlink", "--json", "--retry-open", "2", url]
            if self.proxy:
                cmd = ["streamlink", "--json", "--http-proxy", self.proxy, "--retry-open", "2", url]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
            data = json.loads(stdout.decode())
            if data.get("streams"):
                return ModelStatus.PUBLIC, None, 0
            return ModelStatus.OFFLINE, None, 0
        except Exception:
            logger.debug("suppressed exception", exc_info=True)

        # Strategy 2: yt-dlp
        if self._manager and self._manager._ytdlp_available:
            try:
                cmd = ["yt-dlp", "--dump-json", "--no-download", url]
                if self.proxy:
                    cmd = ["yt-dlp", "--proxy", self.proxy, "--dump-json", "--no-download", url]
                proc = await asyncio.create_subprocess_exec(
                    *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
                )
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
                if proc.returncode == 0 and stdout.strip():
                    info = json.loads(stdout.decode())
                    viewers = info.get("view_count") or info.get("concurrent_view_count") or 0
                    is_live = info.get("is_live", False) or info.get("live_status") == "is_live"
                    if is_live or viewers:
                        return ModelStatus.PUBLIC, int(viewers) if viewers else None, 0
                    return ModelStatus.OFFLINE, None, 0
            except Exception:
                logger.debug("suppressed exception", exc_info=True)

        return ModelStatus.OFFLINE, None, 0

    async def _do_record(self, output_path: str) -> bool:
        # Ensure URL is resolved
        if not self._resolved_url:
            self._resolved_url = await self._resolve_share_url()

        url = self._get_stream_url()

        # Strategy 1: streamlink
        result = await self._record_with_streamlink(output_path, url, quality=self.quality)
        if result:
            return True
        logger.info(f"[{self.info.username}] streamlink failed, trying yt-dlp")

        # Strategy 2: yt-dlp
        if self._manager and self._manager._ytdlp_available:
            return await self._try_ytdlp_record(output_path)

        logger.warning(f"[{self.info.username}] All recording methods failed")
        self._last_stop_reason = "process_exit_error"
        return False
