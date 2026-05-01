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

class DouyinRecorder(BaseLiveRecorder):
    platform = "douyin"

    def __init__(self, identifier: str, output_dir: str,
                 proxy: str = "", on_state_change=None):
        # 抖音不需要代理
        super().__init__(identifier, output_dir, proxy="", on_state_change=on_state_change)
        self.room_id = identifier
        self.info.platform = "douyin"
        self.info.live_url = f"https://live.douyin.com/{identifier}"
        self._streamer_name = None
        self._ttwid = ""
        self._ttwid_time = 0
        # 抖音 CDN 较慢，放宽断流检测
        self.stall_timeout = 30
        self.grace_period = 90
        # 抖音弹幕默认开启（WebSocket 协议稳定）
        self._danmaku_enabled = True
        self._cached_stream_url = ""  # API 返回的流地址缓存

    def _get_stream_url(self) -> str:
        return f"https://live.douyin.com/{self.room_id}"

    async def _get_ttwid(self) -> str:
        """获取抖音 ttwid cookie（缓存 1 小时）"""
        if self._ttwid and (time.time() - self._ttwid_time) < 3600:
            return self._ttwid
        try:
            session = self._get_http_session()
            async with session.get(
                "https://live.douyin.com/",
                headers={"Accept-Encoding": "gzip, deflate"},
                timeout=aiohttp.ClientTimeout(total=10),
                allow_redirects=True,
            ) as resp:
                    ttwid = ""
                    # 从 Set-Cookie header 提取
                    for h in resp.headers.getall("Set-Cookie", []):
                        if "ttwid=" in h:
                            ttwid = h.split("ttwid=")[1].split(";")[0]
                            break
                    if not ttwid:
                        cookies = resp.cookies
                        for cookie in cookies.values():
                            if cookie.key == "ttwid":
                                ttwid = cookie.value
                                break
                    if ttwid:
                        first_time = not self._ttwid
                        self._ttwid = ttwid
                        self._ttwid_time = time.time()
                        if first_time:
                            logger.info(f"[{self.info.username}] ttwid obtained successfully")
                        return ttwid
        except Exception as e:
            logger.warning(f"[{self.info.username}] Failed to get ttwid: {e}")
        return self._ttwid

    async def check_status(self) -> tuple[ModelStatus, Optional[int], int]:
        """用抖音 webcast API 检测直播状态"""
        try:
            ttwid = await self._get_ttwid()
            url = (
                f"https://live.douyin.com/webcast/room/web/enter/"
                f"?aid=6383&app_name=douyin_web&live_id=1"
                f"&device_platform=web&language=zh-CN"
                f"&browser_language=zh-CN&browser_platform=MacIntel"
                f"&browser_name=Chrome&browser_version=120"
                f"&web_rid={self.room_id}"
            )
            headers = {
                "User-Agent": self.user_agent,
                "Referer": f"https://live.douyin.com/{self.room_id}",
                "Accept-Encoding": "gzip, deflate",
            }
            cookies = {"ttwid": ttwid} if ttwid else {}

            session = self._get_http_session()
            async with session.get(
                url, headers=headers, cookies=cookies,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"[{self.info.username}] Douyin API returned {resp.status}")
                    return await self._check_status_fallback()
                data = await resp.json()

            if data.get("status_code") != 0:
                logger.warning(f"[{self.info.username}] Douyin API error: {data.get('status_code')}")
                return await self._check_status_fallback()

            try:
                room_data = data.get("data", {})
                if not isinstance(room_data, dict):
                    return await self._check_status_fallback()

                # 提取主播信息（顶层 user）
                user = room_data.get("user", {})
                if isinstance(user, dict):
                    nickname = user.get("nickname", "")
                    if nickname and not self._streamer_name:
                        self._streamer_name = nickname
                        self.info.username = nickname
                        logger.info(f"[{self.info.username}] Douyin streamer: {nickname}")
                        self._save_meta()
                    # 提取头像
                    avatar = user.get("avatar_thumb", {})
                    if isinstance(avatar, dict):
                        url_list = avatar.get("url_list", [])
                        if url_list and not self.info.thumbnail_url:
                            self.info.thumbnail_url = url_list[0]

                # 检测在线状态 — 多种方式兼容不同 API 版本
                is_live = False
                viewers = 0

                # 方式1: data.data[0].status == 2 (新版 API)
                inner_rooms = room_data.get("data", [])
                if isinstance(inner_rooms, list) and inner_rooms:
                    room = inner_rooms[0] if isinstance(inner_rooms[0], dict) else {}
                    room_status = room.get("status", 0)
                    is_live = room_status == 2
                    # 提取观众数
                    viewers_str = room.get("user_count_str", "0")
                    try:
                        viewers = int(str(viewers_str).replace("万", "0000").replace("+", "").replace("w", "0000"))
                    except (ValueError, TypeError):
                        pass
                    # 缓存流地址（如果 API 返回了）
                    stream_url_data = room.get("stream_url", {})
                    if isinstance(stream_url_data, dict):
                        flv_url = stream_url_data.get("flv_pull_url", {})
                        hls_url = stream_url_data.get("hls_pull_url_map", {})
                        if isinstance(flv_url, dict) and flv_url:
                            # 取最高画质
                            self._cached_stream_url = list(flv_url.values())[-1]
                        elif isinstance(hls_url, dict) and hls_url:
                            self._cached_stream_url = list(hls_url.values())[-1]

                # 方式2: 顶层 room_status (旧版 API)
                if not is_live:
                    top_status = room_data.get("room_status", 0)
                    if top_status == 1:
                        is_live = True

                # 方式3: web_stream_url 存在说明在线
                if not is_live:
                    web_stream = room_data.get("web_stream_url")
                    if web_stream:
                        is_live = True
                        self._cached_stream_url = web_stream

                if is_live:
                    return ModelStatus.PUBLIC, int(self.room_id), viewers
                return ModelStatus.OFFLINE, int(self.room_id), 0
            except (KeyError, TypeError, AttributeError) as e:
                logger.warning(f"[{self.info.username}] Douyin API response parse error: {e}")
                return await self._check_status_fallback()

        except Exception as e:
            logger.warning(f"[{self.info.username}] Douyin API check error: {e}")
            return await self._check_status_fallback()

    async def _check_status_fallback(self) -> tuple[ModelStatus, Optional[int], int]:
        """Fallback: 用 streamlink 检测"""
        try:
            proc = await asyncio.create_subprocess_exec(
                "streamlink", "--json", "--retry-open", "2", self._get_stream_url(),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
            data = json.loads(stdout.decode())
            if data.get("streams"):
                author = data.get("metadata", {}).get("author", "")
                if author and not self._streamer_name:
                    self._streamer_name = author
                    self.info.username = author
                    self._save_meta()
                return ModelStatus.PUBLIC, int(self.room_id), 0
            return ModelStatus.OFFLINE, int(self.room_id), 0
        except Exception as e:
            logger.debug(f"[{self.info.username}] Douyin streamlink fallback error: {e}")
            return ModelStatus.UNKNOWN, None, 0

    async def _do_record(self, output_path: str) -> bool:
        """录制抖音直播：API流地址 > 自定义流地址 > streamlink > Playwright+ffmpeg"""
        # 方案0: 用户提供了自定义流地址
        if self.custom_stream_url:
            logger.info(f"[{self.info.username}] Using custom stream URL")
            return await self._record_with_ffmpeg(output_path, self.custom_stream_url)

        # 方案1: API 返回的流地址（最可靠）
        if self._cached_stream_url:
            logger.info(f"[{self.info.username}] Using API stream URL: {self._cached_stream_url[:80]}...")
            result = await self._record_with_ffmpeg(output_path, self._cached_stream_url)
            if result:
                return True
            logger.info(f"[{self.info.username}] API stream URL failed, trying streamlink")
            self._cached_stream_url = ""  # 清除失效的缓存

        q = self.quality if self.quality != "best" else "origin"

        # 方案2: streamlink（传入 ttwid + 用户 cookie）
        ttwid = await self._get_ttwid()
        extra_args = []
        cookie_str = f"ttwid={ttwid}" if ttwid else ""
        if self.custom_cookies:
            cookie_str = self.custom_cookies if not cookie_str else f"{cookie_str}; {self.custom_cookies}"
        if cookie_str:
            extra_args = ["--http-cookie", cookie_str]

        cmd = ["streamlink", "--hls-live-edge", "6", "--stream-segment-attempts", "3",
               "--retry-open", "3", "--ringbuffer-size", "32M"]
        cmd += extra_args
        cmd += [self._get_stream_url(), q, "-o", output_path]

        self._active_proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        logger.info(f"[{self.info.username}] streamlink started (pid={self._active_proc.pid})")
        await asyncio.sleep(8)
        if self._active_proc.returncode is not None:
            self._active_proc = None
            logger.info(f"[{self.info.username}] streamlink failed, trying Playwright + ffmpeg")

            # 方案3: Playwright 提取流地址 + ffmpeg
            stream_url = await self._get_stream_url_via_playwright()
            if stream_url:
                return await self._record_with_ffmpeg(output_path, stream_url)

            logger.warning(f"[{self.info.username}] All recording methods failed, cooling down 60s")
            self._last_stop_reason = "process_exit_error"
            await self._sleep(60)
            return False

        return await self._monitor_streamlink(output_path)

    async def _monitor_streamlink(self, output_path: str) -> bool:
        """监控 streamlink 录制进程的文件增长"""
        last_size = 0
        stall_count = 0
        last_bw_time = time.time()
        last_bw_size = 0
        while not self._stop_event.is_set() and self._recording_active:
            await self._sleep(self.stall_check_interval)
            if self._active_proc.returncode is not None:
                break
            if os.path.exists(output_path) and self.info.current_recording:
                current_size = os.path.getsize(output_path)
                self.info.current_recording.file_size = current_size
                self.info.current_recording.duration = time.time() - self.info.current_recording.start_time
                now = time.time()
                dt = now - last_bw_time
                if dt > 0:
                    self.info.current_recording.bandwidth_kbps = max(0, (current_size - last_bw_size) * 8 / dt / 1000)
                    last_bw_time = now
                    last_bw_size = current_size
                await self._notify()
                if current_size > 0 and current_size == last_size:
                    stall_count += 1
                    if stall_count * self.stall_check_interval >= self.stall_timeout:
                        logger.warning(f"[{self.info.username}] streamlink stalled")
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
        # 记录停止原因
        if not self._last_stop_reason:
            if self._stop_event.is_set():
                self._last_stop_reason = "user_stop"
            elif self._active_proc and getattr(self._active_proc, 'returncode', None) == 0:
                self._last_stop_reason = "process_exit_0"
            else:
                self._last_stop_reason = "process_exit_error"
        self._active_proc = None
        return os.path.exists(output_path) and os.path.getsize(output_path) > 100_000

    async def _get_stream_url_via_playwright(self) -> Optional[str]:
        """用 Playwright 打开抖音页面，拦截流地址"""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.warning(f"[{self.info.username}] Playwright not installed")
            return None

        async def _run() -> Optional[str]:
            stream_url = None
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                try:
                    context = await browser.new_context(user_agent=self.user_agent)
                    page = await context.new_page()

                    async def on_response(response):
                        nonlocal stream_url
                        url = response.url
                        if stream_url:
                            return
                        if ".flv" in url and "pull" in url:
                            stream_url = url
                            logger.info(f"[{self.info.username}] Found FLV stream: {url[:80]}...")
                        elif ".m3u8" in url and "pull" in url:
                            stream_url = url
                            logger.info(f"[{self.info.username}] Found HLS stream: {url[:80]}...")

                    page.on("response", on_response)

                    try:
                        await page.goto(self._get_stream_url(), timeout=15000)
                    except Exception:
                        logger.debug("suppressed exception", exc_info=True)

                    # 等待流地址出现（最多 15 秒）
                    for _ in range(30):
                        if stream_url or self._stop_event.is_set():
                            break
                        await asyncio.sleep(0.5)
                finally:
                    await browser.close()
            return stream_url

        try:
            return await asyncio.wait_for(_run(), timeout=45)
        except asyncio.TimeoutError:
            logger.warning(f"[{self.info.username}] Playwright timed out after 45s")
            return None
        except Exception as e:
            logger.warning(f"[{self.info.username}] Playwright error: {e}")
            return None

    async def _record_with_ffmpeg(self, output_path: str, stream_url: str) -> bool:
        """用 ffmpeg 直接录制流"""
        cmd = [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
            "-reconnect", "1", "-reconnect_streamed", "1",
            "-reconnect_on_network_error", "1",
            "-reconnect_on_http_error", "4xx,5xx",
            "-reconnect_delay_max", "30", "-reconnect_max_retries", "10",
            "-thread_queue_size", "1024", "-probesize", "32",
            "-analyzeduration", "0", "-max_muxing_queue_size", "1024",
            "-headers", f"User-Agent: {self.user_agent}\r\nReferer: https://live.douyin.com/\r\n",
            "-i", stream_url,
            "-c", "copy", "-movflags", "+faststart",
            output_path,
        ]
        self._active_proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        logger.info(f"[{self.info.username}] ffmpeg recording started (pid={self._active_proc.pid})")

        last_size = 0
        stall_count = 0
        last_bw_time = time.time()
        last_bw_size = 0
        while not self._stop_event.is_set() and self._recording_active:
            await self._sleep(self.stall_check_interval)
            if self._active_proc.returncode is not None:
                break
            if os.path.exists(output_path) and self.info.current_recording:
                current_size = os.path.getsize(output_path)
                self.info.current_recording.file_size = current_size
                self.info.current_recording.duration = time.time() - self.info.current_recording.start_time
                now = time.time()
                dt = now - last_bw_time
                if dt > 0:
                    self.info.current_recording.bandwidth_kbps = max(0, (current_size - last_bw_size) * 8 / dt / 1000)
                    last_bw_time = now
                    last_bw_size = current_size
                await self._notify()
                if current_size > 0 and current_size == last_size:
                    stall_count += 1
                    if stall_count * self.stall_check_interval >= self.stall_timeout:
                        logger.warning(f"[{self.info.username}] ffmpeg stalled")
                        self._last_stop_reason = "stall_timeout"
                        break
                else:
                    stall_count = 0
                last_size = current_size
                # 自动分割检测
                if current_size > 0 and self._should_split(current_size, self.info.current_recording.start_time):
                    logger.info(f"[{self.info.username}] ffmpeg auto-split triggered (size={current_size/1024/1024:.0f}MB)")
                    self._last_stop_reason = "auto_split"
                    break

        if self._active_proc and self._active_proc.returncode is None:
            self._active_proc.terminate()
            try:
                await asyncio.wait_for(self._active_proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                self._active_proc.kill()
        self._active_proc = None
        return os.path.exists(output_path) and os.path.getsize(output_path) > 100_000


# ========== B站直播 ==========
