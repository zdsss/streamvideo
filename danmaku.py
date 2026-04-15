"""
抖音弹幕抓取引擎
- WebSocket 连接抖音弹幕服务
- HTTP 轮询降级
- 独立 asyncio.Task 运行，不阻塞录制
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import aiohttp

logger = logging.getLogger("danmaku")


@dataclass
class DanmakuMessage:
    """单条弹幕"""
    timestamp: float = 0       # 相对录制开始的秒数
    type: str = "chat"         # chat | gift | like | viewer_count | system
    user: str = ""
    content: str = ""
    extra: dict = field(default_factory=dict)


class DanmakuCapture:
    """录制期间抓取弹幕数据"""

    def __init__(self, room_id: str, username: str, output_dir: Path,
                 ttwid: str = "", session_id: str = ""):
        self.room_id = room_id
        self.username = username
        self.output_dir = output_dir
        self.ttwid = ttwid
        self.session_id = session_id
        self._messages: list[DanmakuMessage] = []
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._recording_start: float = 0
        self._http_session: Optional[aiohttp.ClientSession] = None
        self._cursor = "0"
        self._internal_ext = ""
        self._video_start_offset: float = 0  # 视频第一帧相对弹幕开始的偏移
        self._playwright_page = None  # 外部传入的 Playwright page（复用录制浏览器）
        self.user_agent = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )

    async def start(self, recording_start_time: float):
        """开始抓取弹幕"""
        self._recording_start = recording_start_time
        self._stop_event.clear()
        self._messages = []
        self._task = asyncio.create_task(self._capture_loop())
        logger.info(f"[{self.username}] Danmaku capture started for room {self.room_id}")

    async def stop(self) -> Optional[Path]:
        """停止抓取，写入 JSON，返回文件路径"""
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
        if not self._messages:
            logger.info(f"[{self.username}] No danmaku captured")
            return None
        path = self._flush_to_json()
        logger.info(f"[{self.username}] Danmaku capture stopped: {len(self._messages)} messages → {path.name}")
        return path

    def set_video_start_offset(self, offset: float):
        """设置视频开始偏移（由录制器在视频第一帧写入时调用）"""
        self._video_start_offset = offset

    async def _capture_loop(self):
        """主抓取循环：Playwright 拦截 → HTTP 轮询降级"""
        try:
            # 方案1: Playwright WebSocket 拦截（如果有 page 实例）
            if self._playwright_page:
                try:
                    await self._playwright_capture()
                    return
                except Exception as e:
                    logger.warning(f"[{self.username}] Playwright danmaku failed: {e}, falling back to HTTP")

            # 方案2: 独立 Playwright 实例拦截弹幕
            try:
                await self._standalone_playwright_capture()
                return
            except ImportError:
                logger.info(f"[{self.username}] Playwright not available, using HTTP polling")
            except Exception as e:
                logger.warning(f"[{self.username}] Standalone Playwright danmaku failed: {e}, falling back to HTTP")

            # 方案3: HTTP 轮询降级
            await self._poll_loop()
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.warning(f"[{self.username}] Danmaku capture error: {e}")

    async def _standalone_playwright_capture(self):
        """独立 Playwright 实例，打开直播页面拦截弹幕 WebSocket"""
        from playwright.async_api import async_playwright

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=self.user_agent)
            page = await context.new_page()

            # 拦截 WebSocket 消息
            def on_websocket(ws):
                if "webcast" in ws.url:
                    ws.on("framereceived", lambda payload: self._parse_ws_frame(payload))
                    logger.info(f"[{self.username}] Intercepted danmaku WebSocket: {ws.url[:80]}")

            page.on("websocket", on_websocket)

            # 设置 cookie
            if self.ttwid:
                await context.add_cookies([{
                    "name": "ttwid", "value": self.ttwid,
                    "domain": ".douyin.com", "path": "/",
                }])

            try:
                await page.goto(f"https://live.douyin.com/{self.room_id}", timeout=20000)
            except Exception:
                pass  # 页面加载超时没关系，WebSocket 可能已连接

            # 保持页面打开直到停止
            while not self._stop_event.is_set():
                await self._sleep(2)

            await browser.close()

    async def _playwright_capture(self):
        """复用外部 Playwright page 拦截弹幕"""
        page = self._playwright_page

        def on_websocket(ws):
            if "webcast" in ws.url:
                ws.on("framereceived", lambda payload: self._parse_ws_frame(payload))
                logger.info(f"[{self.username}] Intercepted danmaku WebSocket (shared): {ws.url[:80]}")

        page.on("websocket", on_websocket)

        while not self._stop_event.is_set():
            await self._sleep(2)

    def _parse_ws_frame(self, payload):
        """解析 WebSocket 帧中的弹幕数据"""
        now_offset = time.time() - self._recording_start
        try:
            # 抖音 WebSocket 帧可能是 protobuf 或 JSON
            data = payload if isinstance(payload, (str, bytes)) else str(payload)
            if isinstance(data, bytes):
                try:
                    data = data.decode("utf-8", errors="ignore")
                except Exception:
                    return

            # 尝试 JSON 解析
            if data.startswith("{") or data.startswith("["):
                try:
                    import json as _json
                    parsed = _json.loads(data)
                    if isinstance(parsed, list):
                        for msg in parsed:
                            self._extract_message(msg, now_offset)
                    elif isinstance(parsed, dict):
                        self._extract_message(parsed, now_offset)
                except Exception:
                    pass
            else:
                # 非 JSON 数据（可能是 protobuf），尝试提取可读文本
                # 简单启发式：查找中文字符序列作为弹幕内容
                import re
                chinese_texts = re.findall(r'[\u4e00-\u9fff\u3000-\u303f\uff00-\uffef]{2,}', data)
                for text in chinese_texts[:5]:  # 限制每帧最多 5 条
                    if len(text) >= 2 and len(text) <= 50:
                        self._messages.append(DanmakuMessage(
                            timestamp=now_offset, type="chat", content=text,
                        ))
        except Exception:
            pass

    def _extract_message(self, msg: dict, now_offset: float):
        """从 JSON 消息中提取弹幕"""
        method = msg.get("method", "") or msg.get("type", "")
        payload = msg.get("payload", msg.get("data", msg))

        if "Chat" in method or "chat" in method:
            self._messages.append(DanmakuMessage(
                timestamp=now_offset, type="chat",
                user=self._get_nickname(payload),
                content=payload.get("content", "") or payload.get("text", ""),
            ))
        elif "Gift" in method or "gift" in method:
            self._messages.append(DanmakuMessage(
                timestamp=now_offset, type="gift",
                user=self._get_nickname(payload),
                content=payload.get("gift", {}).get("name", "礼物") if isinstance(payload.get("gift"), dict) else "礼物",
            ))
        elif "Like" in method or "like" in method:
            self._messages.append(DanmakuMessage(
                timestamp=now_offset, type="like",
                user=self._get_nickname(payload), content="点赞",
            ))

    @staticmethod
    def _get_nickname(payload: dict) -> str:
        """从各种嵌套结构中提取用户昵称"""
        user = payload.get("user", payload.get("sender", {}))
        if isinstance(user, dict):
            return user.get("nickname", "") or user.get("name", "") or user.get("nick", "")
        return ""

    async def _poll_loop(self):
        """HTTP 轮询抓取弹幕（2s 间隔）"""
        self._http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10),
            headers={"User-Agent": self.user_agent},
        )
        url = "https://live.douyin.com/webcast/im/fetch/"
        params = {
            "aid": "6383",
            "app_name": "douyin_web",
            "live_id": "1",
            "device_platform": "web",
            "language": "zh-CN",
            "browser_language": "zh-CN",
            "browser_platform": "MacIntel",
            "browser_name": "Chrome",
            "browser_version": "120",
            "room_id": self.room_id,
            "cursor": self._cursor,
            "internal_ext": self._internal_ext,
        }
        cookies = {"ttwid": self.ttwid} if self.ttwid else {}
        headers = {
            "Referer": f"https://live.douyin.com/{self.room_id}",
        }
        consecutive_fails = 0

        while not self._stop_event.is_set():
            try:
                params["cursor"] = self._cursor
                params["internal_ext"] = self._internal_ext
                async with self._http_session.get(
                    url, params=params, cookies=cookies, headers=headers
                ) as resp:
                    if resp.status != 200:
                        consecutive_fails += 1
                        if consecutive_fails > 10:
                            logger.warning(f"[{self.username}] Danmaku polling failed {consecutive_fails} times, stopping")
                            return
                        await self._sleep(5)
                        continue
                    data = await resp.json()
                    consecutive_fails = 0

                # 解析消息
                self._parse_im_response(data)

                # 更新游标
                self._cursor = str(data.get("cursor", self._cursor))
                self._internal_ext = data.get("internal_ext", self._internal_ext)

            except asyncio.CancelledError:
                return
            except Exception as e:
                consecutive_fails += 1
                if consecutive_fails > 10:
                    logger.warning(f"[{self.username}] Danmaku polling error: {e}, stopping")
                    return
                logger.debug(f"[{self.username}] Danmaku poll error: {e}")

            await self._sleep(2)

    def _parse_im_response(self, data: dict):
        """解析抖音 IM 响应中的弹幕消息"""
        now_offset = time.time() - self._recording_start
        messages = data.get("data", [])
        if not isinstance(messages, list):
            messages = []

        for msg in messages:
            msg_type = msg.get("method", "")
            payload = msg.get("payload", {})

            if msg_type == "WebcastChatMessage":
                self._messages.append(DanmakuMessage(
                    timestamp=now_offset,
                    type="chat",
                    user=payload.get("user", {}).get("nickname", ""),
                    content=payload.get("content", ""),
                ))
            elif msg_type == "WebcastGiftMessage":
                gift = payload.get("gift", {})
                self._messages.append(DanmakuMessage(
                    timestamp=now_offset,
                    type="gift",
                    user=payload.get("user", {}).get("nickname", ""),
                    content=gift.get("name", "礼物"),
                    extra={"gift_id": gift.get("id", 0), "count": payload.get("repeatCount", 1)},
                ))
            elif msg_type == "WebcastLikeMessage":
                self._messages.append(DanmakuMessage(
                    timestamp=now_offset,
                    type="like",
                    user=payload.get("user", {}).get("nickname", ""),
                    content="点赞",
                    extra={"count": payload.get("count", 1)},
                ))
            elif msg_type == "WebcastMemberMessage":
                self._messages.append(DanmakuMessage(
                    timestamp=now_offset,
                    type="system",
                    user=payload.get("user", {}).get("nickname", ""),
                    content="进入直播间",
                ))
            elif msg_type == "WebcastRoomStatsMessage":
                self._messages.append(DanmakuMessage(
                    timestamp=now_offset,
                    type="viewer_count",
                    content=str(payload.get("displayLong", "")),
                    extra={"viewer_count": payload.get("displayLong", 0)},
                ))

    def _flush_to_json(self) -> Path:
        """写入弹幕 JSON 文件"""
        model_dir = self.output_dir / self.username
        model_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{self.session_id}_danmaku.json" if self.session_id else f"danmaku_{int(time.time())}.json"
        path = model_dir / filename
        data = {
            "room_id": self.room_id,
            "username": self.username,
            "session_id": self.session_id,
            "recording_start": self._recording_start,
            "video_start_offset": self._video_start_offset,
            "capture_duration": time.time() - self._recording_start,
            "message_count": len(self._messages),
            "stats": self.get_stats(),
            "messages": [
                {
                    "t": round(m.timestamp, 1),
                    "type": m.type,
                    "user": m.user,
                    "content": m.content,
                    **({"extra": m.extra} if m.extra else {}),
                }
                for m in self._messages
            ],
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=1)
        return path

    def get_stats(self) -> dict:
        """获取弹幕统计"""
        if not self._messages:
            return {"total": 0, "chat": 0, "gift": 0, "peak_density": 0}
        chat_count = sum(1 for m in self._messages if m.type == "chat")
        gift_count = sum(1 for m in self._messages if m.type == "gift")
        peak = self.get_peak_density()
        return {
            "total": len(self._messages),
            "chat": chat_count,
            "gift": gift_count,
            "peak_density": round(peak, 2),
        }

    def get_peak_density(self, window: int = 10) -> float:
        """计算弹幕峰值密度（消息/秒）"""
        if not self._messages:
            return 0
        chat_msgs = [m for m in self._messages if m.type == "chat"]
        if not chat_msgs:
            return 0
        max_density = 0
        for i, msg in enumerate(chat_msgs):
            t_start = msg.timestamp
            t_end = t_start + window
            count = sum(1 for m in chat_msgs[i:] if m.timestamp <= t_end)
            density = count / window
            max_density = max(max_density, density)
        return max_density

    def get_density_timeline(self, window: int = 10) -> list[dict]:
        """计算弹幕密度时间线（用于可视化）"""
        if not self._messages:
            return []
        chat_msgs = [m for m in self._messages if m.type == "chat"]
        if not chat_msgs:
            return []
        max_t = max(m.timestamp for m in chat_msgs)
        timeline = []
        for t in range(0, int(max_t) + 1, window):
            count = sum(1 for m in chat_msgs if t <= m.timestamp < t + window)
            timeline.append({"t": t, "density": round(count / window, 2), "count": count})
        return timeline

    def find_keyword_matches(self, keywords: list[str]) -> list[dict]:
        """查找匹配关键词的弹幕"""
        if not keywords:
            return []
        results = []
        for m in self._messages:
            if m.type != "chat":
                continue
            for kw in keywords:
                if kw in m.content:
                    results.append({
                        "timestamp": m.timestamp,
                        "keyword": kw,
                        "user": m.user,
                        "content": m.content,
                    })
                    break
        return results

    async def _sleep(self, seconds: float):
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass
