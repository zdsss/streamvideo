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


class WebhookNotifier:
    """异步 Webhook 通知引擎"""

    def __init__(self):
        self.webhooks: list[dict] = []  # [{"type":"generic|discord|telegram", "url":"...", "events":[...]}]
        self._http_session: Optional[aiohttp.ClientSession] = None

    def _get_http_session(self) -> aiohttp.ClientSession:
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
            )
        return self._http_session

    async def notify(self, event: str, data: dict):
        """发送通知到所有匹配的 webhook（带重试）"""
        if not self.webhooks:
            return
        for wh in self.webhooks:
            if event not in wh.get("events", []):
                continue
            for attempt in range(3):
                try:
                    await self._send(wh, event, data)
                    break
                except Exception as e:
                    if attempt < 2:
                        await asyncio.sleep(2 ** (attempt + 1))
                    else:
                        logger.warning(f"Webhook failed after 3 attempts ({wh.get('type')}): {e}")

    async def _send(self, wh: dict, event: str, data: dict):
        wh_type = wh.get("type", "generic")
        url = wh.get("url", "")

        session = self._get_http_session()
        if wh_type == "discord":
            if not url:
                return
            payload = self._format_discord(event, data)
            async with session.post(url, json=payload) as resp:
                if resp.status >= 400:
                    logger.warning(f"Webhook discord returned {resp.status}")
        elif wh_type == "telegram":
            await self._send_telegram(session, wh, event, data)
        else:
            # Generic webhook: POST event+data as JSON
            if not url:
                return
            payload = {"event": event, "data": data, "timestamp": time.time()}
            async with session.post(url, json=payload) as resp:
                if resp.status >= 400:
                    logger.warning(f"Webhook generic returned {resp.status}")

    def _format_discord(self, event: str, data: dict) -> dict:
        titles = {
            "recording_start": "🔴 开始录制",
            "recording_end": "⏹ 录制结束",
            "merge_done": "✅ 合并完成",
            "error": "❌ 错误",
            "disk_low": "⚠️ 磁盘空间不足",
        }
        desc_parts = []
        if data.get("username"):
            desc_parts.append(f"**主播**: {data['username']}")
        if data.get("filename"):
            desc_parts.append(f"**文件**: {data['filename']}")
        if data.get("size"):
            desc_parts.append(f"**大小**: {data['size']}")
        if data.get("message"):
            desc_parts.append(data["message"])

        return {"embeds": [{
            "title": titles.get(event, event),
            "description": "\n".join(desc_parts) or event,
            "color": {"recording_start": 0xe17055, "recording_end": 0x636e72,
                       "merge_done": 0x00b894, "error": 0xe17055, "disk_low": 0xfdcb6e}.get(event, 0x6c5ce7),
        }]}

    async def _send_telegram(self, session, wh: dict, event: str, data: dict):
        bot_token = wh.get("bot_token", "")
        chat_id = wh.get("chat_id", "")
        if not bot_token or not chat_id:
            return
        titles = {"recording_start": "🔴 开始录制", "recording_end": "⏹ 录制结束",
                  "merge_done": "✅ 合并完成", "error": "❌ 错误", "disk_low": "⚠️ 磁盘不足"}
        text = f"*{titles.get(event, event)}*"
        if data.get("username"):
            text += f"\n主播: {data['username']}"
        if data.get("filename"):
            text += f"\n文件: {data['filename']}"
        if data.get("message"):
            text += f"\n{data['message']}"

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        async with session.post(url, json={
            "chat_id": chat_id, "text": text, "parse_mode": "Markdown",
        }, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status >= 400:
                logger.warning(f"Telegram webhook returned {resp.status}")

    async def test(self, wh: dict) -> bool:
        """测试 webhook 连通性（含类型字段验证）"""
        wh_type = wh.get("type", "discord")
        if wh_type == "telegram" and (not wh.get("bot_token") or not wh.get("chat_id")):
            raise ValueError("Telegram 需要 bot_token 和 chat_id")
        if wh_type == "discord" and not wh.get("url"):
            raise ValueError("Discord 需要 URL")
        if wh_type == "generic" and not wh.get("url"):
            raise ValueError("Generic webhook 需要 URL")
        try:
            await self._send(wh, "test", {"message": "StreamVideo webhook test"})
            return True
        except Exception:
            return False

    async def close(self):
        """Close the shared HTTP session"""
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
            self._http_session = None


# ========== 定时录制 ==========

def check_schedule(schedule: Optional[dict]) -> bool:
    """检查当前时间是否在定时计划内"""
    if not schedule or not schedule.get("enabled"):
        return True  # 无计划 = 始终允许
    now = datetime.now()
    weekday = now.weekday()  # 0=Monday
    if weekday not in schedule.get("days", [0, 1, 2, 3, 4, 5, 6]):
        return False
    start_str = schedule.get("start", "00:00")
    end_str = schedule.get("end", "23:59")
    try:
        start_h, start_m = map(int, start_str.split(":"))
        end_h, end_m = map(int, end_str.split(":"))
    except (ValueError, AttributeError):
        logger.warning(f"Invalid schedule time format: start={start_str}, end={end_str}")
        return False
    start_min = start_h * 60 + start_m
    end_min = end_h * 60 + end_m
    now_min = now.hour * 60 + now.minute
    if start_min <= end_min:
        return start_min <= now_min <= end_min
    else:  # 跨午夜，如 20:00 - 02:00
        return now_min >= start_min or now_min <= end_min
