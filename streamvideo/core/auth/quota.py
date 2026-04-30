"""
配额管理系统 — Freemium 模型
free: 3 clips/day + 水印 + 720p 最高
pro: 50 clips/day + 无水印 + 1080p + 优先处理
team: 无限 + 无水印 + 1080p + 多账号 + API 访问
"""

import logging
import time
from datetime import datetime
from typing import Optional

logger = logging.getLogger("quota")

# 套餐定义
TIERS = {
    "free": {
        "name": "免费版",
        "daily_clips": 3,
        "max_resolution": "720p",
        "watermark": True,
        "priority": False,
        "api_access": False,
        "max_models": 3,
        "max_clip_duration": 60,       # 秒
        "cloud_upload": False,
        "h265_transcode": False,
    },
    "pro": {
        "name": "专业版",
        "daily_clips": 50,
        "max_resolution": "1080p",
        "watermark": False,
        "priority": True,
        "api_access": False,
        "max_models": 20,
        "max_clip_duration": 180,
        "cloud_upload": True,
        "h265_transcode": True,
    },
    "team": {
        "name": "团队版",
        "daily_clips": 999999,
        "max_resolution": "1080p",
        "watermark": False,
        "priority": True,
        "api_access": True,
        "max_models": 999999,
        "max_clip_duration": 300,
        "cloud_upload": True,
        "h265_transcode": True,
    },
}


class QuotaManager:
    """配额和用户等级管理"""

    def __init__(self, db):
        self.db = db

    def check_quota(self, username: str) -> tuple[bool, int, int]:
        """检查配额。返回 (allowed, used_today, daily_limit)"""
        tier = self.get_tier(username)
        limit = TIERS.get(tier, TIERS["free"])["daily_clips"]
        today = datetime.now().strftime("%Y-%m-%d")
        used = self._get_today_count(username, today)
        return used < limit, used, limit

    def consume_quota(self, username: str, count: int = 1):
        """消耗配额"""
        today = datetime.now().strftime("%Y-%m-%d")
        conn = self.db._conn()
        try:
            conn.execute("""
                INSERT INTO user_quotas (username, date, clips_generated)
                VALUES (?, ?, ?)
                ON CONFLICT(username, date) DO UPDATE SET
                    clips_generated = clips_generated + ?
            """, (username, today, count, count))
            conn.commit()
        except Exception:
            conn.rollback()
        finally:
            conn.close()

    def get_tier(self, username: str) -> str:
        """获取用户等级，兼顾订阅状态和过期时间"""
        conn = self.db._conn()
        try:
            row = conn.execute(
                "SELECT tier, expires_at FROM user_tiers WHERE username = ?",
                (username,)
            ).fetchone()
            if not row:
                return "free"
            tier = row[0]
            expires = row[1] or 0
            if expires > 0 and time.time() > expires:
                conn.execute("UPDATE user_tiers SET tier = 'free' WHERE username = ?", (username,))
                conn.commit()
                return "free"
            return tier
        except Exception:
            return "free"
        finally:
            conn.close()

    def get_tier_info(self, username: str) -> dict:
        """获取用户完整套餐信息"""
        tier = self.get_tier(username)
        today = datetime.now().strftime("%Y-%m-%d")
        used = self._get_today_count(username, today)
        tier_def = TIERS.get(tier, TIERS["free"])
        return {
            "tier": tier,
            "tier_name": tier_def["name"],
            "used_today": used,
            "daily_limit": tier_def["daily_clips"],
            "remaining": max(0, tier_def["daily_clips"] - used),
            "allowed": used < tier_def["daily_clips"],
            "features": tier_def,
        }

    def set_tier(self, username: str, tier: str, expires_at: Optional[float] = None):
        """设置用户等级"""
        if tier not in TIERS:
            raise ValueError(f"无效套餐: {tier}")
        conn = self.db._conn()
        try:
            conn.execute("""
                INSERT INTO user_tiers (username, tier, expires_at)
                VALUES (?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET
                    tier = excluded.tier, expires_at = excluded.expires_at
            """, (username, tier, expires_at or 0))
            conn.commit()
            logger.info(f"[{username}] Tier set to: {tier}")
        except Exception:
            conn.rollback()
        finally:
            conn.close()

    def should_watermark(self, username: str) -> bool:
        """是否需要添加水印"""
        tier = self.get_tier(username)
        return TIERS.get(tier, TIERS["free"])["watermark"]

    def check_feature(self, username: str, feature: str) -> bool:
        """检查用户是否有某个功能权限"""
        tier = self.get_tier(username)
        tier_def = TIERS.get(tier, TIERS["free"])
        return tier_def.get(feature, False)

    def get_usage_history(self, username: str, days: int = 30) -> list[dict]:
        """获取最近 N 天的使用历史"""
        conn = self.db._conn()
        try:
            rows = conn.execute(
                "SELECT date, clips_generated FROM user_quotas WHERE username = ? ORDER BY date DESC LIMIT ?",
                (username, days)
            ).fetchall()
            return [{"date": r[0], "clips": r[1]} for r in rows]
        except Exception:
            return []
        finally:
            conn.close()

    def get_all_tiers(self) -> list[dict]:
        """获取所有用户等级"""
        conn = self.db._conn()
        try:
            rows = conn.execute("SELECT username, tier, expires_at FROM user_tiers ORDER BY username").fetchall()
            return [{"username": r[0], "tier": r[1], "expires_at": r[2]} for r in rows]
        except Exception:
            return []
        finally:
            conn.close()

    @staticmethod
    def get_tier_definitions() -> dict:
        """获取所有套餐定义（用于前端展示）"""
        return TIERS

    def _get_today_count(self, username: str, today: str) -> int:
        conn = self.db._conn()
        try:
            row = conn.execute(
                "SELECT clips_generated FROM user_quotas WHERE username = ? AND date = ?",
                (username, today)
            ).fetchone()
            return row[0] if row else 0
        except Exception:
            return 0
        finally:
            conn.close()
