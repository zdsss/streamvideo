"""
配额管理系统 — Freemium 模型
free: 3 clips/day + 水印
pro: 无限 + 无水印
team: 无限 + 无水印 + 多账号
"""

import logging
import time
from datetime import datetime
from typing import Optional

logger = logging.getLogger("quota")

TIER_LIMITS = {
    "free": 3,
    "pro": 999999,
    "team": 999999,
}


class QuotaManager:
    """配额和用户等级管理"""

    def __init__(self, db):
        self.db = db

    def check_quota(self, username: str) -> tuple[bool, int, int]:
        """检查配额。返回 (allowed, used_today, daily_limit)"""
        tier = self.get_tier(username)
        limit = TIER_LIMITS.get(tier, 3)
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
        """获取用户等级"""
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
            # 检查是否过期
            if expires > 0 and time.time() > expires:
                # 过期，降级为 free
                conn.execute(
                    "UPDATE user_tiers SET tier = 'free' WHERE username = ?",
                    (username,)
                )
                conn.commit()
                return "free"
            return tier
        except Exception:
            return "free"
        finally:
            conn.close()

    def set_tier(self, username: str, tier: str, expires_at: Optional[float] = None):
        """设置用户等级"""
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
        """是否需要添加水印（free 用户需要）"""
        return self.get_tier(username) == "free"

    def get_all_tiers(self) -> list[dict]:
        """获取所有用户等级"""
        conn = self.db._conn()
        try:
            rows = conn.execute("SELECT * FROM user_tiers ORDER BY username").fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []
        finally:
            conn.close()

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
