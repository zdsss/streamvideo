"""用户与认证仓储"""
from __future__ import annotations

import time
from typing import Optional

from streamvideo.infrastructure.database.repositories.base import BaseRepository


class UserRepository(BaseRepository):
    """用户仓储"""

    def get_by_id(self, user_id: str) -> Optional[dict]:
        return self.fetch_one("SELECT * FROM users WHERE user_id = ?", (user_id,))

    def get_by_email(self, email: str) -> Optional[dict]:
        return self.fetch_one("SELECT * FROM users WHERE email = ?", (email,))

    def get_by_stripe_subscription(self, subscription_id: str) -> Optional[dict]:
        return self.fetch_one(
            "SELECT * FROM users WHERE stripe_subscription_id = ?",
            (subscription_id,),
        )

    def get_by_stripe_customer(self, customer_id: str) -> Optional[dict]:
        return self.fetch_one(
            "SELECT * FROM users WHERE stripe_customer_id = ?", (customer_id,)
        )

    def update_stripe_info(self, user_id: str, **kwargs) -> None:
        if not kwargs:
            return
        set_clause = ",".join(f"{k}=?" for k in kwargs)
        params = tuple(kwargs.values()) + (user_id,)
        self.execute(
            f"UPDATE users SET {set_clause} WHERE user_id = ?", params
        )

    def get_tier_info(self, user_id: str) -> Optional[dict]:
        return self.fetch_one(
            "SELECT * FROM user_tiers WHERE user_id = ?", (user_id,)
        )

    def set_tier(self, user_id: str, tier: str) -> None:
        self.execute(
            """
            INSERT INTO user_tiers (user_id, tier, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET tier=excluded.tier, updated_at=excluded.updated_at
            """,
            (user_id, tier, time.time()),
        )
