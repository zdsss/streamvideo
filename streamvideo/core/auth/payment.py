"""
支付模块 — Stripe 订阅管理
支持 Free / Pro / Team 套餐的订阅、续费、取消
"""
import logging
import os
import time
from typing import Optional

logger = logging.getLogger("payment")

STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")

STRIPE_PRICE_IDS = {
    "pro": os.environ.get("STRIPE_PRICE_PRO", ""),
    "team": os.environ.get("STRIPE_PRICE_TEAM", ""),
}

TIER_FEATURES = {
    "free": {
        "name": "Free",
        "price": 0,
        "clips_per_day": 3,
        "max_resolution": "720p",
        "watermark": True,
        "max_models": 3,
        "cloud_upload": False,
        "h265": False,
        "api_access": False,
    },
    "pro": {
        "name": "Pro",
        "price": 29,
        "clips_per_day": 50,
        "max_resolution": "1080p",
        "watermark": False,
        "max_models": 20,
        "cloud_upload": True,
        "h265": True,
        "api_access": False,
    },
    "team": {
        "name": "Team",
        "price": 99,
        "clips_per_day": -1,
        "max_resolution": "1080p",
        "watermark": False,
        "max_models": -1,
        "cloud_upload": True,
        "h265": True,
        "api_access": True,
    },
}


class PaymentManager:
    def __init__(self, db=None):
        self.db = db
        self._stripe = None
        if STRIPE_SECRET_KEY:
            try:
                import stripe
                stripe.api_key = STRIPE_SECRET_KEY
                self._stripe = stripe
                logger.info("Stripe initialized")
            except ImportError:
                logger.warning("stripe package not installed. Run: pip install stripe")
        else:
            logger.info("STRIPE_SECRET_KEY not set, payment features disabled")

    @property
    def available(self) -> bool:
        return self._stripe is not None

    def get_tier_features(self, tier: str = "free") -> dict:
        return TIER_FEATURES.get(tier, TIER_FEATURES["free"])

    async def create_checkout_session(self, user_id: str, user_email: str, tier: str) -> dict:
        if not self.available:
            return {"error": "Payment not configured. Set STRIPE_SECRET_KEY environment variable."}

        price_id = STRIPE_PRICE_IDS.get(tier)
        if not price_id:
            return {"error": f"No price configured for tier '{tier}'. Set STRIPE_PRICE_{tier.upper()} environment variable."}

        try:
            customer_kwargs = {}
            if self.db:
                user = self.db.get_user_by_id(user_id)
                if user:
                    cid = user.get("stripe_customer_id") or ""
                    if cid:
                        customer_kwargs["customer"] = cid
            if not customer_kwargs:
                customer_kwargs["customer_email"] = user_email

            session = self._stripe.checkout.Session.create(
                **customer_kwargs,
                mode="subscription",
                line_items=[{"price": price_id, "quantity": 1}],
                success_url=f"{os.environ.get('APP_URL','http://localhost:8080')}?payment=success",
                cancel_url=f"{os.environ.get('APP_URL','http://localhost:8080')}?payment=cancelled",
                metadata={"user_id": user_id, "tier": tier},
            )
            return {"url": session.url, "session_id": session.id}
        except Exception as e:
            logger.error(f"Stripe checkout error: {e}")
            return {"error": str(e)}

    async def handle_webhook(self, payload: bytes, sig_header: str) -> dict:
        if not self.available:
            return {"error": "Payment not configured"}

        try:
            event = self._stripe.Webhook.construct_event(
                payload, sig_header, STRIPE_WEBHOOK_SECRET
            )
        except Exception as e:
            return {"error": f"Webhook verification failed: {e}"}

        event_type = event["type"]
        data = event["data"]["object"]

        if event_type == "checkout.session.completed":
            user_id = data.get("metadata", {}).get("user_id")
            tier = data.get("metadata", {}).get("tier")
            customer_id = data.get("customer", "")
            subscription_id = data.get("subscription", "")
            if user_id and tier and self.db:
                self.db.update_stripe_info(
                    user_id,
                    stripe_customer_id=customer_id,
                    stripe_subscription_id=subscription_id,
                    subscription_status="active",
                    subscription_expires_at=time.time() + 30 * 86400,
                )
                self.db.set_user_tier(user_id, tier)
                logger.info(f"User {user_id} upgraded to {tier}")

        elif event_type == "invoice.payment_succeeded":
            subscription_id = data.get("subscription", "")
            customer_id = data.get("customer", "")
            period_end = data.get("lines", {}).get("data", [{}])[0].get("period", {}).get("end", 0)
            if self.db:
                user = (self.db.get_user_by_stripe_subscription(subscription_id)
                        if subscription_id else
                        self.db.get_user_by_stripe_customer(customer_id) if customer_id else None)
                if user:
                    expires_at = float(period_end) if period_end else time.time() + 30 * 86400
                    self.db.update_stripe_info(
                        user["user_id"],
                        subscription_status="active",
                        subscription_expires_at=expires_at,
                    )
                    logger.info(f"Subscription renewed for user {user['user_id']}")

        elif event_type == "invoice.payment_failed":
            subscription_id = data.get("subscription", "")
            customer_id = data.get("customer", "")
            if self.db:
                user = (self.db.get_user_by_stripe_subscription(subscription_id)
                        if subscription_id else
                        self.db.get_user_by_stripe_customer(customer_id) if customer_id else None)
                if user:
                    self.db.update_stripe_info(user["user_id"], subscription_status="past_due")
                    logger.warning(f"Payment failed for user {user['user_id']}, marked past_due")

        elif event_type == "customer.subscription.deleted":
            customer_id = data.get("customer", "")
            subscription_id = data.get("id", "")
            if self.db:
                user = (self.db.get_user_by_stripe_subscription(subscription_id)
                        if subscription_id else
                        self.db.get_user_by_stripe_customer(customer_id) if customer_id else None)
                if user:
                    self.db.update_stripe_info(user["user_id"], subscription_status="cancelled")
                    self.db.set_user_tier(user["user_id"], "free")
                    logger.info(f"Subscription cancelled for user {user['user_id']}, downgraded to free")

        return {"received": True, "type": event_type}

    async def cancel_subscription(self, user_id: str) -> dict:
        if not self.available:
            return {"error": "Payment not configured"}
        if not self.db:
            return {"error": "Database not available"}

        user = self.db.get_user_by_id(user_id)
        if not user:
            return {"error": "User not found"}

        subscription_id = user.get("stripe_subscription_id", "")
        if not subscription_id:
            return {"error": "No active subscription"}

        try:
            self._stripe.Subscription.cancel(subscription_id)
            self.db.update_stripe_info(user_id, subscription_status="cancelled")
            self.db.set_user_tier(user_id, "free")
            return {"success": True}
        except Exception as e:
            logger.error(f"Cancel subscription error: {e}")
            return {"error": str(e)}

    def get_subscription_status(self, user_id: str) -> dict:
        if not self.db:
            return {"tier": "free", "status": "free", "features": TIER_FEATURES["free"]}

        user = self.db.get_user_by_id(user_id)
        if not user:
            return {"tier": "free", "status": "free", "features": TIER_FEATURES["free"]}

        tier_row = self.db.get_user_tier_info(user_id)
        tier = tier_row.get("tier", "free") if tier_row else "free"

        return {
            "tier": tier,
            "status": user.get("subscription_status", "free"),
            "expires_at": user.get("subscription_expires_at", 0),
            "stripe_customer_id": user.get("stripe_customer_id", ""),
            "features": self.get_tier_features(tier),
            "payment_available": self.available,
        }
