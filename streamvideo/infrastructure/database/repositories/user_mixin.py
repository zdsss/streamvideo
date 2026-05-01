"""Database UserMixin — user mixin"""
import json
import time
from typing import Optional


class UserMixin:
    def update_stripe_info(self, user_id: str, **kwargs):
        """更新用户 Stripe 相关字段"""
        allowed = {"stripe_customer_id", "stripe_subscription_id", "subscription_status", "subscription_expires_at"}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return
        set_clause = ", ".join(f"{k}=?" for k in fields)
        conn = self._conn()
        try:
            conn.execute(
                f"UPDATE users SET {set_clause} WHERE user_id=?",
                (*fields.values(), user_id)
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise


    def get_user_by_id(self, user_id: str) -> Optional[dict]:
        """通过 user_id 获取用户信息"""
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
            return {k: row[k] for k in row.keys()} if row else None


    def get_user_by_stripe_subscription(self, subscription_id: str) -> Optional[dict]:
        """通过 Stripe subscription_id 查找用户"""
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT * FROM users WHERE stripe_subscription_id=?", (subscription_id,)
            ).fetchone()
            return {k: row[k] for k in row.keys()} if row else None


    def get_user_by_stripe_customer(self, customer_id: str) -> Optional[dict]:
        """通过 Stripe customer_id 查找用户"""
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT * FROM users WHERE stripe_customer_id=?", (customer_id,)
            ).fetchone()
            return {k: row[k] for k in row.keys()} if row else None


    def get_user_tier_info(self, user_id: str) -> Optional[dict]:
        """获取用户套餐信息（user_tiers.username 存储 user_id）"""
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM user_tiers WHERE username=?", (user_id,)).fetchone()
            return {k: row[k] for k in row.keys()} if row else None


    def set_user_tier(self, user_id: str, tier: str):
        """设置用户套餐"""
        conn = self._conn()
        try:
            conn.execute("""
                INSERT INTO user_tiers (username, tier, expires_at) VALUES (?, ?, 0)
                ON CONFLICT(username) DO UPDATE SET tier=excluded.tier
            """, (user_id, tier))
            conn.commit()
        except Exception:
            conn.rollback()
            raise


    # ========== Merge Queue ==========

    def upsert_merge_queue(self, session_id: str, username: str,
                           segments: list, confidence: float, reasons: list):
        conn = self._conn()
        try:
            conn.execute("""
                INSERT INTO merge_queue (session_id, username, segments, confidence, reasons, status)
                VALUES (?, ?, ?, ?, ?, 'pending')
                ON CONFLICT(session_id) DO UPDATE SET
                    segments=excluded.segments,
                    confidence=excluded.confidence,
                    reasons=excluded.reasons,
                    status='pending',
                    created_at=strftime('%s','now')
            """, (session_id, username, json.dumps(segments), confidence, json.dumps(reasons)))
            conn.commit()
        except Exception:
            conn.rollback()
            raise


    def get_merge_queue(self, status: str = "pending") -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM merge_queue WHERE status=? ORDER BY created_at DESC",
                (status,)
            ).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["segments"] = json.loads(d["segments"]) if d["segments"] else []
                d["reasons"] = json.loads(d["reasons"]) if d["reasons"] else []
                result.append(d)
            return result


    def update_merge_queue_status(self, session_id: str, status: str):
        conn = self._conn()
        try:
            conn.execute(
                "UPDATE merge_queue SET status=? WHERE session_id=?",
                (status, session_id)
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise


    def count_merge_queue(self) -> int:
        conn = self._conn()
        try:
            return conn.execute(
                "SELECT COUNT(*) FROM merge_queue WHERE status='pending'"
            ).fetchone()[0]


    def cleanup_expired_merge_queue(self, days: int = 7) -> int:
        cutoff = time.time() - days * 86400
        conn = self._conn()
        try:
            cur = conn.execute(
                "DELETE FROM merge_queue WHERE status='pending' AND created_at < ?",
                (cutoff,)
            )
            conn.commit()
            return cur.rowcount


    # ========== Translation Cache ==========

    def get_translation_cache(self, text: str, source_lang: str, target_lang: str) -> Optional[str]:
        import hashlib
        h = hashlib.sha256(f"{source_lang}:{target_lang}:{text}".encode()).hexdigest()
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT translated FROM translation_cache WHERE text_hash=? AND source_lang=? AND target_lang=?",
                (h, source_lang, target_lang)
            ).fetchone()
            if row:
                conn.execute(
                    "UPDATE translation_cache SET hit_count=hit_count+1 WHERE text_hash=? AND source_lang=? AND target_lang=?",
                    (h, source_lang, target_lang)
                )
                conn.commit()
                return row["translated"]
            return None


    def set_translation_cache(self, text: str, source_lang: str, target_lang: str, translated: str, model: str = ""):
        import hashlib
        h = hashlib.sha256(f"{source_lang}:{target_lang}:{text}".encode()).hexdigest()
        conn = self._conn()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO translation_cache (text_hash, source_lang, target_lang, translated, model) VALUES (?,?,?,?,?)",
                (h, source_lang, target_lang, translated, model)
            )
            conn.commit()

