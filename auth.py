"""
用户认证模块 — 注册/登录/Session 管理
兼容原有 SV_TOKEN 单 token 模式
"""

import bcrypt
import hashlib
import logging
import secrets
import time
from typing import Optional

logger = logging.getLogger("auth")

SESSION_DURATION = 7 * 24 * 3600  # 7 天


def _hash_password(password: str) -> str:
    """bcrypt 密码哈希"""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def _verify_password(password: str, stored_hash: str) -> bool:
    """验证密码，支持旧格式（SHA256）和新格式（bcrypt）迁移"""
    # 检测旧格式：salt:hash
    if ":" in stored_hash and len(stored_hash.split(":")) == 2:
        # 旧格式 SHA256 验证
        parts = stored_hash.split(":", 1)
        salt = parts[0]
        h = hashlib.sha256(f"{salt}:{password}".encode()).hexdigest()
        return f"{salt}:{h}" == stored_hash

    # 新格式 bcrypt 验证
    try:
        return bcrypt.checkpw(password.encode(), stored_hash.encode())
    except Exception:
        return False


class AuthManager:
    """用户认证管理"""

    def __init__(self, db):
        self.db = db

    def register(self, email: str, password: str, display_name: str = "") -> dict:
        """注册新用户，返回用户信息"""
        email = email.strip().lower()
        if not email or not password:
            raise ValueError("邮箱和密码不能为空")
        if len(password) < 6:
            raise ValueError("密码至少 6 位")

        conn = self.db._conn()
        try:
            # 检查邮箱是否已注册
            existing = conn.execute("SELECT user_id FROM users WHERE email = ?", (email,)).fetchone()
            if existing:
                raise ValueError("该邮箱已注册")

            user_id = f"u_{secrets.token_hex(8)}"
            password_hash = _hash_password(password)
            now = time.time()

            conn.execute(
                "INSERT INTO users (user_id, email, display_name, password_hash, role, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (user_id, email, display_name or email.split("@")[0], password_hash, "user", now)
            )
            conn.commit()
            logger.info(f"User registered: {email} ({user_id})")
            return {"user_id": user_id, "email": email, "display_name": display_name or email.split("@")[0], "role": "user"}
        except ValueError:
            raise
        except Exception as e:
            conn.rollback()
            raise ValueError(f"注册失败: {e}")
        finally:
            conn.close()

    def login(self, email: str, password: str) -> dict:
        """登录，返回 session token + 用户信息"""
        email = email.strip().lower()
        conn = self.db._conn()
        try:
            row = conn.execute(
                "SELECT user_id, email, display_name, password_hash, role FROM users WHERE email = ?",
                (email,)
            ).fetchone()
            if not row:
                raise ValueError("邮箱或密码错误")

            user_id, db_email, display_name, password_hash, role = row
            if not _verify_password(password, password_hash):
                raise ValueError("邮箱或密码错误")

            # 密码迁移：如果使用旧格式（SHA256），重新哈希为 bcrypt
            if ":" in password_hash and len(password_hash.split(":")) == 2:
                new_hash = _hash_password(password)
                conn.execute("UPDATE users SET password_hash = ? WHERE user_id = ?", (new_hash, user_id))
                logger.info(f"Migrated password hash for user: {email}")

            # 创建 session
            session_token = secrets.token_urlsafe(32)
            now = time.time()
            expires_at = now + SESSION_DURATION

            conn.execute(
                "INSERT INTO user_sessions (session_token, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
                (session_token, user_id, now, expires_at)
            )
            conn.execute("UPDATE users SET last_login = ? WHERE user_id = ?", (now, user_id))
            conn.commit()

            logger.info(f"User logged in: {email}")
            return {
                "session_token": session_token,
                "user": {"user_id": user_id, "email": db_email, "display_name": display_name, "role": role},
                "expires_at": expires_at,
            }
        except ValueError:
            raise
        except Exception as e:
            conn.rollback()
            raise ValueError(f"登录失败: {e}")
        finally:
            conn.close()

    def validate_session(self, session_token: str) -> Optional[dict]:
        """验证 session token，返回用户信息或 None"""
        if not session_token:
            return None
        conn = self.db._conn()
        try:
            row = conn.execute(
                """SELECT u.user_id, u.email, u.display_name, u.role, s.expires_at
                   FROM user_sessions s JOIN users u ON s.user_id = u.user_id
                   WHERE s.session_token = ?""",
                (session_token,)
            ).fetchone()
            if not row:
                return None
            user_id, email, display_name, role, expires_at = row
            if time.time() > expires_at:
                # Session 过期，清理
                conn.execute("DELETE FROM user_sessions WHERE session_token = ?", (session_token,))
                conn.commit()
                return None
            return {"user_id": user_id, "email": email, "display_name": display_name, "role": role}
        finally:
            conn.close()

    def logout(self, session_token: str):
        """注销 session"""
        conn = self.db._conn()
        try:
            conn.execute("DELETE FROM user_sessions WHERE session_token = ?", (session_token,))
            conn.commit()
        finally:
            conn.close()

    def get_user(self, user_id: str) -> Optional[dict]:
        """获取用户信息"""
        conn = self.db._conn()
        try:
            row = conn.execute(
                "SELECT user_id, email, display_name, role, created_at, last_login FROM users WHERE user_id = ?",
                (user_id,)
            ).fetchone()
            if not row:
                return None
            return dict(zip(["user_id", "email", "display_name", "role", "created_at", "last_login"], row))
        finally:
            conn.close()

    def get_users(self) -> list[dict]:
        """获取所有用户"""
        conn = self.db._conn()
        try:
            rows = conn.execute("SELECT user_id, email, display_name, role, created_at, last_login FROM users ORDER BY created_at DESC").fetchall()
            return [dict(zip(["user_id", "email", "display_name", "role", "created_at", "last_login"], r)) for r in rows]
        finally:
            conn.close()

    def cleanup_expired_sessions(self):
        """清理过期 session"""
        conn = self.db._conn()
        try:
            conn.execute("DELETE FROM user_sessions WHERE expires_at < ?", (time.time(),))
            conn.commit()
        finally:
            conn.close()
