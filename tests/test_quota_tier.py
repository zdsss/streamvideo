"""
测试 quota.py 的套餐降级逻辑（订阅状态 + 过期时间）
"""
import tempfile
import time
import pytest
from database import Database
from quota import QuotaManager


@pytest.fixture
def db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    d = Database(path)
    conn = d._conn()
    conn.execute(
        "INSERT INTO users (user_id, email, display_name, password_hash, role, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("u1", "u@test.com", "U", "s:h", "user", time.time())
    )
    conn.commit()
    conn.close()
    return d


@pytest.fixture
def qm(db):
    return QuotaManager(db)


# ===== 订阅状态降级 =====

def test_past_due_downgrades_to_free(db, qm):
    db.set_user_tier("u1", "pro")
    db.update_stripe_info("u1", subscription_status="past_due")
    assert qm.get_tier("u1") == "free"


def test_cancelled_downgrades_to_free(db, qm):
    db.set_user_tier("u1", "team")
    db.update_stripe_info("u1", subscription_status="cancelled")
    assert qm.get_tier("u1") == "free"


def test_unpaid_downgrades_to_free(db, qm):
    db.set_user_tier("u1", "pro")
    db.update_stripe_info("u1", subscription_status="unpaid")
    assert qm.get_tier("u1") == "free"


def test_active_subscription_keeps_tier(db, qm):
    db.set_user_tier("u1", "pro")
    db.update_stripe_info("u1", subscription_status="active")
    assert qm.get_tier("u1") == "pro"


def test_no_subscription_status_keeps_tier(db, qm):
    db.set_user_tier("u1", "pro")
    # subscription_status 为 NULL
    assert qm.get_tier("u1") == "pro"


# ===== 过期时间降级 =====

def test_expired_tier_downgrades_to_free(db, qm):
    conn = db._conn()
    conn.execute(
        "INSERT OR REPLACE INTO user_tiers (username, tier, expires_at) VALUES (?, ?, ?)",
        ("u1", "pro", time.time() - 1)  # 已过期
    )
    conn.commit()
    conn.close()
    assert qm.get_tier("u1") == "free"


def test_future_expiry_keeps_tier(db, qm):
    conn = db._conn()
    conn.execute(
        "INSERT OR REPLACE INTO user_tiers (username, tier, expires_at) VALUES (?, ?, ?)",
        ("u1", "pro", time.time() + 86400)
    )
    conn.commit()
    conn.close()
    assert qm.get_tier("u1") == "pro"


def test_zero_expiry_never_expires(db, qm):
    conn = db._conn()
    conn.execute(
        "INSERT OR REPLACE INTO user_tiers (username, tier, expires_at) VALUES (?, ?, ?)",
        ("u1", "team", 0)
    )
    conn.commit()
    conn.close()
    assert qm.get_tier("u1") == "team"


# ===== get_tier_info 包含 tier_name =====

def test_get_tier_info_includes_tier_name(db, qm):
    db.set_user_tier("u1", "pro")
    info = qm.get_tier_info("u1")
    assert "tier_name" in info
    assert info["tier_name"] != ""


def test_get_tier_info_free_default(qm):
    info = qm.get_tier_info("nonexistent_user")
    assert info["tier"] == "free"
