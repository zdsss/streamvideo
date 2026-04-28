"""
测试 database.py 新增的凭据管理和用户套餐方法
"""
import tempfile
import time
import pytest
from database import Database


@pytest.fixture
def db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    d = Database(path)
    # 创建测试用户
    conn = d._conn()
    conn.execute(
        "INSERT INTO users (user_id, email, display_name, password_hash, role, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("u_test1", "test@example.com", "Test", "salt:hash", "user", time.time())
    )
    conn.commit()
    conn.close()
    yield d


# ===== platform_credentials =====

def test_save_and_get_credential(db):
    db.save_credential("u1", "douyin", "tok123", "ref456", "oid789", "Alice", time.time() + 3600)
    cred = db.get_credential("u1", "douyin")
    assert cred is not None
    assert cred["access_token"] == "tok123"
    assert cred["refresh_token"] == "ref456"
    assert cred["openid"] == "oid789"
    assert cred["display_name"] == "Alice"


def test_get_credential_not_found(db):
    assert db.get_credential("nobody", "douyin") is None


def test_save_credential_upsert(db):
    db.save_credential("u1", "kuaishou", "old_tok", "old_ref", "oid1", "", time.time() + 100)
    db.save_credential("u1", "kuaishou", "new_tok", "old_ref", "oid1", "", time.time() + 200)
    cred = db.get_credential("u1", "kuaishou")
    assert cred["access_token"] == "new_tok"


def test_delete_credential(db):
    db.save_credential("u1", "douyin", "tok", "", "", "", 0)
    db.delete_credential("u1", "douyin")
    assert db.get_credential("u1", "douyin") is None


def test_delete_nonexistent_credential(db):
    # 删除不存在的凭据不应抛异常
    db.delete_credential("nobody", "douyin")


def test_credentials_isolated_by_user(db):
    db.save_credential("u1", "douyin", "tok_u1", "", "", "", 0)
    db.save_credential("u2", "douyin", "tok_u2", "", "", "", 0)
    assert db.get_credential("u1", "douyin")["access_token"] == "tok_u1"
    assert db.get_credential("u2", "douyin")["access_token"] == "tok_u2"


# ===== update_stripe_info =====

def test_update_stripe_info(db):
    db.update_stripe_info("u_test1", stripe_customer_id="cus_123", subscription_status="active")
    user = db.get_user_by_id("u_test1")
    assert user["stripe_customer_id"] == "cus_123"
    assert user["subscription_status"] == "active"


def test_update_stripe_info_partial(db):
    db.update_stripe_info("u_test1", stripe_subscription_id="sub_abc")
    user = db.get_user_by_id("u_test1")
    assert user["stripe_subscription_id"] == "sub_abc"


def test_update_stripe_info_ignores_unknown_fields(db):
    # 不应抛异常
    db.update_stripe_info("u_test1", unknown_field="value", subscription_status="cancelled")
    user = db.get_user_by_id("u_test1")
    assert user["subscription_status"] == "cancelled"


# ===== get_user_by_id =====

def test_get_user_by_id_found(db):
    user = db.get_user_by_id("u_test1")
    assert user is not None
    assert user["email"] == "test@example.com"


def test_get_user_by_id_not_found(db):
    assert db.get_user_by_id("nonexistent") is None


def test_get_user_by_id_returns_dict(db):
    user = db.get_user_by_id("u_test1")
    assert isinstance(user, dict)


# ===== get_user_by_stripe_subscription / get_user_by_stripe_customer =====

def test_get_user_by_stripe_subscription(db):
    db.update_stripe_info("u_test1", stripe_subscription_id="sub_xyz")
    user = db.get_user_by_stripe_subscription("sub_xyz")
    assert user is not None
    assert user["user_id"] == "u_test1"


def test_get_user_by_stripe_subscription_not_found(db):
    assert db.get_user_by_stripe_subscription("sub_nonexistent") is None


def test_get_user_by_stripe_customer(db):
    db.update_stripe_info("u_test1", stripe_customer_id="cus_abc")
    user = db.get_user_by_stripe_customer("cus_abc")
    assert user is not None
    assert user["user_id"] == "u_test1"


def test_get_user_by_stripe_customer_not_found(db):
    assert db.get_user_by_stripe_customer("cus_nonexistent") is None


# ===== set_user_tier / get_user_tier_info =====

def test_set_and_get_user_tier(db):
    db.set_user_tier("u_test1", "pro")
    info = db.get_user_tier_info("u_test1")
    assert info is not None
    assert info["tier"] == "pro"


def test_set_user_tier_upsert(db):
    db.set_user_tier("u_test1", "pro")
    db.set_user_tier("u_test1", "team")
    info = db.get_user_tier_info("u_test1")
    assert info["tier"] == "team"


def test_get_user_tier_info_not_found(db):
    assert db.get_user_tier_info("nobody") is None
