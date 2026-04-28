"""
测试 payment.py — 使用 Mock 避免真实 Stripe API 调用
"""
import tempfile
import time
import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from payment import PaymentManager, TIER_FEATURES
from database import Database


@pytest.fixture
def db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    d = Database(path)
    conn = d._conn()
    conn.execute(
        "INSERT INTO users (user_id, email, display_name, password_hash, role, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("u1", "user@test.com", "User", "s:h", "user", time.time())
    )
    conn.commit()
    conn.close()
    return d


@pytest.fixture
def pm_no_stripe(db):
    """未配置 Stripe 的 PaymentManager"""
    return PaymentManager(db=db)


@pytest.fixture
def pm_with_stripe(db):
    """Mock Stripe 的 PaymentManager"""
    pm = PaymentManager.__new__(PaymentManager)
    pm.db = db
    mock_stripe = MagicMock()
    pm._stripe = mock_stripe
    return pm, mock_stripe


# ===== 初始化 =====

def test_payment_manager_no_stripe_key(pm_no_stripe):
    assert pm_no_stripe.available is False


def test_payment_manager_with_mock_stripe(pm_with_stripe):
    pm, _ = pm_with_stripe
    assert pm.available is True


# ===== get_tier_features =====

def test_tier_features_free(pm_no_stripe):
    f = pm_no_stripe.get_tier_features("free")
    assert f["clips_per_day"] == 3
    assert f["watermark"] is True
    assert f["max_models"] == 3
    assert f["price"] == 0


def test_tier_features_pro(pm_no_stripe):
    f = pm_no_stripe.get_tier_features("pro")
    assert f["clips_per_day"] == 50
    assert f["watermark"] is False
    assert f["h265"] is True
    assert f["price"] == 29


def test_tier_features_team(pm_no_stripe):
    f = pm_no_stripe.get_tier_features("team")
    assert f["clips_per_day"] == -1  # 无限
    assert f["api_access"] is True
    assert f["max_models"] == -1


def test_tier_features_unknown_defaults_to_free(pm_no_stripe):
    f = pm_no_stripe.get_tier_features("enterprise")
    assert f == TIER_FEATURES["free"]


# ===== create_checkout_session =====

@pytest.mark.asyncio
async def test_checkout_session_no_stripe(pm_no_stripe):
    result = await pm_no_stripe.create_checkout_session("u1", "user@test.com", "pro")
    assert "error" in result
    assert "STRIPE_SECRET_KEY" in result["error"]


@pytest.mark.asyncio
async def test_checkout_session_no_price_id(pm_with_stripe):
    pm, mock_stripe = pm_with_stripe
    # STRIPE_PRICE_PRO 未配置
    result = await pm.create_checkout_session("u1", "user@test.com", "pro")
    assert "error" in result
    assert "STRIPE_PRICE_PRO" in result["error"]


@pytest.mark.asyncio
async def test_checkout_session_uses_customer_email_for_new_user(pm_with_stripe):
    pm, mock_stripe = pm_with_stripe
    import payment as pay_module
    pay_module.STRIPE_PRICE_IDS["pro"] = "price_test_pro"

    mock_session = MagicMock()
    mock_session.url = "https://checkout.stripe.com/test"
    mock_session.id = "cs_test_123"
    mock_stripe.checkout.Session.create.return_value = mock_session

    result = await pm.create_checkout_session("u1", "user@test.com", "pro")
    assert result.get("url") == "https://checkout.stripe.com/test"
    call_kwargs = mock_stripe.checkout.Session.create.call_args[1]
    assert "customer_email" in call_kwargs
    assert "customer" not in call_kwargs

    pay_module.STRIPE_PRICE_IDS["pro"] = ""  # 还原


@pytest.mark.asyncio
async def test_checkout_session_uses_customer_id_for_existing(pm_with_stripe, db):
    pm, mock_stripe = pm_with_stripe
    import payment as pay_module
    pay_module.STRIPE_PRICE_IDS["pro"] = "price_test_pro"

    # 给用户设置 stripe_customer_id
    db.update_stripe_info("u1", stripe_customer_id="cus_existing")

    mock_session = MagicMock()
    mock_session.url = "https://checkout.stripe.com/test2"
    mock_session.id = "cs_test_456"
    mock_stripe.checkout.Session.create.return_value = mock_session

    result = await pm.create_checkout_session("u1", "user@test.com", "pro")
    call_kwargs = mock_stripe.checkout.Session.create.call_args[1]
    assert call_kwargs.get("customer") == "cus_existing"
    assert "customer_email" not in call_kwargs

    pay_module.STRIPE_PRICE_IDS["pro"] = ""  # 还原


# ===== get_subscription_status =====

def test_subscription_status_no_db():
    pm = PaymentManager(db=None)
    status = pm.get_subscription_status("u1")
    assert status["tier"] == "free"
    assert status["status"] == "free"


def test_subscription_status_new_user(pm_no_stripe, db):
    status = pm_no_stripe.get_subscription_status("u1")
    assert status["tier"] == "free"
    assert "features" in status


def test_subscription_status_after_upgrade(pm_no_stripe, db):
    db.set_user_tier("u1", "pro")
    db.update_stripe_info("u1", subscription_status="active")
    status = pm_no_stripe.get_subscription_status("u1")
    assert status["tier"] == "pro"
    assert status["status"] == "active"
    assert status["features"]["watermark"] is False


def test_subscription_status_payment_available_flag(pm_with_stripe, db):
    pm, _ = pm_with_stripe
    status = pm.get_subscription_status("u1")
    assert status["payment_available"] is True


# ===== handle_webhook =====

@pytest.mark.asyncio
async def test_webhook_no_stripe(pm_no_stripe):
    result = await pm_no_stripe.handle_webhook(b"payload", "sig")
    assert "error" in result


@pytest.mark.asyncio
async def test_webhook_checkout_completed(pm_with_stripe, db):
    pm, mock_stripe = pm_with_stripe
    mock_event = {
        "type": "checkout.session.completed",
        "data": {"object": {
            "metadata": {"user_id": "u1", "tier": "pro"},
            "customer": "cus_new",
            "subscription": "sub_new",
        }}
    }
    mock_stripe.Webhook.construct_event.return_value = mock_event
    result = await pm.handle_webhook(b"payload", "sig")
    assert result["received"] is True
    assert result["type"] == "checkout.session.completed"
    user = db.get_user_by_id("u1")
    assert user["stripe_customer_id"] == "cus_new"
    assert user["subscription_status"] == "active"


@pytest.mark.asyncio
async def test_webhook_invoice_payment_succeeded(pm_with_stripe, db):
    pm, mock_stripe = pm_with_stripe
    # 先给用户设置 subscription_id 以便反查
    db.update_stripe_info("u1", stripe_subscription_id="sub_renew")
    mock_event = {
        "type": "invoice.payment_succeeded",
        "data": {"object": {
            "subscription": "sub_renew",
            "customer": "",
            "lines": {"data": [{"period": {"end": 9999999999}}]},
        }}
    }
    mock_stripe.Webhook.construct_event.return_value = mock_event
    result = await pm.handle_webhook(b"payload", "sig")
    assert result["received"] is True
    user = db.get_user_by_id("u1")
    assert user["subscription_status"] == "active"
    assert user["subscription_expires_at"] == 9999999999.0


@pytest.mark.asyncio
async def test_webhook_invoice_payment_failed(pm_with_stripe, db):
    pm, mock_stripe = pm_with_stripe
    db.update_stripe_info("u1", stripe_subscription_id="sub_fail")
    mock_event = {
        "type": "invoice.payment_failed",
        "data": {"object": {"subscription": "sub_fail", "customer": ""}}
    }
    mock_stripe.Webhook.construct_event.return_value = mock_event
    result = await pm.handle_webhook(b"payload", "sig")
    assert result["received"] is True
    user = db.get_user_by_id("u1")
    assert user["subscription_status"] == "past_due"


@pytest.mark.asyncio
async def test_webhook_subscription_deleted_downgrades_to_free(pm_with_stripe, db):
    pm, mock_stripe = pm_with_stripe
    db.update_stripe_info("u1", stripe_subscription_id="sub_del")
    db.set_user_tier("u1", "pro")
    mock_event = {
        "type": "customer.subscription.deleted",
        "data": {"object": {"id": "sub_del", "customer": ""}}
    }
    mock_stripe.Webhook.construct_event.return_value = mock_event
    result = await pm.handle_webhook(b"payload", "sig")
    assert result["received"] is True
    user = db.get_user_by_id("u1")
    assert user["subscription_status"] == "cancelled"
    tier = db.get_user_tier_info("u1")
    assert tier["tier"] == "free"
