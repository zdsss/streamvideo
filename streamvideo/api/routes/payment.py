"""Payment API 路由 - 支付管理"""
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter()

# 全局依赖
db = None
_payment_manager = None


def init_payment_router(database):
    """初始化路由依赖"""
    global db
    db = database


def _get_payment_manager():
    """获取支付管理器单例"""
    global _payment_manager
    if _payment_manager is None:
        try:
            from payment import PaymentManager
            _payment_manager = PaymentManager(db)
        except ImportError:
            _payment_manager = None
    return _payment_manager


@router.get("/api/payment/tiers")
async def payment_tiers():
    """获取套餐定义"""
    pm = _get_payment_manager()
    if not pm:
        return JSONResponse({"error": "payment module not available"}, status_code=503)
    from payment import TIER_FEATURES
    return JSONResponse({"tiers": TIER_FEATURES})


@router.post("/api/payment/checkout")
async def payment_checkout(req: dict, request: Request):
    """创建 Stripe Checkout 会话"""
    pm = _get_payment_manager()
    if not pm:
        return JSONResponse({"error": "payment module not available"}, status_code=503)
    tier = req.get("tier", "pro")
    user_id = req.get("user_id", "default")
    user_email = req.get("email", "")
    result = await pm.create_checkout_session(user_id, user_email, tier)
    if "error" in result:
        return JSONResponse(result, status_code=400)
    return JSONResponse(result)


@router.post("/api/payment/webhook")
async def payment_webhook(request: Request):
    """Stripe Webhook 回调"""
    pm = _get_payment_manager()
    if not pm:
        return JSONResponse({"error": "payment module not available"}, status_code=503)
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")
    result = await pm.handle_webhook(payload, sig_header)
    if "error" in result:
        return JSONResponse(result, status_code=400)
    return JSONResponse(result)


@router.post("/api/payment/cancel")
async def payment_cancel(req: dict):
    """取消订阅"""
    pm = _get_payment_manager()
    if not pm:
        return JSONResponse({"error": "payment module not available"}, status_code=503)
    user_id = req.get("user_id", "default")
    result = await pm.cancel_subscription(user_id)
    if "error" in result:
        return JSONResponse(result, status_code=400)
    return JSONResponse(result)


@router.get("/api/payment/status")
async def payment_status(user_id: str = "default"):
    """查询订阅状态"""
    pm = _get_payment_manager()
    if not pm:
        return JSONResponse({"tier": "free", "status": "free", "payment_unavailable": True})
    result = pm.get_subscription_status(user_id)
    return JSONResponse(result)


