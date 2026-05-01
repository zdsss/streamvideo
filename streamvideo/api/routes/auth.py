"""认证路由 - 用户注册、登录、注销"""
import time
from collections import defaultdict

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api/auth", tags=["auth"])

_login_attempts: dict[str, list[float]] = defaultdict(list)
_MAX_ATTEMPTS = 5
_WINDOW_SECONDS = 60


def _check_rate_limit(ip: str) -> bool:
    """Returns True if rate limit exceeded"""
    now = time.time()
    attempts = _login_attempts[ip]
    # Clean old entries
    _login_attempts[ip] = [t for t in attempts if now - t < _WINDOW_SECONDS]
    if len(_login_attempts[ip]) >= _MAX_ATTEMPTS:
        return True
    _login_attempts[ip].append(now)
    return False


@router.post("/register")
async def auth_register(req: dict):
    """用户注册"""
    from streamvideo.core.auth.manager import AuthManager
    from server import db

    am = AuthManager(db)
    try:
        user = am.register(
            email=req.get("email", ""),
            password=req.get("password", ""),
            display_name=req.get("display_name", "")
        )
        result = am.login(req["email"], req["password"])
        return JSONResponse({"ok": True, **result})
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@router.post("/login")
async def auth_login(req: dict, request: Request):
    """用户登录"""
    # Rate limiting
    client_ip = request.client.host if request.client else "unknown"
    if _check_rate_limit(client_ip):
        return JSONResponse({"error": "登录尝试过于频繁，请稍后再试", "retry_after": 60}, status_code=429)

    from streamvideo.core.auth.manager import AuthManager
    from server import db

    am = AuthManager(db)
    try:
        result = am.login(
            email=req.get("email", ""),
            password=req.get("password", "")
        )
        return JSONResponse({"ok": True, **result})
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@router.post("/logout")
async def auth_logout(req: dict):
    """用户注销"""
    from streamvideo.core.auth.manager import AuthManager
    from server import db

    am = AuthManager(db)
    token = req.get("session_token", "")
    am.logout(token)
    return JSONResponse({"ok": True})


@router.get("/me")
async def auth_me(request: Request):
    """获取当前用户信息"""
    from streamvideo.core.auth.manager import AuthManager
    from streamvideo.core.auth.quota import QuotaManager
    from server import db

    am = AuthManager(db)
    # Prefer Authorization header, fallback to query param
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
    else:
        token = request.query_params.get("session_token") or request.headers.get("X-Session-Token", "")
    user = am.validate_session(token)
    if not user:
        return JSONResponse({"error": "not authenticated"}, status_code=401)

    tier_info = QuotaManager(db).get_tier_info(user["user_id"])
    user["tier"] = tier_info["tier"]
    user["tier_name"] = tier_info["tier_name"]
    return JSONResponse({"ok": True, "user": user})


@router.get("/users")
async def auth_users():
    """获取所有用户（管理员）"""
    from streamvideo.core.auth.manager import AuthManager
    from server import db

    am = AuthManager(db)
    return JSONResponse(am.get_users())
