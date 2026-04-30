"""认证路由 - 用户注册、登录、注销"""
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api/auth", tags=["auth"])


@router.post("/register")
async def auth_register(req: dict):
    """用户注册"""
    from auth import AuthManager
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
async def auth_login(req: dict):
    """用户登录"""
    from auth import AuthManager
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
    from auth import AuthManager
    from server import db

    am = AuthManager(db)
    token = req.get("session_token", "")
    am.logout(token)
    return JSONResponse({"ok": True})


@router.get("/me")
async def auth_me(request: Request):
    """获取当前用户信息"""
    from auth import AuthManager
    from quota import QuotaManager
    from server import db

    am = AuthManager(db)
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
    from auth import AuthManager
    from server import db

    am = AuthManager(db)
    return JSONResponse(am.get_users())
