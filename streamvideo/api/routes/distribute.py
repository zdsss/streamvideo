"""Distribute API 路由 - 分发管理、OAuth"""
import os
import time
import json
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse

router = APIRouter()

# 全局依赖
db = None
_distribute_manager = None


def init_distribute_router(database):
    """初始化路由依赖"""
    global db
    db = database


def _get_distribute_manager():
    """获取分发管理器单例"""
    global _distribute_manager
    if _distribute_manager is None:
        from distribute import (DistributeManager, MockPublisher,
                                DouyinPublisher, KuaishouPublisher,
                                BilibiliAssistPublisher, WeixinVideoPublisher)
        _distribute_manager = DistributeManager(db)
        _distribute_manager.register_publisher("mock", MockPublisher())
        _distribute_manager.set_credentials("mock", {"token": "test"})
        _distribute_manager.register_publisher("douyin", DouyinPublisher())
        _distribute_manager.register_publisher("kuaishou", KuaishouPublisher())
        _distribute_manager.register_publisher("bilibili", BilibiliAssistPublisher())
        _distribute_manager.set_credentials("bilibili", {})
        _distribute_manager.register_publisher("weixinvideo", WeixinVideoPublisher())
        _distribute_manager.set_credentials("weixinvideo", {})
        for platform in ["douyin", "kuaishou"]:
            creds = db.get_oauth_credentials(platform)
            if creds:
                _distribute_manager.set_credentials(platform, creds)
    return _distribute_manager


@router.get("/api/distribute/platforms")
async def get_distribute_platforms():
    """获取可用的分发平台"""
    dm = _get_distribute_manager()
    return JSONResponse({"platforms": dm.get_available_platforms()})


@router.post("/api/distribute")
async def create_distribute_task(req: dict):
    """创建分发任务"""
    clip_id = req.get("clip_id", "")
    platform = req.get("platform", "")
    title = req.get("title", "")
    description = req.get("description", "")
    tags = req.get("tags", [])

    if not clip_id or not platform:
        return JSONResponse({"error": "clip_id and platform required"}, status_code=400)

    clip = db.get_clip(clip_id)
    if not clip:
        return JSONResponse({"error": "clip not found"}, status_code=404)

    file_path = clip.get("output_file", "")
    username = clip.get("username", "")

    dm = _get_distribute_manager()
    try:
        task = await dm.create_task(
            clip_id=clip_id, username=username, platform=platform,
            file_path=file_path, title=title, description=description, tags=tags
        )
        return JSONResponse({"ok": True, "task": task.to_dict()})
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@router.post("/api/distribute/{task_id}/execute")
async def execute_distribute_task(task_id: str):
    """执行分发任务（上传+发布）"""
    dm = _get_distribute_manager()
    task_data = dm.get_task(task_id)
    if not task_data:
        return JSONResponse({"error": "task not found"}, status_code=404)
    try:
        task = await dm.execute_task(task_id)
        return JSONResponse({"ok": True, "task": task.to_dict()})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/distribute/{task_id}/status")
async def check_distribute_status(task_id: str):
    """查询分发任务的平台状态"""
    dm = _get_distribute_manager()
    task = await dm.check_task_status(task_id)
    if not task:
        return JSONResponse({"error": "task not found"}, status_code=404)
    return JSONResponse({"ok": True, "task": task.to_dict()})


@router.get("/api/distribute/tasks")
async def get_distribute_tasks(username: str = "", platform: str = ""):
    """获取分发任务列表"""
    tasks = db.get_distribute_tasks(username=username, platform=platform)
    return JSONResponse(tasks)


@router.get("/api/distribute/tasks/{task_id}")
async def get_distribute_task(task_id: str):
    """获取单个分发任务"""
    task = db.get_distribute_task(task_id)
    if not task:
        return JSONResponse({"error": "task not found"}, status_code=404)
    return JSONResponse(task)


def _get_request_user_id(request: Optional[Request]) -> str:
    """从请求 session token 中获取 user_id，未登录返回 'default'"""
    if request is None:
        return "default"
    token = request.headers.get("X-Session-Token") or request.query_params.get("session_token", "")
    if not token:
        return "default"
    try:
        from auth import AuthManager
        user = AuthManager(db).validate_session(token)
        return user["user_id"] if user else "default"
    except Exception:
        return "default"


# ========== OAuth — 抖音 / 快手 ==========

OAUTH_CONFIGS = {
    "douyin": {
        "client_key": os.environ.get("DOUYIN_CLIENT_KEY", ""),
        "client_secret": os.environ.get("DOUYIN_CLIENT_SECRET", ""),
        "redirect_uri": os.environ.get("DOUYIN_REDIRECT_URI", ""),
        "authorize_url": "https://open.douyin.com/platform/oauth/connect/",
        "token_url": "https://open.douyin.com/oauth/access_token/",
        "refresh_url": "https://open.douyin.com/oauth/refresh_token/",
        "scope": "user_info,video.create",
    },
    "kuaishou": {
        "client_key": os.environ.get("KUAISHOU_CLIENT_KEY", ""),
        "client_secret": os.environ.get("KUAISHOU_CLIENT_SECRET", ""),
        "redirect_uri": os.environ.get("KUAISHOU_REDIRECT_URI", ""),
        "authorize_url": "https://open.kuaishou.com/oauth2/authorize",
        "token_url": "https://open.kuaishou.com/oauth2/access_token",
        "refresh_url": "https://open.kuaishou.com/oauth2/refresh_token",
        "scope": "user_info,photo.publish",
    },
}


@router.get("/api/oauth/{platform}/authorize")
async def oauth_authorize(platform: str, request: Request):
    """返回 OAuth 授权 URL"""
    if platform not in OAUTH_CONFIGS:
        return JSONResponse({"error": f"Unsupported platform: {platform}"}, status_code=400)
    cfg = OAUTH_CONFIGS[platform]
    if not cfg["client_key"]:
        return JSONResponse({
            "error": f"{platform.upper()}_CLIENT_KEY not configured",
            "setup_required": True,
        }, status_code=503)
    import secrets as _secrets
    state = _secrets.token_hex(16)
    if platform == "douyin":
        url = (
            f"{cfg['authorize_url']}?client_key={cfg['client_key']}"
            f"&response_type=code&scope={cfg['scope']}"
            f"&redirect_uri={cfg['redirect_uri']}&state={state}"
        )
    else:
        url = (
            f"{cfg['authorize_url']}?app_id={cfg['client_key']}"
            f"&response_type=code&scope={cfg['scope']}"
            f"&redirect_uri={cfg['redirect_uri']}&state={state}"
        )
    return JSONResponse({"url": url, "state": state})


@router.get("/api/oauth/{platform}/callback")
async def oauth_callback(platform: str, code: str = "", state: str = "", error: str = ""):
    """处理 OAuth 回调，用 code 换取 access_token"""
    if error:
        return JSONResponse({"error": error}, status_code=400)
    if platform not in OAUTH_CONFIGS:
        return JSONResponse({"error": f"Unsupported platform: {platform}"}, status_code=400)
    if not code:
        return JSONResponse({"error": "Missing authorization code"}, status_code=400)

    cfg = OAUTH_CONFIGS[platform]
    if not cfg["client_key"] or not cfg["client_secret"]:
        return JSONResponse({"error": f"{platform.upper()} credentials not configured"}, status_code=503)

    try:
        import aiohttp as _aiohttp
        if platform == "douyin":
            params = {
                "client_key": cfg["client_key"],
                "client_secret": cfg["client_secret"],
                "code": code,
                "grant_type": "authorization_code",
            }
        else:
            params = {
                "app_id": cfg["client_key"],
                "app_secret": cfg["client_secret"],
                "code": code,
                "grant_type": "authorization_code",
            }
        async with _aiohttp.ClientSession() as session:
            async with session.get(cfg["token_url"], params=params) as resp:
                data = await resp.json()

        if platform == "douyin":
            token_data = data.get("data", {})
            access_token = token_data.get("access_token", "")
            refresh_token = token_data.get("refresh_token", "")
            openid = token_data.get("open_id", "")
            expires_in = token_data.get("expires_in", 86400 * 15)
        else:
            access_token = data.get("access_token", "")
            refresh_token = data.get("refresh_token", "")
            openid = data.get("open_id", "")
            expires_in = data.get("expires_in", 86400 * 15)

        if not access_token:
            return JSONResponse({"error": "Failed to get access_token", "detail": data}, status_code=400)

        expires_at = time.time() + expires_in
        # 存储凭据：优先使用登录用户的 user_id，否则用 "default"
        user_id = _get_request_user_id(None)
        db.save_credential(user_id, platform, access_token, refresh_token, openid, "", expires_at)
        # 同步到分发管理器
        dm = _get_distribute_manager()
        dm.set_credentials(platform, {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "openid": openid,
            "expires_at": expires_at,
        })
        logger.info(f"OAuth success: {platform} (openid={openid})")
        return JSONResponse({"ok": True, "platform": platform, "openid": openid, "expires_at": expires_at})
    except Exception as e:
        logger.error(f"OAuth callback error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/oauth/{platform}/refresh")
async def oauth_refresh(platform: str, request: Request):
    """刷新 access_token"""
    if platform not in OAUTH_CONFIGS:
        return JSONResponse({"error": f"Unsupported platform: {platform}"}, status_code=400)
    cfg = OAUTH_CONFIGS[platform]
    user_id = _get_request_user_id(request)
    cred = db.get_credential(user_id, platform)
    if not cred or not cred.get("refresh_token"):
        return JSONResponse({"error": "No refresh_token, re-authorize required"}, status_code=400)

    try:
        import aiohttp as _aiohttp
        if platform == "douyin":
            params = {
                "client_key": cfg["client_key"],
                "refresh_token": cred["refresh_token"],
                "grant_type": "refresh_token",
            }
        else:
            params = {
                "app_id": cfg["client_key"],
                "app_secret": cfg["client_secret"],
                "refresh_token": cred["refresh_token"],
                "grant_type": "refresh_token",
            }
        async with _aiohttp.ClientSession() as session:
            async with session.get(cfg["refresh_url"], params=params) as resp:
                data = await resp.json()

        if platform == "douyin":
            token_data = data.get("data", {})
            access_token = token_data.get("access_token", "")
            expires_in = token_data.get("expires_in", 86400 * 15)
        else:
            access_token = data.get("access_token", "")
            expires_in = data.get("expires_in", 86400 * 15)

        if not access_token:
            return JSONResponse({"error": "Refresh failed", "detail": data}, status_code=400)

        expires_at = time.time() + expires_in
        db.save_credential(user_id, platform, access_token, cred.get("refresh_token", ""),
                           cred.get("openid", ""), cred.get("display_name", ""), expires_at)
        dm = _get_distribute_manager()
        dm.set_credentials(platform, {"access_token": access_token, "openid": cred.get("openid", ""), "expires_at": expires_at})
        return JSONResponse({"ok": True, "expires_at": expires_at})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.delete("/api/oauth/{platform}/revoke")
async def oauth_revoke(platform: str, request: Request):
    """撤销平台授权"""
    if platform not in OAUTH_CONFIGS:
        return JSONResponse({"error": f"Unsupported platform: {platform}"}, status_code=400)
    user_id = _get_request_user_id(request)
    db.delete_credential(user_id, platform)
    dm = _get_distribute_manager()
    dm._credentials.pop(platform, None)
    return JSONResponse({"ok": True})


@router.get("/api/oauth/{platform}/status")
async def oauth_status(platform: str, request: Request):
    """查询平台授权状态"""
    if platform not in OAUTH_CONFIGS and platform not in ("bilibili", "weixinvideo"):
        return JSONResponse({"error": f"Unsupported platform: {platform}"}, status_code=400)
    if platform in ("bilibili", "weixinvideo"):
        return JSONResponse({"authorized": True, "assist_mode": True})
    user_id = _get_request_user_id(request)
    cred = db.get_credential(user_id, platform)
    if not cred or not cred.get("access_token"):
        cfg = OAUTH_CONFIGS.get(platform, {})
        return JSONResponse({
            "authorized": False,
            "setup_required": not cfg.get("client_key"),
        })
    expires_at = cred.get("expires_at", 0)
    expired = expires_at > 0 and expires_at < time.time()
    return JSONResponse({
        "authorized": not expired,
        "expired": expired,
        "openid": cred.get("openid", ""),
        "display_name": cred.get("display_name", ""),
        "expires_at": expires_at,
    })


