"""录制流管理路由 - Models CRUD + 启停控制"""
import logging

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

router = APIRouter(prefix="/api", tags=["streams"])
logger = logging.getLogger("server")

_MODEL_SETTINGS_FIELDS = {
    "quality", "auto_merge", "h265_transcode", "filename_template",
    "split_by_duration_minutes", "split_by_size_mb", "session_reuse_seconds", "notes"
}


class AddModelRequest(BaseModel):
    url: str


@router.get("/models")
async def get_models():
    from server import manager
    return JSONResponse(manager.get_all_info())


@router.post("/models")
async def add_model(req: AddModelRequest):
    from server import manager, db, broadcast, apply_settings_to_recorders, save_config
    from quota import QuotaManager

    qm = QuotaManager(db)
    tier_info = qm.get_tier_info("default")
    max_models = tier_info["features"].get("max_models", 3)
    current_count = len(manager.recorders)
    if max_models > 0 and current_count >= max_models:
        return JSONResponse(
            {"error": f"已达到套餐主播数上限 ({current_count}/{max_models})，请升级套餐",
             "quota_exceeded": True, "limit": max_models},
            status_code=429,
        )
    info = manager.add_model(req.url)
    key = info.username
    await manager.start_model(key)
    apply_settings_to_recorders()
    save_config()
    await broadcast({"type": "model_added", "data": info.to_dict()})
    return JSONResponse(info.to_dict())


@router.delete("/models/{username}")
async def remove_model(username: str):
    from server import manager, db, broadcast, save_config

    await manager.stop_model(username)
    manager.remove_model(username)
    try:
        db.delete_model(username)
    except Exception as e:
        logger.warning(f"Failed to delete model from DB: {e}")
    save_config()
    await broadcast({"type": "model_removed", "data": {"username": username}})
    return JSONResponse({"ok": True})


@router.post("/models/{username}/start")
async def start_model(username: str):
    from server import manager
    await manager.start_model(username)
    return JSONResponse({"ok": True})


@router.post("/models/{username}/stop")
async def stop_model(username: str):
    from server import manager
    await manager.stop_model(username)
    return JSONResponse({"ok": True})


@router.post("/start-all")
async def start_all():
    from server import manager
    await manager.start_all()
    return JSONResponse({"ok": True})


@router.post("/stop-all")
async def stop_all():
    from server import manager
    await manager.stop_all()
    return JSONResponse({"ok": True})


@router.post("/models/{username}/auto-merge")
async def toggle_model_auto_merge(username: str, req: dict):
    from server import manager, broadcast

    rec = manager.recorders.get(username)
    if not rec:
        return JSONResponse({"error": "主播不存在"}, status_code=404)
    enabled = req.get("auto_merge", True)
    rec.auto_merge = enabled
    rec.info.auto_merge = enabled
    await broadcast({"type": "model_update", "data": rec.info.to_dict()})
    return JSONResponse({"ok": True, "auto_merge": enabled})


@router.get("/models/{username}/settings")
async def get_model_settings(username: str):
    from server import manager

    rec = manager.recorders.get(username)
    if not rec:
        return JSONResponse({"error": "主播不存在"}, status_code=404)
    settings = {
        "quality": rec.quality,
        "auto_merge": rec.auto_merge,
        "h265_transcode": getattr(rec, "_per_model_h265", None),
        "filename_template": getattr(rec, "_per_model_filename_template", None),
        "split_by_duration_minutes": getattr(rec, "_per_model_split_duration", None),
        "split_by_size_mb": getattr(rec, "_per_model_split_size", None),
        "session_reuse_seconds": getattr(rec, "_per_model_session_reuse", None),
        "notes": getattr(rec, "_notes", ""),
    }
    return JSONResponse(settings)


@router.put("/models/{username}/settings")
async def update_model_settings(username: str, req: dict):
    from server import manager, save_config, broadcast

    rec = manager.recorders.get(username)
    if not rec:
        return JSONResponse({"error": "主播不存在"}, status_code=404)
    unknown = set(req.keys()) - _MODEL_SETTINGS_FIELDS
    if unknown:
        return JSONResponse({"error": f"不支持的配置项: {unknown}"}, status_code=400)

    if "quality" in req:
        rec.quality = req["quality"]
        rec.info.quality = req["quality"]
    if "auto_merge" in req:
        rec.auto_merge = bool(req["auto_merge"])
        rec.info.auto_merge = bool(req["auto_merge"])
    if "h265_transcode" in req:
        rec._per_model_h265 = req["h265_transcode"]
    if "filename_template" in req:
        rec._per_model_filename_template = req["filename_template"] or None
    if "split_by_duration_minutes" in req:
        v = req["split_by_duration_minutes"]
        rec._per_model_split_duration = v
        rec.split_by_duration = (v or 0) * 60
    if "split_by_size_mb" in req:
        v = req["split_by_size_mb"]
        rec._per_model_split_size = v
        rec.split_by_size = (v or 0) * 1024 * 1024
    if "session_reuse_seconds" in req:
        v = req["session_reuse_seconds"]
        rec._per_model_session_reuse = v
        if v is not None:
            rec.session_reuse_window = v
    if "notes" in req:
        rec._notes = req["notes"]

    save_config()
    await broadcast({"type": "model_update", "data": rec.info.to_dict()})
    return JSONResponse({"ok": True})


@router.post("/models/{username}/schedule")
async def set_model_schedule(username: str, req: dict):
    from server import manager, save_config

    rec = manager.recorders.get(username)
    if not rec:
        return JSONResponse({"error": "主播不存在"}, status_code=404)
    schedule = req.get("schedule")
    if schedule:
        if not isinstance(schedule, dict):
            return JSONResponse({"error": "schedule 格式无效"}, status_code=400)
        if "start" in schedule and not isinstance(schedule["start"], str):
            return JSONResponse({"error": "start 必须是时间字符串 (HH:MM)"}, status_code=400)
        if "end" in schedule and not isinstance(schedule["end"], str):
            return JSONResponse({"error": "end 必须是时间字符串 (HH:MM)"}, status_code=400)
        if "days" in schedule and not isinstance(schedule["days"], list):
            return JSONResponse({"error": "days 必须是数组"}, status_code=400)
    rec.schedule = schedule
    save_config()
    return JSONResponse({"ok": True, "schedule": rec.schedule})


@router.get("/models/{username}/schedule")
async def get_model_schedule(username: str):
    from server import manager

    rec = manager.recorders.get(username)
    if not rec:
        return JSONResponse({"error": "主播不存在"}, status_code=404)
    return JSONResponse({"schedule": rec.schedule})


@router.post("/models/{username}/quality")
async def set_model_quality(username: str, req: dict):
    from server import manager, save_config, broadcast

    rec = manager.recorders.get(username)
    if not rec:
        return JSONResponse({"error": "主播不存在"}, status_code=404)
    q = req.get("quality", "best")
    allowed = {"best", "1080p", "720p", "480p", "audio_only", "origin", "worst"}
    if q not in allowed:
        return JSONResponse({"error": f"无效的质量选项，可选: {', '.join(sorted(allowed))}"}, status_code=400)
    rec.quality = q
    rec.info.quality = q
    save_config()
    await broadcast({"type": "model_update", "data": rec.info.to_dict()})
    return JSONResponse({"ok": True, "quality": q})


@router.post("/models/{username}/cookies")
async def set_model_cookies(username: str, req: dict):
    from server import manager

    rec = manager.recorders.get(username)
    if not rec:
        return JSONResponse({"error": "主播不存在"}, status_code=404)
    rec.custom_cookies = req.get("cookies", "")
    return JSONResponse({"ok": True})


@router.post("/models/{username}/stream-url")
async def set_model_stream_url(username: str, req: dict):
    from server import manager

    rec = manager.recorders.get(username)
    if not rec:
        return JSONResponse({"error": "主播不存在"}, status_code=404)
    rec.custom_stream_url = req.get("stream_url", "")
    logger.info(f"[{username}] Custom stream URL set: {rec.custom_stream_url[:80] if rec.custom_stream_url else 'cleared'}")
    return JSONResponse({"ok": True})
