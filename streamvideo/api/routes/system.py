"""System API 路由 - 系统配置、Whisper 检查、配置导入导出"""
import asyncio
import logging
import time
import json
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import JSONResponse, Response

logger = logging.getLogger("system")

router = APIRouter()

_start_time = time.time()
_tool_versions_cache: dict = {}
_tool_versions_ts: float = 0
_TOOL_VERSIONS_TTL: float = 300

# 全局依赖
db = None
manager = None
app_settings = None
RECORDINGS_DIR = None
DEFAULT_SETTINGS = None
apply_settings_to_recorders = None
save_config = None
broadcast = None


def init_system_router(database, recorder_manager, settings, recordings_dir, default_settings, apply_fn, save_fn, ws_broadcast):
    """初始化路由依赖"""
    global db, manager, app_settings, RECORDINGS_DIR, DEFAULT_SETTINGS, apply_settings_to_recorders, save_config, broadcast
    db = database
    manager = recorder_manager
    app_settings = settings
    RECORDINGS_DIR = recordings_dir
    DEFAULT_SETTINGS = default_settings
    apply_settings_to_recorders = apply_fn
    save_config = save_fn
    broadcast = ws_broadcast


@router.get("/api/health")
async def health_check():
    """Lightweight health check for Docker/orchestrators"""
    return JSONResponse({
        "status": "ok",
        "recordings": sum(1 for r in manager.recorders.values() if r.state.value == "recording"),
        "models": len(manager.recorders),
        "uptime": int(time.time() - _start_time),
    })


@router.get("/api/system")
async def get_system():
    """获取系统信息"""
    import platform
    import shutil

    global _tool_versions_cache, _tool_versions_ts
    if time.time() - _tool_versions_ts > _TOOL_VERSIONS_TTL:
        async def _get_version(cmd):
            try:
                proc = await asyncio.create_subprocess_exec(
                    *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
                )
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=5)
                return stdout.decode().strip().split('\n')[0][:50]
            except Exception:
                return None

        ffmpeg_v, streamlink_v, ytdlp_v = await asyncio.gather(
            _get_version(["ffmpeg", "-version"]),
            _get_version(["streamlink", "--version"]),
            _get_version(["yt-dlp", "--version"]),
        )
        _tool_versions_cache = {
            "ffmpeg_version": ffmpeg_v.replace("ffmpeg version ", "") if ffmpeg_v else "-",
            "ffmpeg_available": shutil.which("ffmpeg") is not None,
            "streamlink_version": streamlink_v.replace("streamlink ", "") if streamlink_v else "-",
            "streamlink_available": shutil.which("streamlink") is not None,
            "ytdlp_version": ytdlp_v or "-",
            "ytdlp_available": shutil.which("yt-dlp") is not None,
        }
        _tool_versions_ts = time.time()

    # Playwright check
    pw_available = False
    try:
        import importlib
        importlib.import_module("playwright")
        pw_available = True
    except Exception:
        logger.debug("suppressed exception", exc_info=True)

    uptime = ""
    try:
        import psutil
        boot = psutil.boot_time()
        up = time.time() - boot
        h, m = int(up // 3600), int((up % 3600) // 60)
        uptime = f"{h}h {m}m"
    except Exception:
        uptime = "-"

    return JSONResponse({
        "python_version": platform.python_version(),
        **_tool_versions_cache,
        "playwright_available": pw_available,
        "uptime": uptime,
        "models_count": len(manager.recorders),
    })


@router.get("/api/system/whisper")
async def check_whisper():
    """检查 Whisper 是否可用"""
    try:
        from streamvideo.core.processor.subtitle_gen import is_whisper_available
        available = is_whisper_available()
        return JSONResponse({"available": available})
    except Exception:
        return JSONResponse({"available": False})


@router.post("/api/settings")
async def update_settings(req: dict):
    """更新设置"""
    allowed = set(DEFAULT_SETTINGS.keys())
    for k, v in req.items():
        if k in allowed:
            app_settings[k] = v
    apply_settings_to_recorders()
    save_config()
    return JSONResponse({"ok": True, **app_settings})

@router.get("/api/config/export")
async def export_config():
    """导出完整配置（settings + models + schedules）"""
    models_data = []
    for key, rec in manager.recorders.items():
        m = {
            "username": rec.info.username,
            "url": rec.info.live_url or rec.identifier,
            "platform": rec.info.platform,
            "quality": rec.quality,
            "auto_merge": rec.auto_merge,
            "schedule": rec.schedule,
            "split_by_size_mb": int(rec.split_by_size / 1024 / 1024) if rec.split_by_size else 0,
            "split_by_duration_minutes": int(rec.split_by_duration / 60) if rec.split_by_duration else 0,
            "h265_transcode": getattr(rec, "_per_model_h265", None),
            "filename_template": getattr(rec, "_per_model_filename_template", None),
            "custom_cookies": rec.custom_cookies or "",
            "custom_stream_url": rec.custom_stream_url or "",
            "session_reuse_seconds": getattr(rec, "_per_model_session_reuse", 0) or 0,
            "notes": getattr(rec, "_notes", "") or "",
        }
        models_data.append(m)

    payload = {
        "version": "1.0",
        "exported_at": time.time(),
        "settings": {k: v for k, v in app_settings.items()},
        "models": models_data,
    }
    import datetime
    filename = f"streamvideo_config_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    content = json.dumps(payload, ensure_ascii=False, indent=2)
    return Response(
        content=content,
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/api/config/import")
async def import_config(req: dict):
    """导入配置（合并或覆盖）"""
    mode = req.get("mode", "merge")  # merge | overwrite
    data = req.get("data")
    if not data or not isinstance(data, dict):
        return JSONResponse({"error": "data 字段缺失或格式错误"}, status_code=400)

    version = data.get("version", "")
    if not str(version).startswith(("1.", "2")):
        return JSONResponse({"error": f"不支持的配置版本: {version}"}, status_code=400)

    imported_settings = 0
    imported_models = 0
    errors = []

    # 导入 settings
    settings_data = data.get("settings", {})
    allowed = set(DEFAULT_SETTINGS.keys())
    for k, v in settings_data.items():
        if k in allowed:
            app_settings[k] = v
            imported_settings += 1
    apply_settings_to_recorders()
    save_config()

    # 导入 models
    models_data = data.get("models", [])
    for m in models_data:
        url = m.get("url", "")
        if not url:
            errors.append("跳过：缺少 url 字段")
            continue
        username = m.get("username", "")
        if mode == "merge" and username and username in manager.recorders:
            errors.append(f"跳过已存在主播: {username}")
            continue
        try:
            info = manager.add_model(url)
            rec = manager.recorders.get(info.username)
            if rec:
                rec.quality = m.get("quality", "best")
                rec.auto_merge = m.get("auto_merge", True)
                rec.schedule = m.get("schedule")
                split_size = m.get("split_by_size_mb", 0)
                split_dur = m.get("split_by_duration_minutes", 0)
                rec.split_by_size = split_size * 1024 * 1024 if split_size else 0
                rec.split_by_duration = split_dur * 60 if split_dur else 0
                if m.get("h265_transcode") is not None:
                    rec._per_model_h265 = m["h265_transcode"]
                if m.get("filename_template"):
                    rec._per_model_filename_template = m["filename_template"]
                if m.get("custom_cookies"):
                    rec.custom_cookies = m["custom_cookies"]
                if m.get("custom_stream_url"):
                    rec.custom_stream_url = m["custom_stream_url"]
                if m.get("session_reuse_seconds") is not None:
                    rec._per_model_session_reuse = m["session_reuse_seconds"]
                    if m["session_reuse_seconds"] is not None:
                        rec.session_reuse_window = m["session_reuse_seconds"]
                if m.get("notes"):
                    rec._notes = m["notes"]
            imported_models += 1
        except Exception as e:
            errors.append(f"导入 {url} 失败: {e}")

    save_config()
    await broadcast({"type": "models_update"})
    return JSONResponse({
        "ok": True,
        "imported_settings": imported_settings,
        "imported_models": imported_models,
        "errors": errors,
    })

