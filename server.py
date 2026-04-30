"""
直播监控录制系统 - Web 服务器
FastAPI + WebSocket 实时推送 + REST API
"""

import asyncio
import json
import logging
import os
import subprocess
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Depends, HTTPException
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional

from streamvideo.core.recorder import ModelInfo, RecorderManager, RecordingSession, RecordingState
from streamvideo.infrastructure.database.database import Database
from streamvideo.infrastructure.messaging.task_queue import task_queue, Priority


def _safe_username(username: str) -> bool:
    """验证 username 不含路径遍历字符"""
    return ".." not in username and "/" not in username and "\\" not in username and username.strip() != ""


def validate_username(username: str) -> str:
    """FastAPI dependency: 验证 username 路径安全"""
    if not _safe_username(username):
        raise HTTPException(status_code=400, detail="Invalid username")
    return username


def validate_filename(filename: str) -> str:
    """FastAPI dependency: 验证 filename 路径安全"""
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    return filename

# 日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("server")

# 日志环形缓冲区（供 API 查询）
log_buffer: deque[str] = deque(maxlen=500)


class BufferHandler(logging.Handler):
    def emit(self, record):
        log_buffer.append(self.format(record))


_buf_handler = BufferHandler()
_buf_handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s %(message)s", datefmt="%H:%M:%S"))
logging.getLogger().addHandler(_buf_handler)

# 配置
BASE_DIR = Path(__file__).parent
RECORDINGS_DIR = str(BASE_DIR / "recordings")
PROXY = os.environ.get("SV_PROXY", "http://127.0.0.1:7890")
CONFIG_FILE = BASE_DIR / "config.json"

# 工具版本缓存（5 分钟 TTL，避免每次 /api/system 都 fork 子进程）
_tool_versions_cache: dict = {}
_tool_versions_ts: float = 0.0
_TOOL_VERSIONS_TTL = 300

# 默认设置
DEFAULT_SETTINGS = {
    "auto_merge": True,
    "merge_gap_minutes": 15,
    "auto_delete_originals": True,
    "min_segment_size_kb": 500,
    "smart_rename": False,
    "h265_transcode": False,
    "cloud_upload": {},
    "webhooks": [],
    "merge_timeout_minutes": 240,
    "session_reuse_seconds": 30,
    "retention_days": 0,
    # Phase 3 新增
    "split_by_size_mb": 0,        # 0=禁用，按文件大小自动分割（MB）
    "split_by_duration_minutes": 0,  # 0=禁用，按时长自动分割（分钟）
    "post_process_script": "",     # 录后脚本路径
    "filename_template": "{username}_{date}_{duration}_merged",  # 文件名模板
    # V2.0: 高光检测 + 片段生成
    "danmaku_capture": True,
    "highlight_auto_detect": False,
    "highlight_keywords": ["上链接", "秒杀", "666", "抢到了", "买买买"],
    "highlight_min_score": 0.6,
    "highlight_min_duration": 15,
    "highlight_max_duration": 60,
    "highlight_padding_before": 5,
    "highlight_padding_after": 3,
    "clip_format": "vertical",
    "clip_resolution": "1080x1920",
    "clip_watermark": "",
    "clip_danmaku_overlay": True,
    # 磁盘预警阈值（单位 MB）
    "disk_warn_critical_mb": 500,
    "disk_warn_error_mb": 5120,
    "disk_warn_warning_mb": 10240,
    # 防熄屏模式: "always"=始终防熄屏, "recording"=录制时防熄屏, "never"=不干预
    "prevent_sleep_mode": "recording",
}

# 运行时设置（从 config.json 加载）
app_settings: dict = {**DEFAULT_SETTINGS}

# SQLite 数据库
db = Database(str(BASE_DIR / "streamvideo.db"))

# WebSocket 连接管理
ws_clients: set[WebSocket] = set()

# 事件环形缓冲区（用于 WebSocket 断线恢复）
_event_buffer: deque[dict] = deque(maxlen=100)


async def broadcast(data: dict):
    """广播消息给所有 WebSocket 客户端，并缓存到事件缓冲区"""
    data["_ts"] = time.time()
    _event_buffer.append(data)
    if not ws_clients:
        return
    msg = json.dumps(data, ensure_ascii=False)
    dead = set()
    for ws in list(ws_clients):
        try:
            await ws.send_text(msg)
        except Exception:
            dead.add(ws)
    ws_clients.difference_update(dead)


async def on_state_change(info: ModelInfo):
    """录制状态变化回调"""
    await broadcast({"type": "model_update", "data": info.to_dict()})


async def on_merge_update(username: str, merge_id: str, status: str, **kwargs):
    """合并状态变化回调"""
    data = {"username": username, "merge_id": merge_id, "status": status, **kwargs}
    if status == "auto_merge_done":
        msg_type = "auto_merge_done"
    elif status == "done":
        msg_type = "merge_done"
    elif status == "merge_confirm_required":
        msg_type = "merge_confirm_required"
    elif status == "merge_failed_permanent":
        msg_type = "merge_failed_permanent"
    elif status == "merge_low_confidence":
        msg_type = "merge_low_confidence"
    else:
        msg_type = "merge_error"
    await broadcast({"type": msg_type, "data": data})


async def on_merge_progress(username: str, merge_id: str, progress: float, message: str):
    """合并进度回调"""
    await broadcast({"type": "merge_progress", "data": {
        "username": username, "merge_id": merge_id,
        "progress": progress, "message": message,
    }})


async def on_disk_warning(username: str, free_mb: int, critical: bool):
    """磁盘空间警告回调"""
    await broadcast({"type": "disk_warning", "data": {
        "username": username, "free_mb": free_mb, "critical": critical,
    }})


# 全局 manager
manager = RecorderManager(
    output_dir=RECORDINGS_DIR,
    proxy=PROXY,
    on_state_change=on_state_change,
    db=db,
)
manager._merge_callback = on_merge_update
manager._merge_progress_callback = on_merge_progress
manager._disk_warning_callback = on_disk_warning


def load_config():
    """加载配置（优先 SQLite，fallback JSON）"""
    # 从 SQLite 加载设置
    db_settings = db.get_settings()
    if db_settings:
        for key in DEFAULT_SETTINGS:
            if key in db_settings:
                app_settings[key] = db_settings[key]

    # 从 SQLite 加载主播列表
    db_models = db.get_models()
    if db_models:
        return {"models": [{"url": m["url"], "name": m["display_name"] or m["username"],
                            "schedule": m.get("schedule"), "quality": m.get("quality", "best")}
                           for m in db_models], **app_settings}

    # Fallback: JSON
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE) as f:
                data = json.load(f)
            for key in DEFAULT_SETTINGS:
                if key in data:
                    app_settings[key] = data[key]
            # 迁移到 SQLite
            db.set_settings(app_settings)
            return data
        except Exception:
            logger.debug("suppressed exception", exc_info=True)
    return {"models": [], **DEFAULT_SETTINGS}


def save_config():
    """保存配置到 SQLite + JSON 备份"""
    # 保存到 SQLite
    db.set_settings(app_settings)
    current_usernames = set()
    for key, rec in manager.recorders.items():
        db.upsert_model(rec.info.username, rec.info.live_url or rec.identifier,
                        platform=rec.info.platform, display_name=rec.info.username,
                        quality=rec.quality, auto_merge=rec.auto_merge,
                        schedule=rec.schedule)
        current_usernames.add(rec.info.username)

    # 同步删除 SQLite 中不再存在的模型
    try:
        db_models = db.get_models()
        for m in db_models:
            if m["username"] not in current_usernames:
                db.delete_model(m["username"])
                logger.info(f"Cleaned stale model from DB: {m['username']}")
    except Exception as e:
        logger.warning(f"Failed to sync-delete stale models: {e}")

    # JSON 备份
    models = []
    for key, rec in manager.recorders.items():
        item = {
            "url": rec.info.live_url or rec.identifier,
            "name": rec.info.username,
        }
        if rec.schedule:
            item["schedule"] = rec.schedule
        if rec.quality != "best":
            item["quality"] = rec.quality
        # per-model 配置保存
        if getattr(rec, "_per_model_h265", None) is not None:
            item["h265_transcode"] = rec._per_model_h265
        if getattr(rec, "_per_model_filename_template", None) is not None:
            item["filename_template"] = rec._per_model_filename_template
        if getattr(rec, "_per_model_split_duration", None) is not None:
            item["split_by_duration_minutes"] = rec._per_model_split_duration
        if getattr(rec, "_per_model_split_size", None) is not None:
            item["split_by_size_mb"] = rec._per_model_split_size
        if getattr(rec, "_per_model_session_reuse", None) is not None:
            item["session_reuse_seconds"] = rec._per_model_session_reuse
        if getattr(rec, "_notes", ""):
            item["notes"] = rec._notes
        models.append(item)
    data = {"models": models, **app_settings}
    with open(CONFIG_FILE, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await remux_stale_raw_files()

    config = load_config()
    for item in config.get("models", []):
        # 兼容字符串和 dict 格式
        if isinstance(item, dict):
            url = item.get("url", "")
            saved_name = item.get("name", "")
            saved_schedule = item.get("schedule")
            saved_quality = item.get("quality")
            # per-model 配置
            saved_per_model = {k: item[k] for k in (
                "h265_transcode", "filename_template", "split_by_duration_minutes",
                "split_by_size_mb", "session_reuse_seconds", "notes",
            ) if k in item}
        else:
            url = item
            saved_name = ""
            saved_schedule = None
            saved_per_model = {}
        if not url:
            continue
        info = manager.add_model(url)
        # 恢复保存的显示名（meta.json 优先，config 次之）
        if saved_name and info.username != saved_name:
            rec = manager.recorders.get(info.username)
            if rec and not rec.info.last_online:  # meta 未恢复时用 config 的名字
                old_key = info.username
                rec.info.username = saved_name
                # 更新 recorders dict 的 key
                if old_key != saved_name and old_key in manager.recorders:
                    manager.recorders[saved_name] = manager.recorders.pop(old_key)
        # 恢复定时计划和质量
        final_name = saved_name or info.username
        rec = manager.recorders.get(final_name)
        if rec:
            if saved_schedule:
                rec.schedule = saved_schedule
            if saved_quality:
                rec.quality = saved_quality
            # 恢复 per-model 配置
            if "h265_transcode" in saved_per_model:
                rec._per_model_h265 = saved_per_model["h265_transcode"]
            if "filename_template" in saved_per_model:
                rec._per_model_filename_template = saved_per_model["filename_template"]
            if "split_by_duration_minutes" in saved_per_model:
                v = saved_per_model["split_by_duration_minutes"]
                rec._per_model_split_duration = v
                rec.split_by_duration = (v or 0) * 60
            if "split_by_size_mb" in saved_per_model:
                v = saved_per_model["split_by_size_mb"]
                rec._per_model_split_size = v
                rec.split_by_size = (v or 0) * 1024 * 1024
            if "session_reuse_seconds" in saved_per_model:
                v = saved_per_model["session_reuse_seconds"]
                rec._per_model_session_reuse = v
                if v is not None:
                    rec.session_reuse_window = v
            if "notes" in saved_per_model:
                rec._notes = saved_per_model["notes"]
                rec.info.quality = saved_quality
    apply_settings_to_recorders()
    await manager.start_all()
    logger.info(f"已启动 {len(manager.recorders)} 个主播监控 (auto_merge={app_settings['auto_merge']})")

    # 启动时自动合并遗留片段
    if app_settings["auto_merge"]:
        asyncio.create_task(startup_auto_merge())

    # 录制保留策略定时任务
    if app_settings.get("retention_days", 0) > 0:
        asyncio.create_task(_retention_cleanup_loop())

    # 过期 session 定期清理（每小时）
    asyncio.create_task(_session_cleanup_loop())
    # 磁盘空间定时检查（每 5 分钟）
    asyncio.create_task(_disk_check_loop())
    # 防熄屏守护（每 30 秒动态管理 caffeinate）
    asyncio.create_task(_sleep_guard_loop())
    # 启动任务队列
    task_queue.start()

    yield

    await manager.stop_all()
    save_config()
    logger.info("服务已关闭")


async def startup_auto_merge():
    """启动时恢复未完成的会话并触发合并（使用 SQLite）"""
    await asyncio.sleep(3)  # 等待服务完全启动
    rec_path = Path(RECORDINGS_DIR)
    if not rec_path.exists():
        return

    # 1. 从 SQLite 恢复 active/merging 会话
    for d in rec_path.iterdir():
        if not d.is_dir() or d.name in ("thumbs", "logs"):
            continue
        username = d.name
        try:
            db_sessions = db.get_sessions(username)
            if not db_sessions:
                continue
            sessions = [RecordingSession.from_dict(s) for s in db_sessions]
            changed = False
            for s in sessions:
                if s.status == "active":
                    rec = manager.recorders.get(username)
                    is_active = rec and rec.info.state.value in ("recording", "reconnecting")
                    if not is_active:
                        s.status = "ended"
                        s.ended_at = time.time()
                        changed = True
                        logger.info(f"[{username}] Recovered orphaned session: {s.session_id}")
                elif s.status == "merging":
                    merge_age_ref = s.merge_started_at or s.ended_at or 0
                    if merge_age_ref and (time.time() - merge_age_ref) > 1800:
                        s.status = "ended"
                        s.merge_error = ""
                        s.merge_started_at = 0
                        changed = True
                        # 清理残留文件
                        for f in d.iterdir():
                            if f.name.endswith("_merged.mp4") and f.stat().st_size == 0:
                                f.unlink()
                                logger.info(f"[{username}] Cleaned up empty merge output: {f.name}")
                            elif f.name.startswith(".concat_") and f.name.endswith(".txt"):
                                f.unlink()
                                logger.info(f"[{username}] Cleaned up concat file: {f.name}")
                        logger.info(f"[{username}] Reset stale merging session: {s.session_id}")
            if changed:
                manager._persist_sessions(username, sessions)
                rec = manager.recorders.get(username)
                if rec:
                    rec._sessions = sessions
        except Exception as e:
            logger.error(f"Startup session recovery error for {username}: {e}")

    # 2. 触发所有主播的自动合并（session-aware）
    for username in list(manager.recorders.keys()):
        try:
            await manager.auto_merge_for_model(username)
        except Exception as e:
            logger.error(f"Startup auto-merge error for {username}: {e}")

    # 3. 也扫描不在 recorders 中但有录制文件的目录
    for d in rec_path.iterdir():
        if d.is_dir() and d.name not in ("thumbs", "logs") and d.name not in manager.recorders:
            files = list(d.glob("*.mp4"))
            non_raw = [f for f in files if ".raw." not in f.name and "_merged" not in f.name]
            if len(non_raw) >= 2:
                try:
                    await manager.auto_merge_for_model(d.name)
                except Exception as e:
                    logger.error(f"Startup auto-merge error for {d.name}: {e}")


async def _retention_cleanup_loop():
    """每日清理超过保留天数的录制文件"""
    while True:
        await asyncio.sleep(86400)  # 24 小时
        days = app_settings.get("retention_days", 0)
        if days <= 0:
            continue
        try:
            await _do_retention_cleanup(days)
        except Exception as e:
            logger.error(f"Retention cleanup error: {e}")


async def _session_cleanup_loop():
    """每小时清理过期的用户 session"""
    while True:
        await asyncio.sleep(3600)
        try:
            from streamvideo.core.auth.manager import AuthManager
            AuthManager(db).cleanup_expired_sessions()
        except Exception as e:
            logger.error(f"Session cleanup error: {e}")
        try:
            cleaned = db.cleanup_expired_merge_queue(days=7)
            if cleaned > 0:
                logger.info(f"Cleaned {cleaned} expired merge_queue entries")
        except Exception as e:
            logger.error(f"Merge queue cleanup error: {e}")


async def _disk_check_loop():
    """每 5 分钟检查磁盘空间，推送告警"""
    import shutil as _shutil
    _last_level = "ok"
    while True:
        await asyncio.sleep(300)
        try:
            free = _shutil.disk_usage(RECORDINGS_DIR).free
            free_mb = int(free / 1024 / 1024)
            critical_mb = app_settings.get("disk_warn_critical_mb", 500)
            error_mb = app_settings.get("disk_warn_error_mb", 5120)
            warning_mb = app_settings.get("disk_warn_warning_mb", 10240)
            if free_mb < critical_mb:
                level = "critical"
            elif free_mb < error_mb:
                level = "error"
            elif free_mb < warning_mb:
                level = "warning"
            else:
                level = "ok"
            if level != "ok" and level != _last_level:
                await broadcast({"type": "disk_warning", "data": {
                    "free_mb": free_mb,
                    "critical": level == "critical",
                    "level": level,
                }})
            _last_level = level
        except Exception as e:
            logger.debug(f"Disk check error: {e}")


async def _do_retention_cleanup(days: int):
    """执行保留策略清理"""
    cutoff = time.time() - days * 86400
    rec_path = Path(RECORDINGS_DIR)
    if not rec_path.exists():
        return
    cleaned = 0
    cleaned_size = 0
    for d in rec_path.iterdir():
        if not d.is_dir() or d.name in ("thumbs", "logs"):
            continue
        for f in d.glob("*.mp4"):
            if ".raw." in f.name:
                continue
            try:
                if f.stat().st_mtime < cutoff:
                    size = f.stat().st_size
                    f.unlink()
                    cleaned += 1
                    cleaned_size += size
            except Exception:
                logger.debug("suppressed exception", exc_info=True)
    if cleaned > 0:
        logger.info(f"Retention cleanup: deleted {cleaned} files ({cleaned_size/1024/1024:.1f} MB) older than {days} days")


def apply_settings_to_recorders():
    """将全局设置应用到所有录制器"""
    manager._post_process_rename = app_settings.get("smart_rename", False)
    manager._post_process_h265 = app_settings.get("h265_transcode", False)
    manager._merge_timeout = app_settings.get("merge_timeout_minutes", 240) * 60
    manager._post_process_script = app_settings.get("post_process_script", "")
    manager._filename_template = app_settings.get("filename_template", "{username}_{date}_{duration}_merged")
    manager._highlight_auto_detect = app_settings.get("highlight_auto_detect", False)
    manager._highlight_config = {k: v for k, v in app_settings.items() if k.startswith("highlight_")}
    manager.cloud.config = app_settings.get("cloud_upload") or None
    manager.webhook.webhooks = app_settings.get("webhooks", [])
    manager._merge_gap_minutes = app_settings.get("merge_gap_minutes", 15)
    reuse_window = app_settings.get("session_reuse_seconds", 30)
    split_size = app_settings.get("split_by_size_mb", 0)
    split_duration = app_settings.get("split_by_duration_minutes", 0)
    for rec in manager.recorders.values():
        rec.auto_merge = app_settings["auto_merge"]
        rec.info.auto_merge = app_settings["auto_merge"]
        rec.min_segment_size = app_settings["min_segment_size_kb"] * 1024
        rec.auto_delete_originals = app_settings["auto_delete_originals"]
        rec.session_reuse_window = reuse_window
        rec.split_by_size = split_size * 1024 * 1024 if split_size > 0 else 0
        rec.split_by_duration = split_duration * 60 if split_duration > 0 else 0
        rec._danmaku_enabled = app_settings.get("danmaku_capture", True)


async def remux_stale_raw_files():
    """启动时自动 remux 遗留的 .raw.mp4 文件"""
    import glob
    raw_files = glob.glob(str(Path(RECORDINGS_DIR) / "**" / "*.raw.mp4"), recursive=True)
    if not raw_files:
        return
    logger.info(f"发现 {len(raw_files)} 个未处理的 raw 文件，开始 remux...")
    for raw_path in raw_files:
        mp4_path = raw_path.replace(".raw.mp4", ".mp4")
        if os.path.exists(mp4_path):
            os.remove(raw_path)
            continue
        size = os.path.getsize(raw_path)
        if size < 100_000:
            os.remove(raw_path)
            continue
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
            "-i", raw_path, "-c", "copy", "-movflags", "+faststart", mp4_path,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.wait()
        if proc.returncode == 0 and os.path.exists(mp4_path):
            os.remove(raw_path)
            logger.info(f"  Remuxed: {os.path.basename(mp4_path)} ({size/1024/1024:.1f} MB)")
        else:
            os.rename(raw_path, mp4_path)
            logger.info(f"  Renamed: {os.path.basename(mp4_path)}")


app = FastAPI(title="直播监控录制系统", lifespan=lifespan)

# ========== 注册新架构路由（Phase 2 拆分） ==========
# 注入依赖后注册路由，新旧端点并存，逐步迁移
from streamvideo.api.routes.clips import router as clips_router, init_clips_router
from streamvideo.api.routes.system import router as system_router, init_system_router
from streamvideo.api.routes.distribute import router as distribute_router, init_distribute_router
from streamvideo.api.routes.payment import router as payment_router, init_payment_router
from streamvideo.api.routes.tasks import router as tasks_router, init_tasks_router

init_clips_router(db, manager, app_settings, RECORDINGS_DIR, broadcast)
init_system_router(db, manager, app_settings, RECORDINGS_DIR, DEFAULT_SETTINGS,
                   apply_settings_to_recorders, save_config, broadcast)
init_distribute_router(db)
init_payment_router(db)
init_tasks_router(task_queue)

# 同时注册已有的 auth/streams/storage/highlights 路由
from streamvideo.api.routes.auth import router as auth_router
from streamvideo.api.routes.streams import router as streams_router
from streamvideo.api.routes.storage import router as storage_router
from streamvideo.api.routes.highlights import router as highlights_router

for _router in [auth_router, streams_router, storage_router, highlights_router,
                clips_router, system_router, distribute_router, payment_router, tasks_router]:
    app.include_router(_router)

# ========== 可选认证 ==========
# 设置环境变量 SV_TOKEN 启用认证，如: SV_TOKEN=mysecret python server.py
AUTH_TOKEN = os.environ.get("SV_TOKEN", "")

if AUTH_TOKEN:
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.responses import JSONResponse as StarletteJSONResponse

    class AuthMiddleware(BaseHTTPMiddleware):
        OPEN_PATHS = {"/", "/favicon.ico", "/static/styles.css"}

        async def dispatch(self, request, call_next):
            path = request.url.path
            # 静态文件和首页免认证
            if path in self.OPEN_PATHS or path.startswith("/static/"):
                return await call_next(request)
            # 检查 token（query param 或 header）
            token = request.query_params.get("token") or request.headers.get("Authorization", "").replace("Bearer ", "")
            if token != AUTH_TOKEN:
                # WebSocket 检查
                if path == "/ws":
                    token = request.query_params.get("token", "")
                    if token != AUTH_TOKEN:
                        return StarletteJSONResponse({"error": "unauthorized"}, status_code=401)
                else:
                    return StarletteJSONResponse({"error": "unauthorized"}, status_code=401)
            return await call_next(request)

    app.add_middleware(AuthMiddleware)
    logger.info("Authentication enabled (SV_TOKEN)")

# 静态文件
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")



# ========== 核心端点（首页 + WebSocket） ==========

@app.get("/")
async def index():
    return FileResponse(str(BASE_DIR / "static" / "index.html"))


@app.get("/favicon.ico")
async def favicon():
    return Response(status_code=204)


# ========== WebSocket ==========

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    ws_clients.add(ws)
    try:
        # 发送当前状态
        await ws.send_text(json.dumps({
            "type": "init",
            "data": manager.get_all_info(),
        }, ensure_ascii=False))
        # 发送待确认合并队列数量
        await ws.send_text(json.dumps({
            "type": "merge_queue_update",
            "count": db.count_merge_queue(),
        }))

        # 保持连接
        while True:
            data = await ws.receive_text()
            # 客户端可以发送 ping
            if data == "ping":
                await ws.send_text(json.dumps({"type": "pong"}))
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.debug(f"WebSocket error: {e}")
    finally:
        ws_clients.discard(ws)

# ========== 启动 ==========

_caffeinate_proc: "subprocess.Popen | None" = None


def _stop_caffeinate():
    global _caffeinate_proc
    if _caffeinate_proc and _caffeinate_proc.poll() is None:
        _caffeinate_proc.terminate()
        _caffeinate_proc = None


def _start_caffeinate_proc():
    global _caffeinate_proc
    import sys
    if sys.platform != "darwin":
        return
    if _caffeinate_proc and _caffeinate_proc.poll() is None:
        return  # already running
    try:
        _caffeinate_proc = subprocess.Popen(
            ["caffeinate", "-di"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        pass


def _has_active_recordings() -> bool:
    for rec in manager.recorders.values():
        if rec.info.state.value in ("recording", "reconnecting"):
            return True
    return False


async def _sleep_guard_loop():
    """Poll every 30s and manage caffeinate based on prevent_sleep_mode setting."""
    import sys
    if sys.platform != "darwin":
        return
    while True:
        await asyncio.sleep(30)
        mode = app_settings.get("prevent_sleep_mode", "recording")
        if mode == "always":
            _start_caffeinate_proc()
        elif mode == "recording":
            if _has_active_recordings():
                _start_caffeinate_proc()
            else:
                _stop_caffeinate()
        else:  # "never"
            _stop_caffeinate()


if __name__ == "__main__":
    import uvicorn

    mode = app_settings.get("prevent_sleep_mode", "recording")
    if mode == "always":
        _start_caffeinate_proc()
        print("caffeinate: always-on mode, display sleep prevented")

    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")
