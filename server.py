"""
直播监控录制系统 - Web 服务器
FastAPI + WebSocket 实时推送 + REST API
"""

import asyncio
import json
import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional

from recorder import ModelInfo, RecorderManager, RecordingSession, RecordingState
from database import Database
from task_queue import task_queue, Priority


def _safe_username(username: str) -> bool:
    """验证 username 不含路径遍历字符"""
    return ".." not in username and "/" not in username and "\\" not in username and username.strip() != ""

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
            pass
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
            from auth import AuthManager
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
                pass
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


# ========== 用户认证 ==========

@app.post("/api/auth/register")
async def auth_register(req: dict):
    """用户注册"""
    from auth import AuthManager
    am = AuthManager(db)
    try:
        user = am.register(
            email=req.get("email", ""),
            password=req.get("password", ""),
            display_name=req.get("display_name", "")
        )
        # 注册后自动登录
        result = am.login(req["email"], req["password"])
        return JSONResponse({"ok": True, **result})
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.post("/api/auth/login")
async def auth_login(req: dict):
    """用户登录"""
    from auth import AuthManager
    am = AuthManager(db)
    try:
        result = am.login(
            email=req.get("email", ""),
            password=req.get("password", "")
        )
        return JSONResponse({"ok": True, **result})
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.post("/api/auth/logout")
async def auth_logout(req: dict):
    """用户注销"""
    from auth import AuthManager
    am = AuthManager(db)
    token = req.get("session_token", "")
    am.logout(token)
    return JSONResponse({"ok": True})


@app.get("/api/auth/me")
async def auth_me(request: Request):
    """获取当前用户信息"""
    from auth import AuthManager
    from quota import QuotaManager
    am = AuthManager(db)
    token = request.query_params.get("session_token") or request.headers.get("X-Session-Token", "")
    user = am.validate_session(token)
    if not user:
        return JSONResponse({"error": "not authenticated"}, status_code=401)
    # 附加套餐信息，前端用于显示升级按钮
    tier_info = QuotaManager(db).get_tier_info(user["user_id"])
    user["tier"] = tier_info["tier"]
    user["tier_name"] = tier_info["tier_name"]
    return JSONResponse({"ok": True, "user": user})


@app.get("/api/auth/users")
async def auth_users():
    """获取所有用户（管理员）"""
    from auth import AuthManager
    am = AuthManager(db)
    return JSONResponse(am.get_users())


# ========== 页面 ==========

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


# ========== REST API ==========

@app.get("/api/models")
async def get_models():
    return JSONResponse(manager.get_all_info())


class AddModelRequest(BaseModel):
    url: str


@app.post("/api/models")
async def add_model(req: AddModelRequest):
    from quota import QuotaManager
    qm = QuotaManager(db)
    username_key = "default"
    tier_info = qm.get_tier_info(username_key)
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


@app.delete("/api/models/{username}")
async def remove_model(username: str):
    await manager.stop_model(username)
    manager.remove_model(username)
    # 从 SQLite 删除（关键：防止重启后复活）
    try:
        db.delete_model(username)
    except Exception as e:
        logger.warning(f"Failed to delete model from DB: {e}")
    save_config()
    await broadcast({"type": "model_removed", "data": {"username": username}})
    return JSONResponse({"ok": True})


@app.post("/api/models/{username}/start")
async def start_model(username: str):
    await manager.start_model(username)
    return JSONResponse({"ok": True})


@app.post("/api/models/{username}/stop")
async def stop_model(username: str):
    await manager.stop_model(username)
    return JSONResponse({"ok": True})


@app.post("/api/start-all")
async def start_all():
    await manager.start_all()
    return JSONResponse({"ok": True})


@app.post("/api/stop-all")
async def stop_all():
    await manager.stop_all()
    return JSONResponse({"ok": True})


@app.get("/api/disk")
async def get_disk_usage():
    import shutil
    rec_path = Path(RECORDINGS_DIR)
    rec_path.mkdir(parents=True, exist_ok=True)

    def _scan():
        total_rec = sum(f.stat().st_size for f in rec_path.rglob("*") if f.is_file())
        disk = shutil.disk_usage(str(rec_path))
        return total_rec, disk.free, disk.total

    total_rec, free, total = await asyncio.to_thread(_scan)
    return JSONResponse({
        "recordings_bytes": total_rec,
        "free_bytes": free,
        "total_bytes": total,
    })


@app.get("/api/settings")
async def get_settings():
    return JSONResponse(app_settings)


@app.get("/api/stats")
async def get_stats():
    from datetime import datetime
    rec_path = Path(RECORDINGS_DIR)

    def _scan():
        total_files = 0
        total_size = 0
        today_files = 0
        today = datetime.now().strftime("%Y%m%d")
        for f in rec_path.rglob("*.mp4"):
            if ".raw." in f.name:
                continue
            total_files += 1
            total_size += f.stat().st_size
            if f.name.startswith(today):
                today_files += 1
        return total_files, total_size, today_files

    total_files, total_size, today_files = await asyncio.to_thread(_scan)
    return JSONResponse({
        "total_files": total_files,
        "total_size_bytes": total_size,
        "models_count": len(manager.recorders),
        "recordings_today": today_files,
    })


@app.get("/api/stats/{username}")
async def get_model_stats(username: str):
    """获取单个主播的统计信息"""
    files = manager.get_recordings(username)
    sessions = manager.get_sessions(username)
    total_size = sum(f["size"] for f in files)
    merged_count = sum(1 for f in files if "_merged" in f["filename"])
    session_count = len(sessions)
    merged_sessions = sum(1 for s in sessions if s.get("status") == "merged")
    # 估算总录制时长（从 session 数据）
    total_duration = 0
    for s in sessions:
        if s.get("ended_at") and s.get("started_at"):
            total_duration += s["ended_at"] - s["started_at"]
    return JSONResponse({
        "total_files": len(files),
        "total_size": total_size,
        "merged_count": merged_count,
        "session_count": session_count,
        "merged_sessions": merged_sessions,
        "total_duration": total_duration,
    })


@app.get("/api/logs")
async def get_logs(limit: int = 200):
    """获取最近的日志"""
    entries = list(log_buffer)
    if limit:
        entries = entries[-limit:]
    return JSONResponse(entries)


@app.get("/api/search")
async def search_recordings(q: str = "", date_from: str = "", date_to: str = "", min_duration: int = 0, max_duration: int = 0):
    """全局搜索录制文件"""
    results = []
    rec_path = Path(RECORDINGS_DIR)
    if not rec_path.exists():
        return JSONResponse(results)

    for user_dir in rec_path.iterdir():
        if not user_dir.is_dir() or user_dir.name in ("thumbs", "logs"):
            continue
        username = user_dir.name
        files = manager.get_recordings(username)

        for f in files:
            # 文件名匹配
            if q and q.lower() not in f["filename"].lower():
                continue
            # 日期范围
            if date_from and f["date"] < date_from:
                continue
            if date_to and f["date"] > date_to:
                continue
            # 时长范围
            if min_duration and f.get("duration", 0) < min_duration:
                continue
            if max_duration and f.get("duration", 0) > max_duration:
                continue

            results.append({
                "username": username,
                "filename": f["filename"],
                "size": f["size"],
                "date": f["date"],
                "duration": f.get("duration", 0),
                "thumbnail_url": f.get("thumbnail_url", "")
            })

    return JSONResponse(sorted(results, key=lambda x: x["date"], reverse=True))


@app.get("/api/storage/cleanup-suggestions")
async def get_cleanup_suggestions(min_score: float = 0.3):
    """智能存储清理建议"""
    suggestions = []
    rec_path = Path(RECORDINGS_DIR)
    if not rec_path.exists():
        return JSONResponse(suggestions)

    now = time.time()
    for user_dir in rec_path.iterdir():
        if not user_dir.is_dir() or user_dir.name in ("thumbs", "logs"):
            continue
        username = user_dir.name
        files = manager.get_recordings(username)

        for f in files:
            # 计算重要性评分
            score = 0.0
            file_path = rec_path / username / f["filename"]

            # 时长因素（越长越重要）
            duration = f.get("duration", 0)
            if duration > 3600: score += 0.3
            elif duration > 1800: score += 0.2
            elif duration < 300: score -= 0.2

            # 最后访问时间（越久未访问越不重要）
            if file_path.exists():
                days_old = (now - file_path.stat().st_atime) / 86400
                if days_old > 90: score -= 0.3
                elif days_old > 30: score -= 0.2
                elif days_old < 7: score += 0.1

            # 高光密度（有高光记录的更重要）
            highlights = manager.get_highlights(username)
            has_highlight = any(h.get("source_file") == f["filename"] for h in highlights)
            if has_highlight: score += 0.4

            # 合并文件更重要
            if "_merged" in f["filename"]: score += 0.2

            # 低于阈值的建议清理
            if score < min_score:
                suggestions.append({
                    "username": username,
                    "filename": f["filename"],
                    "size": f["size"],
                    "date": f["date"],
                    "duration": duration,
                    "score": round(score, 2),
                    "reason": _get_cleanup_reason(score, duration, days_old if file_path.exists() else 0, has_highlight)
                })

    return JSONResponse(sorted(suggestions, key=lambda x: x["score"]))


def _get_cleanup_reason(score: float, duration: int, days_old: float, has_highlight: bool) -> str:
    """生成清理建议原因"""
    reasons = []
    if duration < 300: reasons.append("时长过短")
    if days_old > 90: reasons.append("超过90天未访问")
    elif days_old > 30: reasons.append("超过30天未访问")
    if not has_highlight: reasons.append("无高光片段")
    if score < 0: reasons.append("综合评分过低")
    return " · ".join(reasons) if reasons else "低价值文件"


@app.get("/api/storage/breakdown")
async def get_storage_breakdown():
    """按主播的存储占用明细"""
    rec_path = Path(RECORDINGS_DIR)
    if not rec_path.exists():
        return JSONResponse([])

    def _scan():
        breakdown = []
        for d in sorted(rec_path.iterdir()):
            if not d.is_dir() or d.name in ("thumbs", "logs"):
                continue
            files = [f for f in d.glob("*.mp4") if ".raw." not in f.name]
            if not files:
                continue
            # 缓存 stat 结果，避免重复调用
            file_stats = [(f, f.stat()) for f in files]
            total_size = sum(st.st_size for _, st in file_stats)
            merged_count = sum(1 for f, _ in file_stats if "_merged" in f.name)
            oldest = min((st.st_mtime for _, st in file_stats), default=0)
            breakdown.append({
                "username": d.name,
                "total_size": total_size,
                "file_count": len(file_stats),
                "merged_count": merged_count,
                "unmerged_count": len(file_stats) - merged_count,
                "oldest_file": oldest,
            })
        breakdown.sort(key=lambda x: x["total_size"], reverse=True)
        return breakdown

    result = await asyncio.to_thread(_scan)
    return JSONResponse(result)


@app.get("/api/network")
async def get_network():
    """获取网络状态"""
    import aiohttp
    proxy = PROXY
    proxy_ok = False
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://httpbin.org/ip", proxy=proxy, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                proxy_ok = resp.status == 200
    except Exception:
        pass

    # 各平台连接状态
    platforms = {}
    for rec in manager.recorders.values():
        p = rec.info.platform
        if p not in platforms:
            platforms[p] = {"online": 0, "total": 0}
        platforms[p]["total"] += 1
        if rec.info.status.value == "public":
            platforms[p]["online"] += 1

    # 当前录制带宽
    bandwidth = []
    for rec in manager.recorders.values():
        if rec.info.state.value == "recording" and rec.info.current_recording:
            bandwidth.append({
                "username": rec.info.username,
                "bandwidth_kbps": rec.info.current_recording.bandwidth_kbps,
            })

    return JSONResponse({
        "proxy": proxy,
        "proxy_ok": proxy_ok,
        "ws_clients": len(ws_clients),
        "platforms": platforms,
        "bandwidth": bandwidth,
    })


@app.get("/api/system")
async def get_system():
    """获取系统信息"""
    import platform
    import shutil
    import subprocess

    def get_version(cmd):
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            return r.stdout.strip().split('\n')[0][:50]
        except Exception:
            return None

    ffmpeg_v = get_version(["ffmpeg", "-version"])
    streamlink_v = get_version(["streamlink", "--version"])
    ytdlp_v = get_version(["yt-dlp", "--version"])

    # Playwright check
    pw_available = False
    try:
        import importlib
        importlib.import_module("playwright")
        pw_available = True
    except Exception:
        pass

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
        "ffmpeg_version": ffmpeg_v.replace("ffmpeg version ", "") if ffmpeg_v else "-",
        "ffmpeg_available": shutil.which("ffmpeg") is not None,
        "streamlink_version": streamlink_v.replace("streamlink ", "") if streamlink_v else "-",
        "streamlink_available": shutil.which("streamlink") is not None,
        "ytdlp_version": ytdlp_v or "-",
        "ytdlp_available": shutil.which("yt-dlp") is not None,
        "playwright_available": pw_available,
        "uptime": uptime,
        "models_count": len(manager.recorders),
    })


@app.post("/api/settings")
async def update_settings(req: dict):
    # 白名单校验：只允许已知的设置 key
    allowed = set(DEFAULT_SETTINGS.keys())
    filtered = {k: v for k, v in req.items() if k in allowed}
    if not filtered:
        return JSONResponse({"error": "无有效设置项"}, status_code=400)
    for key, value in filtered.items():
        app_settings[key] = value
    apply_settings_to_recorders()
    save_config()
    await broadcast({"type": "settings_update", "data": app_settings})
    return JSONResponse(app_settings)


class MergeRequest(BaseModel):
    files: list[str]
    delete_originals: bool = False


@app.get("/api/recordings/{username}/groups")
async def get_recording_groups(username: str, gap: int = 15):
    groups = manager.get_grouped_recordings(username, gap_minutes=gap)
    return JSONResponse(groups)


@app.post("/api/recordings/{username}/merge")
async def merge_recordings(username: str, req: MergeRequest):
    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    if len(req.files) < 2:
        return JSONResponse({"error": "至少需要2个文件"}, status_code=400)
    for fn in req.files:
        if ".." in fn or "/" in fn:
            return JSONResponse({"error": f"非法文件名: {fn}"}, status_code=400)
    try:
        merge_id = await manager.merge_segments(
            username, req.files, delete_originals=req.delete_originals
        )
        status = manager._active_merges.get(merge_id, {}).get("status", "unknown")
        return JSONResponse({"merge_id": merge_id, "status": status})
    except (ValueError, FileNotFoundError) as e:
        logger.warning(f"Merge request failed for {username}: {e}")
        return JSONResponse({"error": str(e)}, status_code=400)


@app.delete("/api/recordings/{username}/{filename}")
async def delete_recording(username: str, filename: str):
    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    if ".." in filename or "/" in filename:
        return JSONResponse({"error": "invalid filename"}, status_code=400)
    video_path = Path(RECORDINGS_DIR) / username / filename
    if not video_path.exists():
        return JSONResponse({"error": "not found"}, status_code=404)
    try:
        video_path.unlink()
    except OSError as e:
        return JSONResponse({"error": f"删除失败: {e}"}, status_code=500)
    return JSONResponse({"ok": True})


@app.post("/api/recordings/{username}/{filename}/rename")
async def rename_recording(username: str, filename: str, req: dict):
    """重命名录制文件"""
    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    new_name = req.get("new_name", "").strip()
    if not new_name or ".." in new_name or "/" in new_name:
        return JSONResponse({"error": "无效的文件名"}, status_code=400)
    if len(new_name) > 200:
        return JSONResponse({"error": "文件名过长"}, status_code=400)
    if not new_name.endswith(".mp4"):
        new_name += ".mp4"
    if ".." in filename or "/" in filename:
        return JSONResponse({"error": "invalid filename"}, status_code=400)
    old_path = Path(RECORDINGS_DIR) / username / filename
    new_path = Path(RECORDINGS_DIR) / username / new_name
    if not old_path.exists():
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    if new_path.exists():
        return JSONResponse({"error": "目标文件名已存在"}, status_code=400)
    try:
        old_path.rename(new_path)
    except OSError as e:
        return JSONResponse({"error": f"重命名失败: {e}"}, status_code=500)
    return JSONResponse({"ok": True, "new_name": new_name})


@app.get("/api/recordings/{username}/export")
async def export_recordings_csv(username: str):
    """导出录制历史为 CSV"""
    import csv
    import io
    files = manager.get_recordings(username)
    sessions = manager.get_sessions(username)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["文件名", "大小(MB)", "创建时间", "会话ID", "状态"])
    session_map = {}
    for s in sessions:
        for seg in s.get("segments", []):
            session_map[seg] = (s.get("session_id", ""), s.get("status", ""))
    for f in files:
        sid, status = session_map.get(f["filename"], ("", ""))
        from datetime import datetime
        created = datetime.fromtimestamp(f["created"]).strftime("%Y-%m-%d %H:%M:%S") if f.get("created") else ""
        writer.writerow([f["filename"], f"{f['size']/1024/1024:.1f}", created, sid, status])
    content = output.getvalue()
    return Response(
        content=content,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{username}_recordings.csv"'},
    )


@app.post("/api/sessions/merge-all-ended")
async def merge_all_ended_sessions():
    """批量合并所有待合并会话"""
    results = []
    for username, rec in list(manager.recorders.items()):
        sessions = manager.get_sessions(username)
        for s in sessions:
            if s.get("status") == "ended" and len(s.get("segments", [])) >= 2:
                try:
                    merge_id = await manager.merge_segments(
                        username, s["segments"],
                        delete_originals=app_settings.get("auto_delete_originals", True))
                    merge_info = manager._active_merges.get(merge_id, {})
                    manager.update_session_status(
                        username, s["session_id"],
                        "merged" if merge_info.get("status") == "done" else "error",
                        merged_file=merge_info.get("filename", ""),
                        merge_error=merge_info.get("error", ""))
                    results.append({"username": username, "session_id": s["session_id"],
                                    "status": merge_info.get("status", "unknown")})
                except Exception as e:
                    manager.update_session_status(username, s["session_id"], "error", merge_error=str(e))
                    results.append({"username": username, "session_id": s["session_id"],
                                    "status": "error", "error": str(e)})
    return JSONResponse({"merged": len(results), "results": results})


@app.post("/api/sessions/cleanup")
async def cleanup_stale_sessions():
    """清理卡住的会话（merging>1h → ended, orphaned active → ended）"""
    fixed = 0
    now = time.time()
    for username, rec in list(manager.recorders.items()):
        for s in rec._sessions:
            if s.status == "merging":
                merge_age_ref = s.merge_started_at or s.ended_at or 0
                if merge_age_ref and (now - merge_age_ref) > 1800:
                    s.status = "ended"
                    s.merge_error = ""
                    s.merge_started_at = 0
                    fixed += 1
            elif s.status == "active" and rec.info.state not in ("recording", "reconnecting"):
                if s.started_at and (now - s.started_at) > 7200:
                    s.status = "ended"
                    s.ended_at = now
                    fixed += 1
        if fixed:
            rec._save_sessions()
    return JSONResponse({"fixed": fixed})


@app.post("/api/recordings/cleanup-merged")
async def cleanup_merged_originals():
    """清理所有已合并的原始片段文件"""
    cleaned = 0
    cleaned_size = 0
    for username, rec in list(manager.recorders.items()):
        sessions = manager.get_sessions(username)
        model_dir = Path(RECORDINGS_DIR) / username
        for s in sessions:
            if s.get("status") != "merged" or not s.get("merged_file"):
                continue
            merged_path = model_dir / s["merged_file"]
            if not merged_path.exists():
                continue
            for fn in s.get("segments", []):
                fp = model_dir / fn
                if fp.exists() and fp.name != s["merged_file"]:
                    cleaned_size += fp.stat().st_size
                    fp.unlink()
                    cleaned += 1
    return JSONResponse({"cleaned": cleaned, "freed_bytes": cleaned_size,
                         "freed_mb": round(cleaned_size / 1024 / 1024, 1)})


@app.get("/api/recordings/{username}")
async def get_recordings(username: str):
    files = manager.get_recordings(username)
    return JSONResponse(files)


@app.get("/api/sessions/{username}")
async def get_sessions(username: str):
    """获取指定主播的所有录制会话"""
    sessions = manager.get_sessions(username)
    return JSONResponse(sessions)


@app.get("/api/sessions/{username}/summary")
async def get_sessions_summary(username: str):
    """获取主播的会话摘要列表（用于 Storage 会话分组视图）"""
    sessions = manager.get_sessions(username)
    recordings = {f["filename"]: f for f in manager.get_recordings(username)}
    model_dir = Path(RECORDINGS_DIR) / username

    result = []
    for s in sorted(sessions, key=lambda x: x.get("started_at", 0), reverse=True):
        segments = s.get("segments", [])
        merged_file = s.get("merged_file", "")
        status = s.get("status", "unknown")

        # 收集关联文件信息
        files = []
        total_size = 0
        cover_url = ""
        for fn in segments:
            if fn in recordings:
                files.append(recordings[fn])
                total_size += recordings[fn]["size"]
                if not cover_url and recordings[fn].get("thumbnail_url"):
                    cover_url = recordings[fn]["thumbnail_url"]
            elif (model_dir / fn).exists():
                stat = (model_dir / fn).stat()
                files.append({"filename": fn, "size": stat.st_size, "created": stat.st_mtime, "thumbnail_url": ""})
                total_size += stat.st_size

        # 合并后文件的封面
        merged_info = recordings.get(merged_file) if merged_file else None
        if merged_info:
            total_size = merged_info["size"]
            if merged_info.get("thumbnail_url"):
                cover_url = merged_info["thumbnail_url"]

        started_at = s.get("started_at", 0)
        ended_at = s.get("ended_at", 0)
        duration = int(ended_at - started_at) if ended_at and started_at else 0

        result.append({
            "session_id": s.get("session_id", ""),
            "status": status,
            "started_at": started_at,
            "ended_at": ended_at,
            "duration": duration,
            "segment_count": len(segments),
            "total_size": total_size,
            "merged_file": merged_file,
            "merged_info": merged_info,
            "segments": files,
            "merge_error": s.get("merge_error", ""),
            "merge_type": s.get("merge_type", ""),
            "rollback_deadline": s.get("rollback_deadline", 0),
            "original_segments": s.get("original_segments", []),
            "retry_count": s.get("retry_count", 0),
            "cover_url": cover_url,
        })

    return JSONResponse(result)


@app.post("/api/sessions/{username}/{session_id}/merge")
async def merge_session(username: str, session_id: str):
    """手动触发指定会话的合并"""
    sessions = manager.get_sessions(username)
    target = None
    for s in sessions:
        if s.get("session_id") == session_id:
            target = s
            break
    if not target:
        return JSONResponse({"error": "会话不存在"}, status_code=404)
    if target["status"] not in ("ended", "error"):
        return JSONResponse({"error": f"会话状态为 {target['status']}，无法合并"}, status_code=400)
    segments = target.get("segments", [])
    if len(segments) < 2:
        return JSONResponse({"error": "片段不足，无需合并"}, status_code=400)

    # 先验证所有段文件存在
    model_dir = Path(RECORDINGS_DIR) / username
    missing = [fn for fn in segments if not (model_dir / fn).exists()]
    if missing:
        return JSONResponse({"error": f"片段文件缺失: {', '.join(missing)}"}, status_code=400)

    # 验证通过后再更新状态为 merging
    manager.update_session_status(username, session_id, "merging")

    try:
        merge_id = await manager.merge_segments(
            username, segments, delete_originals=app_settings.get("auto_delete_originals", True)
        )
        merge_info = manager._active_merges.get(merge_id, {})
        if merge_info.get("status") == "done":
            import time as _time
            merged_file = merge_info.get("filename", "")
            savings = merge_info.get("savings_bytes", 0)
            manager.update_session_status(username, session_id, "merged",
                                          merged_file=merged_file,
                                          merge_type="manual",
                                          rollback_deadline=_time.time() + 72 * 3600,
                                          original_segments=list(segments))
            await broadcast({
                "type": "merge_done",
                "data": {
                    "username": username,
                    "merge_id": merge_id,
                    "filename": merged_file,
                    "input_count": len(segments),
                    "input_size": merge_info.get("input_size", 0),
                    "savings_bytes": savings,
                    "savings_pct": round(savings / max(merge_info.get("input_size", 1), 1) * 100, 1),
                }
            })
        else:
            manager.update_session_status(username, session_id, "error",
                                          merge_error=merge_info.get("error", "合并失败"))
        return JSONResponse({"merge_id": merge_id, "status": merge_info.get("status", "unknown"),
                             "filename": merge_info.get("filename", "")})
    except (ValueError, FileNotFoundError) as e:
        manager.update_session_status(username, session_id, "error", merge_error=str(e))
        return JSONResponse({"error": str(e)}, status_code=400)


@app.post("/api/models/{username}/auto-merge")
async def toggle_model_auto_merge(username: str, req: dict):
    """切换单个主播的自动合并开关"""
    rec = manager.recorders.get(username)
    if not rec:
        return JSONResponse({"error": "主播不存在"}, status_code=404)
    enabled = req.get("auto_merge", True)
    rec.auto_merge = enabled
    rec.info.auto_merge = enabled
    await broadcast({"type": "model_update", "data": rec.info.to_dict()})
    return JSONResponse({"ok": True, "auto_merge": enabled})


# P4: per-model 配置的允许字段
_MODEL_SETTINGS_FIELDS = {
    "quality", "auto_merge", "h265_transcode", "filename_template",
    "split_by_duration_minutes", "split_by_size_mb", "session_reuse_seconds", "notes",
}


@app.get("/api/models/{username}/settings")
async def get_model_settings(username: str):
    """获取主播的独立配置"""
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


@app.put("/api/models/{username}/settings")
async def update_model_settings(username: str, req: dict):
    """更新主播的独立配置（覆盖全局设置）"""
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
        v = req["h265_transcode"]
        rec._per_model_h265 = v  # None = 继承全局
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


@app.get("/api/merge-history/{username}")
async def get_merge_history(username: str):
    """获取指定主播的合并历史"""
    if not _safe_username(username):
        return JSONResponse({"error": "无效用户名"}, status_code=400)
    history = db.get_merge_history(username)
    return JSONResponse(history)


@app.get("/api/stats/daily")
async def get_daily_stats(days: int = 30):
    """获取每日录制统计"""
    stats = db.get_daily_stats(days)
    return JSONResponse(stats)


@app.post("/api/models/{username}/schedule")
async def set_model_schedule(username: str, req: dict):
    """设置主播的定时录制计划"""
    rec = manager.recorders.get(username)
    if not rec:
        return JSONResponse({"error": "主播不存在"}, status_code=404)
    schedule = req.get("schedule")
    if schedule:
        # 验证 schedule 格式
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


@app.get("/api/models/{username}/schedule")
async def get_model_schedule(username: str):
    """获取主播的定时录制计划"""
    rec = manager.recorders.get(username)
    if not rec:
        return JSONResponse({"error": "主播不存在"}, status_code=404)
    return JSONResponse({"schedule": rec.schedule})


@app.post("/api/models/{username}/quality")
async def set_model_quality(username: str, req: dict):
    """设置主播的录制质量"""
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


@app.post("/api/models/{username}/cookies")
async def set_model_cookies(username: str, req: dict):
    """设置主播的自定义 cookie（从浏览器导出）"""
    rec = manager.recorders.get(username)
    if not rec:
        return JSONResponse({"error": "主播不存在"}, status_code=404)
    rec.custom_cookies = req.get("cookies", "")
    return JSONResponse({"ok": True})


@app.post("/api/models/{username}/stream-url")
async def set_model_stream_url(username: str, req: dict):
    """设置主播的自定义流地址"""
    rec = manager.recorders.get(username)
    if not rec:
        return JSONResponse({"error": "主播不存在"}, status_code=404)
    rec.custom_stream_url = req.get("stream_url", "")
    logger.info(f"[{username}] Custom stream URL set: {rec.custom_stream_url[:80] if rec.custom_stream_url else 'cleared'}")
    return JSONResponse({"ok": True})


@app.post("/api/webhooks/test")
async def test_webhook(req: dict):
    """测试 webhook 连通性"""
    try:
        ok = await manager.webhook.test(req)
        return JSONResponse({"ok": ok})
    except ValueError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


@app.get("/api/thumb/{username}")
async def get_thumbnail(username: str):
    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    thumb_path = Path(RECORDINGS_DIR) / "thumbs" / f"{username}.jpg"
    if thumb_path.exists():
        return FileResponse(
            str(thumb_path),
            media_type="image/jpeg",
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )
    return JSONResponse({"error": "no thumbnail"}, status_code=404)


@app.get("/api/video/{username}/{filename}")
async def get_video(username: str, filename: str, download: int = 0):
    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    if ".." in filename or "/" in filename:
        return JSONResponse({"error": "invalid filename"}, status_code=400)
    video_path = Path(RECORDINGS_DIR) / username / filename
    if video_path.exists():
        if download:
            return FileResponse(
                str(video_path), media_type="video/mp4",
                headers={"Content-Disposition": f'attachment; filename="{filename}"'},
            )
        return FileResponse(str(video_path), media_type="video/mp4")
    return JSONResponse({"error": "not found"}, status_code=404)


# ========== 新增 API: 合并取消 / 批量重试 / 健康检查 / 保留策略 / 合并统计 ==========

@app.post("/api/recordings/{username}/merge/{merge_id}/cancel")
async def cancel_merge(username: str, merge_id: str):
    """取消正在运行的合并任务"""
    ok = await manager.cancel_merge(merge_id)
    if ok:
        # 回退对应 session 状态
        for s_dict in manager.get_sessions(username):
            if s_dict.get("status") == "merging":
                manager.update_session_status(username, s_dict["session_id"], "ended", merge_error="")
        await broadcast({"type": "merge_cancelled", "data": {"username": username, "merge_id": merge_id}})
        return JSONResponse({"ok": True})
    return JSONResponse({"error": "合并任务不存在或已完成"}, status_code=400)


@app.post("/api/sessions/retry-failed")
async def retry_failed_sessions():
    """批量重试所有失败的合并会话"""
    retried = 0
    for username, rec in list(manager.recorders.items()):
        for s in rec._sessions:
            if s.status == "error" and s.retry_count < 3 and len(s.segments) >= 2:
                s.status = "ended"
                s.merge_error = ""
                retried += 1
        if retried:
            rec._save_sessions()
    # 触发自动合并
    for username in list(manager.recorders.keys()):
        try:
            await manager.auto_merge_for_model(username)
        except Exception as e:
            logger.error(f"Retry-failed merge error for {username}: {e}")
    return JSONResponse({"retried": retried})


@app.get("/api/recordings/{username}/health")
async def health_check_recordings(username: str):
    """对指定主播的所有录制文件执行健康检查"""
    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    model_dir = Path(RECORDINGS_DIR) / username
    if not model_dir.exists():
        return JSONResponse([])

    async def _probe(fp: Path) -> dict:
        result = {"filename": fp.name, "size": fp.stat().st_size, "valid": False,
                  "duration": 0, "codec": "", "resolution": ""}
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffprobe", "-v", "quiet", "-select_streams", "v:0",
                "-show_entries", "stream=codec_name,width,height,duration",
                "-of", "json", str(fp),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
            data = json.loads(stdout.decode())
            streams = data.get("streams", [])
            if streams:
                s = streams[0]
                result["codec"] = s.get("codec_name", "")
                result["resolution"] = f"{s.get('width', 0)}x{s.get('height', 0)}"
                result["duration"] = float(s.get("duration", 0) or 0)
                result["valid"] = result["duration"] > 0
        except Exception:
            pass
        return result

    files = sorted(model_dir.glob("*.mp4"))
    files = [f for f in files if ".raw." not in f.name]
    results = await asyncio.gather(*[_probe(f) for f in files])
    return JSONResponse(list(results))


@app.post("/api/recordings/cleanup-old")
async def cleanup_old_recordings():
    """手动触发保留策略清理"""
    days = app_settings.get("retention_days", 0)
    if days <= 0:
        return JSONResponse({"error": "retention_days 未设置或为 0"}, status_code=400)
    await _do_retention_cleanup(days)
    return JSONResponse({"ok": True, "retention_days": days})


@app.get("/api/stats/merge-savings")
async def get_merge_savings():
    """获取合并节省的存储空间统计"""
    history = db.get_all_merge_history(limit=1000)
    total_savings = sum(h.get("savings_bytes", 0) for h in history)
    total_merges = len([h for h in history if h.get("status") == "done"])
    total_input = sum(h.get("input_size", 0) for h in history if h.get("status") == "done")
    avg_ratio = round((1 - (total_input - total_savings) / total_input) * 100, 1) if total_input > 0 else 0
    return JSONResponse({
        "total_savings_bytes": total_savings,
        "total_savings_mb": round(total_savings / 1024 / 1024, 1),
        "total_merges": total_merges,
        "avg_compression_pct": avg_ratio,
    })


@app.get("/api/merge-history")
async def get_all_merge_history(limit: int = 50):
    """获取全局合并历史"""
    history = db.get_all_merge_history(limit=limit)
    return JSONResponse(history)


# ========== Merge Queue ==========


@app.get("/api/merge-queue")
async def get_merge_queue():
    """获取待确认合并队列"""
    items = db.get_merge_queue(status="pending")
    return JSONResponse({"items": items, "count": len(items)})


@app.post("/api/merge-queue/{session_id}/confirm")
async def confirm_merge_queue(session_id: str):
    """确认合并队列中的某个 session"""
    items = db.get_merge_queue(status="pending")
    item = next((i for i in items if i["session_id"] == session_id), None)
    if not item:
        return JSONResponse({"error": "未找到待确认项"}, status_code=404)
    username = item["username"]
    segments = item["segments"]
    db.update_merge_queue_status(session_id, "processing")
    try:
        merge_id = await manager.merge_segments(username, segments, delete_originals=True)
        db.update_merge_queue_status(session_id, "done")
        # 更新 session 状态
        if db:
            db.update_session_status(session_id, "merged")
        await broadcast({"type": "merge_queue_update", "count": db.count_merge_queue()})
        return JSONResponse({"ok": True, "merge_id": merge_id})
    except Exception as e:
        db.update_merge_queue_status(session_id, "error")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/merge-queue/{session_id}/dismiss")
async def dismiss_merge_queue(session_id: str):
    """忽略队列中的某个合并任务"""
    db.update_merge_queue_status(session_id, "dismissed")
    await broadcast({"type": "merge_queue_update", "count": db.count_merge_queue()})
    return JSONResponse({"ok": True})


@app.post("/api/merge-queue/confirm-all")
async def confirm_all_merge_queue():
    """批量确认所有待合并任务"""
    items = db.get_merge_queue(status="pending")
    results = []
    for item in items:
        session_id = item["session_id"]
        username = item["username"]
        segments = item["segments"]
        db.update_merge_queue_status(session_id, "processing")
        try:
            merge_id = await manager.merge_segments(username, segments, delete_originals=True)
            db.update_merge_queue_status(session_id, "done")
            db.update_session_status(session_id, "merged")
            results.append({"session_id": session_id, "ok": True, "merge_id": merge_id})
        except Exception as e:
            db.update_merge_queue_status(session_id, "error")
            results.append({"session_id": session_id, "ok": False, "error": str(e)})
    await broadcast({"type": "merge_queue_update", "count": db.count_merge_queue()})
    return JSONResponse({"results": results})


@app.post("/api/sessions/{session_id}/rollback")
async def rollback_session_merge(session_id: str):
    """撤回自动合并：删除合并结果，恢复原始分片（72h 内有效）"""
    import time as _time
    sessions = db.get_sessions_by_id(session_id)
    if not sessions:
        return JSONResponse({"error": "Session 不存在"}, status_code=404)
    session = sessions[0]
    if session.get("status") != "merged":
        return JSONResponse({"error": "Session 未处于合并状态"}, status_code=400)
    deadline = session.get("rollback_deadline", 0)
    if deadline and _time.time() > deadline:
        return JSONResponse({"error": "已超过 72 小时撤回期限"}, status_code=400)
    original_segs = session.get("original_segments", [])
    if not original_segs:
        return JSONResponse({"error": "无原始分片记录，无法撤回"}, status_code=400)

    username = session["username"]
    model_dir = Path(manager.output_dir) / username
    merged_file = session.get("merged_file", "")

    # 验证原始分片存在（未被删除）
    missing = [s for s in original_segs if not (model_dir / s).exists()]
    if missing:
        return JSONResponse({"error": f"原始分片已不存在: {missing}"}, status_code=400)

    # 删除合并结果
    if merged_file:
        merged_path = model_dir / merged_file
        if merged_path.exists():
            merged_path.unlink()

    # 恢复 session 状态
    db.update_session_status(session_id, "ended",
                             merged_file="",
                             merge_type="",
                             rollback_deadline=0,
                             segments=original_segs,
                             original_segments=[])
    # 同步内存中的 recorder session
    rec = manager.recorders.get(username)
    if rec:
        for s in rec._sessions:
            if s.session_id == session_id:
                s.status = "ended"
                s.merged_file = ""
                s.merge_type = ""
                s.rollback_deadline = 0
                s.segments = list(original_segs)
                s.original_segments = []
                break
    await broadcast({"type": "session_rollback", "session_id": session_id, "username": username})
    return JSONResponse({"ok": True, "restored_segments": original_segs})


# ========== Phase 2: 合并预览 / 事件缓冲 ==========


@app.post("/api/recordings/{username}/merge/preview")
async def merge_preview(username: str, req: dict):
    """合并预览/Dry-Run：返回 segment 详情、codec 一致性、预估输出"""
    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    filenames = req.get("files", [])
    if not filenames:
        # 默认预览所有 ended session 的 segments
        sessions = manager.get_sessions(username)
        for s in sessions:
            if s.get("status") == "ended" and len(s.get("segments", [])) >= 2:
                filenames = s["segments"]
                break
    if len(filenames) < 2:
        return JSONResponse({"error": "至少需要2个文件"}, status_code=400)

    model_dir = Path(RECORDINGS_DIR) / username
    segments = []
    total_size = 0
    total_duration = 0
    warnings = []

    for fn in filenames:
        fp = model_dir / fn
        if not fp.exists():
            warnings.append(f"文件缺失: {fn}")
            continue
        info = {"filename": fn, "size": fp.stat().st_size, "codec": "", "resolution": "",
                "duration": 0, "valid": False}
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffprobe", "-v", "quiet", "-select_streams", "v:0",
                "-show_entries", "stream=codec_name,width,height,duration",
                "-of", "json", str(fp),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
            data = json.loads(stdout.decode())
            streams = data.get("streams", [])
            if streams:
                s = streams[0]
                info["codec"] = s.get("codec_name", "")
                info["resolution"] = f"{s.get('width', 0)}x{s.get('height', 0)}"
                info["duration"] = float(s.get("duration", 0) or 0)
                info["valid"] = info["duration"] > 0
        except Exception:
            pass
        segments.append(info)
        total_size += info["size"]
        total_duration += info["duration"]

    # 检查 codec 一致性
    codecs = set(f"{s['codec']}_{s['resolution']}" for s in segments if s["valid"])
    codec_consistent = len(codecs) <= 1
    if not codec_consistent:
        warnings.append(f"编码不一致: {' vs '.join(sorted(codecs))}（将自动重编码）")

    invalid = [s["filename"] for s in segments if not s["valid"]]
    if invalid:
        warnings.append(f"损坏片段: {', '.join(invalid)}")

    return JSONResponse({
        "segments": segments,
        "total_size": total_size,
        "total_duration": total_duration,
        "estimated_output_size": total_size,  # copy 模式大小基本不变
        "codec_consistent": codec_consistent,
        "warnings": warnings,
        "segment_count": len([s for s in segments if s["valid"]]),
    })


@app.get("/api/events/since")
async def get_events_since(ts: float = 0):
    """获取指定时间戳之后的事件（用于 WebSocket 断线恢复）"""
    events = [e for e in _event_buffer if e.get("_ts", 0) > ts]
    return JSONResponse(events)


# ========== Phase 3: 缩略图 / 录后脚本 ==========


@app.post("/api/recordings/{username}/{filename}/thumbnail")
async def generate_thumbnail(username: str, filename: str):
    """从录制文件生成缩略图"""
    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    if ".." in filename or "/" in filename:
        return JSONResponse({"error": "invalid filename"}, status_code=400)
    video_path = Path(RECORDINGS_DIR) / username / filename
    if not video_path.exists():
        return JSONResponse({"error": "文件不存在"}, status_code=404)

    thumbs_dir = Path(RECORDINGS_DIR) / "thumbs"
    thumbs_dir.mkdir(parents=True, exist_ok=True)
    thumb_name = f"{username}_{filename.replace('.mp4', '')}.jpg"
    thumb_path = thumbs_dir / thumb_name

    try:
        # 提取 10% 位置的帧
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "quiet", "-show_entries", "format=duration",
            "-of", "json", str(video_path),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
        data = json.loads(stdout.decode())
        duration = float(data.get("format", {}).get("duration", 0) or 0)
        seek_time = max(1, duration * 0.1)

        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-ss", str(seek_time),
            "-i", str(video_path),
            "-vframes", "1", "-vf", "scale=320:-1",
            str(thumb_path),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=30)

        if thumb_path.exists():
            return JSONResponse({
                "ok": True,
                "thumbnail_url": f"/api/thumb/file/{username}/{thumb_name}",
            })
        return JSONResponse({"error": "缩略图生成失败"}, status_code=500)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/thumb/file/{username}/{thumb_name}")
async def get_file_thumbnail(username: str, thumb_name: str):
    """获取录制文件的缩略图"""
    if not _safe_username(username) or ".." in thumb_name:
        return JSONResponse({"error": "invalid"}, status_code=400)
    thumb_path = Path(RECORDINGS_DIR) / "thumbs" / thumb_name
    if thumb_path.exists():
        return FileResponse(str(thumb_path), media_type="image/jpeg",
                            headers={"Cache-Control": "max-age=3600"})
    return JSONResponse({"error": "not found"}, status_code=404)


@app.post("/api/recordings/{username}/{filename}/subtitle")
async def generate_subtitle(username: str, filename: str, req: dict = {}):
    """为录制文件生成字幕（需要 openai-whisper）"""
    if not _safe_username(username) or ".." in filename or "/" in filename:
        return JSONResponse({"error": "invalid"}, status_code=400)
    video_path = Path(RECORDINGS_DIR) / username / filename
    if not video_path.exists():
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    try:
        from subtitle_gen import SubtitleGenerator, is_whisper_available
        if not is_whisper_available():
            return JSONResponse({"error": "Whisper 未安装，请运行: pip install openai-whisper"}, status_code=503)
        model_size = req.get("model_size", "small")
        fmt = req.get("format", "srt")
        gen = SubtitleGenerator(model_size=model_size)
        output_dir = video_path.parent
        if fmt == "vtt":
            sub_path = await gen.generate_vtt(video_path, output_dir)
        else:
            sub_path = await gen.generate_srt(video_path, output_dir)
        if sub_path and sub_path.exists():
            return JSONResponse({"ok": True, "subtitle_file": sub_path.name})
        return JSONResponse({"error": "字幕生成失败"}, status_code=500)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/post-script/test")
async def test_post_script(req: dict):
    """测试录后脚本（dry-run）"""
    script = req.get("script", "").strip()
    if not script:
        return JSONResponse({"error": "脚本路径为空"}, status_code=400)
    script_path = Path(script)
    if not script_path.exists():
        return JSONResponse({"error": f"脚本不存在: {script}"}, status_code=400)
    if not os.access(str(script_path), os.X_OK):
        return JSONResponse({"error": f"脚本无执行权限: {script}"}, status_code=400)
    # 模拟执行
    env = {
        **os.environ,
        "SV_USERNAME": "test_user",
        "SV_FILE_PATH": "/tmp/test_recording.mp4",
        "SV_FILE_SIZE": "1048576",
        "SV_PLATFORM": "test",
        "SV_SESSION_ID": "s_test_000000",
        "SV_EVENT": "test",
    }
    try:
        proc = await asyncio.create_subprocess_exec(
            str(script_path),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
        return JSONResponse({
            "ok": proc.returncode == 0,
            "exit_code": proc.returncode,
            "stdout": stdout.decode()[:500],
            "stderr": stderr.decode()[:500],
        })
    except asyncio.TimeoutError:
        return JSONResponse({"error": "脚本执行超时 (30s)"}, status_code=500)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ========== V2.0: 高光检测 + 片段生成 API ==========

@app.post("/api/highlights/{username}/detect")
async def detect_highlights(username: str, req: dict = {}):
    """触发高光检测"""
    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    filename = req.get("filename", "")
    session_id = req.get("session_id", "")
    if not filename:
        # 自动选择最新的合并文件
        files = manager.get_recordings(username)
        merged = [f for f in files if "_merged" in f["filename"]]
        if merged:
            filename = merged[0]["filename"]
        elif files:
            filename = files[0]["filename"]
    if not filename:
        return JSONResponse({"error": "无可检测的录制文件"}, status_code=400)

    video_path = Path(RECORDINGS_DIR) / username / filename
    if not video_path.exists():
        return JSONResponse({"error": f"文件不存在: {filename}"}, status_code=404)

    try:
        from highlight import HighlightDetector
        config = {k: v for k, v in app_settings.items() if k.startswith("highlight_")}
        detector = HighlightDetector(config)

        # 查找弹幕文件
        danmaku_path = None
        if session_id:
            dp = video_path.parent / f"{session_id}_danmaku.json"
            if dp.exists():
                danmaku_path = dp
        if not danmaku_path:
            # 尝试匹配任何弹幕文件
            for dp in video_path.parent.glob("*_danmaku.json"):
                danmaku_path = dp
                break

        highlights = await detector.detect(video_path, danmaku_path)

        # 存入数据库
        import uuid as _uuid
        results = []
        for h in highlights:
            hid = f"h_{int(time.time())}_{_uuid.uuid4().hex[:6]}"
            db.insert_highlight(
                highlight_id=hid, session_id=session_id, username=username,
                video_file=filename, start_time=h.start_time, end_time=h.end_time,
                score=h.score, category=h.category, signals=h.signals, title=h.title,
            )
            results.append({
                "highlight_id": hid, "start_time": h.start_time, "end_time": h.end_time,
                "score": h.score, "category": h.category, "title": h.title,
            })

        await broadcast({"type": "highlight_detected", "data": {
            "username": username, "count": len(results), "video_file": filename}})
        return JSONResponse({"ok": True, "highlights": results, "count": len(results)})
    except Exception as e:
        logger.error(f"Highlight detection error for {username}: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/highlights/{username}")
async def get_highlights(username: str, limit: int = 50):
    """列出高光"""
    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    return JSONResponse(db.get_highlights(username, limit))


@app.delete("/api/highlights/item/{highlight_id}")
async def delete_highlight(highlight_id: str):
    """删除高光"""
    db.delete_highlight(highlight_id)
    return JSONResponse({"ok": True})


@app.post("/api/highlights/item/{highlight_id}/clip")
async def generate_clip_from_highlight(highlight_id: str, req: dict = {}):
    """从高光生成片段"""
    h = db.get_highlight(highlight_id)
    if not h:
        return JSONResponse({"error": "高光不存在"}, status_code=404)

    username = h["username"]
    from quota import QuotaManager
    allowed, used, limit = QuotaManager(db).check_quota(username)
    if not allowed:
        return JSONResponse({"error": f"今日配额已用完 ({used}/{limit})，请升级套餐", "quota_exceeded": True}, status_code=429)

    video_path = Path(RECORDINGS_DIR) / username / h["video_file"]
    if not video_path.exists():
        return JSONResponse({"error": f"视频文件不存在: {h['video_file']}"}, status_code=404)

    # 查找弹幕文件
    danmaku_path = None
    if h.get("session_id"):
        dp = video_path.parent / f"{h['session_id']}_danmaku.json"
        if dp.exists():
            danmaku_path = dp

    from clipgen import ClipGenerator, ClipConfig
    config = ClipConfig(
        resolution=req.get("resolution", app_settings.get("clip_resolution", "1080x1920")),
        format=req.get("format", app_settings.get("clip_format", "vertical")),
        watermark=req.get("watermark", app_settings.get("clip_watermark", "")),
        danmaku_overlay=app_settings.get("clip_danmaku_overlay", True),
    )
    gen = ClipGenerator(config, RECORDINGS_DIR)
    result = await gen.generate_clip(video_path, h, danmaku_path)

    if result.get("status") == "done":
        QuotaManager(db).consume_quota(username)
        db.insert_clip(
            clip_id=result["clip_id"], highlight_id=highlight_id, username=username,
            output_file=result.get("output_file", ""), resolution=result.get("resolution", ""),
            duration=result.get("duration", 0), format=result.get("format", ""),
            size=result.get("size", 0), status="done",
        )
        db.update_highlight_status(highlight_id, "clipped")
        await broadcast({"type": "clip_done", "data": {
            "username": username, "clip_id": result["clip_id"], "filename": result.get("filename", "")}})
    return JSONResponse(result)


@app.post("/api/highlights/batch-clip")
async def batch_generate_clips(req: dict):
    """批量生成片段"""
    highlight_ids = req.get("highlight_ids", [])
    if not highlight_ids:
        return JSONResponse({"error": "未选择高光"}, status_code=400)

    from clipgen import ClipGenerator, ClipConfig
    config = ClipConfig(
        resolution=req.get("resolution", app_settings.get("clip_resolution", "1080x1920")),
        format=req.get("format", app_settings.get("clip_format", "vertical")),
        watermark=req.get("watermark", app_settings.get("clip_watermark", "")),
        danmaku_overlay=app_settings.get("clip_danmaku_overlay", True),
    )
    gen = ClipGenerator(config, RECORDINGS_DIR)

    results = []
    for i, hid in enumerate(highlight_ids):
        h = db.get_highlight(hid)
        if not h:
            continue
        username = h["username"]

        from quota import QuotaManager
        allowed, used, limit = QuotaManager(db).check_quota(username)
        if not allowed:
            results.append({"status": "quota_exceeded", "highlight_id": hid,
                            "error": f"今日配额已用完 ({used}/{limit})"})
            continue

        video_path = Path(RECORDINGS_DIR) / username / h["video_file"]
        if not video_path.exists():
            continue

        danmaku_path = None
        if h.get("session_id"):
            dp = video_path.parent / f"{h['session_id']}_danmaku.json"
            if dp.exists():
                danmaku_path = dp

        await broadcast({"type": "clip_progress", "data": {
            "username": username, "current": i + 1, "total": len(highlight_ids)}})

        result = await gen.generate_clip(video_path, h, danmaku_path)
        if result.get("status") == "done":
            QuotaManager(db).consume_quota(username)
            db.insert_clip(
                clip_id=result["clip_id"], highlight_id=hid, username=username,
                output_file=result.get("output_file", ""), resolution=result.get("resolution", ""),
                duration=result.get("duration", 0), format=result.get("format", ""),
                size=result.get("size", 0), status="done",
            )
            db.update_highlight_status(hid, "clipped")
        results.append(result)

    done = sum(1 for r in results if r.get("status") == "done")
    return JSONResponse({"ok": True, "total": len(results), "done": done, "results": results})


@app.get("/api/clips/{username}")
async def get_clips(username: str, limit: int = 50):
    """列出片段"""
    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    return JSONResponse(db.get_clips(username, limit))


@app.get("/api/clips/file/{username}/{filename}")
async def get_clip_file(username: str, filename: str, download: int = 0):
    """播放/下载片段"""
    if not _safe_username(username) or ".." in filename:
        return JSONResponse({"error": "invalid"}, status_code=400)
    clip_path = Path(RECORDINGS_DIR) / username / "clips" / filename
    if clip_path.exists():
        if download:
            return FileResponse(str(clip_path), media_type="video/mp4",
                                headers={"Content-Disposition": f'attachment; filename="{filename}"'})
        return FileResponse(str(clip_path), media_type="video/mp4")
    return JSONResponse({"error": "not found"}, status_code=404)


@app.delete("/api/clips/item/{clip_id}")
async def delete_clip(clip_id: str):
    """删除片段"""
    clip = db.get_clip(clip_id)
    if clip and clip.get("output_file"):
        fp = Path(RECORDINGS_DIR) / clip["output_file"]
        if fp.exists():
            fp.unlink()
    db.delete_clip(clip_id)
    return JSONResponse({"ok": True})


@app.get("/api/highlight-rules")
async def get_highlight_rules(username: str = ""):
    """获取高光规则"""
    return JSONResponse(db.get_highlight_rules(username))


@app.post("/api/highlight-rules")
async def upsert_highlight_rule(req: dict):
    """创建/更新高光规则"""
    rule_id = req.get("rule_id")
    db.upsert_highlight_rule(rule_id, **{k: v for k, v in req.items() if k != "rule_id"})
    return JSONResponse({"ok": True})


@app.delete("/api/highlight-rules/{rule_id}")
async def delete_highlight_rule(rule_id: int):
    """删除高光规则"""
    db.delete_highlight_rule(rule_id)
    return JSONResponse({"ok": True})


@app.get("/api/danmaku/{session_id}")
async def get_danmaku(session_id: str):
    """获取弹幕数据"""
    dm = db.get_danmaku(session_id)
    if not dm:
        return JSONResponse({"error": "not found"}, status_code=404)
    # 加载完整弹幕文件
    if dm.get("file_path") and Path(dm["file_path"]).exists():
        with open(dm["file_path"], encoding="utf-8") as f:
            full_data = json.load(f)
        dm["messages"] = full_data.get("messages", [])
        dm["stats"] = full_data.get("stats", {})
    return JSONResponse(dm)


@app.get("/api/danmaku/{session_id}/timeline")
async def get_danmaku_timeline(session_id: str, window: int = 10):
    """获取弹幕密度时间线"""
    dm = db.get_danmaku(session_id)
    if not dm or not dm.get("file_path"):
        return JSONResponse([])
    fp = Path(dm["file_path"])
    if not fp.exists():
        return JSONResponse([])
    try:
        with open(fp, encoding="utf-8") as f:
            data = json.load(f)
        messages = data.get("messages", [])
        chat_msgs = [m for m in messages if m.get("type") == "chat"]
        if not chat_msgs:
            return JSONResponse([])
        max_t = max(m["t"] for m in chat_msgs)
        timeline = []
        for t in range(0, int(max_t) + 1, window):
            count = sum(1 for m in chat_msgs if t <= m["t"] < t + window)
            timeline.append({"t": t, "density": round(count / window, 2), "count": count})
        return JSONResponse(timeline)
    except Exception:
        return JSONResponse([])


@app.get("/api/analytics/highlights")
async def get_highlight_analytics(username: str = "", days: int = 30):
    """高光分析统计"""
    highlights = db.get_all_highlights(limit=500) if not username else db.get_highlights(username, 500)
    total = len(highlights)
    by_category = {}
    by_score = {"high": 0, "medium": 0, "low": 0}
    for h in highlights:
        cat = h.get("category", "unknown")
        by_category[cat] = by_category.get(cat, 0) + 1
        score = h.get("score", 0)
        if score >= 0.8:
            by_score["high"] += 1
        elif score >= 0.5:
            by_score["medium"] += 1
        else:
            by_score["low"] += 1
    return JSONResponse({
        "total": total, "by_category": by_category, "by_score": by_score,
    })


@app.get("/api/analytics/clips")
async def get_clip_analytics(username: str = ""):
    """片段分析统计"""
    stats = db.get_clip_stats(username)
    return JSONResponse(stats)


# ========== V2.0 扩展: 手动片段 + ZIP + 元数据 + 导出 ==========

@app.post("/api/clips/manual")
async def create_manual_clip(req: dict):
    """手动指定时间范围生成片段（不依赖高光检测）"""
    username = req.get("username", "")
    filename = req.get("filename", "")
    start_time = req.get("start_time", 0)
    end_time = req.get("end_time", 0)
    if not username or not filename or end_time <= start_time:
        return JSONResponse({"error": "参数无效"}, status_code=400)
    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)

    video_path = Path(RECORDINGS_DIR) / username / filename
    if not video_path.exists():
        return JSONResponse({"error": f"文件不存在: {filename}"}, status_code=404)

    from quota import QuotaManager
    qm = QuotaManager(db)
    allowed, used, limit = qm.check_quota(username)
    if not allowed:
        return JSONResponse({"error": f"今日配额已用完 ({used}/{limit})，请升级套餐", "quota_exceeded": True}, status_code=429)

    max_duration = qm.get_tier_info(username)["features"].get("max_clip_duration", 60)
    requested_duration = end_time - start_time
    if requested_duration > max_duration:
        return JSONResponse(
            {"error": f"片段时长 {int(requested_duration)}s 超过套餐上限 {max_duration}s，请升级套餐",
             "quota_exceeded": True, "max_duration": max_duration},
            status_code=429,
        )

    # 构造虚拟 highlight
    import uuid as _uuid
    highlight = {
        "highlight_id": f"manual_{int(time.time())}",
        "username": username,
        "video_file": filename,
        "start_time": start_time,
        "end_time": end_time,
    }

    # 查找弹幕文件
    danmaku_path = None
    for dp in video_path.parent.glob("*_danmaku.json"):
        danmaku_path = dp
        break

    from clipgen import ClipGenerator, ClipConfig
    config = ClipConfig(
        resolution=req.get("resolution", app_settings.get("clip_resolution", "1080x1920")),
        format=req.get("format", app_settings.get("clip_format", "vertical")),
        watermark=app_settings.get("clip_watermark", ""),
        danmaku_overlay=app_settings.get("clip_danmaku_overlay", True),
    )
    gen = ClipGenerator(config, RECORDINGS_DIR)
    result = await gen.generate_clip(video_path, highlight, danmaku_path)

    if result.get("status") == "done":
        title = req.get("title", f"手动片段 {int(start_time//60)}:{int(start_time%60):02d}")
        QuotaManager(db).consume_quota(username)
        db.insert_clip(
            clip_id=result["clip_id"], highlight_id="", username=username,
            output_file=result.get("output_file", ""), resolution=result.get("resolution", ""),
            duration=result.get("duration", 0), format=result.get("format", ""),
            size=result.get("size", 0), status="done",
        )
        # 更新标题
        db.update_clip_status(result["clip_id"], "done", title=title)
        await broadcast({"type": "clip_done", "data": {
            "username": username, "clip_id": result["clip_id"], "filename": result.get("filename", "")}})
    return JSONResponse(result)


@app.post("/api/clips/item/{clip_id}/metadata")
async def update_clip_metadata(clip_id: str, req: dict):
    """更新片段元数据（标题/描述/标签）"""
    clip = db.get_clip(clip_id)
    if not clip:
        return JSONResponse({"error": "片段不存在"}, status_code=404)
    kwargs = {}
    if "title" in req:
        kwargs["title"] = req["title"]
    if "description" in req:
        kwargs["description"] = req["description"]
    if "tags" in req:
        kwargs["tags"] = req["tags"]
    if kwargs:
        db.update_clip_status(clip_id, clip.get("status", "done"), **kwargs)
    return JSONResponse({"ok": True})


@app.post("/api/clips/download-zip")
async def download_clips_zip(req: dict):
    """批量下载片段为 ZIP"""
    clip_ids = req.get("clip_ids", [])
    if not clip_ids:
        return JSONResponse({"error": "未选择片段"}, status_code=400)

    import io
    import zipfile
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_STORED) as zf:
        for cid in clip_ids:
            clip = db.get_clip(cid)
            if not clip or not clip.get("output_file"):
                continue
            fp = Path(RECORDINGS_DIR) / clip["output_file"]
            if fp.exists():
                arcname = fp.name
                if clip.get("title"):
                    safe_title = "".join(c for c in clip["title"] if c.isalnum() or c in " _-").strip()
                    if safe_title:
                        arcname = f"{safe_title}.mp4"
                zf.write(fp, arcname)
    buffer.seek(0)
    from starlette.responses import StreamingResponse
    return StreamingResponse(
        buffer,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="clips_{int(time.time())}.zip"'},
    )


@app.post("/api/clips/item/{clip_id}/export-local")
async def export_clip_local(clip_id: str, req: dict = {}):
    """导出片段到本地目录"""
    clip = db.get_clip(clip_id)
    if not clip or not clip.get("output_file"):
        return JSONResponse({"error": "片段不存在"}, status_code=404)
    export_dir = req.get("export_dir", "") or app_settings.get("clip_export_dir", "")
    if not export_dir:
        return JSONResponse({"error": "未设置导出目录"}, status_code=400)
    export_path = Path(export_dir)
    if not export_path.exists():
        try:
            export_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            return JSONResponse({"error": f"无法创建目录: {e}"}, status_code=400)

    src = Path(RECORDINGS_DIR) / clip["output_file"]
    if not src.exists():
        return JSONResponse({"error": "源文件不存在"}, status_code=404)

    import shutil
    dest_name = src.name
    if clip.get("title"):
        safe_title = "".join(c for c in clip["title"] if c.isalnum() or c in " _-").strip()
        if safe_title:
            dest_name = f"{safe_title}.mp4"
    dest = export_path / dest_name
    try:
        shutil.copy2(str(src), str(dest))
        db.update_clip_status(clip_id, "exported", export_url=str(dest))
        return JSONResponse({"ok": True, "exported_to": str(dest)})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ========== FlashCut V3.0 API ==========

@app.get("/api/quota/{username}")
async def get_quota(username: str):
    """查询用户配额（完整套餐信息）"""
    from quota import QuotaManager
    qm = QuotaManager(db)
    info = qm.get_tier_info(username)
    info["username"] = username
    return JSONResponse(info)


@app.get("/api/quota/{username}/history")
async def get_quota_history(username: str, days: int = 30):
    """查询使用历史"""
    from quota import QuotaManager
    qm = QuotaManager(db)
    return JSONResponse(qm.get_usage_history(username, days))


@app.get("/api/tiers")
async def get_tier_definitions():
    """获取所有套餐定义"""
    from quota import QuotaManager
    return JSONResponse(QuotaManager.get_tier_definitions())


@app.post("/api/tier/{username}")
async def set_tier(username: str, req: dict):
    """设置用户等级"""
    from quota import QuotaManager
    tier = req.get("tier", "free")
    expires_at = req.get("expires_at", 0)
    qm = QuotaManager(db)
    try:
        qm.set_tier(username, tier, expires_at)
        return JSONResponse({"ok": True, "tier": tier})
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.post("/api/flashcut/{username}/auto")
async def trigger_flashcut_auto(username: str, req: dict = {}):
    """手动触发 FlashCut 全自动流水线"""
    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    filename = req.get("filename", "")
    if not filename:
        files = manager.get_recordings(username)
        merged = [f for f in files if "_merged" in f["filename"]]
        if merged:
            filename = merged[0]["filename"]
        elif files:
            filename = files[0]["filename"]
    if not filename:
        return JSONResponse({"error": "无可处理的录制文件"}, status_code=400)

    video_path = Path(RECORDINGS_DIR) / username / filename
    if not video_path.exists():
        return JSONResponse({"error": f"文件不存在: {filename}"}, status_code=404)

    # 异步执行流水线
    asyncio.create_task(manager._auto_flashcut_pipeline(username, video_path))
    return JSONResponse({"ok": True, "message": "FlashCut 流水线已启动", "filename": filename})


@app.get("/api/clips/item/{clip_id}/cover")
async def get_clip_cover(clip_id: str):
    """获取片段封面图"""
    clip = db.get_clip(clip_id)
    if not clip or not clip.get("output_file"):
        return JSONResponse({"error": "not found"}, status_code=404)
    # 封面文件名: {clip_id}_cover.jpg
    clip_path = Path(RECORDINGS_DIR) / clip["output_file"]
    cover_path = clip_path.with_name(clip_path.stem + "_cover.jpg")
    if cover_path.exists():
        return FileResponse(str(cover_path), media_type="image/jpeg",
                            headers={"Cache-Control": "max-age=3600"})
    return JSONResponse({"error": "no cover"}, status_code=404)


@app.get("/api/system/whisper")
async def check_whisper():
    """检查 Whisper 是否可用"""
    try:
        from subtitle_gen import is_whisper_available
        available = is_whisper_available()
        return JSONResponse({"available": available})
    except Exception:
        return JSONResponse({"available": False})


# ========== 分发 ==========

_distribute_manager = None

def _get_distribute_manager():
    global _distribute_manager
    if _distribute_manager is None:
        from distribute import (DistributeManager, MockPublisher,
                                DouyinPublisher, KuaishouPublisher,
                                BilibiliAssistPublisher, WeixinVideoPublisher)
        _distribute_manager = DistributeManager(db)
        # mock 发布器（测试用）
        _distribute_manager.register_publisher("mock", MockPublisher())
        _distribute_manager.set_credentials("mock", {"token": "test"})
        # OAuth 发布器（需要用户授权后才有效凭据）
        _distribute_manager.register_publisher("douyin", DouyinPublisher())
        _distribute_manager.register_publisher("kuaishou", KuaishouPublisher())
        # 辅助投稿（无需凭据，开箱即用）
        _distribute_manager.register_publisher("bilibili", BilibiliAssistPublisher())
        _distribute_manager.set_credentials("bilibili", {})
        _distribute_manager.register_publisher("weixinvideo", WeixinVideoPublisher())
        _distribute_manager.set_credentials("weixinvideo", {})
        # 从数据库加载已保存的 OAuth 凭据
        for platform in ("douyin", "kuaishou"):
            cred = db.get_credential("default", platform)
            if cred and cred.get("access_token"):
                _distribute_manager.set_credentials(platform, {
                    "access_token": cred["access_token"],
                    "refresh_token": cred.get("refresh_token", ""),
                    "openid": cred.get("openid", ""),
                    "expires_at": cred.get("expires_at", 0),
                })
    return _distribute_manager


@app.get("/api/distribute/platforms")
async def get_distribute_platforms():
    """获取可用的分发平台"""
    dm = _get_distribute_manager()
    return JSONResponse({"platforms": dm.get_available_platforms()})


@app.post("/api/distribute")
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


@app.post("/api/distribute/{task_id}/execute")
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


@app.post("/api/distribute/{task_id}/status")
async def check_distribute_status(task_id: str):
    """查询分发任务的平台状态"""
    dm = _get_distribute_manager()
    task = await dm.check_task_status(task_id)
    if not task:
        return JSONResponse({"error": "task not found"}, status_code=404)
    return JSONResponse({"ok": True, "task": task.to_dict()})


@app.get("/api/distribute/tasks")
async def get_distribute_tasks(username: str = "", platform: str = ""):
    """获取分发任务列表"""
    tasks = db.get_distribute_tasks(username=username, platform=platform)
    return JSONResponse(tasks)


@app.get("/api/distribute/tasks/{task_id}")
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


@app.get("/api/oauth/{platform}/authorize")
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


@app.get("/api/oauth/{platform}/callback")
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


@app.post("/api/oauth/{platform}/refresh")
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


@app.delete("/api/oauth/{platform}/revoke")
async def oauth_revoke(platform: str, request: Request):
    """撤销平台授权"""
    if platform not in OAUTH_CONFIGS:
        return JSONResponse({"error": f"Unsupported platform: {platform}"}, status_code=400)
    user_id = _get_request_user_id(request)
    db.delete_credential(user_id, platform)
    dm = _get_distribute_manager()
    dm._credentials.pop(platform, None)
    return JSONResponse({"ok": True})


@app.get("/api/oauth/{platform}/status")
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


# ========== 支付（Stripe） ==========

_payment_manager = None


def _get_payment_manager():
    global _payment_manager
    if _payment_manager is None:
        try:
            from payment import PaymentManager
            _payment_manager = PaymentManager(db)
        except ImportError:
            _payment_manager = None
    return _payment_manager


@app.get("/api/payment/tiers")
async def payment_tiers():
    """获取套餐定义"""
    pm = _get_payment_manager()
    if not pm:
        return JSONResponse({"error": "payment module not available"}, status_code=503)
    from payment import TIER_FEATURES
    return JSONResponse({"tiers": TIER_FEATURES})


@app.post("/api/payment/checkout")
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


@app.post("/api/payment/webhook")
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


@app.post("/api/payment/cancel")
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


@app.get("/api/payment/status")
async def payment_status(user_id: str = "default"):
    """查询订阅状态"""
    pm = _get_payment_manager()
    if not pm:
        return JSONResponse({"tier": "free", "status": "free", "payment_unavailable": True})
    result = pm.get_subscription_status(user_id)
    return JSONResponse(result)


@app.get("/api/tasks")
async def get_tasks(username: str = ""):
    """获取任务队列状态"""
    tasks = task_queue.get_tasks(username=username)
    return JSONResponse({"tasks": tasks})


@app.get("/api/tasks/{task_id}")
async def get_task(task_id: str):
    """获取单个任务状态"""
    task = task_queue.get_task(task_id)
    if not task:
        return JSONResponse({"error": "Task not found"}, status_code=404)
    return JSONResponse({
        "task_id": task.task_id,
        "name": task.name,
        "username": task.username,
        "priority": task.priority.name,
        "status": task.status,
        "progress": task.progress,
        "error": task.error,
        "created_at": task.created_at,
        "started_at": task.started_at,
        "finished_at": task.finished_at,
    })


@app.post("/api/tasks/{task_id}/cancel")
async def cancel_task(task_id: str):
    """取消待执行任务"""
    success = task_queue.cancel(task_id)
    if not success:
        return JSONResponse({"error": "Cannot cancel running/finished task"}, status_code=400)
    return JSONResponse({"success": True})


# ========== 配置导入/导出 ==========

@app.get("/api/config/export")
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


@app.post("/api/config/import")
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


# ========== 启动 ==========

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")
