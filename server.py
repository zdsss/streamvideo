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

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from recorder import ModelInfo, RecorderManager, RecordingSession, RecordingState
from database import Database

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
PROXY = "http://127.0.0.1:7890"
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
}

# 运行时设置（从 config.json 加载）
app_settings: dict = {**DEFAULT_SETTINGS}

# SQLite 数据库
db = Database(str(BASE_DIR / "streamvideo.db"))

# WebSocket 连接管理
ws_clients: set[WebSocket] = set()


async def broadcast(data: dict):
    """广播消息给所有 WebSocket 客户端"""
    if not ws_clients:
        return
    msg = json.dumps(data, ensure_ascii=False)
    dead = set()
    for ws in ws_clients:
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
    msg_type = "merge_done" if status == "done" else "merge_error"
    await broadcast({"type": msg_type, "data": data})


async def on_merge_progress(username: str, merge_id: str, progress: float, message: str):
    """合并进度回调"""
    await broadcast({"type": "merge_progress", "data": {
        "username": username, "merge_id": merge_id,
        "progress": progress, "message": message,
    }})


# 全局 manager
manager = RecorderManager(
    output_dir=RECORDINGS_DIR,
    proxy=PROXY,
    on_state_change=on_state_change,
)
manager._merge_callback = on_merge_update
manager._merge_progress_callback = on_merge_progress


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
    for key, rec in manager.recorders.items():
        db.upsert_model(rec.info.username, rec.info.live_url or rec.identifier,
                        platform=rec.info.platform, display_name=rec.info.username,
                        quality=rec.quality, auto_merge=rec.auto_merge,
                        schedule=rec.schedule)

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
        else:
            url = item
            saved_name = ""
            saved_schedule = None
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
                rec.info.quality = saved_quality
    apply_settings_to_recorders()
    await manager.start_all()
    logger.info(f"已启动 {len(manager.recorders)} 个主播监控 (auto_merge={app_settings['auto_merge']})")

    # 启动时自动合并遗留片段
    if app_settings["auto_merge"]:
        asyncio.create_task(startup_auto_merge())

    yield

    await manager.stop_all()
    save_config()
    logger.info("服务已关闭")


async def startup_auto_merge():
    """启动时扫描所有 sessions.json，恢复未完成的会话并触发合并"""
    await asyncio.sleep(3)  # 等待服务完全启动
    rec_path = Path(RECORDINGS_DIR)
    if not rec_path.exists():
        return

    # 1. 扫描所有目录的 sessions.json，恢复 active/ended 会话
    for d in rec_path.iterdir():
        if not d.is_dir() or d.name in ("thumbs", "logs"):
            continue
        sessions_path = d / "sessions.json"
        if sessions_path.exists():
            try:
                with open(sessions_path) as f:
                    sessions = [RecordingSession.from_dict(s) for s in json.load(f)]
                changed = False
                for s in sessions:
                    if s.status == "active":
                        # 服务重启，active 会话无 recorder 或 recorder 未在录制 → 标记 ended
                        rec = manager.recorders.get(d.name)
                        is_active = rec and rec.info.state.value in ("recording", "reconnecting")
                        if not is_active:
                            s.status = "ended"
                            s.ended_at = time.time()
                            changed = True
                            logger.info(f"[{d.name}] Recovered orphaned session: {s.session_id}")
                if changed:
                    with open(sessions_path, "w") as f:
                        json.dump([s.to_dict() for s in sessions], f, ensure_ascii=False, indent=2)
                    # 同步到 recorder 内存
                    rec = manager.recorders.get(d.name)
                    if rec:
                        rec._sessions = sessions
            except Exception as e:
                logger.error(f"Startup session recovery error for {d.name}: {e}")

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


def apply_settings_to_recorders():
    """将全局设置应用到所有录制器"""
    manager._post_process_rename = app_settings.get("smart_rename", False)
    manager._post_process_h265 = app_settings.get("h265_transcode", False)
    manager.cloud.config = app_settings.get("cloud_upload") or None
    manager.webhook.webhooks = app_settings.get("webhooks", [])
    for rec in manager.recorders.values():
        rec.auto_merge = app_settings["auto_merge"]
        rec.info.auto_merge = app_settings["auto_merge"]
        rec.min_segment_size = app_settings["min_segment_size_kb"] * 1024
        rec.auto_delete_originals = app_settings["auto_delete_originals"]


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

        # 保持连接
        while True:
            data = await ws.receive_text()
            # 客户端可以发送 ping
            if data == "ping":
                await ws.send_text(json.dumps({"type": "pong"}))
    except WebSocketDisconnect:
        pass
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
    # 录制目录大小
    total_rec = sum(f.stat().st_size for f in rec_path.rglob("*") if f.is_file())
    # 磁盘剩余空间
    disk = shutil.disk_usage(str(rec_path))
    return JSONResponse({
        "recordings_bytes": total_rec,
        "free_bytes": disk.free,
        "total_bytes": disk.total,
    })


@app.get("/api/settings")
async def get_settings():
    return JSONResponse(app_settings)


@app.get("/api/stats")
async def get_stats():
    from datetime import datetime
    rec_path = Path(RECORDINGS_DIR)
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


@app.get("/api/storage/breakdown")
async def get_storage_breakdown():
    """按主播的存储占用明细"""
    rec_path = Path(RECORDINGS_DIR)
    if not rec_path.exists():
        return JSONResponse([])
    breakdown = []
    for d in sorted(rec_path.iterdir()):
        if not d.is_dir() or d.name in ("thumbs", "logs"):
            continue
        files = list(d.glob("*.mp4"))
        files = [f for f in files if ".raw." not in f.name]
        total_size = sum(f.stat().st_size for f in files)
        merged = [f for f in files if "_merged" in f.name]
        unmerged = [f for f in files if "_merged" not in f.name]
        oldest = min((f.stat().st_mtime for f in files), default=0)
        breakdown.append({
            "username": d.name,
            "total_size": total_size,
            "file_count": len(files),
            "merged_count": len(merged),
            "unmerged_count": len(unmerged),
            "oldest_file": oldest,
        })
    breakdown.sort(key=lambda x: x["total_size"], reverse=True)
    return JSONResponse(breakdown)


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
    for key in DEFAULT_SETTINGS:
        if key in req:
            app_settings[key] = req[key]
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
    if ".." in filename or "/" in filename:
        return JSONResponse({"error": "invalid filename"}, status_code=400)
    video_path = Path(RECORDINGS_DIR) / username / filename
    if not video_path.exists():
        return JSONResponse({"error": "not found"}, status_code=404)
    video_path.unlink()
    return JSONResponse({"ok": True})


@app.post("/api/recordings/{username}/{filename}/rename")
async def rename_recording(username: str, filename: str, req: dict):
    """重命名录制文件"""
    new_name = req.get("new_name", "").strip()
    if not new_name or ".." in new_name or "/" in new_name:
        return JSONResponse({"error": "无效的文件名"}, status_code=400)
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
    old_path.rename(new_path)
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


@app.get("/api/recordings/{username}")
async def get_recordings(username: str):
    files = manager.get_recordings(username)
    return JSONResponse(files)


@app.get("/api/sessions/{username}")
async def get_sessions(username: str):
    """获取指定主播的所有录制会话"""
    sessions = manager.get_sessions(username)
    return JSONResponse(sessions)


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
            manager.update_session_status(username, session_id, "merged",
                                          merged_file=merge_info.get("filename", ""))
        else:
            manager.update_session_status(username, session_id, "error",
                                          merge_error=merge_info.get("error", "合并失败"))
        return JSONResponse({"merge_id": merge_id, "status": merge_info.get("status", "unknown")})
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


@app.post("/api/models/{username}/schedule")
async def set_model_schedule(username: str, req: dict):
    """设置主播的定时录制计划"""
    rec = manager.recorders.get(username)
    if not rec:
        return JSONResponse({"error": "主播不存在"}, status_code=404)
    rec.schedule = req.get("schedule")
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
    ok = await manager.webhook.test(req)
    return JSONResponse({"ok": ok})


@app.get("/api/thumb/{username}")
async def get_thumbnail(username: str):
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


# ========== 启动 ==========

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")
