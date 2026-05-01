"""高光路由 - 高光检测、片段生成"""
import logging
import time
import uuid as _uuid
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api", tags=["highlights"])
logger = logging.getLogger("server")

# 全局依赖（由 server.py 注入）
db = None
manager = None
app_settings = None
RECORDINGS_DIR = None
broadcast = None


def init_highlights_router(database, recorder_manager, settings, recordings_dir, ws_broadcast):
    """初始化路由依赖"""
    global db, manager, app_settings, RECORDINGS_DIR, broadcast
    db = database
    manager = recorder_manager
    app_settings = settings
    RECORDINGS_DIR = recordings_dir
    broadcast = ws_broadcast


def _safe_username(username: str) -> bool:
    return ".." not in username and "/" not in username and "\\" not in username and username.strip() != ""


def _safe_filename(f: str) -> bool:
    """Reject path traversal and non-printable chars in filenames"""
    if not f or ".." in f:
        return False
    if any(ord(c) < 32 for c in f):
        return False
    if f.startswith("/") or f.startswith("\\"):
        return False
    return True


@router.post("/highlights/{username}/detect")
async def detect_highlights(username: str, req: dict = {}):
    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    filename = req.get("filename", "")
    session_id = req.get("session_id", "")
    if filename and not _safe_filename(filename):
        return JSONResponse({"error": "invalid filename"}, status_code=400)
    if not filename:
        files = manager.get_recordings(username)
        merged = [f for f in files if "_merged" in f["filename"]]
        filename = merged[0]["filename"] if merged else (files[0]["filename"] if files else "")
    if not filename:
        return JSONResponse({"error": "无可检测的录制文件"}, status_code=400)

    video_path = Path(RECORDINGS_DIR) / username / filename
    if not video_path.exists():
        return JSONResponse({"error": f"文件不存在: {filename}"}, status_code=404)

    try:
        from streamvideo.core.processor.highlight import HighlightDetector

        config = {k: v for k, v in app_settings.items() if k.startswith("highlight_")}
        detector = HighlightDetector(config)
        danmaku_path = None
        if session_id:
            dp = video_path.parent / f"{session_id}_danmaku.json"
            if dp.exists():
                danmaku_path = dp
        if not danmaku_path:
            for dp in video_path.parent.glob("*_danmaku.json"):
                danmaku_path = dp
                break

        highlights = await detector.detect(video_path, danmaku_path)
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


@router.get("/highlights")
async def get_all_highlights(limit: int = 100):
    highlights = db.get_all_highlights(limit)
    grouped: dict[str, list] = {}
    for h in highlights:
        grouped.setdefault(h["username"], []).append(h)
    return JSONResponse([{"username": u, "highlights": hl} for u, hl in grouped.items()])


@router.get("/highlights/{username}")
async def get_highlights(username: str, limit: int = 50):
    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    return JSONResponse(db.get_highlights(username, limit))


@router.delete("/highlights/item/{highlight_id}")
async def delete_highlight(highlight_id: str):
    db.delete_highlight(highlight_id)
    return JSONResponse({"ok": True})


@router.post("/highlights/item/{highlight_id}/clip")
async def generate_clip_from_highlight(highlight_id: str, req: dict = {}):
    from streamvideo.core.auth.quota import QuotaManager

    h = db.get_highlight(highlight_id)
    if not h:
        return JSONResponse({"error": "高光不存在"}, status_code=404)

    username = h["username"]
    allowed, used, limit = QuotaManager(db).check_quota(username)
    if not allowed:
        return JSONResponse({"error": f"今日配额已用完 ({used}/{limit})，请升级套餐", "quota_exceeded": True}, status_code=429)

    video_path = Path(RECORDINGS_DIR) / username / h["video_file"]
    if not video_path.exists():
        return JSONResponse({"error": f"视频文件不存在: {h['video_file']}"}, status_code=404)

    danmaku_path = None
    if h.get("session_id"):
        dp = video_path.parent / f"{h['session_id']}_danmaku.json"
        if dp.exists():
            danmaku_path = dp

    from streamvideo.core.processor.clipgen import ClipGenerator, ClipConfig
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


@router.post("/highlights/batch-clip")
async def batch_generate_clips(req: dict):
    from streamvideo.core.auth.quota import QuotaManager
    from streamvideo.core.processor.clipgen import ClipGenerator, ClipConfig

    highlight_ids = req.get("highlight_ids", [])
    if not highlight_ids:
        return JSONResponse({"error": "未选择高光"}, status_code=400)

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
