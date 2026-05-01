"""高光与片段路由 - 高光检测、片段生成、FlashCut"""
import json
import logging
import time
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

router = APIRouter(prefix="/api", tags=["highlights"])
logger = logging.getLogger("server")


def _safe_username(username: str) -> bool:
    return ".." not in username and "/" not in username and "\\" not in username and username.strip() != ""


@router.post("/highlights/{username}/detect")
async def detect_highlights(username: str, req: dict = {}):
    from server import manager, db, broadcast, app_settings, RECORDINGS_DIR

    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    filename = req.get("filename", "")
    session_id = req.get("session_id", "")
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
        import uuid as _uuid

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
    from server import db

    highlights = db.get_all_highlights(limit)
    grouped: dict[str, list] = {}
    for h in highlights:
        grouped.setdefault(h["username"], []).append(h)
    return JSONResponse([{"username": u, "highlights": hl} for u, hl in grouped.items()])


@router.get("/highlights/{username}")
async def get_highlights(username: str, limit: int = 50):
    from server import db

    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    return JSONResponse(db.get_highlights(username, limit))


@router.delete("/highlights/item/{highlight_id}")
async def delete_highlight(highlight_id: str):
    from server import db
    db.delete_highlight(highlight_id)
    return JSONResponse({"ok": True})


@router.post("/highlights/item/{highlight_id}/clip")
async def generate_clip_from_highlight(highlight_id: str, req: dict = {}):
    from server import db, broadcast, app_settings, RECORDINGS_DIR
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
    from server import db, broadcast, app_settings, RECORDINGS_DIR
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


@router.get("/clips/{username}")
async def get_clips(username: str, limit: int = 50):
    from server import db

    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    return JSONResponse(db.get_clips(username, limit))


@router.get("/clips/file/{username}/{filename}")
async def get_clip_file(username: str, filename: str, download: int = 0):
    from server import RECORDINGS_DIR

    if not _safe_username(username) or ".." in filename:
        return JSONResponse({"error": "invalid"}, status_code=400)
    clip_path = Path(RECORDINGS_DIR) / username / "clips" / filename
    if clip_path.exists():
        if download:
            return FileResponse(str(clip_path), media_type="video/mp4",
                                headers={"Content-Disposition": f'attachment; filename="{filename}"'})
        return FileResponse(str(clip_path), media_type="video/mp4")
    return JSONResponse({"error": "not found"}, status_code=404)


@router.delete("/clips/item/{clip_id}")
async def delete_clip(clip_id: str):
    from server import db, RECORDINGS_DIR

    clip = db.get_clip(clip_id)
    if clip and clip.get("output_file"):
        fp = Path(RECORDINGS_DIR) / clip["output_file"]
        if fp.exists():
            fp.unlink()
    db.delete_clip(clip_id)
    return JSONResponse({"ok": True})


@router.get("/highlight-rules")
async def get_highlight_rules(username: str = ""):
    from server import db
    return JSONResponse(db.get_highlight_rules(username))


@router.post("/highlight-rules")
async def upsert_highlight_rule(req: dict):
    from server import db
    rule_id = req.get("rule_id")
    db.upsert_highlight_rule(rule_id, **{k: v for k, v in req.items() if k != "rule_id"})
    return JSONResponse({"ok": True})


@router.delete("/highlight-rules/{rule_id}")
async def delete_highlight_rule(rule_id: int):
    from server import db
    db.delete_highlight_rule(rule_id)
    return JSONResponse({"ok": True})


@router.get("/danmaku/{session_id}")
async def get_danmaku(session_id: str):
    from server import db

    dm = db.get_danmaku(session_id)
    if not dm:
        return JSONResponse({"error": "not found"}, status_code=404)
    if dm.get("file_path") and Path(dm["file_path"]).exists():
        with open(dm["file_path"], encoding="utf-8") as f:
            full_data = json.load(f)
        dm["messages"] = full_data.get("messages", [])
        dm["stats"] = full_data.get("stats", {})
    return JSONResponse(dm)


@router.get("/danmaku/{session_id}/timeline")
async def get_danmaku_timeline(session_id: str, window: int = 10):
    from server import db

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


@router.get("/analytics/highlights")
async def get_highlight_analytics(username: str = "", days: int = 30):
    from server import db

    highlights = db.get_all_highlights(limit=500) if not username else db.get_highlights(username, 500)
    total = len(highlights)
    by_category = {}
    by_score = {"high": 0, "medium": 0, "low": 0}
    for h in highlights:
        cat = h.get("category", "unknown")
        by_category[cat] = by_category.get(cat, 0) + 1
        score = h.get("score", 0)
        if score >= 0.8: by_score["high"] += 1
        elif score >= 0.5: by_score["medium"] += 1
        else: by_score["low"] += 1
    return JSONResponse({"total": total, "by_category": by_category, "by_score": by_score})


@router.get("/analytics/clips")
async def get_clip_analytics(username: str = ""):
    from server import db
    return JSONResponse(db.get_clip_stats(username))


@router.post("/clips/manual")
async def create_manual_clip(req: dict):
    from server import db, broadcast, app_settings, RECORDINGS_DIR
    from streamvideo.core.auth.quota import QuotaManager

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

    import uuid as _uuid
    highlight = {
        "highlight_id": f"manual_{int(time.time())}",
        "username": username, "video_file": filename,
        "start_time": start_time, "end_time": end_time,
    }
    danmaku_path = None
    for dp in video_path.parent.glob("*_danmaku.json"):
        danmaku_path = dp
        break

    from streamvideo.core.processor.clipgen import ClipGenerator, ClipConfig
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
        db.update_clip_status(result["clip_id"], "done", title=title)
        await broadcast({"type": "clip_done", "data": {
            "username": username, "clip_id": result["clip_id"], "filename": result.get("filename", "")}})
    return JSONResponse(result)


@router.post("/clips/item/{clip_id}/metadata")
async def update_clip_metadata(clip_id: str, req: dict):
    from server import db

    clip = db.get_clip(clip_id)
    if not clip:
        return JSONResponse({"error": "片段不存在"}, status_code=404)
    kwargs = {k: req[k] for k in ("title", "description", "tags") if k in req}
    if kwargs:
        db.update_clip_status(clip_id, clip.get("status", "done"), **kwargs)
    return JSONResponse({"ok": True})


@router.post("/clips/download-zip")
async def download_clips_zip(req: dict):
    from server import db, RECORDINGS_DIR
    import io
    import zipfile

    clip_ids = req.get("clip_ids", [])
    if not clip_ids:
        return JSONResponse({"error": "未选择片段"}, status_code=400)

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
    return StreamingResponse(
        buffer, media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="clips_{int(time.time())}.zip"'},
    )


@router.post("/clips/item/{clip_id}/export-local")
async def export_clip_local(clip_id: str, req: dict = {}):
    from server import db, app_settings, RECORDINGS_DIR
    import shutil

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


@router.get("/clips/item/{clip_id}/cover")
async def get_clip_cover(clip_id: str):
    from server import db, RECORDINGS_DIR

    clip = db.get_clip(clip_id)
    if not clip or not clip.get("output_file"):
        return JSONResponse({"error": "not found"}, status_code=404)
    clip_path = Path(RECORDINGS_DIR) / clip["output_file"]
    cover_path = clip_path.with_name(clip_path.stem + "_cover.jpg")
    if cover_path.exists():
        return FileResponse(str(cover_path), media_type="image/jpeg")
    return JSONResponse({"error": "cover not found"}, status_code=404)


@router.post("/flashcut/{username}/auto")
async def trigger_flashcut_auto(username: str, req: dict = {}):
    import asyncio
    from server import manager, RECORDINGS_DIR

    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    filename = req.get("filename", "")
    if not filename:
        files = manager.get_recordings(username)
        merged = [f for f in files if "_merged" in f["filename"]]
        filename = merged[0]["filename"] if merged else (files[0]["filename"] if files else "")
    if not filename:
        return JSONResponse({"error": "无可处理的录制文件"}, status_code=400)
    video_path = Path(RECORDINGS_DIR) / username / filename
    if not video_path.exists():
        return JSONResponse({"error": f"文件不存在: {filename}"}, status_code=404)
    asyncio.create_task(manager._auto_flashcut_pipeline(username, video_path))
    return JSONResponse({"ok": True, "message": "FlashCut 流水线已启动", "filename": filename})
