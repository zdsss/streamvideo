"""Clips API 路由 - 片段管理、FlashCut、配额"""
import asyncio
import io
import json
import shutil
import time
import zipfile
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

router = APIRouter()

# 全局依赖（由 server.py 注入）
db = None
manager = None
app_settings = None
RECORDINGS_DIR = None
broadcast = None


def init_clips_router(database, recorder_manager, settings, recordings_dir, ws_broadcast):
    """初始化路由依赖"""
    global db, manager, app_settings, RECORDINGS_DIR, broadcast
    db = database
    manager = recorder_manager
    app_settings = settings
    RECORDINGS_DIR = recordings_dir
    broadcast = ws_broadcast


def _safe_username(username: str) -> bool:
    """验证 username 不含路径遍历字符"""
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


@router.get("/api/clips")
async def get_all_clips(limit: int = 100):
    """列出所有片段（按用户名分组）"""
    clips = db.get_all_clips(limit)
    grouped: dict[str, list] = {}
    for c in clips:
        grouped.setdefault(c["username"], []).append(c)
    return JSONResponse([{"username": u, "clips": cl} for u, cl in grouped.items()])


@router.get("/api/clips/{username}")
async def get_clips(username: str, limit: int = 50):
    """列出片段"""
    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    return JSONResponse(db.get_clips(username, limit))


@router.get("/api/clips/file/{username}/{filename}")
async def get_clip_file(username: str, filename: str, download: int = 0):
    """播放/下载片段"""
    if not _safe_username(username) or not _safe_filename(filename):
        return JSONResponse({"error": "invalid"}, status_code=400)
    clip_path = Path(RECORDINGS_DIR) / username / "clips" / filename
    if clip_path.exists():
        if download:
            return FileResponse(str(clip_path), media_type="video/mp4",
                                headers={"Content-Disposition": f'attachment; filename="{filename}"'})
        return FileResponse(str(clip_path), media_type="video/mp4")
    return JSONResponse({"error": "not found"}, status_code=404)


@router.delete("/api/clips/item/{clip_id}")
async def delete_clip(clip_id: str):
    """删除片段"""
    clip = db.get_clip(clip_id)
    if clip and clip.get("output_file"):
        fp = Path(RECORDINGS_DIR) / clip["output_file"]
        if fp.exists():
            fp.unlink()
    db.delete_clip(clip_id)
    return JSONResponse({"ok": True})


@router.get("/api/highlight-rules")
async def get_highlight_rules(username: str = ""):
    """获取高光规则"""
    return JSONResponse(db.get_highlight_rules(username))


@router.post("/api/highlight-rules")
async def upsert_highlight_rule(req: dict):
    """创建/更新高光规则"""
    rule_id = req.get("rule_id")
    db.upsert_highlight_rule(rule_id, **{k: v for k, v in req.items() if k != "rule_id"})
    return JSONResponse({"ok": True})


@router.delete("/api/highlight-rules/{rule_id}")
async def delete_highlight_rule(rule_id: int):
    """删除高光规则"""
    db.delete_highlight_rule(rule_id)
    return JSONResponse({"ok": True})


@router.get("/api/danmaku/{session_id}")
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


@router.get("/api/danmaku/{session_id}/timeline")
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


@router.get("/api/analytics/highlights")
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


@router.get("/api/analytics/clips")
async def get_clip_analytics(username: str = ""):
    """片段分析统计"""
    stats = db.get_clip_stats(username)
    return JSONResponse(stats)


# ========== V2.0 扩展: 手动片段 + ZIP + 元数据 + 导出 ==========

@router.post("/api/clips/manual")
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
    if not _safe_filename(filename):
        return JSONResponse({"error": "invalid filename"}, status_code=400)

    video_path = Path(RECORDINGS_DIR) / username / filename
    if not video_path.exists():
        return JSONResponse({"error": f"文件不存在: {filename}"}, status_code=404)

    from streamvideo.core.auth.quota import QuotaManager
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
        # 更新标题
        db.update_clip_status(result["clip_id"], "done", title=title)
        await broadcast({"type": "clip_done", "data": {
            "username": username, "clip_id": result["clip_id"], "filename": result.get("filename", "")}})
    return JSONResponse(result)


@router.post("/api/clips/item/{clip_id}/metadata")
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


@router.post("/api/clips/download-zip")
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


@router.post("/api/clips/item/{clip_id}/export-local")
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

@router.get("/api/quota/{username}")
async def get_quota(username: str):
    """查询用户配额（完整套餐信息）"""
    from streamvideo.core.auth.quota import QuotaManager
    qm = QuotaManager(db)
    info = qm.get_tier_info(username)
    info["username"] = username
    return JSONResponse(info)


@router.get("/api/quota/{username}/history")
async def get_quota_history(username: str, days: int = 30):
    """查询使用历史"""
    from streamvideo.core.auth.quota import QuotaManager
    qm = QuotaManager(db)
    return JSONResponse(qm.get_usage_history(username, days))


@router.get("/api/tiers")
async def get_tier_definitions():
    """获取所有套餐定义"""
    from streamvideo.core.auth.quota import QuotaManager
    return JSONResponse(QuotaManager.get_tier_definitions())


@router.post("/api/tier/{username}")
async def set_tier(username: str, req: dict):
    """设置用户等级"""
    from streamvideo.core.auth.quota import QuotaManager
    tier = req.get("tier", "free")
    expires_at = req.get("expires_at", 0)
    qm = QuotaManager(db)
    try:
        qm.set_tier(username, tier, expires_at)
        return JSONResponse({"ok": True, "tier": tier})
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@router.post("/api/flashcut/{username}/auto")
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


@router.get("/api/clips/item/{clip_id}/cover")
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
