"""存储管理路由 - 录制文件、会话、合并、清理"""
import asyncio
import logging
import time
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

router = APIRouter(prefix="/api", tags=["storage"])
logger = logging.getLogger("server")


class MergeRequest(BaseModel):
    files: list[str]
    delete_originals: bool = True


def _safe_username(username: str) -> bool:
    return ".." not in username and "/" not in username and "\\" not in username and username.strip() != ""


def _get_cleanup_reason(score: float, duration: int, days_old: float, has_highlight: bool) -> str:
    reasons = []
    if duration < 300: reasons.append("时长过短")
    if days_old > 90: reasons.append("超过90天未访问")
    elif days_old > 30: reasons.append("超过30天未访问")
    if not has_highlight: reasons.append("无高光片段")
    if score < 0: reasons.append("综合评分过低")
    return " · ".join(reasons) if reasons else "低价值文件"


@router.get("/storage/cleanup-suggestions")
async def get_cleanup_suggestions(min_score: float = 0.3):
    from server import manager, RECORDINGS_DIR

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
            score = 0.0
            file_path = rec_path / username / f["filename"]
            duration = f.get("duration", 0)
            if duration > 3600: score += 0.3
            elif duration > 1800: score += 0.2
            elif duration < 300: score -= 0.2
            days_old = 0
            if file_path.exists():
                days_old = (now - file_path.stat().st_atime) / 86400
                if days_old > 90: score -= 0.3
                elif days_old > 30: score -= 0.2
                elif days_old < 7: score += 0.1
            highlights = manager.get_highlights(username)
            has_highlight = any(h.get("source_file") == f["filename"] for h in highlights)
            if has_highlight: score += 0.4
            if "_merged" in f["filename"]: score += 0.2
            if score < min_score:
                suggestions.append({
                    "username": username,
                    "filename": f["filename"],
                    "size": f["size"],
                    "date": f["date"],
                    "duration": duration,
                    "score": round(score, 2),
                    "reason": _get_cleanup_reason(score, duration, days_old, has_highlight)
                })
    return JSONResponse(sorted(suggestions, key=lambda x: x["score"]))


@router.get("/storage/breakdown")
async def get_storage_breakdown():
    from server import RECORDINGS_DIR

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


@router.get("/recordings/{username}/groups")
async def get_recording_groups(username: str, gap: int = 15):
    from server import manager

    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    groups = manager.get_grouped_recordings(username, gap_minutes=gap)
    return JSONResponse(groups)


@router.post("/recordings/{username}/merge")
async def merge_recordings(username: str, req: MergeRequest):
    from server import manager

    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    if len(req.files) < 2:
        return JSONResponse({"error": "至少需要2个文件"}, status_code=400)
    for fn in req.files:
        if ".." in fn or "/" in fn:
            return JSONResponse({"error": f"非法文件名: {fn}"}, status_code=400)
    try:
        merge_id = await manager.merge_segments(username, req.files, delete_originals=req.delete_originals)
        status = manager._active_merges.get(merge_id, {}).get("status", "unknown")
        return JSONResponse({"merge_id": merge_id, "status": status})
    except (ValueError, FileNotFoundError) as e:
        logger.warning(f"Merge request failed for {username}: {e}")
        return JSONResponse({"error": str(e)}, status_code=400)


@router.delete("/recordings/{username}/{filename}")
async def delete_recording(username: str, filename: str):
    from server import RECORDINGS_DIR

    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    if ".." in filename or "/" in filename:
        return JSONResponse({"error": "invalid filename"}, status_code=400)
    video_path = Path(RECORDINGS_DIR) / username / filename
    if not await asyncio.to_thread(video_path.exists):
        return JSONResponse({"error": "not found"}, status_code=404)
    try:
        await asyncio.to_thread(video_path.unlink)
    except OSError as e:
        return JSONResponse({"error": f"删除失败: {e}"}, status_code=500)
    return JSONResponse({"ok": True})


@router.post("/recordings/{username}/{filename}/rename")
async def rename_recording(username: str, filename: str, req: dict):
    from server import RECORDINGS_DIR

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


@router.get("/recordings/{username}/export")
async def export_recordings_csv(username: str):
    import csv
    import io
    from datetime import datetime
    from server import manager

    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
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
        created = datetime.fromtimestamp(f["created"]).strftime("%Y-%m-%d %H:%M:%S") if f.get("created") else ""
        writer.writerow([f["filename"], f"{f['size']/1024/1024:.1f}", created, sid, status])
    content = output.getvalue()
    return Response(
        content=content,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{username}_recordings.csv"'},
    )


@router.post("/sessions/merge-all-ended")
async def merge_all_ended_sessions():
    from server import manager, app_settings

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
                    await manager.update_session_status(
                        username, s["session_id"],
                        "merged" if merge_info.get("status") == "done" else "error",
                        merged_file=merge_info.get("filename", ""),
                        merge_error=merge_info.get("error", ""))
                    results.append({"username": username, "session_id": s["session_id"],
                                    "status": merge_info.get("status", "unknown")})
                except Exception as e:
                    await manager.update_session_status(username, s["session_id"], "error", merge_error=str(e))
                    results.append({"username": username, "session_id": s["session_id"],
                                    "status": "error", "error": str(e)})
    return JSONResponse({"merged": len(results), "results": results})


@router.post("/sessions/cleanup")
async def cleanup_stale_sessions():
    from server import manager

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


@router.post("/recordings/cleanup-merged")
async def cleanup_merged_originals():
    from server import manager, RECORDINGS_DIR

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


@router.get("/recordings/{username}")
async def get_recordings(username: str):
    from server import manager

    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    files = manager.get_recordings(username)
    return JSONResponse(files)


@router.get("/sessions/{username}")
async def get_sessions(username: str):
    from server import manager

    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    sessions = manager.get_sessions(username)
    return JSONResponse(sessions)


@router.get("/sessions/{username}/summary")
async def get_sessions_summary(username: str):
    from server import manager, RECORDINGS_DIR

    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    sessions = manager.get_sessions(username)
    recordings = {f["filename"]: f for f in manager.get_recordings(username)}
    model_dir = Path(RECORDINGS_DIR) / username

    result = []
    for s in sorted(sessions, key=lambda x: x.get("started_at", 0), reverse=True):
        segments = s.get("segments", [])
        merged_file = s.get("merged_file", "")
        status = s.get("status", "unknown")
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


@router.post("/sessions/{username}/{session_id}/merge")
async def merge_session(username: str, session_id: str):
    from server import manager, app_settings, broadcast, RECORDINGS_DIR

    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    sessions = manager.get_sessions(username)
    target = next((s for s in sessions if s.get("session_id") == session_id), None)
    if not target:
        return JSONResponse({"error": "会话不存在"}, status_code=404)
    if target["status"] not in ("ended", "error"):
        return JSONResponse({"error": f"会话状态为 {target['status']}，无法合并"}, status_code=400)
    segments = target.get("segments", [])
    if len(segments) < 2:
        return JSONResponse({"error": "片段不足，无需合并"}, status_code=400)
    for fn in segments:
        if ".." in fn or "/" in fn:
            return JSONResponse({"error": f"非法文件名: {fn}"}, status_code=400)
    model_dir = Path(RECORDINGS_DIR) / username
    missing = [fn for fn in segments if not await asyncio.to_thread((model_dir / fn).exists)]
    if missing:
        return JSONResponse({"error": f"片段文件缺失: {', '.join(missing)}"}, status_code=400)
    await manager.update_session_status(username, session_id, "merging")
    try:
        merge_id = await manager.merge_segments(
            username, segments, delete_originals=app_settings.get("auto_delete_originals", True))
        merge_info = manager._active_merges.get(merge_id, {})
        if merge_info.get("status") == "done":
            merged_file = merge_info.get("filename", "")
            savings = merge_info.get("savings_bytes", 0)
            await manager.update_session_status(username, session_id, "merged",
                                          merged_file=merged_file,
                                          merge_type="manual",
                                          rollback_deadline=time.time() + 72 * 3600,
                                          original_segments=list(segments))
            await broadcast({
                "type": "merge_done",
                "data": {
                    "username": username, "merge_id": merge_id, "filename": merged_file,
                    "input_count": len(segments), "input_size": merge_info.get("input_size", 0),
                    "savings_bytes": savings,
                    "savings_pct": round(savings / max(merge_info.get("input_size", 1), 1) * 100, 1),
                }
            })
        else:
            await manager.update_session_status(username, session_id, "error",
                                          merge_error=merge_info.get("error", "合并失败"))
        return JSONResponse({"merge_id": merge_id, "status": merge_info.get("status", "unknown"),
                             "filename": merge_info.get("filename", "")})
    except (ValueError, FileNotFoundError) as e:
        await manager.update_session_status(username, session_id, "error", merge_error=str(e))
        return JSONResponse({"error": str(e)}, status_code=400)


@router.get("/merge-history/{username}")
async def get_merge_history(username: str):
    from server import db

    if not _safe_username(username):
        return JSONResponse({"error": "无效用户名"}, status_code=400)
    return JSONResponse(db.get_merge_history(username))


@router.get("/merge-history")
async def get_all_merge_history():
    from server import db
    return JSONResponse(db.get_merge_history(None))


@router.post("/recordings/{username}/merge/{merge_id}/cancel")
async def cancel_merge(username: str, merge_id: str):
    from server import manager, broadcast

    ok = await manager.cancel_merge(merge_id)
    if ok:
        for s_dict in manager.get_sessions(username):
            if s_dict.get("status") == "merging":
                await manager.update_session_status(username, s_dict["session_id"], "ended", merge_error="")
        await broadcast({"type": "merge_cancelled", "data": {"username": username, "merge_id": merge_id}})
        return JSONResponse({"ok": True})
    return JSONResponse({"error": "合并任务不存在或已完成"}, status_code=400)


@router.post("/sessions/retry-failed")
async def retry_failed_sessions():
    from server import manager

    retried = 0
    for username, rec in list(manager.recorders.items()):
        for s in rec._sessions:
            if s.status == "error" and s.retry_count < 3 and len(s.segments) >= 2:
                s.status = "ended"
                s.merge_error = ""
                retried += 1
        if retried:
            rec._save_sessions()
    for username in list(manager.recorders.keys()):
        try:
            await manager.auto_merge_for_model(username)
        except Exception as e:
            logger.error(f"Retry-failed merge error for {username}: {e}")
    return JSONResponse({"retried": retried})


@router.get("/recordings/{username}/health")
async def health_check_recordings(username: str):
    from server import RECORDINGS_DIR

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
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
            import json
            data = json.loads(stdout)
            streams = data.get("streams", [])
            if streams:
                s = streams[0]
                result["valid"] = True
                result["duration"] = float(s.get("duration", 0))
                result["codec"] = s.get("codec_name", "")
                w, h = s.get("width", 0), s.get("height", 0)
                result["resolution"] = f"{w}x{h}" if w and h else ""
        except (json.JSONDecodeError, OSError):
            pass
        return result

    mp4_files = [f for f in model_dir.glob("*.mp4") if ".raw." not in f.name]
    results = await asyncio.gather(*[_probe(f) for f in mp4_files[:20]])
    return JSONResponse(list(results))


@router.post("/recordings/cleanup-old")
async def cleanup_old_recordings(req: dict = None):
    from server import manager, RECORDINGS_DIR

    req = req or {}
    days = req.get("days", 30)
    dry_run = req.get("dry_run", True)
    cutoff = time.time() - days * 86400
    cleaned = 0
    freed = 0
    rec_path = Path(RECORDINGS_DIR)
    for user_dir in rec_path.iterdir():
        if not user_dir.is_dir() or user_dir.name in ("thumbs", "logs"):
            continue
        for fp in user_dir.glob("*.mp4"):
            st = await asyncio.to_thread(fp.stat)
            if st.st_mtime < cutoff and ".raw." not in fp.name:
                freed += st.st_size
                if not dry_run:
                    await asyncio.to_thread(fp.unlink)
                cleaned += 1
    return JSONResponse({"cleaned": cleaned, "freed_bytes": freed,
                         "freed_mb": round(freed / 1024 / 1024, 1), "dry_run": dry_run})


@router.get("/merge-queue")
async def get_merge_queue():
    from server import manager

    queue = []
    for username, rec in manager.recorders.items():
        for s in rec._sessions:
            if s.status == "ended" and len(s.segments) >= 2:
                queue.append({
                    "session_id": s.session_id,
                    "username": username,
                    "segments": s.segments,
                    "started_at": s.started_at,
                    "ended_at": s.ended_at,
                })
    return JSONResponse(queue)


@router.post("/merge-queue/{session_id}/confirm")
async def confirm_merge_queue(session_id: str, req: dict = None):
    from server import manager, app_settings

    req = req or {}
    for username, rec in manager.recorders.items():
        for s in rec._sessions:
            if s.session_id == session_id and s.status == "ended":
                try:
                    merge_id = await manager.merge_segments(
                        username, s.segments,
                        delete_originals=req.get("delete_originals",
                                                  app_settings.get("auto_delete_originals", True)))
                    return JSONResponse({"ok": True, "merge_id": merge_id})
                except Exception as e:
                    return JSONResponse({"error": str(e)}, status_code=500)
    return JSONResponse({"error": "会话不存在或状态不符"}, status_code=404)


@router.post("/merge-queue/{session_id}/dismiss")
async def dismiss_merge_queue(session_id: str):
    from server import manager

    for username, rec in manager.recorders.items():
        for s in rec._sessions:
            if s.session_id == session_id:
                s.status = "error"
                s.merge_error = "用户跳过"
                rec._save_sessions()
                return JSONResponse({"ok": True})
    return JSONResponse({"error": "会话不存在"}, status_code=404)


@router.post("/merge-queue/confirm-all")
async def confirm_all_merge_queue(req: dict = None):
    from server import manager, app_settings

    req = req or {}
    confirmed = 0
    for username, rec in list(manager.recorders.items()):
        for s in rec._sessions:
            if s.status == "ended" and len(s.segments) >= 2:
                try:
                    await manager.merge_segments(
                        username, s.segments,
                        delete_originals=req.get("delete_originals",
                                                  app_settings.get("auto_delete_originals", True)))
                    confirmed += 1
                except Exception as e:
                    logger.error(f"confirm-all merge error for {username}/{s.session_id}: {e}")
    return JSONResponse({"confirmed": confirmed})


@router.post("/sessions/{session_id}/rollback")
async def rollback_session(session_id: str):
    from server import manager, RECORDINGS_DIR

    for username, rec in manager.recorders.items():
        for s in rec._sessions:
            if s.session_id == session_id:
                if s.status != "merged":
                    return JSONResponse({"error": "只有已合并的会话才能撤回"}, status_code=400)
                if s.rollback_deadline and time.time() > s.rollback_deadline:
                    return JSONResponse({"error": "撤回期限已过（72小时）"}, status_code=400)
                model_dir = Path(RECORDINGS_DIR) / username
                merged_path = model_dir / s.merged_file if s.merged_file else None
                if merged_path and merged_path.exists():
                    merged_path.unlink()
                s.status = "ended"
                s.merged_file = ""
                s.merge_error = ""
                s.rollback_deadline = 0
                rec._save_sessions()
                return JSONResponse({"ok": True, "restored_segments": s.original_segments})
    return JSONResponse({"error": "会话不存在"}, status_code=404)


@router.post("/recordings/{username}/merge/preview")
async def preview_merge(username: str, req: dict = None):
    from server import manager

    req = req or {}
    if not _safe_username(username):
        return JSONResponse({"error": "invalid username"}, status_code=400)
    gap = req.get("gap_minutes", 15)
    groups = manager.get_grouped_recordings(username, gap_minutes=gap)
    preview = []
    for g in groups:
        if len(g.get("files", [])) >= 2:
            total_size = sum(f.get("size", 0) for f in g["files"])
            preview.append({
                "files": [f["filename"] for f in g["files"]],
                "count": len(g["files"]),
                "total_size": total_size,
                "estimated_output": f"{total_size / 1024 / 1024:.1f}MB",
            })
    return JSONResponse({"groups": preview, "total_groups": len(preview)})
