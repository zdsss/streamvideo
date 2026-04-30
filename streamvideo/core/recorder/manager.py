import asyncio
import json
import logging
import os
import re
import shutil
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import urlparse

import aiohttp

logger = logging.getLogger("recorder")

from streamvideo.core.recorder.models import *
from streamvideo.core.recorder.base import BaseLiveRecorder
from streamvideo.core.recorder.uploader import CloudUploader
from streamvideo.core.recorder.notifier import WebhookNotifier
from streamvideo.core.recorder.engines.douyin import DouyinRecorder
from streamvideo.core.recorder.engines.bilibili import BilibiliRecorder
from streamvideo.core.recorder.engines.twitch import TwitchRecorder
from streamvideo.core.recorder.engines.youtube import YouTubeRecorder
from streamvideo.core.recorder.engines.huya import HuyaRecorder
from streamvideo.core.recorder.engines.douyu import DouyuRecorder
from streamvideo.core.recorder.engines.kick import KickRecorder
from streamvideo.core.recorder.engines.generic import GenericRecorder

class RecorderManager:
    """管理多个主播的录制器"""

    def __init__(self, output_dir: str = "recordings",
                 proxy: str = "",
                 on_state_change: Optional[Callable] = None,
                 db=None):
        self.output_dir = output_dir
        self.proxy = proxy or os.environ.get("SV_PROXY", "http://127.0.0.1:7890")
        self.on_state_change = on_state_change
        self.db = db  # Database instance for merge history
        self.recorders: dict[str, BaseLiveRecorder] = {}
        self._thumb_task: Optional[asyncio.Task] = None
        self._active_merges: dict[str, dict] = {}
        self._merge_timeout = 14400  # 默认 4 小时（秒），可通过设置覆盖
        self._merge_gap_minutes = 15  # 合并分组时间间隔（分钟）
        self._post_process_rename = False  # 智能重命名开关
        self._post_process_h265 = False  # H.265 转码开关
        self._post_process_script = ""  # 录后脚本路径
        self._filename_template = "{username}_{date}_{duration}_merged"  # 文件名模板
        self._highlight_auto_detect = False  # 合并后自动检测高光
        self._highlight_config = {}  # 高光检测配置
        self._disk_warning_callback = None  # 磁盘警告回调
        self.cloud = CloudUploader()
        self.webhook = WebhookNotifier()
        self._ytdlp_available = shutil.which("yt-dlp") is not None
        self._streamlink_available = shutil.which("streamlink") is not None
        logger.info(f"Recording engines: yt-dlp={'yes' if self._ytdlp_available else 'no'}, streamlink={'yes' if self._streamlink_available else 'no'}")

    def _persist_sessions(self, username: str, sessions: list):
        """统一持久化 sessions：SQLite 为主，JSON 为备份"""
        # SQLite
        if self.db:
            try:
                for s in sessions:
                    self.db.upsert_session(s.to_dict() if hasattr(s, 'to_dict') else s)
            except Exception as e:
                logger.warning(f"[{username}] Failed to persist sessions to SQLite: {e}")
        # JSON 备份
        model_dir = Path(self.output_dir) / username
        model_dir.mkdir(parents=True, exist_ok=True)
        try:
            data = [s.to_dict() if hasattr(s, 'to_dict') else s for s in sessions]
            with open(model_dir / "sessions.json", "w") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"[{username}] Failed to persist sessions JSON: {e}")

    def add_model(self, url_or_id: str) -> ModelInfo:
        """添加主播，自动检测平台"""
        platform, identifier, display_name = detect_platform(url_or_id)

        # 用 display_name 作为 key（避免重复）
        key = display_name
        if key in self.recorders:
            return self.recorders[key].info

        cls = PLATFORM_CLASSES.get(platform, GenericRecorder)
        rec = cls(
            identifier=identifier, output_dir=self.output_dir,
            proxy=self.proxy, on_state_change=self.on_state_change,
        )
        # 只在 meta 未恢复名字时使用默认 display_name
        if rec.info.username == identifier:
            rec.info.username = display_name
        key = rec.info.username  # 用恢复后的名字作为 key
        if key in self.recorders:
            return self.recorders[key].info
        rec._manager = self
        rec._load_sessions()  # 重新加载，此时 _manager 已设置，可从 SQLite 读取
        self.recorders[key] = rec
        logger.info(f"Added [{platform}] {display_name} (id={identifier})")
        return rec.info

    def remove_model(self, username: str):
        if username in self.recorders:
            rec = self.recorders.pop(username)
            asyncio.ensure_future(rec.stop())

    async def start_model(self, username: str):
        if username in self.recorders:
            self.recorders[username].info.enabled = True
            await self.recorders[username].start()

    async def stop_model(self, username: str):
        if username in self.recorders:
            self.recorders[username].info.enabled = False
            await self.recorders[username].stop()

    async def start_all(self):
        for rec in self.recorders.values():
            if rec.info.enabled:
                await rec.start()
        if not self._thumb_task or self._thumb_task.done():
            self._thumb_task = asyncio.create_task(self._thumbnail_loop())

    async def stop_all(self):
        for rec in self.recorders.values():
            await rec.stop()
        if self._thumb_task:
            self._thumb_task.cancel()

    def _rec_to_dict(self, rec: BaseLiveRecorder) -> dict:
        d = rec.info.to_dict()
        d["custom_cookies"] = rec.custom_cookies or ""
        d["custom_stream_url"] = rec.custom_stream_url or ""
        d["schedule"] = rec.schedule
        # 当前活跃 session 的开始时间（用于前端显示本场累计时长）
        active_session = next(
            (s for s in reversed(rec._sessions) if s.status == "active"), None
        )
        d["session_started_at"] = active_session.started_at if active_session else None
        d["session_segment_count"] = len(active_session.segments) if active_session else 0
        return d

    def get_all_info(self) -> list[dict]:
        return [self._rec_to_dict(rec) for rec in self.recorders.values()]

    def get_model_info(self, username: str) -> Optional[dict]:
        if username in self.recorders:
            return self._rec_to_dict(self.recorders[username])
        return None

    def get_recordings(self, username: str) -> list[dict]:
        model_dir = Path(self.output_dir) / username
        if not model_dir.exists():
            return []
        thumbs_dir = model_dir / "thumbs"
        files = []
        for f in sorted(model_dir.glob("*.mp4"), reverse=True):
            if ".raw." in f.name:
                continue
            stat = f.stat()
            thumb_name = f"{f.stem}.jpg"
            thumb_url = f"/api/thumb/file/{username}/{thumb_name}" if (thumbs_dir / thumb_name).exists() else ""
            files.append({"filename": f.name, "path": str(f), "size": stat.st_size,
                          "created": stat.st_mtime, "thumbnail_url": thumb_url})
        return files

    def get_grouped_recordings(self, username: str, gap_minutes: int = 15) -> list[dict]:
        files = self.get_recordings(username)
        if not files:
            return []
        files = [f for f in files if "_merged" not in f["filename"]]
        rec = self.recorders.get(username)
        if rec and rec.info.current_recording:
            active_name = Path(rec.info.current_recording.file_path).name
            files = [f for f in files if f["filename"] != active_name]
        files = list(reversed(files))

        groups, current_group, prev_time = [], [], None
        for f in files:
            name = f["filename"].replace(".mp4", "")
            try:
                ts = datetime.strptime(name, "%Y%m%d_%H%M%S")
            except ValueError:
                continue
            if prev_time and (ts - prev_time).total_seconds() > gap_minutes * 60:
                if len(current_group) >= 2:
                    groups.append(current_group)
                current_group = []
            current_group.append(f)
            prev_time = ts
        if len(current_group) >= 2:
            groups.append(current_group)

        return [{"id": g[0]["filename"].replace(".mp4", ""), "files": g,
                 "total_size": sum(f["size"] for f in g), "count": len(g)} for g in groups]

    def _calc_merge_confidence(self, username: str, session: "RecordingSession",
                                valid_files: list[str], codec_consistent: bool,
                                gap_minutes: float = 15) -> tuple[float, list[str]]:
        """计算合并信心度评分，返回 (score, reasons)"""
        # session_id 精确匹配 → 直接高信心，无需用户确认
        if session.session_id:
            return 0.9, ["精确会话匹配"]

        score = 0.0
        reasons = []

        # 文件名前缀同一用户
        if valid_files and all(f.startswith(username) for f in valid_files):
            score += 0.2
            reasons.append("同一主播")

        # 编码一致不需重编码
        if codec_consistent:
            score += 0.1
            reasons.append("编码一致")

        # 时间间隔检查（从文件名解析时间戳）
        if len(valid_files) >= 2:
            max_gap_min = 0.0
            try:
                import re as _re
                timestamps = []
                for fn in valid_files:
                    m = _re.search(r'(\d{8}_\d{6})', fn)
                    if m:
                        from datetime import datetime as _dt
                        ts = _dt.strptime(m.group(1), "%Y%m%d_%H%M%S").timestamp()
                        timestamps.append(ts)
                if len(timestamps) >= 2:
                    timestamps.sort()
                    gaps = [(timestamps[i+1] - timestamps[i]) / 60 for i in range(len(timestamps)-1)]
                    max_gap_min = max(gaps)
                    if max_gap_min < gap_minutes:
                        score += 0.3
                        reasons.append(f"时间间隔正常({max_gap_min:.0f}min)")
                    elif max_gap_min > 60:
                        score -= 0.3
                        reasons.append(f"间隔过大({max_gap_min:.0f}min,可能跨直播)")
            except Exception as e:
                logger.debug(f"[{username}] Timestamp parse failed: {e}")

        # 编码不一致降低信心
        if not codec_consistent:
            score -= 0.2
            reasons.append("编码不一致需重编")

        return max(0.0, min(1.0, score)), reasons

    def _get_per_model_config(self, username: str) -> dict:
        """获取主播的 per-model 配置（覆盖全局配置）"""
        rec = self.recorders.get(username)
        if not rec:
            return {}
        return {
            "h265_transcode": getattr(rec, "_per_model_h265", None),
            "filename_template": getattr(rec, "_per_model_filename_template", None),
        }

    async def auto_merge_for_model(self, username: str):
        """自动合并：优先使用 session 数据，fallback 到文件名分组"""
        model_dir = Path(self.output_dir) / username
        min_size = 500 * 1024

        # 读取 per-model 配置（覆盖全局）
        per_model = self._get_per_model_config(username)
        pm_h265 = per_model.get("h265_transcode")  # None = 继承全局
        pm_template = per_model.get("filename_template")  # None = 继承全局

        # 1. 基于 session 的精确合并（从内存或 SQLite 加载）
        session_merged = False
        rec = self.recorders.get(username)
        sessions = list(rec._sessions) if rec else []
        if not sessions and self.db:
            try:
                db_sessions = self.db.get_sessions(username)
                sessions = [RecordingSession.from_dict(s) for s in db_sessions]
            except Exception as e:
                logger.warning(f"[{username}] Failed to load sessions from DB: {e}")
        if not sessions:
            sessions_path = model_dir / "sessions.json"
            if sessions_path.exists():
                try:
                    with open(sessions_path) as f:
                        sessions = [RecordingSession.from_dict(s) for s in json.load(f)]
                except Exception as e:
                    logger.warning(f"[{username}] Failed to load sessions from JSON: {e}")
            try:
                for session in sessions:
                    if session.status != "ended":
                        continue
                    # 重试次数限制
                    if session.retry_count >= 3:
                        logger.warning(f"[{username}] Session {session.session_id}: max retries reached, skipping")
                        # P0: 合并失败超过重试次数时发送 webhook 警告
                        try:
                            await self.webhook.notify("merge_failed", {
                                "username": username,
                                "session_id": session.session_id,
                                "error": session.merge_error or "超过最大重试次数",
                                "retry_count": session.retry_count,
                                "segments": len(session.segments),
                            })
                        except Exception:
                            logger.debug("suppressed exception", exc_info=True)
                        if hasattr(self, '_merge_callback') and self._merge_callback:
                            try:
                                await self._merge_callback(username, session.session_id,
                                                           "merge_failed_permanent",
                                                           error=session.merge_error or "超过最大重试次数",
                                                           retry_count=session.retry_count)
                            except Exception:
                                logger.debug("suppressed exception", exc_info=True)
                        continue
                    # 过滤：只保留存在且足够大的片段
                    valid = []
                    for fn in session.segments:
                        fp = model_dir / fn
                        if fp.exists() and fp.stat().st_size >= min_size:
                            valid.append(fn)
                        elif fp.exists() and fp.stat().st_size < min_size:
                            logger.info(f"[{username}] Auto-cleanup small segment: {fn} ({fp.stat().st_size/1024:.0f} KB)")
                            fp.unlink()

                    if len(valid) == 0:
                        session.status = "merged"
                        session.merged_file = ""
                        continue

                    # 单片段：跳过合并，但执行后处理
                    if len(valid) == 1:
                        session.status = "merged"
                        single_file = model_dir / valid[0]
                        await self._post_process_fix_timestamps(single_file)
                        await self._post_process_transcode(single_file, username, h265_override=pm_h265)
                        final_name = self._generate_smart_name(username, valid, valid[0], template_override=pm_template)
                        if final_name != valid[0]:
                            final_path = model_dir / final_name
                            if not final_path.exists():
                                single_file.rename(final_path)
                                session.merged_file = final_name
                            else:
                                session.merged_file = valid[0]
                        else:
                            session.merged_file = valid[0]
                        session_merged = True
                        continue

                    # ffprobe 校验编码一致性（自动跳过损坏片段）
                    consistent, probe_valid, skipped, codec_error, codec_map = await self._check_codec_consistency(model_dir, valid)
                    if skipped:
                        logger.info(f"[{username}] Skipped {len(skipped)} corrupted segments: {skipped}")
                        # 通知前端损坏片段
                        if hasattr(self, '_merge_callback') and self._merge_callback:
                            await self._merge_callback(username, "", "segment_warning",
                                                       skipped_files=skipped, reason="corrupted")
                    if len(probe_valid) < 2:
                        if len(probe_valid) == 1:
                            session.status = "merged"
                            single_file = model_dir / probe_valid[0]
                            await self._post_process_fix_timestamps(single_file)
                            await self._post_process_transcode(single_file, username)
                            session.merged_file = probe_valid[0]
                        else:
                            session.status = "merged"
                            session.merged_file = ""
                        continue

                    # Codec 不一致时自动重编码
                    if not consistent and len(probe_valid) >= 2:
                        logger.info(f"[{username}] Session {session.session_id}: codec mismatch, attempting auto re-encode")
                        try:
                            probe_valid = await self._reencode_segments_to_dominant(
                                model_dir, probe_valid, codec_map, username)
                        except Exception as e:
                            logger.warning(f"[{username}] Re-encode failed: {e}, skipping auto-merge")
                            session.status = "error"
                            session.merge_error = f"重编码失败: {e}"
                            session.retry_count += 1
                            continue
                    valid = probe_valid

                    # P0: 合并信心度评分
                    merge_gap = getattr(self, '_merge_gap_minutes', 15)
                    confidence, confidence_reasons = self._calc_merge_confidence(
                        username, session, valid, consistent, gap_minutes=merge_gap)
                    logger.info(f"[{username}] Session {session.session_id}: merge confidence={confidence:.2f} ({', '.join(confidence_reasons)})")

                    if confidence < 0.4:
                        logger.warning(f"[{username}] Session {session.session_id}: low confidence ({confidence:.2f}), skipping auto-merge")
                        skip_reason = f"信心度过低({confidence*100:.0f}%)：{', '.join(confidence_reasons)}"
                        session.merge_error = skip_reason
                        if hasattr(self, '_merge_callback') and self._merge_callback:
                            try:
                                await self._merge_callback(username, session.session_id,
                                                           "merge_low_confidence",
                                                           confidence=confidence,
                                                           reasons=confidence_reasons,
                                                           skip_reason=skip_reason,
                                                           files=valid)
                            except Exception:
                                logger.debug("suppressed exception", exc_info=True)
                        continue
                    elif confidence < 0.7:
                        logger.info(f"[{username}] Session {session.session_id}: medium confidence ({confidence:.2f}), auto-merging (smart mode)")
                        merge_type_label = "auto_smart"
                    else:
                        merge_type_label = "auto_high"

                    total = sum((model_dir / fn).stat().st_size for fn in valid)
                    logger.info(f"[{username}] Session {session.session_id}: merging {len(valid)} segments ({total/1024/1024:.1f} MB)...")
                    session.status = "merging"
                    session.merge_started_at = time.time()
                    session.original_segments = list(valid)
                    # 持久化 merging 状态
                    self._persist_sessions(username, sessions)

                    try:
                        rec_obj = self.recorders.get(username)
                        should_delete = getattr(rec_obj, 'auto_delete_originals', False)
                        merge_id = await self.merge_segments(username, valid, delete_originals=should_delete)
                        merge_info = self._active_merges.get(merge_id, {})
                        if merge_info.get("status") == "done":
                            session.status = "merged"
                            session.merged_file = merge_info.get("filename", "")
                            session.merge_type = merge_type_label
                            session.rollback_deadline = time.time() + 72 * 3600
                            await self._notify_merge(username, session.session_id, "auto_merge_done",
                                                     filename=session.merged_file,
                                                     merge_type=merge_type_label,
                                                     confidence=confidence,
                                                     savings_bytes=merge_info.get("savings_bytes", 0),
                                                     total_duration=int(ended_at - started_at) if (ended_at := session.ended_at or 0) and (started_at := session.started_at or 0) else 0)
                        else:
                            session.status = "error"
                            session.merge_error = merge_info.get("error", "合并失败")
                            session.retry_count += 1
                    except Exception as e:
                        session.status = "error"
                        session.merge_error = str(e)
                        session.retry_count += 1
                        logger.error(f"[{username}] Session merge error: {e}")
                    session_merged = True

                # 保存更新后的 session 状态
                self._persist_sessions(username, sessions)
                # 同步到 recorder 的内存
                rec = self.recorders.get(username)
                if rec:
                    rec._sessions = sessions
            except Exception as e:
                logger.error(f"[{username}] Session-based merge error: {e}")

        # 2. Fallback: 文件名时间戳分组（兼容无 session 的历史文件）
        if not session_merged:
            groups = self.get_grouped_recordings(username, gap_minutes=15)
            if not groups:
                return
            for group in groups:
                valid = []
                for f in group["files"]:
                    fp = model_dir / f["filename"]
                    if fp.exists() and fp.stat().st_size >= min_size:
                        valid.append(f["filename"])
                    elif fp.exists():
                        logger.info(f"[{username}] Auto-cleanup small segment: {f['filename']} ({fp.stat().st_size/1024:.0f} KB)")
                        fp.unlink()
                if len(valid) < 2:
                    continue
                total = sum((model_dir / fn).stat().st_size for fn in valid if (model_dir / fn).exists())
                logger.info(f"[{username}] Fallback auto-merging {len(valid)} segments ({total/1024/1024:.1f} MB)...")
                try:
                    await self.merge_segments(username, valid, delete_originals=True)
                except Exception as e:
                    logger.error(f"[{username}] Auto-merge error: {e}")

    async def _check_codec_consistency(self, model_dir: Path, filenames: list[str]) -> tuple[bool, list[str], list[str], str, dict]:
        """用 ffprobe 检查所有片段的视频编码是否一致，跳过损坏片段
        返回 (consistent, valid_files, skipped_files, error_detail, codec_map)
        codec_map: {filename: codec_key} 用于重编码决策"""
        if not shutil.which("ffprobe"):
            return True, filenames, [], "", {}
        codecs = {}  # fn -> codec_key
        skipped = []
        for fn in filenames:
            fp = model_dir / fn
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
                if not streams:
                    logger.warning(f"[codec check] No video stream in {fn}, skipping")
                    skipped.append(fn)
                    continue
                s = streams[0]
                dur = float(s.get("duration", 0) or 0)
                if dur <= 0:
                    logger.warning(f"[codec check] Zero duration in {fn}, skipping")
                    skipped.append(fn)
                    continue
                codecs[fn] = f"{s.get('codec_name')}_{s.get('width')}x{s.get('height')}"
            except Exception as e:
                logger.warning(f"[codec check] Failed to probe {fn}: {e}, skipping")
                skipped.append(fn)
        valid = [fn for fn in filenames if fn in codecs]
        if not valid:
            return False, [], skipped, f"全部 {len(filenames)} 个片段均无有效视频流", {}
        unique_codecs = set(codecs.values())
        consistent = len(unique_codecs) <= 1
        if not consistent:
            detail = " vs ".join(sorted(unique_codecs))
            logger.warning(f"[codec check] Codec mismatch: {unique_codecs}")
            return False, valid, skipped, f"编码不一致: {detail}", codecs
        return consistent, valid, skipped, "", codecs

    async def _reencode_segments_to_dominant(self, model_dir: Path, filenames: list[str],
                                              codec_map: dict, username: str) -> list[str]:
        """将 codec 不一致的 segments 重编码为多数派 codec，返回统一后的文件名列表"""
        from collections import Counter
        # 找到多数派 codec
        codec_counts = Counter(codec_map[fn] for fn in filenames if fn in codec_map)
        dominant_codec = codec_counts.most_common(1)[0][0]
        codec_name, resolution = dominant_codec.rsplit("_", 1)
        width, height = resolution.split("x")

        result_files = []
        temp_files = []  # 跟踪临时文件以便清理
        for fn in filenames:
            if codec_map.get(fn) == dominant_codec:
                result_files.append(fn)
                continue
            # 需要重编码
            reencode_name = fn.replace(".mp4", f".reenc.mp4")
            reencode_path = model_dir / reencode_name
            src_path = model_dir / fn
            logger.info(f"[{username}] Re-encoding {fn} ({codec_map.get(fn)}) → {dominant_codec}")
            try:
                # 检查磁盘空间
                file_size = src_path.stat().st_size
                disk_free = shutil.disk_usage(str(model_dir)).free
                if disk_free < file_size * 1.5:
                    logger.warning(f"[{username}] Insufficient disk for re-encode, skipping {fn}")
                    result_files.append(fn)  # 保留原文件，让 ffmpeg concat 尝试
                    continue

                # 根据目标 codec 选择编码器
                if codec_name == "hevc" or codec_name == "h265":
                    v_codec = ["-c:v", "libx265", "-crf", "23", "-preset", "fast", "-tag:v", "hvc1"]
                else:
                    v_codec = ["-c:v", "libx264", "-crf", "23", "-preset", "fast"]

                proc = await asyncio.create_subprocess_exec(
                    "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
                    "-i", str(src_path),
                    *v_codec,
                    "-vf", f"scale={width}:{height}",
                    "-c:a", "aac", "-b:a", "128k",
                    "-movflags", "+faststart",
                    str(reencode_path),
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
                )
                _, stderr = await asyncio.wait_for(proc.communicate(), timeout=3600)
                if proc.returncode == 0 and reencode_path.exists() and reencode_path.stat().st_size > 0:
                    logger.info(f"[{username}] Re-encoded {fn} → {reencode_name} "
                               f"({file_size/1024/1024:.1f}MB → {reencode_path.stat().st_size/1024/1024:.1f}MB)")
                    result_files.append(reencode_name)
                    temp_files.append(reencode_name)
                else:
                    error_msg = stderr.decode()[:200] if stderr else "unknown"
                    logger.warning(f"[{username}] Re-encode failed for {fn}: {error_msg}")
                    if reencode_path.exists():
                        reencode_path.unlink()
                    result_files.append(fn)  # fallback to original
            except asyncio.TimeoutError:
                logger.warning(f"[{username}] Re-encode timed out for {fn}")
                if reencode_path.exists():
                    reencode_path.unlink()
                result_files.append(fn)
            except Exception as e:
                logger.warning(f"[{username}] Re-encode error for {fn}: {e}")
                if reencode_path.exists():
                    reencode_path.unlink()
                result_files.append(fn)

        # 存储临时文件列表以便合并后清理
        self._active_merges.setdefault("_temp_reenc_files", {})[username] = [
            str(model_dir / f) for f in temp_files
        ]
        return result_files

    async def merge_segments(self, username: str, filenames: list[str],
                             delete_originals: bool = False) -> str:
        model_dir = Path(self.output_dir) / username
        for fn in filenames:
            if ".." in fn or "/" in fn:
                raise ValueError(f"非法文件名: {fn}")
            if not (model_dir / fn).exists():
                raise FileNotFoundError(f"文件不存在: {fn}")

        rec = self.recorders.get(username)
        if rec and rec.info.current_recording:
            active_name = Path(rec.info.current_recording.file_path).name
            if active_name in filenames:
                raise ValueError("不能合并正在录制的文件")

        base = filenames[0].replace(".mp4", "")
        merge_id = f"{base}_merged"
        output_name = f"{merge_id}.mp4"
        output_path = model_dir / output_name

        if merge_id in self._active_merges and self._active_merges[merge_id].get("status") == "running":
            return merge_id

        # 计算预期总大小（用于进度追踪）
        expected_size = sum((model_dir / fn).stat().st_size for fn in filenames if (model_dir / fn).exists())

        # 合并前磁盘空间检查：需要至少 expected_size + 500MB 缓冲
        disk_free = shutil.disk_usage(str(model_dir)).free
        required = expected_size + 500 * 1024 * 1024
        if disk_free < required:
            raise OSError(f"磁盘空间不足：需要 {required//1024//1024}MB，剩余 {disk_free//1024//1024}MB")

        self._active_merges[merge_id] = {"status": "running", "progress": 0, "expected_size": expected_size}
        concat_path = model_dir / f".concat_{int(time.time())}.txt"
        merge_ok = False

        # 获取所有片段总时长（用于精确进度计算）
        total_duration_us = 0
        for fn in filenames:
            try:
                fp = model_dir / fn
                proc = await asyncio.create_subprocess_exec(
                    "ffprobe", "-v", "quiet", "-show_entries", "format=duration",
                    "-of", "json", str(fp),
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
                )
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
                data = json.loads(stdout.decode())
                dur = float(data.get("format", {}).get("duration", 0) or 0)
                total_duration_us += int(dur * 1_000_000)
            except Exception as e:
                logger.debug(f"[{username}] ffprobe duration parse failed: {e}")
        progress_stop = asyncio.Event()
        merge_start_time = time.time()
        # 共享进度状态（由 ffmpeg stdout reader 更新）
        progress_state = {"current_time_us": 0}

        async def read_ffmpeg_progress(stdout_pipe):
            """从 ffmpeg -progress pipe:1 输出中解析进度"""
            try:
                while not progress_stop.is_set():
                    line = await stdout_pipe.readline()
                    if not line:
                        break
                    text = line.decode().strip()
                    if text.startswith("out_time_us="):
                        try:
                            progress_state["current_time_us"] = int(text.split("=")[1])
                        except (ValueError, IndexError):
                            pass
            except asyncio.CancelledError:
                return
            except Exception:
                logger.debug("suppressed exception", exc_info=True)

        async def monitor_progress():
            while not progress_stop.is_set():
                try:
                    if total_duration_us > 0:
                        raw_progress = progress_state["current_time_us"] / total_duration_us
                    elif output_path.exists() and expected_size > 0:
                        # fallback: 文件大小估算
                        raw_progress = output_path.stat().st_size / expected_size
                    else:
                        raw_progress = 0
                    progress = min(raw_progress * 0.90, 0.90)
                    self._active_merges[merge_id]["progress"] = progress
                    # ETA
                    elapsed = time.time() - merge_start_time
                    eta_str = ""
                    if raw_progress > 0.01 and elapsed > 5:
                        total_est = elapsed / raw_progress
                        remaining = max(0, total_est - elapsed)
                        if remaining >= 3600:
                            eta_str = f" · 预计剩余 {int(remaining/3600)}h{int(remaining%3600/60)}m"
                        elif remaining >= 60:
                            eta_str = f" · 预计剩余 {int(remaining/60)}:{int(remaining%60):02d}"
                        else:
                            eta_str = f" · 预计剩余 {int(remaining)}s"
                    if hasattr(self, '_merge_progress_callback') and self._merge_progress_callback:
                        await self._merge_progress_callback(
                            username, merge_id, progress,
                            f"合并 {len(filenames)} 个片段{eta_str}"
                        )
                except Exception:
                    logger.debug("suppressed exception", exc_info=True)
                try:
                    await asyncio.wait_for(progress_stop.wait(), timeout=2)
                    break
                except asyncio.TimeoutError:
                    pass

        progress_task = asyncio.create_task(monitor_progress())

        try:
            with open(concat_path, "w") as f:
                for fn in filenames:
                    f.write(f"file '{fn}'\n")
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
                "-f", "concat", "-safe", "0", "-i", str(concat_path),
                "-c", "copy", "-movflags", "+faststart",
                "-progress", "pipe:1",
                str(output_path),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            # 启动 ffmpeg 进度读取协程
            progress_reader = asyncio.create_task(read_ffmpeg_progress(proc.stdout))
            # 存储进程引用和停止事件，支持取消
            self._active_merges[merge_id]["_proc"] = proc
            self._active_merges[merge_id]["_stop"] = progress_stop
            self._active_merges[merge_id]["_output_path"] = str(output_path)
            merge_timeout = getattr(self, '_merge_timeout', 14400)
            try:
                await asyncio.wait_for(proc.wait(), timeout=merge_timeout)
                stderr = await proc.stderr.read()
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                progress_stop.set()
                progress_reader.cancel()
                await progress_task
                if output_path.exists():
                    output_path.unlink()
                timeout_h = merge_timeout / 3600
                total_mb = expected_size / 1024 / 1024
                self._active_merges[merge_id] = {"status": "error", "error": f"合并超时 (>{timeout_h:.0f}h, {total_mb:.0f}MB)"}
                logger.error(f"[{username}] Merge timed out after {timeout_h:.0f}h ({total_mb:.0f}MB)")
                return merge_id

            progress_stop.set()
            progress_reader.cancel()
            await progress_task

            if proc.returncode != 0:
                if output_path.exists():
                    output_path.unlink()
                error_msg = stderr.decode()[:300] if stderr else "未知错误"
                self._active_merges[merge_id] = {"status": "error", "error": error_msg}
                return merge_id

            merge_ok = True
            result_size = output_path.stat().st_size if output_path.exists() else 0

            # 后处理阶段进度: 0.90 → 0.95 → 1.0
            async def _update_progress(progress: float, phase: str):
                self._active_merges[merge_id]["progress"] = progress
                if hasattr(self, '_merge_progress_callback') and self._merge_progress_callback:
                    await self._merge_progress_callback(username, merge_id, progress, phase)

            # 后处理：时间戳修复
            await _update_progress(0.91, "修复时间戳...")
            await self._post_process_fix_timestamps(output_path)
            result_size = output_path.stat().st_size if output_path.exists() else result_size

            # 后处理：H.265 转码（可选，耗时）
            await _update_progress(0.93, "转码处理...")
            await self._post_process_transcode(output_path, username)
            result_size = output_path.stat().st_size if output_path.exists() else result_size

            # 后处理：智能重命名
            await _update_progress(0.96, "智能重命名...")
            final_name = self._generate_smart_name(username, filenames, output_name)
            if final_name != output_name:
                final_path = model_dir / final_name
                if not final_path.exists():
                    output_path.rename(final_path)
                    output_name = final_name
                    output_path = final_path
                    logger.info(f"[{username}] Renamed to: {final_name}")

            # 后处理：云存储上传（可选）
            if output_path.exists():
                cloud_url = await self.cloud.upload(output_path, username)

            # 后处理：录后脚本（可选）
            if output_path.exists():
                await self._run_post_script(output_path, username, "merge_done")

            # 后处理：FlashCut 全自动流水线（检测高光 + 生成片段）
            if getattr(self, '_highlight_auto_detect', False) and output_path.exists():
                await _update_progress(0.98, "FlashCut 自动处理中...")
                await self._auto_flashcut_pipeline(username, output_path)

            # 后处理：自动生成缩略图
            if output_path.exists():
                asyncio.ensure_future(self._generate_file_thumbnail(output_path, username))

            self._active_merges[merge_id] = {
                "status": "done", "filename": output_name, "size": result_size,
                "progress": 1.0, "input_count": len(filenames), "input_size": expected_size,
                "savings_bytes": max(0, expected_size - result_size),
                "savings_pct": round(max(0, expected_size - result_size) / expected_size * 100, 1) if expected_size > 0 else 0,
            }
            logger.info(f"[{username}] Merged {len(filenames)} files -> {output_name} ({result_size/1024/1024:.1f} MB)")
            # 同步 session 状态（手动合并也能更新对应 session）
            self._sync_sessions_after_merge(username, filenames, output_name)
            if delete_originals:
                for fn in filenames:
                    fp = model_dir / fn
                    if fp.exists():
                        fp.unlink()
            # 清理重编码临时文件
            temp_reenc = self._active_merges.get("_temp_reenc_files", {}).pop(username, [])
            for tmp_path in temp_reenc:
                try:
                    p = Path(tmp_path)
                    if p.exists():
                        p.unlink()
                except Exception:
                    logger.debug("suppressed exception", exc_info=True)
        except Exception as e:
            progress_stop.set()
            if not merge_ok and output_path.exists():
                output_path.unlink()
            self._active_merges[merge_id] = {"status": "error", "error": str(e)}
            logger.error(f"[{username}] Merge error: {e}")
        finally:
            if concat_path.exists():
                concat_path.unlink()

        try:
            status_info = self._active_merges.get(merge_id, {})
            if status_info.get("status") == "done":
                await self._notify_merge(username, merge_id, "done",
                                         filename=output_name, size=status_info.get("size", 0),
                                         input_count=len(filenames), input_size=expected_size,
                                         savings_bytes=status_info.get("savings_bytes", 0),
                                         savings_pct=status_info.get("savings_pct", 0))
                # Webhook: 合并完成
                await self.webhook.notify("merge_done", {
                    "username": username, "filename": output_name,
                    "size": f"{result_size/1024/1024:.1f}MB", "segments": len(filenames)})
                # 写入合并历史
                if self.db:
                    try:
                        self.db.insert_merge_history(
                            username=username, input_files=filenames,
                            input_size=expected_size, output_file=output_name,
                            output_size=status_info.get("size", 0),
                            savings_bytes=status_info.get("savings_bytes", 0))
                    except Exception as he:
                        logger.warning(f"[{username}] Failed to write merge history: {he}")
            elif status_info.get("status") == "error":
                await self._notify_merge(username, merge_id, "error", error=status_info.get("error", ""))
                await self.webhook.notify("error", {
                    "username": username, "message": f"合并失败: {status_info.get('error','')}"})
                if self.db:
                    try:
                        self.db.insert_merge_history(
                            username=username, input_files=filenames,
                            input_size=expected_size, status="error",
                            error=status_info.get("error", ""))
                    except Exception as he:
                        logger.warning(f"[{username}] Failed to write merge history: {he}")
        except Exception as e:
            logger.warning(f"[{username}] Merge notification failed: {e}")

        # 60 秒后清理 _active_merges 条目，防止内存泄漏
        loop = asyncio.get_event_loop()
        _mid = merge_id
        loop.call_later(60, lambda: self._active_merges.pop(_mid, None))

        return merge_id

    async def cancel_merge(self, merge_id: str) -> bool:
        """取消正在运行的合并任务"""
        info = self._active_merges.get(merge_id)
        if not info or info.get("status") != "running":
            return False
        proc = info.get("_proc")
        stop_event = info.get("_stop")
        output_path = info.get("_output_path")
        # 终止 ffmpeg 进程
        if proc and proc.returncode is None:
            proc.kill()
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass
        # 停止进度监控
        if stop_event:
            stop_event.set()
        # 清理输出文件
        if output_path:
            p = Path(output_path)
            if p.exists():
                try:
                    p.unlink()
                except Exception:
                    logger.debug("suppressed exception", exc_info=True)
        self._active_merges[merge_id] = {"status": "cancelled", "error": "用户取消"}
        logger.info(f"Merge cancelled: {merge_id}")
        return True

    async def _generate_file_thumbnail(self, file_path: Path, username: str):
        """为录制文件生成关键帧缩略图（10% 位置）"""
        try:
            thumbs_dir = file_path.parent / "thumbs"
            thumbs_dir.mkdir(parents=True, exist_ok=True)
            thumb_name = f"{file_path.stem}.jpg"
            thumb_path = thumbs_dir / thumb_name
            if thumb_path.exists():
                return
            proc = await asyncio.create_subprocess_exec(
                "ffprobe", "-v", "quiet", "-show_entries", "format=duration",
                "-of", "json", str(file_path),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
            duration = float(json.loads(stdout.decode()).get("format", {}).get("duration", 0) or 0)
            seek_time = max(1, duration * 0.1)
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y", "-ss", str(seek_time),
                "-i", str(file_path), "-vframes", "1", "-vf", "scale=320:-1",
                str(thumb_path),
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            )
            await asyncio.wait_for(proc.wait(), timeout=30)
        except Exception as e:
            logger.debug(f"[{username}] Thumbnail generation failed: {e}")

    async def _post_process_fix_timestamps(self, file_path: Path):
        """合并后修复时间戳，确保播放器兼容"""
        if not file_path.exists():
            return
        # 检查磁盘空间（需要至少 1.1 倍文件大小）
        try:
            file_size = file_path.stat().st_size
            disk_free = shutil.disk_usage(str(file_path.parent)).free
            if disk_free < file_size * 1.1:
                logger.warning(f"Post-process skipped: insufficient disk space ({disk_free/1024/1024:.0f}MB free, need {file_size*1.1/1024/1024:.0f}MB)")
                return
        except Exception:
            logger.debug("suppressed exception", exc_info=True)
        fixed_path = file_path.with_suffix(".fixed.mp4")
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
                "-fflags", "+genpts", "-i", str(file_path),
                "-c", "copy", "-movflags", "+faststart", str(fixed_path),
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()
            if proc.returncode == 0 and fixed_path.exists() and fixed_path.stat().st_size > 0:
                file_path.unlink()
                fixed_path.rename(file_path)
                logger.info(f"Post-process: timestamps fixed for {file_path.name}")
            else:
                error_msg = stderr.decode()[:200] if stderr else "unknown"
                logger.warning(f"Post-process timestamp fix failed (rc={proc.returncode}): {error_msg}")
                if fixed_path.exists():
                    fixed_path.unlink()
        except Exception as e:
            logger.warning(f"Post-process timestamp fix failed: {e}")
            if fixed_path.exists():
                fixed_path.unlink()

    async def _post_process_transcode(self, file_path: Path, username: str, h265_override=None):
        """合并后转码为 H.265 压缩（可选，耗时较长）"""
        use_h265 = h265_override if h265_override is not None else self._post_process_h265
        if not use_h265:
            return
        if not file_path.exists():
            return
        # 检查磁盘空间
        try:
            file_size = file_path.stat().st_size
            disk_free = shutil.disk_usage(str(file_path.parent)).free
            if disk_free < file_size:
                logger.warning(f"[{username}] Transcode skipped: insufficient disk space")
                return
        except Exception:
            logger.debug("suppressed exception", exc_info=True)

        h265_path = file_path.with_suffix(".h265.mp4")
        logger.info(f"[{username}] Transcoding to H.265: {file_path.name} ({file_size/1024/1024:.0f}MB)...")

        try:
            # 使用 libx265，CRF 28（较好的压缩率），保留音频
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
                "-i", str(file_path),
                "-c:v", "libx265", "-crf", "28", "-preset", "medium",
                "-c:a", "aac", "-b:a", "128k",
                "-movflags", "+faststart",
                "-tag:v", "hvc1",  # Apple 兼容
                str(h265_path),
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()

            if proc.returncode == 0 and h265_path.exists() and h265_path.stat().st_size > 0:
                new_size = h265_path.stat().st_size
                ratio = (1 - new_size / file_size) * 100 if file_size > 0 else 0
                file_path.unlink()
                h265_path.rename(file_path)
                logger.info(f"[{username}] Transcode done: {file_size/1024/1024:.0f}MB → {new_size/1024/1024:.0f}MB ({ratio:.0f}% smaller)")
                # Webhook 通知
                await self.webhook.notify("merge_done", {
                    "username": username, "message": f"H.265 转码完成，压缩 {ratio:.0f}%"})
            else:
                error_msg = stderr.decode()[:200] if stderr else "unknown"
                logger.warning(f"[{username}] Transcode failed: {error_msg}")
                if h265_path.exists():
                    h265_path.unlink()
        except Exception as e:
            logger.warning(f"[{username}] Transcode error: {e}")
            if h265_path.exists():
                h265_path.unlink()

    def _generate_smart_name(self, username: str, input_files: list[str], default_name: str,
                              template_override: str = None) -> str:
        """生成智能文件名，支持模板变量"""
        if not self._post_process_rename:
            return default_name
        try:
            first = input_files[0].replace(".mp4", "")
            ts = datetime.strptime(first, "%Y%m%d_%H%M%S")

            last = input_files[-1].replace(".mp4", "")
            ts_last = datetime.strptime(last, "%Y%m%d_%H%M%S")
            span = (ts_last - ts).total_seconds()
            if span < 3600:
                dur_str = f"{int(span/60)}m"
            else:
                dur_str = f"{int(span/3600)}h{int(span%3600/60)}m"

            safe_name = re.sub(r'[<>:"/\\|?*]', '_', username)

            # 使用模板（per-model 覆盖 > 全局配置）
            template = template_override or getattr(self, '_filename_template', '{username}_{date}_{duration}_merged')
            variables = {
                "username": safe_name,
                "platform": "",
                "date": ts.strftime("%Y-%m-%d"),
                "time": ts.strftime("%H-%M"),
                "datetime": ts.strftime("%Y-%m-%d_%H-%M"),
                "duration": dur_str,
                "segments": str(len(input_files)),
                "quality": "",
                "year": ts.strftime("%Y"),
                "month": ts.strftime("%m"),
                "day": ts.strftime("%d"),
            }
            # 获取平台和质量信息
            rec = self.recorders.get(username)
            if rec:
                variables["platform"] = rec.info.platform
                variables["quality"] = rec.quality

            try:
                result = template.format_map(variables) + ".mp4"
            except (KeyError, ValueError):
                result = f"{safe_name}_{ts.strftime('%Y-%m-%d_%H-%M')}_{dur_str}_merged.mp4"

            # 清理非法字符
            result = re.sub(r'[<>:"/\\|?*]', '_', result)
            logger.info(f"[{username}] Smart rename: {default_name} → {result}")
            return result
        except ValueError as e:
            logger.warning(f"[{username}] Smart rename failed (filename parse error): {e}, using default")
            return default_name
        except Exception as e:
            logger.warning(f"[{username}] Smart rename failed: {e}, using default")
            return default_name

    async def _notify_merge(self, username: str, merge_id: str, status: str, **kwargs):
        if hasattr(self, '_merge_callback') and self._merge_callback:
            await self._merge_callback(username, merge_id, status, **kwargs)

    async def _run_post_script(self, file_path: Path, username: str, event: str, session_id: str = ""):
        """执行录后脚本"""
        script = getattr(self, '_post_process_script', '')
        if not script or not Path(script).exists():
            return
        env = {
            **os.environ,
            "SV_USERNAME": username,
            "SV_FILE_PATH": str(file_path),
            "SV_FILE_SIZE": str(file_path.stat().st_size) if file_path.exists() else "0",
            "SV_PLATFORM": "",
            "SV_SESSION_ID": session_id,
            "SV_EVENT": event,
        }
        rec = self.recorders.get(username)
        if rec:
            env["SV_PLATFORM"] = rec.info.platform
        try:
            proc = await asyncio.create_subprocess_exec(
                script,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                env=env,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
            if proc.returncode == 0:
                logger.info(f"[{username}] Post-script OK: {event}")
            else:
                logger.warning(f"[{username}] Post-script exit {proc.returncode}: {stderr.decode()[:200]}")
        except asyncio.TimeoutError:
            logger.warning(f"[{username}] Post-script timed out (300s)")
        except Exception as e:
            logger.warning(f"[{username}] Post-script error: {e}")

    async def _detect_highlights(self, username: str, video_path: Path, session_id: str = ""):
        """运行高光检测并存入数据库"""
        try:
            from highlight import HighlightDetector
            config = getattr(self, '_highlight_config', {})
            detector = HighlightDetector(config)

            # 查找对应的弹幕文件
            danmaku_path = None
            if session_id:
                dp = video_path.parent / f"{session_id}_danmaku.json"
                if dp.exists():
                    danmaku_path = dp
            if not danmaku_path:
                # 尝试从数据库查找
                if self.db:
                    try:
                        dm = self.db.get_danmaku(session_id) if session_id else None
                        if dm and dm.get("file_path"):
                            dp = Path(dm["file_path"])
                            if dp.exists():
                                danmaku_path = dp
                    except Exception:
                        logger.debug("suppressed exception", exc_info=True)

            highlights = await detector.detect(video_path, danmaku_path)
            if not highlights:
                return

            # 存入数据库
            for h in highlights:
                hid = f"h_{int(time.time())}_{uuid.uuid4().hex[:6]}"
                if self.db:
                    self.db.insert_highlight(
                        highlight_id=hid, session_id=session_id, username=username,
                        video_file=video_path.name, start_time=h.start_time,
                        end_time=h.end_time, score=h.score, category=h.category,
                        signals=h.signals, title=h.title,
                    )

            logger.info(f"[{username}] Auto-detected {len(highlights)} highlights in {video_path.name}")

            # WebSocket 通知
            if hasattr(self, '_merge_callback') and self._merge_callback:
                await self._merge_callback(username, "", "highlight_detected",
                                           count=len(highlights), video_file=video_path.name)
        except Exception as e:
            logger.warning(f"[{username}] Highlight detection error: {e}")

    async def _auto_flashcut_pipeline(self, username: str, video_path: Path, session_id: str = ""):
        """FlashCut 全自动流水线：检测高光 → 生成字幕片段 → 生成封面"""
        try:
            # 广播进度
            async def _broadcast(msg):
                if hasattr(self, '_merge_callback') and self._merge_callback:
                    await self._merge_callback(username, "", "flashcut_progress", message=msg)

            await _broadcast("正在分析高光时刻...")

            # 1. 检测高光
            await self._detect_highlights(username, video_path, session_id)
            if not self.db:
                return

            highlights = self.db.get_highlights(username, limit=20)
            # 只处理刚检测到的（status=detected）
            new_highlights = [h for h in highlights if h.get("status") == "detected"
                             and h.get("video_file") == video_path.name]
            if not new_highlights:
                await _broadcast("未检测到高光片段")
                return

            await _broadcast(f"检测到 {len(new_highlights)} 个高光，正在生成短视频...")

            # 2. 配额检查
            from quota import QuotaManager
            quota_mgr = QuotaManager(self.db)
            allowed, used, limit = quota_mgr.check_quota(username)
            force_watermark = quota_mgr.should_watermark(username)
            remaining = limit - used

            # 3. 查找弹幕文件
            danmaku_path = None
            if session_id:
                dp = video_path.parent / f"{session_id}_danmaku.json"
                if dp.exists():
                    danmaku_path = dp

            # 4. 逐个生成片段
            from clipgen import ClipGenerator, ClipConfig
            config = ClipConfig(
                resolution=self._highlight_config.get("clip_resolution", "1080x1920"),
                format=self._highlight_config.get("clip_format", "vertical"),
                watermark=self._highlight_config.get("clip_watermark", ""),
                danmaku_overlay=self._highlight_config.get("clip_danmaku_overlay", True),
            )
            gen = ClipGenerator(config, self.output_dir)
            auto_subtitle = self._highlight_config.get("auto_subtitle", True)

            generated = 0
            for i, h in enumerate(new_highlights):
                # 配额检查
                if not allowed and remaining <= 0:
                    await _broadcast(f"已达每日配额上限 ({limit} 条)，已生成 {generated} 条")
                    break

                await _broadcast(f"正在生成短视频 ({i+1}/{len(new_highlights)})...")

                result = await gen.generate_clip(
                    video_path, h, danmaku_path,
                    auto_subtitle=auto_subtitle,
                    auto_cover=True,
                    force_watermark=force_watermark,
                )

                if result.get("status") == "done":
                    self.db.insert_clip(
                        clip_id=result["clip_id"], highlight_id=h.get("highlight_id", ""),
                        username=username, output_file=result.get("output_file", ""),
                        resolution=result.get("resolution", ""), duration=result.get("duration", 0),
                        format=result.get("format", ""), size=result.get("size", 0), status="done",
                    )
                    self.db.update_highlight_status(h["highlight_id"], "clipped")
                    quota_mgr.consume_quota(username)
                    generated += 1
                    remaining -= 1
                    allowed = remaining > 0

            await _broadcast(f"完成！生成了 {generated} 条短视频")

            # WebSocket 通知
            if hasattr(self, '_merge_callback') and self._merge_callback:
                await self._merge_callback(username, "", "flashcut_done",
                                           count=generated, video_file=video_path.name)

            logger.info(f"[{username}] FlashCut pipeline: {generated} clips from {video_path.name}")

        except Exception as e:
            logger.error(f"[{username}] FlashCut pipeline error: {e}")

    def _sync_sessions_after_merge(self, username: str, merged_files: list[str], output_file: str):
        """手动合并后，同步更新对应 session 状态为 merged"""
        rec = self.recorders.get(username)
        if not rec:
            return
        merged_set = set(merged_files)
        changed = False
        for s in rec._sessions:
            if s.status not in ("ended", "error"):
                continue
            if set(s.segments).issubset(merged_set) and s.segments:
                s.status = "merged"
                s.merged_file = output_file
                s.merge_error = ""
                changed = True
                logger.info(f"[{username}] Session {s.session_id} synced to merged via manual merge")
        if changed:
            rec._save_sessions()

    def get_sessions(self, username: str) -> list[dict]:
        """获取指定主播的所有会话"""
        # 优先从内存中的 recorder 获取
        rec = self.recorders.get(username)
        if rec:
            return [s.to_dict() for s in rec._sessions]
        # fallback: SQLite
        if self.db:
            try:
                return self.db.get_sessions(username)
            except Exception:
                logger.debug("suppressed exception", exc_info=True)
        # fallback: JSON
        sessions_path = Path(self.output_dir) / username / "sessions.json"
        if sessions_path.exists():
            try:
                with open(sessions_path) as f:
                    return json.load(f)
            except Exception:
                logger.debug("suppressed exception", exc_info=True)
        return []

    def update_session_status(self, username: str, session_id: str, status: str, **kwargs):
        """更新指定会话的状态"""
        rec = self.recorders.get(username)
        sessions = rec._sessions if rec else []
        if not sessions:
            if self.db:
                try:
                    db_sessions = self.db.get_sessions(username)
                    sessions = [RecordingSession.from_dict(s) for s in db_sessions]
                except Exception:
                    logger.debug("suppressed exception", exc_info=True)
            if not sessions:
                sessions_path = Path(self.output_dir) / username / "sessions.json"
                if sessions_path.exists():
                    try:
                        with open(sessions_path) as f:
                            sessions = [RecordingSession.from_dict(s) for s in json.load(f)]
                    except Exception:
                        return
        for s in sessions:
            if s.session_id == session_id:
                s.status = status
                for k, v in kwargs.items():
                    if hasattr(s, k):
                        setattr(s, k, v)
                break
        # 持久化（SQLite 为主，JSON 为备份）
        self._persist_sessions(username, sessions)

    async def _thumbnail_loop(self):
        """定期从录制中的视频生成缩略图，离线时保留已有缩略图"""
        thumbs_dir = Path(self.output_dir) / "thumbs"
        thumbs_dir.mkdir(parents=True, exist_ok=True)

        while True:
            try:
                for rec in self.recorders.values():
                    name = rec.info.username
                    thumb_file = thumbs_dir / f"{name}.jpg"

                    if rec.info.state == RecordingState.RECORDING and rec.info.current_recording:
                        # 录制中：从视频生成新缩略图
                        fp = rec.info.current_recording.file_path
                        for path in [fp, fp.replace(".mp4", ".raw.mp4")]:
                            if os.path.exists(path) and os.path.getsize(path) > 50_000:
                                await self._generate_thumbnail(path, str(thumb_file))
                                rec.info.thumbnail_url = f"/api/thumb/{name}"
                                rec._save_meta()
                                break
                    elif not rec.info.thumbnail_url and thumb_file.exists():
                        # 非录制：恢复已有缩略图
                        rec.info.thumbnail_url = f"/api/thumb/{name}"
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Thumbnail error: {e}")
            await asyncio.sleep(15)

    async def _generate_thumbnail(self, video_path: str, thumb_path: str):
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-sseof", "-1",
            "-i", video_path,
            "-vframes", "1", "-vf", "scale=320:-1",
            thumb_path,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.wait()

