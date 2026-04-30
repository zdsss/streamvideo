"""
片段生成引擎 — ffmpeg 切割 + 格式转换 + 弹幕字幕 + 水印
"""

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger("clipgen")


@dataclass
class ClipConfig:
    resolution: str = "1080x1920"
    format: str = "vertical"       # vertical (9:16) | horizontal (16:9) | square (1:1)
    watermark: str = ""
    danmaku_overlay: bool = True
    output_codec: str = "libx264"
    crf: int = 23


class ClipGenerator:
    """从高光片段生成短视频"""

    def __init__(self, config: ClipConfig = None, output_dir: str = "recordings"):
        self.config = config or ClipConfig()
        self.output_dir = Path(output_dir)

    async def generate_clip(self, video_path: Path, highlight: dict,
                            danmaku_path: Optional[Path] = None,
                            auto_subtitle: bool = False,
                            auto_cover: bool = True,
                            force_watermark: bool = False) -> dict:
        """从单个高光生成片段（含字幕+封面）"""
        clip_id = f"c_{int(time.time())}_{uuid.uuid4().hex[:6]}"
        start = highlight["start_time"]
        end = highlight["end_time"]
        duration = end - start
        username = highlight["username"]

        clips_dir = self.output_dir / username / "clips"
        clips_dir.mkdir(parents=True, exist_ok=True)
        output_file = clips_dir / f"{clip_id}.mp4"

        # 1. 生成语音字幕（如果启用）
        speech_ass_path = None
        if auto_subtitle:
            try:
                from streamvideo.core.processor.subtitle_gen import SubtitleGenerator, is_whisper_available
                if is_whisper_available():
                    # 先切出临时片段用于语音识别（避免对整个长视频做 STT）
                    temp_clip = clips_dir / f".{clip_id}_temp.mp4"
                    proc = await asyncio.create_subprocess_exec(
                        "ffmpeg", "-y", "-ss", str(start), "-t", str(duration),
                        "-i", str(video_path), "-c", "copy", str(temp_clip),
                        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
                    )
                    await asyncio.wait_for(proc.wait(), timeout=60)
                    if temp_clip.exists():
                        gen = SubtitleGenerator()
                        speech_ass_path = await gen.generate(temp_clip, clips_dir)
                        temp_clip.unlink()
            except Exception as e:
                logger.warning(f"[{username}] Speech subtitle failed: {e}")

        # 2. 弹幕字幕（如果启用且无语音字幕）
        danmaku_ass_path = None
        if not speech_ass_path and self.config.danmaku_overlay and danmaku_path and danmaku_path.exists():
            danmaku_ass_path = self._generate_ass_subtitles(danmaku_path, start, duration, clips_dir / f"{clip_id}.ass")

        # 选择字幕源：语音优先，弹幕降级
        ass_path = speech_ass_path or danmaku_ass_path

        # 强制水印（免费用户）
        original_watermark = self.config.watermark
        if force_watermark and not self.config.watermark:
            self.config.watermark = "FlashCut"

        cmd = self._build_ffmpeg_cmd(video_path, output_file, start, duration, ass_path)

        # 恢复水印设置
        self.config.watermark = original_watermark

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)

            if proc.returncode == 0 and output_file.exists() and output_file.stat().st_size > 0:
                # 3. 生成封面图（如果启用）
                cover_path = None
                if auto_cover:
                    try:
                        from streamvideo.core.processor.cover_gen import CoverGenerator
                        cover_gen = CoverGenerator()
                        title = highlight.get("title", "")
                        cover_path = await cover_gen.generate(output_file, title, username, clips_dir)
                    except Exception as e:
                        logger.warning(f"[{username}] Cover generation failed: {e}")

                result = {
                    "clip_id": clip_id,
                    "highlight_id": highlight.get("highlight_id", ""),
                    "username": username,
                    "output_file": str(output_file.relative_to(self.output_dir)),
                    "filename": output_file.name,
                    "duration": round(duration, 1),
                    "resolution": self.config.resolution,
                    "format": self.config.format,
                    "size": output_file.stat().st_size,
                    "status": "done",
                    "cover": str(cover_path.name) if cover_path else "",
                    "has_subtitle": bool(speech_ass_path),
                }
                logger.info(f"[{username}] Clip generated: {output_file.name} "
                           f"({duration:.0f}s, {output_file.stat().st_size/1024/1024:.1f}MB)")
                return result
            else:
                error = (stderr.decode() if stderr else "")[:300]
                logger.warning(f"[{username}] Clip generation failed: {error}")
                return {"clip_id": clip_id, "status": "error", "error": error}
        except asyncio.TimeoutError:
            logger.warning(f"[{username}] Clip generation timed out (300s)")
            if output_file.exists():
                output_file.unlink()
            return {"clip_id": clip_id, "status": "error", "error": "生成超时"}
        except Exception as e:
            logger.warning(f"[{username}] Clip generation error: {e}")
            return {"clip_id": clip_id, "status": "error", "error": str(e)}
        finally:
            # 清理临时字幕文件
            if ass_path and ass_path.exists():
                try:
                    ass_path.unlink()
                except Exception:
                    pass

    async def batch_generate(self, video_path: Path, highlights: list[dict],
                             danmaku_path: Optional[Path] = None,
                             progress_callback: Optional[Callable] = None) -> list[dict]:
        """批量生成片段（顺序执行避免 CPU 过载）"""
        results = []
        for i, h in enumerate(highlights):
            result = await self.generate_clip(video_path, h, danmaku_path)
            results.append(result)
            if progress_callback:
                await progress_callback(i + 1, len(highlights), result)
        return results

    def _is_vertical(self, video_path: Path) -> bool:
        """检测视频是否为竖屏（w/h < 1）"""
        try:
            import subprocess
            result = subprocess.run(
                ["ffprobe", "-v", "quiet", "-select_streams", "v:0",
                 "-show_entries", "stream=width,height", "-of", "json", str(video_path)],
                capture_output=True, text=True, timeout=10,
            )
            import json as _json
            data = _json.loads(result.stdout)
            streams = data.get("streams", [])
            if streams:
                w = int(streams[0].get("width", 1920))
                h = int(streams[0].get("height", 1080))
                return w < h
        except Exception:
            pass
        return False  # 默认假设横屏

    def _build_ffmpeg_cmd(self, video_path: Path, output_path: Path,
                          start: float, duration: float,
                          ass_path: Optional[Path] = None) -> list[str]:
        """构建 ffmpeg 命令"""
        w, h = self.config.resolution.split("x")

        cmd = [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
            "-ss", str(start), "-t", str(duration),
            "-i", str(video_path),
        ]

        # 视频滤镜链
        vf_parts = []

        # 检测输入视频宽高比（用于智能裁剪决策）
        input_is_vertical = self._is_vertical(video_path)

        if self.config.format == "vertical":
            if input_is_vertical:
                # 输入已经是竖屏，直接缩放
                vf_parts.append(f"scale={w}:{h}:force_original_aspect_ratio=decrease")
                vf_parts.append(f"pad={w}:{h}:(ow-iw)/2:(oh-ih)/2")
            else:
                # 横屏输入 → 中心裁剪为 9:16
                vf_parts.append("crop=ih*9/16:ih")
                vf_parts.append(f"scale={w}:{h}")
        elif self.config.format == "square":
            vf_parts.append("crop=min(iw\\,ih):min(iw\\,ih)")
            vf_parts.append(f"scale={w}:{h}")
        else:
            # 16:9 横屏
            vf_parts.append(f"scale={w}:{h}:force_original_aspect_ratio=decrease")
            vf_parts.append(f"pad={w}:{h}:(ow-iw)/2:(oh-ih)/2")

        # 弹幕字幕叠加
        if ass_path and ass_path.exists():
            # 使用 ass 滤镜（需要转义路径中的特殊字符）
            safe_path = str(ass_path).replace("\\", "/").replace(":", "\\:")
            vf_parts.append(f"ass='{safe_path}'")

        # 水印
        if self.config.watermark:
            text = self.config.watermark.replace("'", "\\'")
            vf_parts.append(
                f"drawtext=text='{text}':fontsize=24:fontcolor=white@0.7"
                f":x=w-tw-20:y=h-th-20:shadowcolor=black@0.5:shadowx=1:shadowy=1"
            )

        if vf_parts:
            cmd += ["-vf", ",".join(vf_parts)]

        cmd += [
            "-c:v", self.config.output_codec, "-crf", str(self.config.crf),
            "-preset", "fast",
            "-c:a", "aac", "-b:a", "128k",
            "-movflags", "+faststart",
            str(output_path),
        ]
        return cmd

    def _generate_ass_subtitles(self, danmaku_path: Path, start: float,
                                 duration: float, output_path: Path) -> Optional[Path]:
        """将弹幕 JSON 转换为 ASS 字幕文件（滚动弹幕效果）"""
        try:
            with open(danmaku_path, encoding="utf-8") as f:
                data = json.load(f)
            messages = data.get("messages", [])
            # 过滤时间范围内的聊天消息
            chat_msgs = [
                m for m in messages
                if m.get("type") == "chat" and start <= m["t"] < start + duration
            ]
            if not chat_msgs:
                return None

            w, h = self.config.resolution.split("x")
            play_w, play_h = int(w), int(h)

            # ASS 头部
            ass_lines = [
                "[Script Info]",
                "ScriptType: v4.00+",
                f"PlayResX: {play_w}",
                f"PlayResY: {play_h}",
                "WrapStyle: 2",
                "",
                "[V4+ Styles]",
                "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding",
                f"Style: Danmaku,Arial,28,&H00FFFFFF,&H000000FF,&H00000000,&H80000000,-1,0,0,0,100,100,0,0,1,2,0,8,10,10,10,1",
                "",
                "[Events]",
                "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text",
            ]

            # 生成滚动弹幕事件
            scroll_duration = 8  # 弹幕滚动时间（秒）
            y_slots = play_h // 40  # 弹幕行数
            slot_used = [0.0] * y_slots  # 每行的最后使用时间

            for m in chat_msgs:
                t_rel = m["t"] - start  # 相对于片段开始的时间
                content = m.get("content", "").replace("\\", "\\\\").replace("{", "\\{")
                if not content:
                    continue

                # 找到空闲行
                slot = 0
                min_time = float("inf")
                for i in range(y_slots):
                    if slot_used[i] < min_time:
                        min_time = slot_used[i]
                        slot = i
                slot_used[slot] = t_rel + scroll_duration

                y_pos = 20 + slot * 40
                t_start = self._format_ass_time(t_rel)
                t_end = self._format_ass_time(t_rel + scroll_duration)

                # 从右到左滚动
                move = f"\\move({play_w + 200},{y_pos},-200,{y_pos})"
                ass_lines.append(
                    f"Dialogue: 0,{t_start},{t_end},Danmaku,,0,0,0,,{{{move}}}{content}"
                )

            with open(output_path, "w", encoding="utf-8") as f:
                f.write("\n".join(ass_lines))
            return output_path

        except Exception as e:
            logger.warning(f"Failed to generate ASS subtitles: {e}")
            return None

    @staticmethod
    def _format_ass_time(seconds: float) -> str:
        """格式化为 ASS 时间格式 H:MM:SS.CC"""
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = int(seconds % 60)
        cs = int((seconds % 1) * 100)
        return f"{h}:{m:02d}:{s:02d}.{cs:02d}"
