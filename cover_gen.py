"""
封面生成引擎 — 纯 ffmpeg 方案
从视频提取关键帧 + 添加标题/主播名文字叠加
"""

import asyncio
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger("cover")


class CoverGenerator:
    """从视频片段生成封面图"""

    async def generate(self, video_path: Path, title: str = "",
                       username: str = "", output_dir: Optional[Path] = None) -> Optional[Path]:
        """生成封面图，返回 JPG 路径"""
        if not video_path.exists():
            return None
        out_dir = output_dir or video_path.parent
        cover_path = out_dir / f"{video_path.stem}_cover.jpg"

        try:
            # 获取视频时长
            duration = await self._get_duration(video_path)
            seek_time = max(1, duration * 0.1)

            # 提取帧 + 添加文字叠加（一步完成）
            vf_parts = ["scale=1080:1920:force_original_aspect_ratio=increase",
                        "crop=1080:1920"]

            # 底部渐变遮罩
            vf_parts.append(
                "drawbox=x=0:y=ih*0.7:w=iw:h=ih*0.3:color=black@0.6:t=fill"
            )

            # 标题文字
            if title:
                safe_title = title.replace("'", "\\'").replace(":", "\\:")
                vf_parts.append(
                    f"drawtext=text='{safe_title}'"
                    f":fontsize=48:fontcolor=white"
                    f":x=(w-tw)/2:y=h*0.78"
                    f":shadowcolor=black@0.8:shadowx=2:shadowy=2"
                )

            # 主播名
            if username:
                safe_name = username.replace("'", "\\'").replace(":", "\\:")
                vf_parts.append(
                    f"drawtext=text='{safe_name}'"
                    f":fontsize=32:fontcolor=white@0.8"
                    f":x=(w-tw)/2:y=h*0.85"
                    f":shadowcolor=black@0.5:shadowx=1:shadowy=1"
                )

            cmd = [
                "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
                "-ss", str(seek_time),
                "-i", str(video_path),
                "-vframes", "1",
                "-vf", ",".join(vf_parts),
                "-q:v", "2",
                str(cover_path),
            ]

            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)

            if proc.returncode == 0 and cover_path.exists():
                logger.info(f"Cover generated: {cover_path.name}")
                return cover_path
            else:
                error = (stderr.decode() if stderr else "")[:200]
                logger.warning(f"Cover generation failed: {error}")
                # 降级：不加文字，只提取帧
                return await self._fallback_extract(video_path, cover_path, seek_time)

        except Exception as e:
            logger.warning(f"Cover generation error: {e}")
            return None

    async def _fallback_extract(self, video_path: Path, cover_path: Path,
                                 seek_time: float) -> Optional[Path]:
        """降级方案：仅提取帧，不加文字"""
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y", "-ss", str(seek_time),
                "-i", str(video_path),
                "-vframes", "1", "-vf", "scale=1080:-1",
                "-q:v", "2", str(cover_path),
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            )
            await asyncio.wait_for(proc.wait(), timeout=15)
            if cover_path.exists():
                return cover_path
        except Exception:
            pass
        return None

    async def _get_duration(self, video_path: Path) -> float:
        """获取视频时长"""
        try:
            import json
            proc = await asyncio.create_subprocess_exec(
                "ffprobe", "-v", "quiet", "-show_entries", "format=duration",
                "-of", "json", str(video_path),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
            data = json.loads(stdout.decode())
            return float(data.get("format", {}).get("duration", 0) or 0)
        except Exception:
            return 10  # 默认 10 秒
