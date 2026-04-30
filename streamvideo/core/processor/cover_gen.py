"""
封面生成引擎 — 纯 ffmpeg 方案
从视频提取关键帧 + 添加标题/主播名文字叠加
支持基于高光时刻选帧、多候选帧评分、多尺寸输出
"""

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger("cover")

# 预设尺寸
SIZES = {
    "vertical": (1080, 1920),    # 9:16 竖屏
    "horizontal": (1920, 1080),  # 16:9 横屏
    "square": (1080, 1080),      # 1:1 方形
}


class CoverGenerator:
    """从视频片段生成封面图"""

    async def generate(self, video_path: Path, title: str = "",
                       username: str = "", output_dir: Optional[Path] = None,
                       highlight_time: Optional[float] = None,
                       size: str = "vertical") -> Optional[Path]:
        """生成封面图，返回 JPG 路径

        Args:
            highlight_time: 高光时刻（秒），优先从此处取帧
            size: 输出尺寸 vertical/horizontal/square
        """
        if not video_path.exists():
            return None
        out_dir = output_dir or video_path.parent
        suffix = f"_{size}" if size != "vertical" else ""
        cover_path = out_dir / f"{video_path.stem}_cover{suffix}.jpg"

        try:
            duration = await self._get_duration(video_path)
            seek_time = self._pick_seek_time(duration, highlight_time)
            w, h = SIZES.get(size, SIZES["vertical"])

            # 尝试多候选帧，选最佳
            best_path = await self._extract_best_frame(video_path, cover_path, seek_time, duration, w, h)
            if not best_path:
                return None

            # 添加文字叠加
            final_path = await self._add_text_overlay(best_path, title, username, w, h)
            return final_path or best_path

        except Exception as e:
            logger.warning(f"Cover generation error: {e}")
            return None

    async def generate_multi_size(self, video_path: Path, title: str = "",
                                   username: str = "", output_dir: Optional[Path] = None,
                                   highlight_time: Optional[float] = None) -> dict[str, Optional[Path]]:
        """生成多尺寸封面，返回 {size: path} 字典"""
        results = {}
        for size in SIZES:
            results[size] = await self.generate(
                video_path, title, username, output_dir, highlight_time, size
            )
        return results

    def _pick_seek_time(self, duration: float, highlight_time: Optional[float]) -> float:
        """选择取帧时间点"""
        if highlight_time is not None and 0 < highlight_time < duration:
            # 高光时刻偏移 2 秒（通常高光开始后几秒画面更精彩）
            return min(highlight_time + 2, duration - 1)
        # 默认：视频 30% 位置（比 10% 更可能有内容）
        return max(1, duration * 0.3)

    async def _extract_best_frame(self, video_path: Path, cover_path: Path,
                                   seek_time: float, duration: float,
                                   w: int, h: int) -> Optional[Path]:
        """提取多候选帧，选亮度最佳的一帧"""
        candidates = []
        # 在 seek_time 附近取 3 帧（-2s, 0, +2s）
        offsets = [0, -2, 2]
        for i, off in enumerate(offsets):
            t = max(0.5, min(duration - 0.5, seek_time + off))
            tmp_path = cover_path.parent / f"_tmp_cover_{i}.jpg"
            try:
                proc = await asyncio.create_subprocess_exec(
                    "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
                    "-ss", str(t),
                    "-i", str(video_path),
                    "-vframes", "1",
                    "-vf", f"scale={w}:{h}:force_original_aspect_ratio=increase,crop={w}:{h}",
                    "-q:v", "2",
                    str(tmp_path),
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
                )
                await asyncio.wait_for(proc.communicate(), timeout=15)
                if tmp_path.exists() and tmp_path.stat().st_size > 1000:
                    brightness = await self._measure_brightness(tmp_path)
                    candidates.append((tmp_path, brightness))
            except Exception:
                pass

        if not candidates:
            # 最后降级：简单提取
            return await self._fallback_extract(video_path, cover_path, seek_time, w, h)

        # 选亮度最接近中间值的帧（不要太暗也不要太亮）
        candidates.sort(key=lambda c: abs(c[1] - 120))
        best_tmp, _ = candidates[0]

        # 重命名为最终路径
        if best_tmp != cover_path:
            best_tmp.rename(cover_path)

        # 清理其他候选
        for tmp, _ in candidates:
            if tmp.exists() and tmp != cover_path:
                tmp.unlink(missing_ok=True)

        return cover_path if cover_path.exists() else None

    async def _measure_brightness(self, image_path: Path) -> float:
        """用 ffprobe 测量图片平均亮度（0-255）"""
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffprobe", "-v", "quiet",
                "-f", "lavfi", "-i",
                f"movie={image_path},signalstats",
                "-show_entries", "frame_tags=lavfi.signalstats.YAVG",
                "-of", "json",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
            data = json.loads(stdout.decode())
            frames = data.get("frames", [])
            if frames:
                tags = frames[0].get("tags", {})
                return float(tags.get("lavfi.signalstats.YAVG", 100))
        except Exception:
            pass
        # 降级：用文件大小估算（更大的文件通常更亮/更丰富）
        return min(200, image_path.stat().st_size / 500)

    async def _add_text_overlay(self, image_path: Path, title: str,
                                 username: str, w: int, h: int) -> Optional[Path]:
        """在封面上添加文字叠加"""
        if not title and not username:
            return image_path

        output_path = image_path  # 原地覆盖
        tmp_path = image_path.parent / f"_tmp_overlay{image_path.suffix}"

        vf_parts = []

        # 底部渐变遮罩
        vf_parts.append(
            "drawbox=x=0:y=ih*0.7:w=iw:h=ih*0.3:color=black@0.6:t=fill"
        )

        if title:
            safe_title = title.replace("'", "\\'").replace(":", "\\:").replace("%", "%%")
            vf_parts.append(
                f"drawtext=text='{safe_title}'"
                f":fontsize=48:fontcolor=white"
                f":x=(w-tw)/2:y=h*0.78"
                f":shadowcolor=black@0.8:shadowx=2:shadowy=2"
            )

        if username:
            safe_name = username.replace("'", "\\'").replace(":", "\\:").replace("%", "%%")
            vf_parts.append(
                f"drawtext=text='{safe_name}'"
                f":fontsize=32:fontcolor=white@0.8"
                f":x=(w-tw)/2:y=h*0.85"
                f":shadowcolor=black@0.5:shadowx=1:shadowy=1"
            )

        try:
            cmd = [
                "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
                "-i", str(image_path),
                "-vf", ",".join(vf_parts),
                "-q:v", "2",
                str(tmp_path),
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=15)

            if proc.returncode == 0 and tmp_path.exists():
                tmp_path.rename(output_path)
                return output_path
            else:
                # 文字叠加失败，返回无文字版本
                if tmp_path.exists():
                    tmp_path.unlink(missing_ok=True)
                return image_path
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            return image_path

    async def _fallback_extract(self, video_path: Path, cover_path: Path,
                                 seek_time: float, w: int, h: int) -> Optional[Path]:
        """降级方案：仅提取帧"""
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y", "-ss", str(seek_time),
                "-i", str(video_path),
                "-vframes", "1",
                "-vf", f"scale={w}:-1",
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
            proc = await asyncio.create_subprocess_exec(
                "ffprobe", "-v", "quiet", "-show_entries", "format=duration",
                "-of", "json", str(video_path),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
            data = json.loads(stdout.decode())
            return float(data.get("format", {}).get("duration", 0) or 0)
        except Exception:
            return 10
