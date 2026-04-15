"""
语音字幕引擎 — 基于 OpenAI Whisper 本地模型
生成 SRT/ASS 字幕文件，用于 ffmpeg 烧录到短视频
"""

import asyncio
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger("subtitle")

# Whisper 模型缓存（避免重复加载）
_whisper_model = None
_whisper_model_size = None


def _get_whisper_model(model_size: str = "base"):
    """懒加载 Whisper 模型"""
    global _whisper_model, _whisper_model_size
    if _whisper_model is not None and _whisper_model_size == model_size:
        return _whisper_model
    try:
        import whisper
        logger.info(f"Loading Whisper model: {model_size}")
        _whisper_model = whisper.load_model(model_size)
        _whisper_model_size = model_size
        logger.info(f"Whisper model loaded: {model_size}")
        return _whisper_model
    except ImportError:
        logger.warning("openai-whisper not installed. Run: pip install openai-whisper")
        return None
    except Exception as e:
        logger.error(f"Failed to load Whisper model: {e}")
        return None


class SubtitleGenerator:
    """Whisper 语音识别 + 字幕生成"""

    def __init__(self, model_size: str = "base", language: str = "zh"):
        self.model_size = model_size
        self.language = language

    async def generate(self, video_path: Path, output_dir: Optional[Path] = None) -> Optional[Path]:
        """生成字幕文件（ASS 格式），返回路径"""
        if not video_path.exists():
            return None
        out_dir = output_dir or video_path.parent
        ass_path = out_dir / f"{video_path.stem}.sub.ass"

        try:
            segments = await self._transcribe(video_path)
            if not segments:
                logger.warning(f"No speech detected in {video_path.name}")
                return None
            ass_content = self._to_ass(segments)
            with open(ass_path, "w", encoding="utf-8") as f:
                f.write(ass_content)
            logger.info(f"Subtitles generated: {ass_path.name} ({len(segments)} segments)")
            return ass_path
        except Exception as e:
            logger.error(f"Subtitle generation failed: {e}")
            return None

    async def generate_srt(self, video_path: Path, output_dir: Optional[Path] = None) -> Optional[Path]:
        """生成 SRT 字幕文件"""
        if not video_path.exists():
            return None
        out_dir = output_dir or video_path.parent
        srt_path = out_dir / f"{video_path.stem}.srt"

        try:
            segments = await self._transcribe(video_path)
            if not segments:
                return None
            srt_content = self._to_srt(segments)
            with open(srt_path, "w", encoding="utf-8") as f:
                f.write(srt_content)
            return srt_path
        except Exception as e:
            logger.error(f"SRT generation failed: {e}")
            return None

    async def _transcribe(self, video_path: Path) -> list[dict]:
        """运行 Whisper 语音识别（在线程池中执行避免阻塞）"""
        model = _get_whisper_model(self.model_size)
        if model is None:
            return []

        def _run():
            result = model.transcribe(
                str(video_path),
                language=self.language,
                verbose=False,
            )
            return result.get("segments", [])

        return await asyncio.to_thread(_run)

    @staticmethod
    def _to_srt(segments: list[dict]) -> str:
        """转换为 SRT 格式"""
        lines = []
        for i, seg in enumerate(segments, 1):
            start = SubtitleGenerator._format_srt_time(seg["start"])
            end = SubtitleGenerator._format_srt_time(seg["end"])
            text = seg.get("text", "").strip()
            if text:
                lines.append(f"{i}\n{start} --> {end}\n{text}\n")
        return "\n".join(lines)

    @staticmethod
    def _to_ass(segments: list[dict], font_size: int = 36) -> str:
        """转换为 ASS 格式（带样式，适合 ffmpeg 烧录）"""
        header = f"""[Script Info]
ScriptType: v4.00+
PlayResX: 1080
PlayResY: 1920
WrapStyle: 0

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default,Arial,{font_size},&H00FFFFFF,&H000000FF,&H00000000,&H80000000,-1,0,0,0,100,100,0,0,1,3,1,2,30,30,60,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""
        events = []
        for seg in segments:
            start = SubtitleGenerator._format_ass_time(seg["start"])
            end = SubtitleGenerator._format_ass_time(seg["end"])
            text = seg.get("text", "").strip().replace("\n", "\\N")
            if text:
                events.append(f"Dialogue: 0,{start},{end},Default,,0,0,0,,{text}")

        return header + "\n".join(events) + "\n"

    @staticmethod
    def _format_srt_time(seconds: float) -> str:
        """SRT 时间格式: HH:MM:SS,mmm"""
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = int(seconds % 60)
        ms = int((seconds % 1) * 1000)
        return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"

    @staticmethod
    def _format_ass_time(seconds: float) -> str:
        """ASS 时间格式: H:MM:SS.CC"""
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = int(seconds % 60)
        cs = int((seconds % 1) * 100)
        return f"{h}:{m:02d}:{s:02d}.{cs:02d}"


def is_whisper_available() -> bool:
    """检查 Whisper 是否可用"""
    try:
        import whisper
        return True
    except ImportError:
        return False
