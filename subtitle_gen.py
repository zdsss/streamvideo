"""
语音字幕引擎 — 基于 OpenAI Whisper 本地模型
生成 SRT/ASS/VTT 字幕文件，用于 ffmpeg 烧录到短视频
"""

import asyncio
import logging
import re
from pathlib import Path
from typing import Optional

logger = logging.getLogger("subtitle")

# Whisper 模型缓存（避免重复加载）
_whisper_model = None
_whisper_model_size = None

# 中文标点恢复规则
_PUNCT_RULES = [
    # 句末缺标点 → 加句号
    (re.compile(r'([^。！？，、；：\u201c\u201d\u2018\u2019（）.!?,])$'), r'\1。'),
    # 连续空格 → 逗号
    (re.compile(r'(\S)\s{2,}(\S)'), r'\1，\2'),
]

# 中文句末标点
_SENTENCE_ENDS = set('。！？!?')

# Whisper 中文初始提示词（引导模型输出更好的中文）
_ZH_INITIAL_PROMPT = "以下是普通话的句子，请使用简体中文输出。"


def _get_whisper_model(model_size: str = "small"):
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


def _restore_punctuation(text: str) -> str:
    """恢复中文标点（Whisper 输出常缺标点）"""
    text = text.strip()
    if not text:
        return text
    for pattern, replacement in _PUNCT_RULES:
        text = pattern.sub(replacement, text)
    return text


def _split_long_segments(segments: list[dict], max_chars: int = 20) -> list[dict]:
    """将过长的字幕段拆分为多行（中文每行不超过 max_chars 字）"""
    result = []
    for seg in segments:
        text = seg.get("text", "").strip()
        if not text:
            continue
        if len(text) <= max_chars:
            result.append(seg)
            continue

        # 按标点分割
        parts = re.split(r'([。！？，、；：])', text)
        lines = []
        current = ""
        for part in parts:
            if part in '。！？，、；：':
                current += part
                if len(current) >= max_chars * 0.6:
                    lines.append(current)
                    current = ""
            else:
                if len(current) + len(part) > max_chars and current:
                    lines.append(current)
                    current = part
                else:
                    current += part
        if current:
            lines.append(current)

        if not lines:
            result.append(seg)
            continue

        # 按比例分配时间
        total_chars = sum(len(l) for l in lines)
        seg_start = seg["start"]
        seg_duration = seg["end"] - seg["start"]
        for line in lines:
            ratio = len(line) / total_chars if total_chars > 0 else 1 / len(lines)
            line_duration = seg_duration * ratio
            result.append({
                "start": seg_start,
                "end": seg_start + line_duration,
                "text": line,
            })
            seg_start += line_duration

    return result


class SubtitleGenerator:
    """Whisper 语音识别 + 字幕生成"""

    def __init__(self, model_size: str = "small", language: str = "zh"):
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
            segments = self._post_process(segments)
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
            segments = self._post_process(segments)
            srt_content = self._to_srt(segments)
            with open(srt_path, "w", encoding="utf-8") as f:
                f.write(srt_content)
            return srt_path
        except Exception as e:
            logger.error(f"SRT generation failed: {e}")
            return None

    async def generate_vtt(self, video_path: Path, output_dir: Optional[Path] = None) -> Optional[Path]:
        """生成 WebVTT 字幕文件"""
        if not video_path.exists():
            return None
        out_dir = output_dir or video_path.parent
        vtt_path = out_dir / f"{video_path.stem}.vtt"

        try:
            segments = await self._transcribe(video_path)
            if not segments:
                return None
            segments = self._post_process(segments)
            vtt_content = self._to_vtt(segments)
            with open(vtt_path, "w", encoding="utf-8") as f:
                f.write(vtt_content)
            return vtt_path
        except Exception as e:
            logger.error(f"VTT generation failed: {e}")
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
                initial_prompt=_ZH_INITIAL_PROMPT if self.language == "zh" else None,
                condition_on_previous_text=True,
            )
            return result.get("segments", [])

        return await asyncio.to_thread(_run)

    def _post_process(self, segments: list[dict]) -> list[dict]:
        """后处理：标点恢复 + 长句分割"""
        # 标点恢复
        for seg in segments:
            seg["text"] = _restore_punctuation(seg.get("text", ""))

        # 长句分割
        segments = _split_long_segments(segments, max_chars=20)

        # 过滤空段
        return [s for s in segments if s.get("text", "").strip()]

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
    def _to_vtt(segments: list[dict]) -> str:
        """转换为 WebVTT 格式"""
        lines = ["WEBVTT", ""]
        for i, seg in enumerate(segments, 1):
            start = SubtitleGenerator._format_vtt_time(seg["start"])
            end = SubtitleGenerator._format_vtt_time(seg["end"])
            text = seg.get("text", "").strip()
            if text:
                lines.append(f"{i}")
                lines.append(f"{start} --> {end}")
                lines.append(text)
                lines.append("")
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
    def _format_vtt_time(seconds: float) -> str:
        """VTT 时间格式: HH:MM:SS.mmm"""
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = int(seconds % 60)
        ms = int((seconds % 1) * 1000)
        return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"

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
