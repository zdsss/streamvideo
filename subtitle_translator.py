"""
字幕翻译模块 — 调用 Claude Haiku API，批量翻译 SRT 字幕
"""

import asyncio
import hashlib
import logging
import re
import time
import uuid
from pathlib import Path
from typing import Optional

logger = logging.getLogger("subtitle_translator")

SUPPORTED_LANGS = {
    "en": "英文", "ja": "日文", "ko": "韩文",
    "fr": "法文", "de": "德文", "es": "西班牙文",
    "zh": "中文",
}


def _parse_srt(path: Path) -> list[dict]:
    """解析 SRT 文件，返回 [{index, start, end, text}]"""
    segments = []
    text = path.read_text(encoding="utf-8", errors="replace")
    blocks = re.split(r"\n\n+", text.strip())
    for block in blocks:
        lines = block.strip().splitlines()
        if len(lines) < 3:
            continue
        try:
            idx = int(lines[0].strip())
        except ValueError:
            continue
        ts = lines[1].strip()
        content = " ".join(l.strip() for l in lines[2:] if l.strip())
        if content:
            segments.append({"index": idx, "timestamp": ts, "text": content})
    return segments


def _write_srt(segments: list[dict], path: Path):
    lines = []
    for i, s in enumerate(segments, 1):
        lines.append(str(i))
        lines.append(s["timestamp"])
        lines.append(s["text"])
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def _text_hash(text: str, source_lang: str, target_lang: str) -> str:
    return hashlib.sha256(f"{source_lang}:{target_lang}:{text}".encode()).hexdigest()


class SubtitleTranslator:
    def __init__(self, db, api_key: str, model: str = "claude-haiku-4-5-20251001"):
        self.db = db
        self.api_key = api_key
        self.model = model

    async def translate_srt(
        self,
        srt_path: Path,
        target_lang: str,
        source_lang: str = "zh",
        translation_id: Optional[str] = None,
    ) -> dict:
        if target_lang not in SUPPORTED_LANGS:
            return {"ok": False, "error": f"unsupported language: {target_lang}"}

        segments = _parse_srt(srt_path)
        if not segments:
            return {"ok": False, "error": "empty or invalid SRT file"}

        translated, cache_hits, api_calls = await self._translate_segments(
            segments, source_lang, target_lang
        )

        output_path = srt_path.with_suffix(f".{target_lang}.srt")
        _write_srt(translated, output_path)

        return {
            "ok": True,
            "output_file": output_path.name,
            "cache_hits": cache_hits,
            "api_calls": api_calls,
            "segment_count": len(segments),
        }

    async def _translate_segments(
        self, segments: list[dict], source_lang: str, target_lang: str
    ) -> tuple[list[dict], int, int]:
        results = []
        to_translate = []
        cache_hits = 0

        for seg in segments:
            cached = self.db.get_translation_cache(seg["text"], source_lang, target_lang)
            if cached:
                results.append({**seg, "text": cached})
                cache_hits += 1
            else:
                to_translate.append(seg)
                results.append(None)  # placeholder

        api_calls = 0
        if to_translate:
            batch_size = 50
            translated_texts = []
            for i in range(0, len(to_translate), batch_size):
                batch = to_translate[i : i + batch_size]
                texts = await self._call_llm(batch, source_lang, target_lang)
                translated_texts.extend(texts)
                api_calls += 1

            ti = 0
            for i, seg in enumerate(segments):
                if results[i] is None:
                    text = translated_texts[ti] if ti < len(translated_texts) else seg["text"]
                    self.db.set_translation_cache(seg["text"], source_lang, target_lang, text, self.model)
                    results[i] = {**seg, "text": text}
                    ti += 1

        return results, cache_hits, api_calls

    async def _call_llm(self, batch: list[dict], source_lang: str, target_lang: str) -> list[str]:
        import anthropic
        client = anthropic.Anthropic(api_key=self.api_key, timeout=30.0)
        lang_name = SUPPORTED_LANGS.get(target_lang, target_lang)
        texts = "\n".join(f"{i+1}. {s['text']}" for i, s in enumerate(batch))
        prompt = (
            f"将以下字幕从{SUPPORTED_LANGS.get(source_lang, source_lang)}翻译为{lang_name}。"
            f"保持编号，每行一条，只输出翻译结果，不要解释：\n{texts}"
        )

        def _run():
            resp = client.messages.create(
                model=self.model,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )
            return resp.content[0].text.strip()

        last_err: Exception = RuntimeError("no attempts")
        for attempt in range(3):
            try:
                raw = await asyncio.wait_for(asyncio.to_thread(_run), timeout=35.0)
                break
            except (anthropic.APITimeoutError, anthropic.APIConnectionError, TimeoutError) as e:
                last_err = e
                wait = 2 ** attempt
                logger.warning("LLM call attempt %d failed: %s — retrying in %ds", attempt + 1, e, wait)
                await asyncio.sleep(wait)
        else:
            logger.error("LLM call failed after 3 attempts: %s", last_err)
            return [s["text"] for s in batch]
        lines = [l.strip() for l in raw.splitlines() if l.strip()]
        cleaned = [re.sub(r"^\d+\.\s*", "", l) for l in lines]
        # 补齐数量（防止 LLM 少输出）
        while len(cleaned) < len(batch):
            cleaned.append(batch[len(cleaned)]["text"])
        return cleaned[: len(batch)]
