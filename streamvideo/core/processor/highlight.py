"""
高光检测引擎 — 纯 ffmpeg 方案
信号源：音频音量峰值 + 场景切换 + 静音边界 + 弹幕密度 + 弹幕关键词 + 礼物事件
"""

import asyncio
import json
import logging
import re
import statistics
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger("highlight")


# ========== 关键词库（分权重） ==========

# 高权重关键词 — 强烈暗示高光时刻
KEYWORDS_HIGH = [
    # 电商
    "上链接", "秒杀", "抢到了", "买买买", "拍了", "下单", "已拍",
    "开抢", "倒计时", "3 2 1", "321", "炸了", "爆单",
    # 游戏
    "五杀", "四杀", "三杀", "超神", "MVP", "绝杀", "翻盘",
    "ACE", "团灭", "carry", "逆转",
]

# 中权重关键词 — 较强互动信号
KEYWORDS_MEDIUM = [
    # 电商
    "多少钱", "价格", "优惠", "便宜", "划算", "赚到了", "太值了",
    "库存", "还有吗", "补货",
    # 游戏
    "666", "厉害", "牛", "秀", "太强了", "卧槽", "我靠",
    "精彩", "好看", "漂亮",
    # 通用
    "哈哈哈", "笑死", "泪目", "感动", "破防",
]

# 低权重关键词 — 一般互动
KEYWORDS_LOW = [
    "加油", "支持", "来了", "打卡", "签到",
]

# 关键词 → 权重映射
KEYWORD_WEIGHTS = {}
for kw in KEYWORDS_HIGH:
    KEYWORD_WEIGHTS[kw] = 1.0
for kw in KEYWORDS_MEDIUM:
    KEYWORD_WEIGHTS[kw] = 0.6
for kw in KEYWORDS_LOW:
    KEYWORD_WEIGHTS[kw] = 0.3


@dataclass
class HighlightSignal:
    """单个检测信号"""
    type: str           # audio_peak | scene_change | silence_boundary | danmaku_peak | keyword_match | gift_spike
    timestamp: float    # 秒
    strength: float     # 0.0 - 1.0
    detail: str = ""


@dataclass
class Highlight:
    """检测到的高光片段"""
    start_time: float
    end_time: float
    score: float
    category: str       # engagement_spike | audio_peak | keyword_trigger | scene_transition | gift_spike
    signals: list[dict] = field(default_factory=list)
    title: str = ""


class HighlightDetector:
    """基于 ffmpeg 分析 + 弹幕数据的高光检测"""

    def __init__(self, config: dict = None):
        config = config or {}
        self.min_score = config.get("highlight_min_score", 0.6)
        self.min_duration = config.get("highlight_min_duration", 15)
        self.max_duration = config.get("highlight_max_duration", 60)
        self.padding_before = config.get("highlight_padding_before", 5)
        self.padding_after = config.get("highlight_padding_after", 3)

        # 合并用户自定义关键词和内置关键词库
        user_keywords = config.get("highlight_keywords", [])
        self.keyword_weights = dict(KEYWORD_WEIGHTS)
        for kw in user_keywords:
            if kw not in self.keyword_weights:
                self.keyword_weights[kw] = 0.8  # 用户自定义默认中高权重

        self.weights = {
            "audio_peak": 0.20,
            "scene_change": 0.08,
            "silence_boundary": 0.04,
            "danmaku_peak": 0.30,
            "keyword_match": 0.23,
            "gift_spike": 0.15,
        }

    async def detect(self, video_path: Path, danmaku_path: Optional[Path] = None) -> list[Highlight]:
        """运行所有检测并返回评分后的高光列表"""
        duration = await self._get_duration(video_path)
        if duration <= 0:
            logger.warning(f"Cannot detect highlights: video duration is 0")
            return []

        # 并行运行所有分析
        tasks = [
            self._analyze_audio_volume(video_path, duration),
            self._analyze_scene_changes(video_path),
            self._analyze_silence(video_path),
        ]
        if danmaku_path and danmaku_path.exists():
            tasks.append(self._analyze_danmaku_density(danmaku_path, duration))
            tasks.append(self._analyze_danmaku_keywords(danmaku_path))
            tasks.append(self._analyze_gift_events(danmaku_path))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        signals = []
        for r in results:
            if isinstance(r, list):
                signals.extend(r)
            elif isinstance(r, Exception):
                logger.warning(f"Highlight analysis error: {r}")

        if not signals:
            logger.info(f"No highlight signals detected in {video_path.name}")
            return []

        highlights = self._score_and_merge(signals, duration)
        logger.info(f"Detected {len(highlights)} highlights from {len(signals)} signals in {video_path.name}")
        return highlights

    async def _get_duration(self, video_path: Path) -> float:
        """获取视频时长"""
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffprobe", "-v", "quiet", "-show_entries", "format=duration",
                "-of", "json", str(video_path),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
            data = json.loads(stdout.decode())
            return float(data.get("format", {}).get("duration", 0) or 0)
        except Exception as e:
            logger.warning(f"Failed to get duration: {e}")
            return 0

    async def _analyze_audio_volume(self, video_path: Path, duration: float) -> list[HighlightSignal]:
        """分析音频音量变化，找到响亮时刻。使用分段 volumedetect 方案"""
        signals = []
        try:
            segment_duration = 10
            segment_count = max(1, int(duration / segment_duration))
            volumes = []

            for i in range(segment_count):
                start = i * segment_duration
                proc = await asyncio.create_subprocess_exec(
                    "ffmpeg", "-ss", str(start), "-t", str(segment_duration),
                    "-i", str(video_path), "-af", "volumedetect",
                    "-f", "null", "-",
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
                )
                _, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
                output = stderr.decode()
                m = re.search(r'mean_volume:\s*([-\d.]+)\s*dB', output)
                if m:
                    vol = float(m.group(1))
                    volumes.append((start, vol))

            if len(volumes) < 3:
                return signals

            values = [v for _, v in volumes if v > -70]
            if len(values) < 3:
                return signals
            mean_v = statistics.mean(values)
            std_v = statistics.stdev(values) if len(values) > 1 else 5
            threshold = mean_v + 1.5 * std_v

            for t, v in volumes:
                if v > threshold:
                    strength = min(1.0, (v - mean_v) / (3 * std_v)) if std_v > 0 else 0.5
                    signals.append(HighlightSignal(
                        type="audio_peak", timestamp=t + segment_duration / 2,
                        strength=max(0.1, strength), detail=f"音量 {v:.1f}dB"
                    ))

        except asyncio.TimeoutError:
            logger.warning("Audio volume analysis timed out")
        except Exception as e:
            logger.warning(f"Audio volume analysis error: {e}")
        return signals

    async def _analyze_scene_changes(self, video_path: Path) -> list[HighlightSignal]:
        """ffmpeg scene detect 检测场景切换"""
        signals = []
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-i", str(video_path),
                "-vf", "select='gt(scene,0.3)',showinfo",
                "-f", "null", "-",
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
            output = stderr.decode()

            for line in output.split("\n"):
                if "pts_time:" in line:
                    m = re.search(r'pts_time:([\d.]+)', line)
                    scene_m = re.search(r'scene:([\d.]+)', line)
                    if m:
                        t = float(m.group(1))
                        score = float(scene_m.group(1)) if scene_m else 0.5
                        signals.append(HighlightSignal(
                            type="scene_change", timestamp=t,
                            strength=min(1.0, score), detail=f"场景切换 {score:.2f}"
                        ))

        except asyncio.TimeoutError:
            logger.warning("Scene change analysis timed out")
        except Exception as e:
            logger.warning(f"Scene change analysis error: {e}")
        return signals

    async def _analyze_silence(self, video_path: Path) -> list[HighlightSignal]:
        """ffmpeg silencedetect 检测静音边界"""
        signals = []
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-i", str(video_path),
                "-af", "silencedetect=noise=-30dB:d=2",
                "-f", "null", "-",
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
            output = stderr.decode()

            for line in output.split("\n"):
                if "silence_end:" in line:
                    m = re.search(r'silence_end:\s*([\d.]+)', line)
                    if m:
                        t = float(m.group(1))
                        signals.append(HighlightSignal(
                            type="silence_boundary", timestamp=t,
                            strength=0.6, detail="静音结束（可能是关键时刻开始）"
                        ))
                elif "silence_start:" in line:
                    m = re.search(r'silence_start:\s*([\d.]+)', line)
                    if m:
                        t = float(m.group(1))
                        signals.append(HighlightSignal(
                            type="silence_boundary", timestamp=t,
                            strength=0.4, detail="静音开始（可能是关键时刻结束）"
                        ))

        except asyncio.TimeoutError:
            logger.warning("Silence detection timed out")
        except Exception as e:
            logger.warning(f"Silence detection error: {e}")
        return signals

    async def _analyze_danmaku_density(self, danmaku_path: Path, video_duration: float) -> list[HighlightSignal]:
        """分析弹幕密度峰值 — 带时间窗口平滑和动态阈值"""
        signals = []
        try:
            with open(danmaku_path, encoding="utf-8") as f:
                data = json.load(f)
            offset = data.get("video_start_offset", 0) or 0
            messages = data.get("messages", [])
            chat_msgs = [{"t": max(0, m["t"] - offset), **{k: v for k, v in m.items() if k != "t"}}
                         for m in messages if m.get("type") == "chat"]
            if len(chat_msgs) < 5:
                return signals

            # 10 秒滑动窗口，步进 5 秒（减少计算量，同时保持精度）
            window = 10
            step = 5
            max_t = max(m["t"] for m in chat_msgs)
            densities = []
            for t in range(0, int(max_t) + 1, step):
                count = sum(1 for m in chat_msgs if t <= m["t"] < t + window)
                densities.append((t, count / window))

            if len(densities) < 3:
                return signals

            values = [d for _, d in densities]
            mean_d = statistics.mean(values)
            std_d = statistics.stdev(values) if len(values) > 1 else 0.5

            # 动态阈值：短直播（<30min）用 1.2σ，长直播用 1.8σ
            if video_duration < 1800:
                sigma_mult = 1.2
            elif video_duration < 7200:
                sigma_mult = 1.5
            else:
                sigma_mult = 1.8
            threshold = mean_d + sigma_mult * std_d

            # 最低密度门槛：至少 0.3 条/秒（防止低活跃直播误判）
            min_density = max(0.3, mean_d * 1.5)

            # 时间窗口平滑：连续 2 个窗口都超阈值才算
            prev_above = False
            for i, (t, density) in enumerate(densities):
                above = density > threshold and density > min_density
                if above and prev_above:
                    strength = min(1.0, (density - mean_d) / (3 * std_d)) if std_d > 0 else 0.5
                    signals.append(HighlightSignal(
                        type="danmaku_peak", timestamp=float(t),
                        strength=max(0.2, strength), detail=f"弹幕密度 {density:.1f}/s"
                    ))
                elif above and i > 0:
                    # 单点超阈值但特别高（>3σ），也算
                    if density > mean_d + 3 * std_d:
                        strength = min(1.0, (density - mean_d) / (3 * std_d)) if std_d > 0 else 0.7
                        signals.append(HighlightSignal(
                            type="danmaku_peak", timestamp=float(t),
                            strength=max(0.3, strength), detail=f"弹幕爆发 {density:.1f}/s"
                        ))
                prev_above = above

        except Exception as e:
            logger.warning(f"Danmaku density analysis error: {e}")
        return signals

    async def _analyze_danmaku_keywords(self, danmaku_path: Path) -> list[HighlightSignal]:
        """匹配弹幕关键词 — 分权重匹配"""
        signals = []
        if not self.keyword_weights:
            return signals
        try:
            with open(danmaku_path, encoding="utf-8") as f:
                data = json.load(f)
            offset = data.get("video_start_offset", 0) or 0
            messages = data.get("messages", [])

            for m in messages:
                if m.get("type") != "chat":
                    continue
                content = m.get("content", "")
                best_kw = None
                best_weight = 0
                for kw, weight in self.keyword_weights.items():
                    if kw in content and weight > best_weight:
                        best_kw = kw
                        best_weight = weight
                if best_kw:
                    signals.append(HighlightSignal(
                        type="keyword_match",
                        timestamp=max(0, m["t"] - offset),
                        strength=best_weight,
                        detail=f"关键词「{best_kw}」"
                    ))

        except Exception as e:
            logger.warning(f"Danmaku keyword analysis error: {e}")
        return signals

    async def _analyze_gift_events(self, danmaku_path: Path) -> list[HighlightSignal]:
        """分析礼物事件密度 — 礼物集中出现暗示高光"""
        signals = []
        try:
            with open(danmaku_path, encoding="utf-8") as f:
                data = json.load(f)
            offset = data.get("video_start_offset", 0) or 0
            messages = data.get("messages", [])
            gift_msgs = [max(0, m["t"] - offset) for m in messages if m.get("type") == "gift"]

            if len(gift_msgs) < 3:
                return signals

            # 30 秒窗口统计礼物密度
            window = 30
            max_t = max(gift_msgs)
            gift_densities = []
            for t in range(0, int(max_t) + 1, 10):
                count = sum(1 for gt in gift_msgs if t <= gt < t + window)
                gift_densities.append((t, count))

            if not gift_densities:
                return signals

            values = [c for _, c in gift_densities]
            mean_g = statistics.mean(values)
            std_g = statistics.stdev(values) if len(values) > 1 else 1
            threshold = mean_g + 2 * std_g

            for t, count in gift_densities:
                if count > threshold and count >= 3:
                    strength = min(1.0, (count - mean_g) / (3 * std_g)) if std_g > 0 else 0.5
                    signals.append(HighlightSignal(
                        type="gift_spike", timestamp=float(t + window / 2),
                        strength=max(0.3, strength), detail=f"礼物爆发 {count}个/{window}s"
                    ))

        except Exception as e:
            logger.warning(f"Gift event analysis error: {e}")
        return signals

    def _score_and_merge(self, signals: list[HighlightSignal], duration: float) -> list[Highlight]:
        """复合评分 + 合并重叠区域"""
        if not signals or duration <= 0:
            return []

        # 1. 创建 1 秒分辨率时间线
        timeline_len = int(duration) + 1
        timeline = [0.0] * timeline_len
        signal_map = [[] for _ in range(timeline_len)]

        for sig in signals:
            idx = int(sig.timestamp)
            if 0 <= idx < timeline_len:
                weighted = sig.strength * self.weights.get(sig.type, 0.1)
                # 信号影响周围 ±5 秒（高斯衰减）
                for off in range(-5, 6):
                    t = idx + off
                    if 0 <= t < timeline_len:
                        decay = 1.0 / (1 + abs(off) * 0.3)
                        timeline[t] += weighted * decay
                signal_map[idx].append({
                    "type": sig.type, "strength": round(sig.strength, 2), "detail": sig.detail
                })

        # 1.5 跨信号关联加成：不同类型信号在 15 秒内重叠 → 1.5x
        correlated_pairs: set[tuple[int, int]] = set()
        sig_by_type: dict[str, list[int]] = {}
        for sig in signals:
            idx = int(sig.timestamp)
            if 0 <= idx < timeline_len:
                sig_by_type.setdefault(sig.type, []).append(idx)
        type_list = list(sig_by_type.keys())
        for i in range(len(type_list)):
            for j in range(i + 1, len(type_list)):
                for t_a in sig_by_type[type_list[i]]:
                    for t_b in sig_by_type[type_list[j]]:
                        if abs(t_a - t_b) <= 15:
                            pair = (min(i, j), max(i, j))
                            if pair not in correlated_pairs:
                                correlated_pairs.add(pair)
                                center = (t_a + t_b) // 2
                                for off in range(-5, 6):
                                    t = center + off
                                    if 0 <= t < timeline_len:
                                        decay = 1.0 / (1 + abs(off) * 0.3)
                                        timeline[t] += 0.15 * decay

        # 2. 归一化
        max_score = max(timeline) if timeline else 0
        if max_score > 0:
            timeline = [s / max_score for s in timeline]
        else:
            return []

        # 3. 找到连续高分区域
        regions = []
        in_region = False
        start = 0
        for i, score in enumerate(timeline):
            if score >= self.min_score and not in_region:
                start = i
                in_region = True
            elif score < self.min_score * 0.7 and in_region:
                regions.append((start, i))
                in_region = False
        if in_region:
            regions.append((start, len(timeline) - 1))

        # 4. 应用时长约束 + padding
        highlights = []
        for start_t, end_t in regions:
            s = max(0, start_t - self.padding_before)
            e = min(duration, end_t + self.padding_after)
            dur = e - s

            if dur < self.min_duration:
                center = (s + e) / 2
                s = max(0, center - self.min_duration / 2)
                e = min(duration, s + self.min_duration)
                dur = e - s
            if dur > self.max_duration:
                region_slice = timeline[start_t:end_t + 1]
                if region_slice:
                    peak_offset = region_slice.index(max(region_slice))
                    peak_t = start_t + peak_offset
                else:
                    peak_t = (start_t + end_t) // 2
                s = max(0, peak_t - self.max_duration / 2)
                e = min(duration, s + self.max_duration)

            region_scores = timeline[int(s):int(e) + 1]
            avg_score = statistics.mean(region_scores) if region_scores else 0

            region_signals = []
            for t in range(int(s), min(int(e) + 1, timeline_len)):
                region_signals.extend(signal_map[t])

            category = self._determine_category(region_signals)

            highlights.append(Highlight(
                start_time=round(s, 1),
                end_time=round(e, 1),
                score=round(avg_score, 3),
                category=category,
                signals=region_signals[:20],
                title="",
            ))

        # 5. 合并重叠
        highlights = self._merge_overlapping(highlights)

        # 6. 按分数排序，生成标题
        highlights.sort(key=lambda h: h.score, reverse=True)
        for i, h in enumerate(highlights):
            h.title = self._auto_title(h, i + 1)

        return highlights

    def _merge_overlapping(self, highlights: list[Highlight]) -> list[Highlight]:
        """合并重叠的高光区域"""
        if len(highlights) <= 1:
            return highlights
        highlights.sort(key=lambda h: h.start_time)
        merged = [highlights[0]]
        for h in highlights[1:]:
            prev = merged[-1]
            if h.start_time <= prev.end_time:
                prev.end_time = max(prev.end_time, h.end_time)
                prev.score = max(prev.score, h.score)
                prev.signals.extend(h.signals)
                if h.score > prev.score:
                    prev.category = h.category
            else:
                merged.append(h)
        return merged

    def _determine_category(self, signals: list[dict]) -> str:
        """根据信号类型确定高光类别"""
        if not signals:
            return "unknown"
        type_counts = {}
        for s in signals:
            t = s.get("type", "")
            type_counts[t] = type_counts.get(t, 0) + s.get("strength", 0)
        dominant = max(type_counts, key=type_counts.get)
        return {
            "danmaku_peak": "engagement_spike",
            "keyword_match": "keyword_trigger",
            "audio_peak": "audio_peak",
            "scene_change": "scene_transition",
            "silence_boundary": "scene_transition",
            "gift_spike": "gift_spike",
        }.get(dominant, "unknown")

    def _auto_title(self, highlight: Highlight, index: int) -> str:
        """自动生成标题"""
        category_names = {
            "engagement_spike": "弹幕爆发",
            "keyword_trigger": "关键词触发",
            "audio_peak": "音频高潮",
            "scene_transition": "场景切换",
            "gift_spike": "礼物爆发",
        }
        cat = category_names.get(highlight.category, "高光")
        minutes = int(highlight.start_time // 60)
        seconds = int(highlight.start_time % 60)
        return f"高光 #{index} — {cat} {minutes:02d}:{seconds:02d}"
