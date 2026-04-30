# StreamVideo 实时预览 + 字幕生成 + 字幕翻译 技术方案

> 文档日期：2026-04-29  
> 状态：待评审

---

## 一、背景与目标

**痛点：** 录制直播流时，用户无法快速查看当前录制内容；字幕生成依赖本地 Whisper，无翻译能力。

**目标：**
1. 录制完成后（或录制中）可快速预览视频内容，支持 seek
2. 对录制文件生成字幕（ASR）
3. 对已有字幕翻译成目标语言（LLM）

**参考产品：** 夸克视频播放器 — 支持本地视频字幕生成 + 字幕翻译，核心体验是"一键生成 + 一键翻译"，延迟低、准确度高。

---

## 二、现状分析

| 能力 | 现状 |
|------|------|
| 视频服务 | `FileResponse` 静态返回，无 HTTP Range 支持，无法 seek |
| 字幕生成 | openai-whisper small，同步阻塞，无翻译 |
| 录制中预览 | 不支持（ffmpeg 独占写入 `.raw.mp4`） |
| LLM 集成 | 无 |
| 配额系统 | `quota.py` 已有 free/pro/team 三档，可扩展 |

**核心约束：**
- 录制中文件被 ffmpeg 独占写入，不能直接 seek
- 录制完成后的文件已经过 remux（`-movflags +faststart`），moov 在头部，可直接支持 Range

---

## 三、方案选型

### 方案 A — MVP（推荐首期实施）

**工作量：3-5 天 | 成本：~$0.05/小时视频**

**实时预览：** HTTP Range 请求（仅支持录制完成后的文件）  
**字幕生成：** 现有 openai-whisper（保持不变）  
**字幕翻译：** Claude Haiku API，批量翻译（每批 50 段）

```
录制完成的 MP4（已有 +faststart）
        ↓
[HTTP Range 服务] ← 改造 /api/video 端点，支持 206 响应
        ↓
浏览器原生 <video> 标签（支持 seek）

字幕翻译：SRT → 批量发送 Claude Haiku → 写回新 SRT
```

---

### 方案 B — 生产方案

**工作量：3-4 周 | 成本：~$0.05/小时视频**

**实时预览：** HLS 切片（支持录制中预览，延迟 10-30s）  
**字幕生成：** faster-whisper（比 openai-whisper 快 4x）  
**字幕翻译：** Claude Haiku + 翻译缓存（SQLite）

```
录制中（ffmpeg 写入 .raw.mp4）
        ↓
[HLS 切片进程] ← 独立 ffmpeg，每 10s 生成一个 .ts 片段
        ↓
[m3u8 动态播放列表] ← 实时更新
        ↓
浏览器 hls.js 播放器（实时预览，延迟 ~20s）

录制完成后：HTTP Range 服务（完整 seek）
```

**并发读取可行性：** macOS/Linux 允许多进程同时读写同一文件。HLS 切片进程只读取已写入的字节，不影响录制进程。`.ts` 片段自包含，完全绕过 moov atom 问题。

---

### 方案 C — 高端方案

**工作量：8-12 周 | 成本：$0.05-$0.95/小时视频**

**实时预览：** LLHLS（低延迟 HLS，延迟 2-5s）  
**字幕生成：** faster-whisper large-v3 + 实时流式 ASR（每 30s 一批）  
**字幕翻译：** Claude Sonnet（上下文感知翻译）+ WebSocket 实时推送字幕

---

## 四、推荐实施路径

**第一期（本次）：方案 A**，快速解决核心痛点，验证字幕翻译体验。  
**第二期：** 在方案 A 基础上叠加方案 B 的 HLS 预览和 faster-whisper。  
**第三期：** 按需评估方案 C 的实时字幕流。

---

## 五、技术设计（方案 A 详细）

### 5.1 HTTP Range 支持

**改造文件：** `server.py`，`GET /api/video/{username}/{filename}`

```python
@app.get("/api/video/{username}/{filename}")
async def get_video(request: Request, username: str, filename: str, download: int = 0):
    video_path = Path(RECORDINGS_DIR) / username / filename
    if not video_path.exists():
        raise HTTPException(404)

    file_size = video_path.stat().st_size
    range_header = request.headers.get("Range")

    if download:
        return FileResponse(str(video_path), media_type="video/mp4",
                            headers={"Content-Disposition": f'attachment; filename="{filename}"'})

    if range_header:
        # 解析 Range: bytes=start-end
        match = re.match(r"bytes=(\d+)-(\d*)", range_header)
        start = int(match.group(1))
        end = int(match.group(2)) if match.group(2) else file_size - 1
        end = min(end, file_size - 1)
        chunk_size = end - start + 1

        async def iter_file():
            with open(video_path, "rb") as f:
                f.seek(start)
                remaining = chunk_size
                while remaining > 0:
                    data = f.read(min(65536, remaining))
                    if not data:
                        break
                    remaining -= len(data)
                    yield data

        return StreamingResponse(
            iter_file(),
            status_code=206,
            media_type="video/mp4",
            headers={
                "Content-Range": f"bytes {start}-{end}/{file_size}",
                "Accept-Ranges": "bytes",
                "Content-Length": str(chunk_size),
            },
        )

    return FileResponse(str(video_path), media_type="video/mp4",
                        headers={"Accept-Ranges": "bytes"})
```

### 5.2 字幕翻译 API

**新增端点：** `POST /api/recordings/{username}/{filename}/subtitle/translate`

```python
@app.post("/api/recordings/{username}/{filename}/subtitle/translate")
async def translate_subtitle(username: str, filename: str, req: dict = Body({})):
    source_file = req.get("source_file")   # 如 "video.srt"
    target_lang = req.get("target_lang", "en")
    source_lang = req.get("source_lang", "zh")
    model = req.get("model", "claude-haiku-4-5-20251001")

    # 读取 SRT → 翻译 → 写回新文件
    srt_path = Path(RECORDINGS_DIR) / username / source_file
    translator = SubtitleTranslator(api_key=LLM_API_KEY, model=model)
    output_path = await translator.translate_srt(srt_path, target_lang, source_lang)
    return {"ok": True, "translated_file": output_path.name}
```

**翻译核心逻辑（新增 `subtitle_translator.py`）：**

```python
class SubtitleTranslator:
    def __init__(self, api_key: str, model: str = "claude-haiku-4-5-20251001"):
        import anthropic
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = model

    async def translate_srt(self, srt_path: Path, target_lang: str, source_lang: str = "zh") -> Path:
        segments = self._parse_srt(srt_path)
        translated = await self._translate_batched(segments, target_lang, source_lang)
        output_path = srt_path.with_suffix(f".{target_lang}.srt")
        self._write_srt(translated, output_path)
        return output_path

    async def _translate_batched(self, segments, target_lang, source_lang, batch_size=50):
        results = []
        for i in range(0, len(segments), batch_size):
            batch = segments[i:i+batch_size]
            texts = "\n".join(f"{j+1}. {s['text']}" for j, s in enumerate(batch))
            prompt = (f"将以下{source_lang}字幕翻译为{target_lang}，"
                      f"保持编号格式，每行一条，只输出翻译结果：\n{texts}")
            resp = await asyncio.to_thread(
                self.client.messages.create,
                model=self.model,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )
            lines = resp.content[0].text.strip().split("\n")
            translated_texts = [re.sub(r"^\d+\.\s*", "", l).strip() for l in lines if l.strip()]
            for seg, text in zip(batch, translated_texts):
                results.append({**seg, "text": text})
        return results

    def _parse_srt(self, path: Path) -> list[dict]:
        # 解析 SRT 格式：index → timestamp → text
        ...

    def _write_srt(self, segments: list[dict], path: Path):
        # 写回 SRT 格式
        ...
```

### 5.3 成本控制策略

| 策略 | 实现方式 |
|------|---------|
| 模型选择 | 默认 Claude Haiku（最便宜），Pro 用户可选 Sonnet |
| 批量翻译 | 每批 50 段，减少 API 调用次数 |
| 翻译缓存 | `translation_cache` 表，相同文本只翻译一次 |
| 配额限制 | free 用户每天 1 次翻译，pro 用户 20 次 |

**成本估算（1 小时直播，约 3000 字幕段）：**
- Claude Haiku：~$0.05（约 60,000 tokens）
- Claude Sonnet：~$0.50

### 5.4 LLM API Key 配置

在 `server.py` 环境变量中新增：

```python
LLM_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
LLM_PROXY = os.environ.get("SV_PROXY", "http://127.0.0.1:7890")
```

支持通过 System 页面的配置界面设置（存入 SQLite settings 表）。

---

## 六、数据库变更

```sql
-- 字幕文件表
CREATE TABLE IF NOT EXISTS subtitles (
    subtitle_id   TEXT PRIMARY KEY,
    username      TEXT NOT NULL,
    video_file    TEXT NOT NULL,
    file_path     TEXT NOT NULL,
    format        TEXT DEFAULT 'srt',
    language      TEXT DEFAULT 'zh',
    engine        TEXT DEFAULT 'whisper',
    model_size    TEXT DEFAULT 'small',
    segment_count INTEGER DEFAULT 0,
    status        TEXT DEFAULT 'done',
    error         TEXT DEFAULT '',
    created_at    REAL DEFAULT (strftime('%s','now'))
);

-- 翻译缓存表（成本控制核心）
CREATE TABLE IF NOT EXISTS translation_cache (
    text_hash   TEXT NOT NULL,
    source_lang TEXT NOT NULL,
    target_lang TEXT NOT NULL,
    translated  TEXT NOT NULL,
    model       TEXT DEFAULT 'claude-haiku-4-5-20251001',
    hit_count   INTEGER DEFAULT 0,
    created_at  REAL DEFAULT (strftime('%s','now')),
    PRIMARY KEY (text_hash, source_lang, target_lang)
);

-- 翻译任务表
CREATE TABLE IF NOT EXISTS subtitle_translations (
    translation_id TEXT PRIMARY KEY,
    subtitle_id    TEXT NOT NULL,
    username       TEXT NOT NULL,
    source_lang    TEXT NOT NULL,
    target_lang    TEXT NOT NULL,
    file_path      TEXT DEFAULT '',
    status         TEXT DEFAULT 'pending',
    cache_hits     INTEGER DEFAULT 0,
    api_calls      INTEGER DEFAULT 0,
    created_at     REAL DEFAULT (strftime('%s','now'))
);

-- 配额扩展
ALTER TABLE user_quotas ADD COLUMN subtitles_generated INTEGER DEFAULT 0;
ALTER TABLE user_quotas ADD COLUMN translations_generated INTEGER DEFAULT 0;
```

---

## 七、前端 UI 设计

### Storage 页面改动

**新增操作按钮**（每个录制文件行）：

```
[文件名] [大小] [时长] | [预览▶] [字幕] [翻译] [下载] [...]
```

**预览模态框：**

```html
<video controls>
  <source src="/api/video/{username}/{filename}" type="video/mp4">
  <!-- 字幕轨道（VTT 格式） -->
  <track kind="subtitles" src="/api/subtitle/{id}/vtt" srclang="zh" label="中文" default>
  <track kind="subtitles" src="/api/subtitle/{trans_id}/vtt" srclang="en" label="English">
</video>
```

**字幕操作面板：**

```
[生成字幕]  模型: [small ▼]  语言: [中文 ▼]
[翻译字幕]  目标语言: [英文 ▼]  质量: [标准(Haiku) ▼]
状态: ✓ 已生成中文字幕 (342 段) | ✓ 已翻译英文字幕
```

**交互流程：**
1. 点击"预览" → 打开模态框，视频直接可 seek 播放
2. 点击"生成字幕" → 异步任务，进度条显示，完成后字幕轨道自动加载
3. 点击"翻译字幕" → 选择目标语言，异步翻译，完成后可在播放器切换字幕轨道

---

## 八、API 端点汇总

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/video/{username}/{filename}` | 视频文件（支持 Range，方案 A 改造） |
| POST | `/api/recordings/{username}/{filename}/subtitle` | 生成字幕（现有，扩展） |
| GET | `/api/recordings/{username}/{filename}/subtitles` | 获取字幕列表（新增） |
| POST | `/api/recordings/{username}/{filename}/subtitle/translate` | 翻译字幕（新增） |
| GET | `/api/subtitle/{subtitle_id}/vtt` | 获取 VTT 格式字幕（新增，供 `<track>` 使用） |
| GET | `/api/preview/{username}/{filename}` | 预览信息（方案 B 新增） |

---

## 九、方案 B 补充：HLS 实时预览

> 本节为第二期实施内容，供参考。

### HLS 切片服务

```python
class HLSSegmenter:
    """在录制进行中，将 .raw.mp4 切片为 HLS 流"""

    async def start(self, raw_path: Path, hls_dir: Path):
        hls_dir.mkdir(parents=True, exist_ok=True)
        cmd = [
            "ffmpeg", "-y",
            "-i", str(raw_path),
            "-c", "copy",
            "-hls_time", "10",
            "-hls_list_size", "6",
            "-hls_flags", "delete_segments+append_list",
            str(hls_dir / "stream.m3u8"),
        ]
        self._proc = await asyncio.create_subprocess_exec(*cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL)

    async def stop(self):
        if self._proc and self._proc.returncode is None:
            self._proc.terminate()
            await self._proc.wait()
```

**触发时机：** 在 `recorder.py` 的录制开始回调中启动 `HLSSegmenter`，录制结束时停止。

**前端播放器：** 使用 hls.js（CDN 引入，约 30KB gzip）：

```html
<script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
<video id="live-player"></video>
<script>
  const hls = new Hls();
  hls.loadSource('/api/preview/{username}/stream.m3u8');
  hls.attachMedia(document.getElementById('live-player'));
</script>
```

---

## 十、风险与注意事项

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| LLM API 费用超支 | 高 | 配额限制 + 翻译缓存 + 默认 Haiku 模型 |
| Whisper 本地资源占用 | 中 | 异步任务队列，避免阻塞录制 |
| HLS 切片增加磁盘 I/O | 中 | 仅在用户主动请求预览时启动切片 |
| 翻译质量不稳定 | 低 | 批量翻译 + 上下文提示词优化 |
| API Key 泄露 | 高 | 存入 SQLite，不写入 config.json，不暴露给前端 |
