# The Orchestrator — StreamVideo

多平台直播监控录制系统。粘贴直播 URL，自动监控、录制、智能合并。

## 支持平台

| 平台 | 录制引擎 | 状态检测 | 断流阈值 | 备注 |
|------|---------|---------|---------|------|
| 抖音直播 | streamlink | webcast API | 30s | ⚠️ 实验性，受平台反爬限制 |
| B站直播 | streamlink | B站 API | 20s | |
| Twitch | streamlink | streamlink | 20s | |
| YouTube | streamlink | streamlink | 20s | |
| 虎牙 | streamlink | streamlink | 20s | |
| 斗鱼 | streamlink | streamlink | 20s | |
| Kick | streamlink | streamlink | 20s | |
| 通用 | streamlink → yt-dlp | streamlink | 20s | 任意 streamlink 支持的平台 |

## 功能

### 核心录制

- **多平台录制** — 粘贴 URL 自动识别平台，支持 per-model 画质选择（best / 1080p / 720p / 480p / audio_only）
- **会话追踪（RecordingSession）** — 每次直播自动创建会话，持久化到 sessions.json + SQLite，断流重连时片段归属同一会话
- **智能合并** — 会话结束后自动合并所有片段为一个 MP4，合并前 ffprobe 校验编码一致性，合并失败 30 秒自动重试
- **合并撤回** — 自动/手动合并后 72 小时内可撤回，恢复原始分片
- **断流重连** — 宽限期内自动重连（抖音 90s / 其他 60s），不中断会话
- **启动恢复** — 服务重启后自动扫描 sessions.json，恢复未完成的会话并触发合并
- **批量添加** — 输入框支持多行 URL（逗号或换行分隔），一次添加多个主播
- **定时录制** — 设置每日录制时间窗口 + 星期选择，支持跨午夜，per-model 独立配置

### 合并系统

- **自动合并** — 直播结束 → 会话标记 ended → 自动触发合并 → 删除原始片段
- **合并进度** — WebSocket 实时推送合并进度百分比
- **手动合并** — 按时间间隔分组，支持选择性排除片段
- **合并撤回** — 72h 内可一键撤回，删除合并文件并恢复原始分片
- **后处理流水线**：时间戳修复 → 可选智能重命名 → 可选 H.265 转码 → 可选云端上传

### 弹幕采集

- **抖音** — WebSocket 实时弹幕捕获（自动随录制启停）
- **B站** — WebSocket 协议弹幕抓取（含礼物、SC）
- **Twitch** — IRC 匿名模式弹幕抓取（无需 OAuth）
- 弹幕数据保存为 JSON，可用于高光检测和切片叠加

### 内容资产化（FlashCut）

- **高光检测** — 关键词匹配 + 弹幕密度分析，自动定位精彩时刻
- **短视频切片** — 竖屏/横屏/方形格式，支持弹幕叠加、水印
- **字幕生成** — 基于 Whisper 的语音转文字
- **封面生成** — 自动提取精彩帧作为封面

### Web UI — The Orchestrator

- **Tailwind CSS**（本地构建）+ Alpine.js 单页应用
- **Storage 页面** — 会话分组时间线视图、封面缩略图、合并进度条、撤回按钮
- **Streams 页面** — 紧凑横向卡片，实时码率、合并状态
- **Network 页面** — WebSocket / 代理状态、实时带宽
- **System 页面** — 版本信息、录制引擎状态、日志查看器、配置导入/导出
- **Highlights / Clips 页面** — 高光列表、切片管理、一键分发

## 技术栈

- **后端**: Python 3 / FastAPI / asyncio / WebSocket
- **前端**: Alpine.js / Tailwind CSS（本地构建）
- **存储**: SQLite（主存储）+ JSON（备份）
- **录制引擎**: streamlink / yt-dlp / Playwright（抖音）/ ffmpeg

## 快速开始

```bash
pip install -r requirements.txt
playwright install chromium
brew install ffmpeg streamlink yt-dlp

python server.py
```

浏览器打开 `http://localhost:8080`，粘贴直播 URL 即可开始。

### Docker

```bash
docker build -t streamvideo .
docker run -d -p 8080:8080 \
  -v $(pwd)/recordings:/app/recordings \
  -v $(pwd)/config.json:/app/config.json \
  streamvideo
```

### Token 认证

```bash
SV_TOKEN=secret python server.py
# 访问时附加 ?token=secret
```

### CSS 重新构建

```bash
npx tailwindcss@3 -i static/input.css -o static/styles.css --minify
```

### 字幕翻译

```bash
ANTHROPIC_API_KEY=sk-... python server.py
```

字幕翻译通过 `subtitle_translator.py` 调用 Claude Haiku，支持 SRT 文件批量翻译，结果自动缓存到数据库。

## API

| 方法 | 端点 | 说明 |
|------|------|------|
| GET | `/api/models` | 获取所有主播状态 |
| POST | `/api/models` | 添加主播 `{"url": "..."}` |
| DELETE | `/api/models/{name}` | 删除主播 |
| POST | `/api/models/{name}/start` | 启动监控 |
| POST | `/api/models/{name}/stop` | 停止监控 |
| GET | `/api/recordings/{name}` | 获取录制文件列表 |
| POST | `/api/recordings/{name}/merge` | 合并片段 |
| GET | `/api/sessions/{name}` | 获取录制会话列表 |
| GET | `/api/sessions/{name}/summary` | 会话摘要（Storage 视图用） |
| POST | `/api/sessions/{name}/{id}/merge` | 合并指定会话 |
| POST | `/api/sessions/{id}/rollback` | 撤回合并（72h 内有效） |
| GET | `/api/settings` | 获取设置 |
| POST | `/api/settings` | 更新设置 |
| GET | `/api/config/export` | 导出配置 |
| POST | `/api/config/import` | 导入配置（mode: merge\|overwrite） |
| GET | `/api/disk` | 磁盘使用情况 |
| GET | `/api/storage/breakdown` | 按主播存储占用明细 |
| WS | `/ws` | WebSocket 实时推送 |

### WebSocket 消息类型

| 类型 | 说明 |
|------|------|
| `model_update` | 主播状态变化 |
| `merge_done` | 合并完成 |
| `merge_progress` | 合并进度（0~1） |
| `auto_merge_done` | 自动合并完成（含 merge_type、72h 撤回提示） |
| `merge_low_confidence` | 自动合并跳过（信心度过低） |
| `session_rollback` | 合并撤回完成 |
| `highlight_detected` | 高光检测完成 |
| `clip_done` | 切片生成完成 |

## 会话生命周期

```
直播开始 → active（录制中）
  ↓ 断流 → 宽限期内重连 → 继续 active
  ↓ 宽限期超时
ended → 自动合并 → merging → merged（72h 内可撤回）
                    ↓ 失败
                  error → 30s 后自动重试（最多 3 次）
```

## 项目结构

```
server.py               # FastAPI 服务器 + REST API + WebSocket
recorder.py             # 多平台录制引擎 + 会话追踪 + 弹幕启停
database.py             # SQLite 数据库模块
danmaku.py              # 弹幕采集（抖音 WS / B站 WS / Twitch IRC）
highlight.py            # 高光检测引擎
clipgen.py              # 短视频切片生成
subtitle_gen.py         # Whisper 语音转字幕
subtitle_translator.py  # Claude Haiku 字幕翻译（支持 7 种语言，带缓存）
cover_gen.py            # 封面生成
quota.py                # 配额管理
static/index.html       # Web UI（Alpine.js + Tailwind CSS）
config.json             # 配置文件
recordings/             # 录制文件输出目录
```

## 已知限制

- **抖音录制** ⚠️ 实验性：受平台反爬限制，可能不稳定
- **分发功能**：抖音/快手需要开发者资质，B站/微信视频号为辅助投稿模式
- **字幕生成**：需要本地 Whisper 模型，无网络 fallback
- **字幕翻译**：需配置 `ANTHROPIC_API_KEY`，使用 Claude Haiku，支持中/英/日/韩/法/德/西班牙语互译
