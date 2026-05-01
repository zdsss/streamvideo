# The Orchestrator — StreamVideo

多平台直播监控录制系统。粘贴直播 URL，自动监控、录制、智能合并。

## 支持平台

| 平台 | 录制引擎 | 状态检测 | 断流阈值 | 备注 |
|------|---------|---------|---------|------|
| 抖音直播 | streamlink → ffmpeg | webcast API | 30s | ⚠️ 实验性，受平台反爬限制 |
| B站直播 | streamlink | B站 API | 20s | |
| TikTok | streamlink → yt-dlp | streamlink | 20s | 支持分享链接解析 |
| AfreecaTV / Soop | streamlink → yt-dlp | streamlink | 20s | 含 Soop 国际版 |
| Twitch | streamlink | streamlink | 20s | |
| YouTube | streamlink | streamlink | 20s | |
| 虎牙 | streamlink | streamlink | 20s | |
| 斗鱼 | streamlink | streamlink | 20s | |
| Kick | streamlink | streamlink | 20s | |
| 通用 | streamlink → yt-dlp | streamlink | 20s | 任意 streamlink 支持的平台 |

## 功能

### 核心录制

- **多平台录制** — 粘贴 URL 自动识别平台（抖音 / B站 / TikTok / AfreecaTV / Twitch / YouTube / 虎牙 / 斗鱼 / Kick），支持 per-model 画质选择（best / 1080p / 720p / 480p / audio_only）
- **重复任务检测** — 添加主播时自动检测重复 URL，返回冲突提示
- **代理自动检测** — 从 `SV_PROXY` / `HTTPS_PROXY` / `HTTP_PROXY` 环境变量自动识别，无需手动配置
- **GPU 编码加速** — 自动检测可用硬件编码器（NVENC / VideoToolbox / VA-API / QuickSync），H.265 转码支持 GPU 加速，失败自动回退软件编码
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
- **后处理流水线**：时间戳修复 → 可选智能重命名 → 可选 H.265 转码（GPU 加速） → 可选保留原始文件 → 可选云端上传

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
- **Streams 页面** — 紧凑横向卡片 + 列表视图切换，实时码率、合并状态、重复任务检测
- **Network 页面** — WebSocket / 代理状态、实时带宽
- **System 页面** — 版本信息、录制引擎状态、日志查看器、配置导入/导出
- **Highlights / Clips 页面** — 高光列表、切片管理、一键分发

### 可靠性与安全

- **FFmpeg 重连** — 录制时自动重连（网络断开 / HTTP 错误），最多重试 10 次
- **Streamlink 重试** — `--retry-open 3 --ringbuffer-size 32M`，全平台引擎统一加固
- **登录限流** — 5 次/分钟/IP，防止暴力破解
- **会话密钥自动生成** — 首次启动自动生成并持久化到 `.session_key`，不再使用硬编码默认值
- **全局错误处理** — 前端 JS 错误 + Promise 异常自动捕获并提示
- **健康检查** — `GET /api/health` 轻量级端点，Docker HEALTHCHECK 已配置

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
| GET | `/api/health` | 健康检查（Docker / 编排器） |
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
server.py                   # FastAPI 服务器入口（742 行，已模块化）
streamvideo/                # 主包（分层架构）
├── api/                    # API 层
│   ├── routes/            # 路由模块（9 个，111 个端点）
│   │   ├── auth.py        # 认证 (5 端点)
│   │   ├── streams.py     # 录制流 (15 端点)
│   │   ├── storage.py     # 存储 (26 端点)
│   │   ├── highlights.py  # 高光 (21 端点)
│   │   ├── clips.py       # 片段 (20 端点)
│   │   ├── system.py      # 系统 (5 端点)
│   │   ├── distribute.py  # 分发 (11 端点)
│   │   ├── payment.py     # 支付 (5 端点)
│   │   └── tasks.py       # 任务 (3 端点)
│   ├── middleware/        # 中间件
│   └── schemas/           # Pydantic 模型
├── core/                   # 核心业务层
│   ├── auth/              # 认证模块
│   │   ├── manager.py     # AuthManager
│   │   ├── quota.py       # QuotaManager
│   │   └── payment.py     # PaymentManager
│   ├── recorder/          # 录制引擎（13 个模块）
│   │   ├── models.py      # 数据类和枚举
│   │   ├── base.py        # BaseLiveRecorder
│   │   ├── manager.py     # RecorderManager
│   │   ├── uploader.py    # CloudUploader
│   │   ├── notifier.py    # WebhookNotifier
│   │   └── engines/       # 平台引擎（10 个）
│   ├── processor/         # 处理器
│   │   ├── highlight.py   # 高光检测
│   │   ├── danmaku.py     # 弹幕抓取
│   │   ├── clipgen.py     # 片段生成
│   │   ├── subtitle_gen.py
│   │   ├── subtitle_translator.py
│   │   └── cover_gen.py
│   └── distributor/       # 分发管理
│       └── manager.py
├── infrastructure/         # 基础设施层
│   ├── database/          # 数据库（Mixin 模式，8 个模块）
│   │   ├── connection.py  # 连接管理
│   │   ├── database.py    # 主 Database 类
│   │   └── repositories/  # 功能域 Mixin（6 个）
│   ├── messaging/         # 消息队列
│   │   └── task_queue.py
│   ├── cache/             # 缓存
│   └── storage/           # 存储
├── shared/                 # 共享层
│   ├── config.py          # 配置管理
│   ├── constants.py       # 常量定义
│   ├── errors.py          # 异常类
│   ├── logger.py          # 日志工具
│   └── utils/             # 工具函数
└── tests/                  # 测试
    ├── unit/
    ├── integration/
    └── e2e/

static/index.html           # Web UI（Alpine.js + Tailwind CSS）
config.json                 # 配置文件
recordings/                 # 录制文件输出目录

# 向后兼容（已添加 DeprecationWarning）
recorder.py, database.py, auth.py, quota.py, payment.py, 
distribute.py, highlight.py, danmaku.py, clipgen.py, 
subtitle_gen.py, subtitle_translator.py, cover_gen.py, task_queue.py
```

**架构说明**：
- **分层架构**：API 层 → 核心业务层 → 基础设施层 → 共享层
- **模块化**：73 个 Python 文件，11,755 行代码
- **向后兼容**：根目录旧文件保留并添加 DeprecationWarning
- **详细文档**：参见 `doc/refactor-complete-summary.md`

## 已知限制

- **抖音录制** ⚠️ 实验性：受平台反爬限制，可能不稳定
- **分发功能**：抖音/快手需要开发者资质，B站/微信视频号为辅助投稿模式
- **字幕生成**：需要本地 Whisper 模型，无网络 fallback
- **字幕翻译**：需配置 `ANTHROPIC_API_KEY`，使用 Claude Haiku，支持中/英/日/韩/法/德/西班牙语互译
