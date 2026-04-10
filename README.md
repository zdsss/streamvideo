# The Orchestrator — StreamVideo

多平台直播监控录制系统。粘贴直播 URL，自动监控、录制、智能合并。

## 支持平台

| 平台 | 录制引擎 | 状态检测 | 断流阈值 | 备注 |
|------|---------|---------|---------|------|
| 抖音直播 | streamlink | streamlink | 30s | 检测用原生 webcast API，streamlink 插件已失效 |
| B站直播 | streamlink | B站 API | 20s | |
| Twitch | streamlink | streamlink | 20s | |
| YouTube | streamlink | streamlink | 20s | |
| Stripchat | yt-dlp / Playwright | Stripchat API | 15s | |

任何 streamlink 支持的平台均可通过粘贴 URL 直接添加。

## 功能

### 核心录制

- **多平台录制** — 粘贴 URL 自动识别平台，支持 per-model 画质选择（best / 1080p / 720p / 480p / audio_only）
- **会话追踪（RecordingSession）** — 每次直播自动创建会话，持久化到 sessions.json + SQLite，断流重连时片段归属同一会话（30 秒内快速重连自动复用会话）
- **智能合并** — 会话结束后自动合并所有片段为一个 MP4，合并前 ffprobe 校验编码一致性，合并失败 30 秒自动重试
- **断流重连** — 宽限期内自动重连（抖音 90s / 其他 60s），不中断会话
- **启动恢复** — 服务重启后自动扫描 sessions.json，恢复未完成的会话并触发合并
- **批量添加** — 输入框支持多行 URL（逗号或换行分隔），一次添加多个主播
- **Per-model 自动合并开关** — 每个主播可独立开关自动合并
- **定时录制** — 设置每日录制时间窗口 + 星期选择，支持跨午夜，per-model 独立配置

### 合并系统

- **自动合并** — 直播结束 → 会话标记 ended → 自动触发合并 → 删除原始片段
- **合并进度** — WebSocket 实时推送合并进度百分比
- **手动合并** — 按时间间隔分组，支持选择性排除片段
- **后处理流水线**：
  - 时间戳修复（ffmpeg +genpts）
  - 可选智能重命名为 `主播名_日期_时长.mp4`
  - 可选 H.265 转码（CRF 28）
  - 可选云端上传（S3 / OSS / rclone）
- **磁盘空间检查** — 后处理前检查剩余空间，不足则跳过

### 通知系统

- **浏览器桌面通知** — 录制开始 / 结束 / 合并完成
- **Webhook** — 支持 Generic Webhook / Discord / Telegram Bot
- **事件类型** — `recording_start` / `recording_end` / `merge_done` / `error`
- **测试发送** — 设置面板中一键测试连通性

### 录制韧性

- **可配置断流检测** — 各平台独立阈值，避免误判
- **指数退避重试** — 网络错误自动退避重试
- **实时码率监控** — 每 5 秒计算瞬时码率，前端实时显示
- **编码一致性检查** — 合并前 ffprobe 校验所有片段，不一致则拒绝（保守策略）

### Web UI — The Orchestrator

- **Tailwind CSS**（本地构建，28KB）+ 内联 SVG 图标（零外部字体依赖）
- **窄图标侧边栏**（56px）— Streams / Storage / Network / System 四个页面
- **紧凑横向卡片** — 左侧 4:3 缩略图，右侧信息密集排列
- **三 Tab 文件弹窗** — 文件列表（表格）/ 录制会话 / 手动合并
- **Storage 页面** — 磁盘概览、按主播存储占用明细（进度条）、清理建议
- **Network 页面** — WebSocket / 代理状态、实时带宽、平台连接状态
- **System 页面** — 版本信息、录制引擎状态、日志查看器
- **Settings 弹窗** — 全部开关、Webhook 配置、H.265 转码开关
- **Schedule 弹窗** — 画质选择 + 时间窗口 + 星期选择器
- **文件管理** — 搜索、排序、批量删除、重命名、下载、CSV 导出
- **Toast 通知** — 操作反馈
- **移动端适配** — 底部 Tab 栏
- **PWA 支持** — manifest.json + Service Worker，支持离线缓存

## 技术栈

- **后端**: Python 3 / FastAPI / asyncio / WebSocket
- **前端**: Alpine.js / Tailwind CSS（本地构建）/ 内联 SVG 图标
- **存储**: SQLite（主存储）+ JSON（备份）+ sessions.json（会话持久化）
- **数据库**: database.py，支持从 JSON 自动迁移至 SQLite
- **录制引擎**: streamlink / yt-dlp / Playwright / ffmpeg

## 快速开始

### 依赖安装

```bash
# Python 依赖
pip install fastapi uvicorn aiohttp playwright
playwright install chromium

# 录制工具
brew install ffmpeg streamlink yt-dlp
```

### 启动

```bash
python server.py
```

浏览器打开 `http://localhost:8080`，粘贴直播 URL 即可开始。

### Token 认证

```bash
SV_TOKEN=secret python server.py
```

访问时附加 `?token=secret` 参数即可通过认证。

### 代理配置

默认使用 `http://127.0.0.1:7890` 代理（用于 Stripchat 等需要代理的平台）。抖音、B站等国内平台自动直连。

### CSS 重新构建

```bash
npx tailwindcss@3 -i static/input.css -o static/styles.css --minify
```

## API

| 方法 | 端点 | 说明 |
|------|------|------|
| GET | `/api/models` | 获取所有主播状态 |
| POST | `/api/models` | 添加主播 `{"url": "..."}` |
| DELETE | `/api/models/{name}` | 删除主播 |
| POST | `/api/models/{name}/start` | 启动监控 |
| POST | `/api/models/{name}/stop` | 停止监控 |
| POST | `/api/models/{name}/auto-merge` | 切换自动合并 `{"auto_merge": bool}` |
| POST | `/api/models/{name}/schedule` | 设置定时计划 |
| GET | `/api/models/{name}/schedule` | 获取定时计划 |
| POST | `/api/models/{name}/quality` | 设置画质 |
| GET | `/api/recordings/{name}` | 获取录制文件列表 |
| GET | `/api/recordings/{name}/groups` | 获取可合并的分组 |
| GET | `/api/recordings/{name}/export` | CSV 导出录制列表 |
| POST | `/api/recordings/{name}/merge` | 合并片段 |
| POST | `/api/recordings/{name}/{file}/rename` | 重命名录制文件 |
| DELETE | `/api/recordings/{name}/{file}` | 删除录制文件 |
| GET | `/api/video/{name}/{file}` | 播放 / 下载视频（`?download=1`） |
| GET | `/api/sessions/{name}` | 获取录制会话列表 |
| POST | `/api/sessions/{name}/{id}/merge` | 合并指定会话 |
| GET | `/api/settings` | 获取设置 |
| POST | `/api/settings` | 更新设置 |
| GET | `/api/stats` | 全局录制统计 |
| GET | `/api/stats/{name}` | 单主播统计 |
| GET | `/api/disk` | 磁盘使用情况 |
| GET | `/api/storage/breakdown` | 按主播存储占用明细 |
| GET | `/api/network` | 网络状态（代理、平台、带宽） |
| GET | `/api/system` | 系统信息（版本、引擎状态） |
| GET | `/api/logs` | 系统日志（`?limit=N`） |
| GET | `/api/thumb/{name}` | 主播缩略图 |
| POST | `/api/webhooks/test` | 测试 Webhook 连通性 |
| WS | `/ws` | WebSocket 实时推送 |

### WebSocket 消息类型

| 类型 | 说明 |
|------|------|
| `init` | 连接时推送所有主播状态 |
| `model_update` | 主播状态变化 |
| `model_added` / `model_removed` | 主播增删 |
| `merge_done` | 合并完成（含文件名、大小、片段数） |
| `merge_error` | 合并失败 |
| `merge_progress` | 合并进度（progress 0~1） |
| `settings_update` | 设置变更 |

## 配置项

`config.json` 示例：

```json
{
  "auto_merge": true,
  "merge_gap_minutes": 15,
  "auto_delete_originals": true,
  "min_segment_size_kb": 500,
  "smart_rename": false,
  "h265_transcode": false,
  "cloud_upload": false,
  "webhooks": [
    {
      "type": "discord",
      "url": "https://discord.com/api/webhooks/...",
      "events": ["recording_start", "recording_end", "merge_done", "error"]
    }
  ]
}
```

| 配置项 | 说明 |
|--------|------|
| `auto_merge` | 录制结束后自动合并同一会话的片段 |
| `merge_gap_minutes` | 手动合并时，片段间隔超过此值视为不同分组 |
| `auto_delete_originals` | 合并后自动删除原始片段 |
| `min_segment_size_kb` | 小于此大小的片段自动清理 |
| `smart_rename` | 合并后重命名为 `主播名_日期_时长.mp4` |
| `h265_transcode` | 合并后转码为 H.265（CRF 28） |
| `cloud_upload` | 合并后上传至云端（S3 / OSS / rclone） |
| `webhooks` | Webhook 通知配置列表 |

## 会话生命周期

每次直播自动创建一个 `RecordingSession`，持久化到 sessions.json + SQLite：

```
直播开始 → active（录制中，片段持续追加）
  ↓ 断流
重连中（宽限期内恢复 → 继续 active）
  ↓ 30秒内再次上线 → 复用同一会话
  ↓ 宽限期超时
ended → 触发自动合并 → merging → merged
                         ↓ 失败
                       error → 30秒后自动重试
                                ↓ 仍失败
                              error（可手动重试）
```

## 项目结构

```
server.py          # FastAPI 服务器 + REST API + WebSocket
recorder.py        # 多平台录制引擎 + 会话追踪 + Webhook + 定时录制
database.py        # SQLite 数据库模块，支持从 JSON 自动迁移
static/index.html  # Web UI（Alpine.js + Tailwind CSS 单页应用）
static/styles.css  # Tailwind CSS 本地构建产物
static/manifest.json  # PWA manifest
static/sw.js       # Service Worker（离线缓存）
config.json        # 配置文件（主播列表 + 设置 + Webhook + 定时计划）
streamvideo.db     # SQLite 数据库（自动从 JSON 迁移）
recordings/        # 录制文件输出目录
  {主播名}/
    YYYYMMDD_HHMMSS.mp4        # 录制片段
    YYYYMMDD_HHMMSS_merged.mp4 # 合并后的文件
    meta.json                  # 主播元数据缓存
    sessions.json              # 录制会话记录
  thumbs/                      # 缩略图缓存
```

## 已知限制

### 抖音直播录制

- **检测**：使用抖音原生 webcast API（`room/web/enter`），需要 `ttwid` cookie，自动获取并缓存 1 小时
- **录制**：streamlink 的抖音插件已失效（抖音页面改为纯客户端渲染），系统会自动尝试三级 fallback：
  1. streamlink（传入 ttwid cookie）
  2. Playwright 提取流地址 + ffmpeg 直录
  3. 60 秒冷却后重试
- **反爬**：抖音 API 需要 `__ac_signature` 签名才能返回流地址，Playwright headless 模式被屏蔽
- **后续方向**：集成第三方抖音录制库、支持用户导出浏览器 cookie

## 快捷键

- `Ctrl/Cmd + N` — 聚焦添加输入框
- `Escape` — 关闭弹窗
- 拖拽 URL 到页面 — 自动添加
