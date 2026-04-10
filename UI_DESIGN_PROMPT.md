# StreamVideo UI 设计 Prompt

> 将此 prompt 粘贴到 Figma AI / v0.dev / Bolt / 或任何 UI 设计工具中使用。

---

## 产品概述

StreamVideo 是一个多平台直播流录制管理系统（Web App），用于自动监控、录制、管理直播流视频。用户添加直播间 URL 后，系统自动监控主播在线状态，开播时自动录制，断流时自动重连，录制结束后智能合并片段为完整视频。

目标用户：需要录制保存直播内容的个人用户，运行在本地或私有服务器上。

设计风格参考：类似 Downie（macOS 下载管理器）的简洁专业感，结合 OBS Studio 的功能密度。

---

## 技术栈约束

- 单页应用（SPA），纯 HTML + CSS + Alpine.js，无构建步骤
- 后端 Python FastAPI，通过 REST API + WebSocket 实时通信
- 需要同时支持深色/浅色主题
- 响应式设计，支持桌面和移动端

---

## 页面结构

### 1. 顶部导航栏（Sticky Header）

**左侧：**
- 产品名 "StreamVideo"
- WebSocket 连接状态指示灯（绿色=已连接，红色=断开）

**右侧：**
- 实时统计：`{N} 录制中` (红色脉冲动画) · `{N}/{Total} 在线` · `{Size} 已用`
- 操作按钮：主题切换 · 设置(⚙) · 全部启动 · 全部停止

### 2. 添加栏（Toolbar）

- 全宽输入框，placeholder: "粘贴直播 URL 或输入用户名（支持抖音、B站、Twitch、YouTube、Stripchat）..."
- 支持拖拽 URL 到页面添加
- 添加按钮（带 loading spinner）
- 快捷键 Cmd/Ctrl+N 聚焦输入框

### 3. 主播卡片网格（Main Content）

自适应网格布局，每张卡片最小宽度 320px。

**每张卡片包含：**

**卡片头部：**
- 状态指示灯（颜色编码：绿色=在线，红色脉冲=录制中，橙色脉冲=重连中，灰色=离线）
- 平台标签（抖音=红色，B站=蓝色，Twitch=紫色，YouTube=红色，Stripchat=紫色）
- 主播名（可点击跳转直播间）
- 状态徽章（录制中/监控中/重连中/未启动/错误）

**卡片主体 - 缩略图区域（16:9）：**
- 录制中：显示实时视频截图，左上角 "REC" 红色脉冲徽章，右上角观众数
- 底部渐变遮罩显示：录制时长 + 文件大小
- 离线时：显示主播名的彩色占位符（基于名字 hash 生成 HSL 颜色）
- 点击缩略图打开录制文件管理弹窗

**卡片主体 - 信息网格（2列）：**
- 状态 / 录制文件数
- 录制中时额外显示：时长（红色）/ 大小 / 实时码率(kbps)
- 非录制时显示：最后检查时间 / 观众数
- 错误信息（红色背景条）

**卡片底部：**
- 左侧：Model ID
- ⚡ 自动合并开关图标（亮=开，暗=关，点击切换）
- "文件" 按钮 → 打开录制文件管理
- 启动/停止按钮
- ✕ 删除按钮（红色）

**卡片排序规则：** 录制中 > 重连中 > 监控中 > 错误 > 未启动

**卡片视觉状态：**
- 录制中：红色边框 + 微弱红色阴影
- 在线未录制：绿色边框
- 其他：默认边框，hover 时高亮

### 4. 空状态

居中显示 📡 图标 + "暂无监控主播" + 支持平台列表

---

## 弹窗系统

### 4.1 录制文件管理弹窗（800px 宽）

**头部：** 主播名 + 关闭按钮

**Tab 栏（3个标签页）：**
- 文件列表 | 录制会话 | 手动合并
- Tab 栏右侧显示统计摘要：`{N} 次会话 · {Size} · {Duration}`

**内嵌视频播放器：** 点击任何文件名时，顶部展开 HTML5 video 播放器

---

#### Tab 1: 文件列表

**工具栏：** 搜索框 · 排序(时间/大小/文件名) · 批量模式 · 批量删除

**文件列表项：**
- 复选框（批量模式）
- 文件名（点击播放），已合并文件显示绿色 "已合并" 小标签
- 文件大小 · 日期 · ↓下载按钮 · ✕删除按钮

**底部：** 文件总数 · 总大小

---

#### Tab 2: 录制会话

每个会话是一张可折叠的卡片：

**会话卡片头部：**
- "会话 N" 标题
- 状态徽章（颜色编码）：
  - 🔴 录制中（红色，带脉冲动画）
  - 🟡 待合并（橙色）
  - 🟣 合并中（紫色，带 spinner + 百分比）
  - 🟢 已合并（绿色，显示合并后文件名，可点击播放）
  - 🔴 错误（红色，显示错误信息 + 重试按钮）
- 片段数 · 时间范围 · 时长

**合并进度条：** 合并中时显示在卡片头部下方，3px 高度的紫色进度条

**会话卡片内容：** 片段文件列表（文件名 + 大小，点击可播放）

---

#### Tab 3: 手动合并

**工具栏：** 间隔阈值下拉(5/10/15/30/60分钟) · "合并后删除原文件" 复选框 · "全部合并" 按钮

**分组卡片：** 类似会话卡片，但基于时间间隔自动分组
- 每个片段前有复选框（可排除）
- 合并按钮 / spinner+百分比 / "已合并" 状态
- 合并进度条

---

### 4.2 设置弹窗（480px 宽）

设置项列表，每项为 label + 描述 + 控件：

| 设置项 | 控件类型 | 描述 |
|--------|----------|------|
| 自动合并 | Toggle 开关 | 录制结束后自动合并同一会话的片段 |
| 合并后删除原文件 | Toggle 开关 | 自动合并完成后删除原始片段 |
| 合并间隔阈值 | 下拉选择 | 5/10/15/30/60 分钟 |
| 最小片段大小 | 下拉选择 | 100KB/500KB/1MB/5MB，小于此大小自动清理 |
| 智能重命名 | Toggle 开关 | 合并后重命名为 主播名_日期_时长.mp4 |

**底部信息区：**
- 录制文件总大小 · 磁盘剩余空间
- "查看日志" 按钮

---

### 4.3 日志查看器弹窗（700px 宽）

- 头部：标题 + 刷新按钮 + 关闭
- 内容：等宽字体的日志文本，pre-wrap，最大高度 70vh 可滚动

---

## 通知系统

**Toast 通知：** 固定在右下角，8px 圆角，0.3s 滑入动画，2.5s 后自动消失
- 合并完成：显示主播名 + 文件名 + 片段数 + 大小
- 自动合并：`{主播名} 已自动合并 → {文件名}`
- 操作反馈：添加/删除/启动/停止

**桌面通知（Browser Notification）：**
- 开始录制 / 录制结束 / 合并完成 / 自动合并完成

---

## 设计系统

### 颜色变量（深色主题）

```
背景:     #0f1117
表面:     #1a1d27
表面2:    #242836
边框:     #2e3348
文字:     #e4e6f0
次要文字: #8b8fa3
强调色:   #6c5ce7 (紫色)
成功:     #00b894 (绿色)
危险:     #e17055 (红色/橙红)
警告:     #fdcb6e (橙黄)
链接:     #74b9ff (蓝色)
```

### 颜色变量（浅色主题）

```
背景:     #f5f5f7
表面:     #ffffff
表面2:    #f0f0f2
边框:     #d8dae0
文字:     #1d1d1f
次要文字: #6e6e73
链接:     #0071e3
其余颜色同深色主题
```

### 平台品牌色

```
抖音:     #fe2c55
B站:      #00aeec
Twitch:   #9146ff
YouTube:  #ff0000
Stripchat: #6c5ce7 (同强调色)
```

### 组件规范

- 按钮：6px 圆角，12px 字号，500 字重，hover 加深，active 缩放 0.97
- 卡片：10px 圆角，1px 边框，hover 边框高亮
- 弹窗：12px 圆角，75% 黑色遮罩 + 4px 模糊，最大高度 85vh
- Toggle 开关：32x18px，14px 圆形滑块，紫色激活态
- 徽章：10px 字号，10px 圆角，半透明背景
- 状态指示灯：8px 圆形，在线/录制时带 glow 阴影
- 进度条：3px 高度，2px 圆角，紫色填充，0.3s 过渡动画
- 字体：系统字体栈 (-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif)

### 动画

- `pulse`: 1.2s infinite，opacity 1→0.3→1（用于录制指示灯、REC 徽章）
- `spin`: 0.6s linear infinite，rotate 360deg（用于 loading spinner）
- `slideIn`: 0.3s，translateY(20px)+opacity:0 → 正常（用于 toast）

### 响应式断点

- 移动端 (≤640px)：单列卡片，header 垂直排列，弹窗全屏

---

## 交互细节

- 拖拽 URL 到页面：全屏紫色虚线边框 + "松开添加直播 URL" 提示
- Escape 关闭所有弹窗
- Cmd/Ctrl+N 聚焦添加输入框
- 缩略图每 15 秒刷新（录制中时从视频提取最新帧）
- 磁盘空间不足 1GB 时顶栏显示红色 "磁盘不足!" 警告
- WebSocket 断开时指数退避重连（3s→6s→12s→...→30s max）
- 删除操作需 confirm 确认

---

## API 端点参考（供理解数据结构）

```
GET    /api/models                    → [{username, platform, status, state, current_recording, ...}]
POST   /api/models                    → {url: "..."}
DELETE /api/models/{username}
POST   /api/models/{username}/start
POST   /api/models/{username}/stop
POST   /api/models/{username}/auto-merge → {auto_merge: bool}
GET    /api/recordings/{username}     → [{filename, size, created}]
GET    /api/sessions/{username}       → [{session_id, started_at, ended_at, segments[], status, merged_file}]
POST   /api/sessions/{username}/{id}/merge
GET    /api/recordings/{username}/groups → [{id, files[], total_size, count}]
POST   /api/recordings/{username}/merge → {files[], delete_originals}
GET    /api/video/{username}/{file}?download=1
GET    /api/settings                  → {auto_merge, merge_gap_minutes, ...}
POST   /api/settings
GET    /api/stats/{username}          → {total_files, total_size, session_count, total_duration}
GET    /api/disk                      → {recordings_bytes, free_bytes, total_bytes}
GET    /api/logs                      → ["log line", ...]
WS     /ws                           → 实时推送 model_update/merge_done/merge_progress/...
```

---

## 设计要求

1. 保持信息密度高但不拥挤，参考 macOS 原生应用的留白节奏
2. 录制状态要一目了然（颜色 + 动画 + 位置层级）
3. 合并功能是核心卖点，会话视图要直观展示"一次直播 = 多个片段 → 一个合并文件"的流程
4. 深色主题为主，浅色主题为辅
5. 中文界面，所有文案使用简体中文
