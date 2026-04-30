# StreamVideo 全局代码质量优化与架构改造计划

> 创建时间：2026-04-30  
> 状态：规划中  
> 目标：从"能用"到"工程化"，建立可持续演进的代码基础

---

## 一、现状审计

### 1.1 代码规模统计

```
总代码量：11,478 行 Python
├── server.py       3,439 行 (30%)  ⚠️ 单文件过大
├── recorder.py     3,258 行 (28%)  ⚠️ 单文件过大
├── database.py     1,343 行 (12%)
├── danmaku.py        771 行 (7%)
├── highlight.py      551 行 (5%)
└── 其他模块       2,116 行 (18%)

测试代码：1,148 行
测试覆盖率：~10% ⚠️ 严重不足
```

### 1.2 关键问题识别

#### 🔴 P0 — 架构腐化风险

1. **单体巨石文件**
   - `server.py` 3439 行，148 个函数/类，职责混乱
   - `recorder.py` 3258 行，19 个类，但单类过大
   - 违反单一职责原则，难以维护和测试

2. **测试覆盖严重不足**
   - 核心录制逻辑（recorder.py）无单元测试
   - 合并信心度算法（`_calc_merge_confidence`）无测试
   - API 端点缺少集成测试
   - 现有测试：1148 行，覆盖率 ~10%

3. **依赖管理混乱**
   - `server.py` 中重复 import（`from auth import AuthManager` 出现 5 次）
   - 循环依赖风险（server ↔ recorder ↔ database）
   - 无依赖注入，全局状态耦合严重

4. **错误处理不一致**
   - 部分函数返回 None 表示失败，部分抛异常
   - 缺少统一的错误码体系
   - 日志级别使用不规范

#### 🟡 P1 — 代码质量问题

1. **命名不一致**
   - 混用 snake_case 和 camelCase
   - 函数名过长（如 `auto_merge_for_model_with_confidence`）
   - 魔法数字散落各处（0.7, 0.4, 30, 60）

2. **重复代码**
   - 文件操作逻辑重复（读取、写入、删除）
   - WebSocket 推送逻辑重复
   - 数据库查询模式重复

3. **配置硬编码**
   - 超时时间、阈值、路径硬编码在代码中
   - 缺少配置验证和默认值管理
   - 环境变量读取分散在各个文件

#### 🟢 P2 — 性能与可观测性

1. **性能瓶颈**
   - 同步 I/O 阻塞异步事件循环
   - 数据库查询未使用索引
   - 大文件操作未分块处理

2. **可观测性缺失**
   - 缺少结构化日志
   - 无性能指标采集（Prometheus/StatsD）
   - 无分布式追踪（OpenTelemetry）
   - 错误无上下文信息

---

## 二、架构重构方案

### 2.1 模块拆分策略

#### 目标架构（分层 + 领域驱动）

```
streamvideo/
├── api/                    # API 层（FastAPI 路由）
│   ├── __init__.py
│   ├── routes/
│   │   ├── streams.py      # 录制相关端点
│   │   ├── storage.py      # 存储相关端点
│   │   ├── highlights.py   # 高光相关端点
│   │   ├── clips.py        # 切片相关端点
│   │   ├── auth.py         # 认证相关端点
│   │   └── system.py       # 系统相关端点
│   ├── middleware/
│   │   ├── auth.py         # 认证中间件
│   │   ├── rate_limit.py   # 限流中间件
│   │   └── error.py        # 错误处理中间件
│   └── schemas/            # Pydantic 模型
│       ├── request.py
│       └── response.py
├── core/                   # 核心业务逻辑
│   ├── recorder/
│   │   ├── __init__.py
│   │   ├── manager.py      # RecorderManager
│   │   ├── session.py      # RecordingSession
│   │   ├── engines/        # 录制引擎
│   │   │   ├── streamlink.py
│   │   │   ├── ytdlp.py
│   │   │   └── playwright.py
│   │   └── merger.py       # 合并逻辑
│   ├── processor/          # 后处理
│   │   ├── highlight.py
│   │   ├── subtitle.py
│   │   ├── cover.py
│   │   └── clipgen.py
│   ├── distributor/        # 分发系统
│   │   ├── manager.py
│   │   └── platforms/
│   │       ├── douyin.py
│   │       └── kuaishou.py
│   └── auth/               # 认证授权
│       ├── manager.py
│       └── quota.py
├── infrastructure/         # 基础设施层
│   ├── database/
│   │   ├── __init__.py
│   │   ├── connection.py   # 连接管理
│   │   ├── models.py       # ORM 模型
│   │   └── repositories/   # 仓储模式
│   │       ├── model.py
│   │       ├── session.py
│   │       └── user.py
│   ├── storage/
│   │   ├── local.py        # 本地存储
│   │   └── cloud.py        # 云存储（S3/OSS）
│   ├── messaging/
│   │   ├── websocket.py    # WebSocket 管理
│   │   └── webhook.py      # Webhook 发送
│   └── cache/
│       └── redis.py        # Redis 缓存
├── shared/                 # 共享工具
│   ├── config.py           # 配置管理
│   ├── logger.py           # 日志工具
│   ├── errors.py           # 错误定义
│   ├── constants.py        # 常量定义
│   └── utils/
│       ├── file.py
│       ├── time.py
│       └── validation.py
├── tests/
│   ├── unit/               # 单元测试
│   ├── integration/        # 集成测试
│   └── e2e/                # 端到端测试
└── server.py               # 入口文件（精简到 <200 行）
```

### 2.2 关键重构任务

#### Phase 1：基础设施层重构（Week 1-2）

**Task 1.1：配置管理统一化**
- 创建 `shared/config.py`，使用 Pydantic Settings
- 所有配置项集中管理，支持环境变量覆盖
- 配置验证和默认值
- 估时：2 天

**Task 1.2：日志系统标准化**
- 创建 `shared/logger.py`，使用 structlog
- 统一日志格式（JSON 结构化）
- 日志级别动态调整
- 添加请求 ID 追踪
- 估时：1 天

**Task 1.3：错误处理体系**
- 创建 `shared/errors.py`，定义错误码枚举
- 自定义异常类层次结构
- 统一错误响应格式
- 估时：1 天

**Task 1.4：数据库层重构**
- 拆分 `database.py` → `infrastructure/database/`
- 引入仓储模式（Repository Pattern）
- 连接池管理优化
- 添加数据库迁移工具（Alembic）
- 估时：3 天

#### Phase 2：核心业务层重构（Week 3-4）

**Task 2.1：Recorder 模块拆分**
- `recorder.py` (3258 行) → `core/recorder/` 目录
- 拆分为：manager.py, session.py, merger.py, engines/
- 合并信心度算法独立为 `merger.py`
- 估时：4 天

**Task 2.2：Server 模块拆分**
- `server.py` (3439 行) → `api/routes/` 目录
- 按功能域拆分为 6 个路由文件
- 中间件独立为 `api/middleware/`
- 估时：4 天

**Task 2.3：依赖注入改造**
- 引入依赖注入容器（dependency-injector）
- 消除全局状态
- 提升可测试性
- 估时：3 天

#### Phase 3：测试体系建设（Week 5-6）

**Task 3.1：单元测试补齐**
- 核心算法测试（合并信心度、高光检测）
- 业务逻辑测试（录制、后处理）
- 目标覆盖率：80%
- 估时：5 天

**Task 3.2：集成测试**
- API 端点测试（pytest + httpx）
- 数据库集成测试（pytest-postgresql）
- WebSocket 测试
- 估时：3 天

**Task 3.3：E2E 测试**
- 录制流程端到端测试
- 合并流程端到端测试
- 估时：2 天

#### Phase 4：性能优化（Week 7）

**Task 4.1：异步 I/O 优化**
- 文件操作改为 aiofiles
- 数据库查询改为 asyncpg/aiosqlite
- HTTP 请求改为 httpx
- 估时：3 天

**Task 4.2：数据库优化**
- 添加缺失索引
- 查询优化（N+1 问题）
- 连接池调优
- 估时：2 天

**Task 4.3：缓存策略**
- 引入 Redis 缓存热点数据
- 主播状态缓存
- 配置缓存
- 估时：2 天

---

## 三、代码质量标准

### 3.1 编码规范

**命名规范**
- 模块/包：snake_case
- 类：PascalCase
- 函数/变量：snake_case
- 常量：UPPER_SNAKE_CASE
- 私有成员：_leading_underscore

**函数设计原则**
- 单一职责：一个函数只做一件事
- 长度限制：<50 行（复杂逻辑拆分）
- 参数限制：<5 个（使用对象封装）
- 返回值：明确类型注解

**注释规范**
- 模块：docstring 说明用途
- 类：docstring 说明职责
- 公共函数：docstring（Google 风格）
- 复杂逻辑：行内注释说明 WHY

### 3.2 工具链配置

**代码格式化**
```bash
# pyproject.toml
[tool.black]
line-length = 100
target-version = ['py311']

[tool.isort]
profile = "black"
line_length = 100
```

**静态检查**
```bash
# mypy 类型检查
mypy --strict streamvideo/

# ruff 代码检查
ruff check streamvideo/

# pylint 代码质量
pylint streamvideo/
```

**测试覆盖**
```bash
# pytest 配置
pytest --cov=streamvideo --cov-report=html --cov-report=term-missing
# 目标：80% 覆盖率
```

### 3.3 CI/CD 集成

**Pre-commit Hooks**
```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/psf/black
    rev: 24.3.0
    hooks:
      - id: black
  - repo: https://github.com/pycqa/isort
    rev: 5.13.2
    hooks:
      - id: isort
  - repo: https://github.com/charliermarsh/ruff-pre-commit
    rev: v0.3.4
    hooks:
      - id: ruff
  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.9.0
    hooks:
      - id: mypy
```

**GitHub Actions**
```yaml
# .github/workflows/ci.yml
name: CI
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run tests
        run: |
          pip install -r requirements-dev.txt
          pytest --cov=streamvideo --cov-fail-under=80
      - name: Type check
        run: mypy --strict streamvideo/
      - name: Lint
        run: ruff check streamvideo/
```

---

## 四、实施路线图

### 时间估算（7 周）

| Phase | 任务 | 估时 | 依赖 |
|-------|------|------|------|
| **Phase 1** | 配置管理统一化 | 2d | - |
| | 日志系统标准化 | 1d | - |
| | 错误处理体系 | 1d | - |
| | 数据库层重构 | 3d | - |
| **Phase 2** | Recorder 模块拆分 | 4d | Phase 1 |
| | Server 模块拆分 | 4d | Phase 1 |
| | 依赖注入改造 | 3d | Phase 1 |
| **Phase 3** | 单元测试补齐 | 5d | Phase 2 |
| | 集成测试 | 3d | Phase 2 |
| | E2E 测试 | 2d | Phase 2 |
| **Phase 4** | 异步 I/O 优化 | 3d | Phase 2 |
| | 数据库优化 | 2d | Phase 1 |
| | 缓存策略 | 2d | Phase 1 |

**总计：35 天（7 周）**

### 风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| 重构破坏现有功能 | 高 | 每个 Phase 完成后回归测试 |
| 测试编写耗时超预期 | 中 | 优先核心路径，非核心延后 |
| 依赖注入学习曲线 | 中 | 提前技术预研，编写示例 |
| 异步改造兼容性问题 | 高 | 渐进式改造，保留同步接口 |

---

## 五、成功指标

### 5.1 代码质量指标

- ✅ 单文件代码行数 <1000 行
- ✅ 函数平均长度 <30 行
- ✅ 圈复杂度 <10
- ✅ 测试覆盖率 ≥80%
- ✅ 类型注解覆盖率 100%
- ✅ Ruff/Pylint 评分 ≥9.0

### 5.2 性能指标

- ✅ API 响应时间 P95 <200ms
- ✅ 录制启动延迟 <3s
- ✅ 合并处理速度 >10MB/s
- ✅ 内存占用 <500MB（空闲）
- ✅ 数据库查询 P95 <50ms

### 5.3 可维护性指标

- ✅ 新功能开发周期缩短 30%
- ✅ Bug 修复周期缩短 50%
- ✅ 代码审查通过率 >90%
- ✅ 技术债务减少 70%

---

## 六、关键文件变更清单

### 新增文件（~30 个）

```
streamvideo/
├── api/routes/*.py          (6 个路由文件)
├── api/middleware/*.py      (3 个中间件)
├── api/schemas/*.py         (2 个 schema 文件)
├── core/recorder/*.py       (5 个录制模块)
├── core/processor/*.py      (4 个后处理模块)
├── infrastructure/database/*.py  (4 个数据库模块)
├── shared/*.py              (5 个共享工具)
└── tests/unit/*.py          (~20 个测试文件)
```

### 重构文件（2 个）

- `server.py`：3439 行 → ~150 行（入口文件）
- `recorder.py`：3258 行 → 拆分为 5 个文件

### 配置文件（5 个）

- `pyproject.toml`：工具配置
- `.pre-commit-config.yaml`：Git hooks
- `.github/workflows/ci.yml`：CI 流程
- `alembic.ini`：数据库迁移
- `requirements-dev.txt`：开发依赖

---

## 七、下一步行动

1. ✅ 完成本计划编写
2. ⏭️ 技术预研（依赖注入、Alembic）
3. ⏭️ 创建 `refactor/phase1` 分支
4. ⏭️ 开始 Phase 1 Task 1.1（配置管理）
5. ⏭️ 每周五代码审查 + 进度同步

---

## 附录：参考资源

**架构设计**
- Clean Architecture (Robert C. Martin)
- Domain-Driven Design (Eric Evans)
- Python Application Layouts (Real Python)

**测试策略**
- Test Pyramid (Martin Fowler)
- pytest Best Practices
- Testing FastAPI Applications

**性能优化**
- asyncio Best Practices
- SQLite Performance Tuning
- Python Profiling Tools (cProfile, py-spy)
