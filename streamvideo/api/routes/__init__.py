"""API 路由模块

按功能域拆分：
- auth: 认证相关 (5 端点)
- streams: 录制流管理 (15 端点)
- storage: 存储管理 (26 端点)
- highlights: 高光与片段 (21 端点)
- clips: 片段管理、FlashCut、配额 (12 端点)
- system: 系统配置、Whisper、配置导入导出 (4 端点)
- distribute: 分发管理、OAuth (13 端点)
- payment: 支付管理 (5 端点)
- tasks: 任务管理 (3 端点)
"""
from streamvideo.api.routes import (
    auth,
    clips,
    distribute,
    highlights,
    payment,
    storage,
    streams,
    system,
    tasks,
)

__all__ = [
    "auth",
    "streams",
    "storage",
    "highlights",
    "clips",
    "system",
    "distribute",
    "payment",
    "tasks",
]


def all_routers():
    """返回所有路由器列表，便于一键注册到 FastAPI app"""
    return [
        auth.router,
        streams.router,
        storage.router,
        highlights.router,
        clips.router,
        system.router,
        distribute.router,
        payment.router,
        tasks.router,
    ]
