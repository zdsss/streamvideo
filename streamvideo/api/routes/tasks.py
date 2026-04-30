"""Tasks API 路由 - 任务管理"""
from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()

# 全局依赖
task_queue = None


def init_tasks_router(queue):
    """初始化路由依赖"""
    global task_queue
    task_queue = queue


@router.get("/api/tasks")
async def get_tasks(username: str = ""):
    """获取任务队列状态"""
    tasks = task_queue.get_tasks(username=username)
    return JSONResponse({"tasks": tasks})


@router.get("/api/tasks/{task_id}")
async def get_task(task_id: str):
    """获取单个任务状态"""
    task = task_queue.get_task(task_id)
    if not task:
        return JSONResponse({"error": "Task not found"}, status_code=404)
    return JSONResponse({
        "task_id": task.task_id,
        "name": task.name,
        "username": task.username,
        "priority": task.priority.name,
        "status": task.status,
        "progress": task.progress,
        "error": task.error,
        "created_at": task.created_at,
        "started_at": task.started_at,
        "finished_at": task.finished_at,
    })


@router.post("/api/tasks/{task_id}/cancel")
async def cancel_task(task_id: str):
    """取消待执行任务"""
    success = task_queue.cancel(task_id)
    if not success:
        return JSONResponse({"error": "Cannot cancel running/finished task"}, status_code=400)
    return JSONResponse({"success": True})


