"""
Async task queue for StreamVideo post-processing tasks.
Supports priority levels, concurrency limits, and progress tracking.
"""
import asyncio
import time
import uuid
import logging
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Callable, Coroutine, Optional

logger = logging.getLogger(__name__)


class Priority(IntEnum):
    HIGH = 0    # 字幕生成（用户手动触发）
    NORMAL = 1  # 高光检测
    LOW = 2     # 片段生成、封面生成


@dataclass
class Task:
    task_id: str
    name: str
    priority: Priority
    coro_fn: Callable[[], Coroutine]
    username: str = ""
    created_at: float = field(default_factory=time.time)
    started_at: float = 0.0
    finished_at: float = 0.0
    status: str = "pending"   # pending | running | done | error | cancelled
    error: str = ""
    progress: float = 0.0
    result: Any = None

    def __lt__(self, other: "Task") -> bool:
        if self.priority != other.priority:
            return self.priority < other.priority
        return self.created_at < other.created_at


class TaskQueue:
    def __init__(self, max_concurrent: int = 2):
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._tasks: dict[str, Task] = {}
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._running = False
        self._worker_task: Optional[asyncio.Task] = None

    def start(self):
        if not self._running:
            self._running = True
            self._worker_task = asyncio.create_task(self._worker())

    def stop(self):
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()

    async def submit(self, name: str, coro_fn: Callable[[], Coroutine],
                     username: str = "", priority: Priority = Priority.NORMAL) -> str:
        task_id = str(uuid.uuid4())[:8]
        task = Task(task_id=task_id, name=name, priority=priority,
                    coro_fn=coro_fn, username=username)
        self._tasks[task_id] = task
        await self._queue.put((priority, task))
        logger.info(f"[TaskQueue] Submitted {name} ({task_id}) priority={priority.name}")
        return task_id

    def get_task(self, task_id: str) -> Optional[Task]:
        return self._tasks.get(task_id)

    def get_tasks(self, username: str = "") -> list[dict]:
        tasks = list(self._tasks.values())
        if username:
            tasks = [t for t in tasks if t.username == username]
        return [self._task_to_dict(t) for t in sorted(tasks, key=lambda t: t.created_at, reverse=True)]

    def cancel(self, task_id: str) -> bool:
        task = self._tasks.get(task_id)
        if task and task.status == "pending":
            task.status = "cancelled"
            return True
        return False

    def _task_to_dict(self, t: Task) -> dict:
        return {
            "task_id": t.task_id,
            "name": t.name,
            "username": t.username,
            "priority": t.priority.name,
            "status": t.status,
            "progress": t.progress,
            "error": t.error,
            "created_at": t.created_at,
            "started_at": t.started_at,
            "finished_at": t.finished_at,
        }

    async def _worker(self):
        while self._running:
            try:
                _, task = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            if task.status == "cancelled":
                self._queue.task_done()
                continue

            async with self._semaphore:
                task.status = "running"
                task.started_at = time.time()
                logger.info(f"[TaskQueue] Starting {task.name} ({task.task_id})")
                try:
                    task.result = await task.coro_fn()
                    task.status = "done"
                    logger.info(f"[TaskQueue] Done {task.name} ({task.task_id})")
                except Exception as e:
                    task.status = "error"
                    task.error = str(e)
                    logger.error(f"[TaskQueue] Error {task.name} ({task.task_id}): {e}")
                finally:
                    task.finished_at = time.time()
                    self._queue.task_done()

            # 清理 1 小时前完成的任务
            cutoff = time.time() - 3600
            self._tasks = {k: v for k, v in self._tasks.items()
                           if v.status in ("pending", "running") or v.finished_at > cutoff}


# 全局单例
task_queue = TaskQueue(max_concurrent=2)
