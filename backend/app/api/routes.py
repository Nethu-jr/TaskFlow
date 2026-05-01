"""
REST API endpoints.

  POST   /tasks            -> create + schedule
  GET    /tasks            -> list (paginated)
  GET    /tasks/{id}       -> single task
  POST   /tasks/{id}/run   -> dispatch immediately, ignoring run_at
  GET    /tasks/{id}/retry -> manual retry of a failed task
  GET    /scheduler/stats  -> heap state, queue depth
"""
from datetime import datetime
from fastapi import APIRouter, HTTPException, status
from typing import List

from ..models.schemas import Task, TaskCreate, TaskStatus, TaskType
from ..ml.predictor import predictor
from ..scheduler.service import scheduler_service
from ..db.store import task_store
from ..core.redis_client import get_redis
from ..core.config import settings

router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.post("", response_model=Task, status_code=status.HTTP_201_CREATED)
async def create_task(req: TaskCreate) -> Task:
    """
    Create and schedule a task.
    If `priority` is omitted, the ML model predicts one.
    If `run_at` is omitted, the task runs ASAP.
    """
    priority = req.priority if req.priority is not None else predictor.predict(
        req.task_type.value, req.payload, datetime.utcnow()
    )
    task = Task(
        name=req.name,
        task_type=req.task_type,
        payload=req.payload,
        priority=priority,
        run_at=req.run_at or datetime.utcnow(),
        max_retries=req.max_retries,
    )
    await scheduler_service.schedule(task)
    return task


@router.get("", response_model=List[Task])
async def list_tasks(limit: int = 100) -> List[Task]:
    return await task_store.list_all(limit=limit)


@router.get("/{task_id}", response_model=Task)
async def get_task(task_id: str) -> Task:
    task = await task_store.get(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    return task


@router.post("/{task_id}/run", response_model=Task)
async def run_now(task_id: str) -> Task:
    """Force-dispatch a task immediately (priority=1, run_at=now)."""
    task = await task_store.get(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    if task.status in (TaskStatus.RUNNING, TaskStatus.QUEUED):
        raise HTTPException(409, "Task already in flight")
    task.status = TaskStatus.PENDING
    task.run_at = datetime.utcnow()
    task.priority = 1
    await scheduler_service.schedule(task)
    return task


@router.post("/{task_id}/retry", response_model=Task)
async def retry(task_id: str) -> Task:
    """Manual retry for a failed task (resets retry counter)."""
    task = await task_store.get(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    if task.status != TaskStatus.FAILED:
        raise HTTPException(409, "Only failed tasks can be retried this way")
    task.status = TaskStatus.PENDING
    task.retries = 0
    task.last_error = None
    task.run_at = datetime.utcnow()
    await scheduler_service.schedule(task)
    return task


# --- Scheduler observability ---

stats_router = APIRouter(prefix="/scheduler", tags=["scheduler"])


@stats_router.get("/stats")
async def stats():
    r = await get_redis()
    queue_depth = await r.llen(settings.REDIS_QUEUE_KEY)
    running = await r.scard(settings.REDIS_RUNNING_SET)
    # Active worker count from heartbeat keys
    cursor = 0
    workers = 0
    while True:
        cursor, keys = await r.scan(cursor=cursor, match=f"{settings.REDIS_HEARTBEAT_PREFIX}*", count=100)
        workers += len(keys)
        if cursor == 0:
            break
    return {
        "heap": scheduler_service.pq.stats(),
        "redis_queue_depth": queue_depth,
        "running_tasks": running,
        "active_workers": workers,
    }
