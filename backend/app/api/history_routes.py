"""
History query endpoints. These hit Postgres, not Redis, because:
  - Working state expires from Redis after ~1h to keep memory bounded
  - Postgres holds the immutable record forever (or until you partition-drop)
  - Indexes on (status, completed_at) and (task_type, created_at) make these
    queries fast even at billions of rows
"""
from datetime import datetime, timedelta
from typing import Optional, Any
from fastapi import APIRouter, Query
from sqlalchemy import select, func, desc

from ..db.audit import session
from ..db.models import TaskHistory, TaskEvent

router = APIRouter(prefix="/history", tags=["history"])


@router.get("/tasks/{task_id}/events")
async def task_timeline(task_id: str) -> list[dict[str, Any]]:
    """Full state-transition timeline for a single task (debugging)."""
    async with session() as s:
        rows = (await s.execute(
            select(TaskEvent).where(TaskEvent.task_id == task_id)
                             .order_by(TaskEvent.timestamp)
        )).scalars().all()
    return [
        {
            "event_type": e.event_type,
            "timestamp": e.timestamp.isoformat(),
            "worker_id": e.worker_id,
            "details": e.details,
        }
        for e in rows
    ]


@router.get("/completed")
async def list_completed(
    task_type: Optional[str] = None,
    since_hours: int = Query(default=24, ge=1, le=24 * 365),
    limit: int = Query(default=100, le=1000),
) -> list[dict[str, Any]]:
    """Recent completed/failed tasks. Powers the 'history' dashboard tab."""
    cutoff = datetime.utcnow() - timedelta(hours=since_hours)
    stmt = (select(TaskHistory)
            .where(TaskHistory.completed_at >= cutoff)
            .order_by(desc(TaskHistory.completed_at))
            .limit(limit))
    if task_type:
        stmt = stmt.where(TaskHistory.task_type == task_type)
    async with session() as s:
        rows = (await s.execute(stmt)).scalars().all()
    return [
        {
            "task_id": r.task_id, "name": r.name, "task_type": r.task_type,
            "priority": r.priority, "status": r.status,
            "duration_ms": r.duration_ms, "retries": r.retries,
            "completed_at": r.completed_at.isoformat() if r.completed_at else None,
            "last_error": r.last_error,
        }
        for r in rows
    ]


@router.get("/metrics")
async def history_metrics(since_hours: int = 24) -> dict[str, Any]:
    """
    Aggregated metrics for the dashboard:
      - throughput, success rate, avg duration per task type
    These are the same features the ML retraining pipeline pulls.
    """
    cutoff = datetime.utcnow() - timedelta(hours=since_hours)
    async with session() as s:
        rows = (await s.execute(
            select(
                TaskHistory.task_type,
                TaskHistory.status,
                func.count().label("n"),
                func.avg(TaskHistory.duration_ms).label("avg_ms"),
                func.avg(TaskHistory.retries).label("avg_retries"),
            )
            .where(TaskHistory.completed_at >= cutoff)
            .group_by(TaskHistory.task_type, TaskHistory.status)
        )).all()

    # Reshape for easy frontend consumption
    by_type: dict[str, dict] = {}
    for tt, status, n, avg_ms, avg_retries in rows:
        bucket = by_type.setdefault(tt, {"completed": 0, "failed": 0, "avg_ms": None, "avg_retries": None})
        bucket[status] = n
        if status == "completed":
            bucket["avg_ms"] = float(avg_ms) if avg_ms is not None else None
            bucket["avg_retries"] = float(avg_retries) if avg_retries is not None else None

    return {"since_hours": since_hours, "by_type": by_type}
