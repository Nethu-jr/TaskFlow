"""
Audit writer — async SQLAlchemy session manager + the durable write API.

OPERATING MODES
---------------
1. NORMAL: events are written directly to Postgres via async session
2. DEGRADED: if Postgres is unreachable, events buffer in Redis list
   (`itss:audit_buffer`) and a background drainer flushes them once Postgres
   recovers. This keeps the hot path (worker → Redis update) decoupled from
   Postgres availability.

This is the same "outbox pattern" used by event-driven systems — durability
through a local queue, not through synchronous DB writes on the critical path.
"""
import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.exc import SQLAlchemyError

from .models import Base, TaskHistory, TaskEvent
from ..core.config import settings
from ..core.logging import setup_logging
from ..core.redis_client import get_redis
from ..models.schemas import Task, TaskStatus

log = setup_logging("audit")

AUDIT_BUFFER_KEY = "itss:audit_buffer"      # Redis list, drained on recovery

_engine = None
_Session: Optional[async_sessionmaker[AsyncSession]] = None


async def init_db() -> None:
    """Create the engine and tables. Idempotent — safe to call multiple times."""
    global _engine, _Session
    if _engine is not None:
        return
    _engine = create_async_engine(
        settings.DATABASE_URL,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,                 # detect stale connections
        echo=False,
    )
    _Session = async_sessionmaker(_engine, expire_on_commit=False, class_=AsyncSession)
    # CREATE TABLE IF NOT EXISTS for both audit tables
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    log.info("audit_db_ready")


async def close_db() -> None:
    global _engine, _Session
    if _engine is not None:
        await _engine.dispose()
        _engine, _Session = None, None


@asynccontextmanager
async def session():
    if _Session is None:
        raise RuntimeError("init_db() must be called before session()")
    async with _Session() as s:
        yield s


# -------------- Public write API --------------

async def record_event(task_id: str, event_type: str,
                       worker_id: Optional[str] = None,
                       details: Optional[dict] = None) -> None:
    """Append a state transition. Falls back to Redis buffer on DB failure."""
    payload = {
        "kind": "event",
        "task_id": task_id,
        "event_type": event_type,
        "timestamp": datetime.utcnow().isoformat(),
        "worker_id": worker_id,
        "details": details or {},
    }
    await _try_write_or_buffer(payload)


async def record_terminal(task: Task) -> None:
    """Snapshot a task that has reached completed/failed. Idempotent (PK = task_id)."""
    duration_ms = None
    if task.started_at and task.completed_at:
        duration_ms = (task.completed_at - task.started_at).total_seconds() * 1000.0
    payload = {
        "kind": "history",
        "task_id": task.id,
        "name": task.name,
        "task_type": task.task_type.value,
        "payload": task.payload,
        "priority": task.priority,
        "status": task.status.value,
        "created_at": task.created_at.isoformat(),
        "started_at": task.started_at.isoformat() if task.started_at else None,
        "completed_at": task.completed_at.isoformat() if task.completed_at else None,
        "duration_ms": duration_ms,
        "retries": task.retries,
        "last_error": task.last_error,
        "worker_id": task.worker_id,
        "result": task.result,
    }
    await _try_write_or_buffer(payload)


# -------------- Implementation --------------

async def _try_write_or_buffer(payload: dict) -> None:
    """Attempt direct DB write; on failure, push to Redis buffer."""
    try:
        await _write(payload)
    except SQLAlchemyError as e:
        # DB hiccup — buffer for later. Don't lose the event.
        log.warning("audit_db_unavailable_buffering", extra={"error": str(e)})
        try:
            r = await get_redis()
            await r.rpush(AUDIT_BUFFER_KEY, json.dumps(payload, default=str))
        except Exception as e2:
            # Redis ALSO down — log loudly. We're degraded but the system stays up.
            log.error("audit_total_failure", extra={"error": str(e2), "payload_sample": str(payload)[:200]})


async def _write(payload: dict) -> None:
    async with session() as s:
        if payload["kind"] == "event":
            s.add(TaskEvent(
                task_id=payload["task_id"],
                event_type=payload["event_type"],
                timestamp=datetime.fromisoformat(payload["timestamp"]),
                worker_id=payload.get("worker_id"),
                details=payload.get("details"),
            ))
        else:  # history
            row = TaskHistory(
                task_id=payload["task_id"],
                name=payload["name"],
                task_type=payload["task_type"],
                payload=payload["payload"],
                priority=payload["priority"],
                status=payload["status"],
                created_at=datetime.fromisoformat(payload["created_at"]),
                started_at=datetime.fromisoformat(payload["started_at"]) if payload.get("started_at") else None,
                completed_at=datetime.fromisoformat(payload["completed_at"]) if payload.get("completed_at") else None,
                duration_ms=payload.get("duration_ms"),
                retries=payload["retries"],
                last_error=payload.get("last_error"),
                worker_id=payload.get("worker_id"),
                result=payload.get("result"),
            )
            await s.merge(row)         # upsert by PK — safe under retries
        await s.commit()


# -------------- Buffer drainer (recovery path) --------------

async def drain_audit_buffer() -> None:
    """Background task: drain buffered events into Postgres once it returns."""
    r = await get_redis()
    while True:
        try:
            res = await r.blpop(AUDIT_BUFFER_KEY, timeout=5)
            if res is None:
                continue
            _, raw = res
            try:
                await _write(json.loads(raw))
            except SQLAlchemyError:
                # Still down — push back to the head and back off
                await r.lpush(AUDIT_BUFFER_KEY, raw)
                await asyncio.sleep(5.0)
        except Exception as e:
            log.exception("audit_drain_error", extra={"error": str(e)})
            await asyncio.sleep(2.0)
