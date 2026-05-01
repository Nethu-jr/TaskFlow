"""
Cron-style recurring task scheduling.

DESIGN
------
A CronSchedule is a *template* — when it fires, it creates a fresh Task instance
that flows through the normal scheduler heap. This separation matters:
  - The template is immutable (or rarely edited) and lives in Postgres + Redis.
  - Each fire produces a new task with its own ID, retries, status — fully
    independent of past fires.

STORAGE
-------
Two Redis keys per cron:
  itss:cron:{id}            HASH    full cron definition (name, expr, payload, etc.)
  itss:cron_schedule        ZSET    score=next_fire_epoch, member=cron_id

To find what's due: ZRANGEBYSCORE itss:cron_schedule 0 <now> -> O(log n + k)
After firing: re-compute next_fire_epoch from the cron expression and ZADD again.

DISTRIBUTED CORRECTNESS
-----------------------
Multiple scheduler replicas must NOT each spawn a task instance for the same
fire. Solution: SET NX EX with a per-(cron_id, fire_epoch) key as a fencing
lock. Whichever replica wins the SET creates the instance; the others see the
key exists and skip.
"""
from __future__ import annotations
import time
from datetime import datetime
from typing import Optional, Any
from pydantic import BaseModel, Field
from croniter import croniter
import uuid

from .schemas import TaskType


CRON_KEY_PREFIX = "itss:cron:"
CRON_SCHEDULE_KEY = "itss:cron_schedule"        # ZSET
CRON_FIRE_LOCK_PREFIX = "itss:cron_fire:"       # SET NX EX fencing lock
CRON_INDEX_KEY = "itss:cron_index"              # SET of all cron_ids


class CronCreate(BaseModel):
    """Payload for POST /crons"""
    name: str = Field(..., min_length=1, max_length=200)
    cron_expr: str = Field(..., description="Standard 5-field cron, e.g. '0 */6 * * *'")
    task_type: TaskType = TaskType.GENERIC
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: Optional[int] = Field(default=None, ge=1, le=10)
    max_retries: int = Field(default=3, ge=0, le=10)
    enabled: bool = True


class CronSchedule(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    cron_expr: str
    task_type: TaskType
    payload: dict[str, Any]
    priority: Optional[int] = None              # None -> ML predicts at fire time
    max_retries: int = 3
    enabled: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_fired_at: Optional[datetime] = None
    fire_count: int = 0

    @staticmethod
    def validate_expr(expr: str) -> None:
        """Raises ValueError if the cron expression is malformed."""
        if not croniter.is_valid(expr):
            raise ValueError(f"invalid cron expression: {expr!r}")

    def next_fire_epoch(self, after: Optional[float] = None) -> float:
        """Compute the next fire time strictly after `after` (defaults to now)."""
        base = datetime.utcfromtimestamp(after if after is not None else time.time())
        it = croniter(self.cron_expr, base)
        return it.get_next(float)
