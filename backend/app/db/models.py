"""
PostgreSQL audit log models.

DESIGN
------
Two tables, both append-only (no UPDATE, only INSERT):

  task_history    — one row per task (denormalized snapshot at terminal state)
                    used for ML training (completed-task features + outcome)

  task_events     — one row per state transition
                    used for debugging, audit, replay

Why append-only:
  - Concurrent writes never conflict (no row-level locks)
  - Trivially shardable by created_at (time-series partitioning)
  - History is preserved — you can reconstruct any task's full timeline

Indexes are tuned for the dominant query patterns:
  - "show me task X" → btree on task_id
  - "list yesterday's failures" → btree on (created_at, status)
  - "training query" → btree on (status, completed_at) for filtering completed tasks
"""
from datetime import datetime
from sqlalchemy import (
    Column, String, Integer, DateTime, JSON, Index, Text, Float,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class TaskHistory(Base):
    """One row per task, written when the task reaches a terminal state."""
    __tablename__ = "task_history"

    task_id        = Column(String(36), primary_key=True)
    name           = Column(String(200), nullable=False)
    task_type      = Column(String(50), nullable=False)
    payload        = Column(JSON, nullable=False)
    priority       = Column(Integer, nullable=False)
    status         = Column(String(20), nullable=False)        # completed | failed
    created_at     = Column(DateTime, nullable=False)
    started_at     = Column(DateTime, nullable=True)
    completed_at   = Column(DateTime, nullable=True)
    duration_ms    = Column(Float, nullable=True)              # for ML feature
    retries        = Column(Integer, nullable=False, default=0)
    last_error     = Column(Text, nullable=True)
    worker_id      = Column(String(100), nullable=True)
    result         = Column(JSON, nullable=True)

    __table_args__ = (
        Index("ix_history_status_completed", "status", "completed_at"),
        Index("ix_history_type_created", "task_type", "created_at"),
    )


class TaskEvent(Base):
    """One row per state transition. Used for debugging and audit."""
    __tablename__ = "task_events"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    task_id     = Column(String(36), nullable=False, index=True)
    event_type  = Column(String(50), nullable=False)            # scheduled | queued | started | completed | failed | retrying
    timestamp   = Column(DateTime, nullable=False, default=datetime.utcnow)
    worker_id   = Column(String(100), nullable=True)
    details     = Column(JSON, nullable=True)                   # arbitrary context

    __table_args__ = (
        Index("ix_events_task_time", "task_id", "timestamp"),
        Index("ix_events_time", "timestamp"),
    )
