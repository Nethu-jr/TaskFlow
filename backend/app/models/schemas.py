"""
Pydantic schemas — the contract between API, scheduler, and workers.
Keep these immutable across versions or you'll break in-flight tasks.
"""
from datetime import datetime
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field
import uuid


class TaskStatus(str, Enum):
    PENDING = "pending"        # accepted, sitting in scheduler heap
    QUEUED = "queued"          # popped from heap, in Redis worker queue
    RUNNING = "running"        # picked up by worker
    COMPLETED = "completed"
    FAILED = "failed"          # exhausted retries
    RETRYING = "retrying"      # failed once, awaiting backoff


class TaskType(str, Enum):
    """Task types map 1:1 to handler functions registered in the worker."""
    EMAIL = "email"
    REPORT = "report"
    DATA_SYNC = "data_sync"
    ML_INFERENCE = "ml_inference"
    GENERIC = "generic"


class TaskCreate(BaseModel):
    """Payload for POST /tasks"""
    name: str = Field(..., min_length=1, max_length=200)
    task_type: TaskType = TaskType.GENERIC
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: Optional[int] = Field(
        default=None,
        ge=1, le=10,
        description="1=highest, 10=lowest. If omitted, ML model predicts.",
    )
    run_at: Optional[datetime] = Field(
        default=None,
        description="ISO timestamp for delayed execution. None = ASAP.",
    )
    max_retries: int = Field(default=3, ge=0, le=10)


class Task(BaseModel):
    """Full task record — stored in DB, dispatched to workers."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    task_type: TaskType
    payload: dict[str, Any]
    priority: int                         # 1..10, lower = more urgent
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.utcnow)
    run_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    retries: int = 0
    max_retries: int = 3
    last_error: Optional[str] = None
    result: Optional[Any] = None
    worker_id: Optional[str] = None       # set when picked up

    def to_redis_payload(self) -> str:
        """Serialize for Redis transport."""
        return self.model_dump_json()

    @classmethod
    def from_redis_payload(cls, raw: str) -> "Task":
        return cls.model_validate_json(raw)


class TaskUpdate(BaseModel):
    """Worker → backend status updates."""
    task_id: str
    status: TaskStatus
    result: Optional[Any] = None
    error: Optional[str] = None
    worker_id: Optional[str] = None
