"""
SchedulerService — runs as an asyncio background task inside the FastAPI process
(or as a separate sidecar in production).

Loop:
  1. Peek heap. If empty or next task in future, sleep until then.
  2. Pop ready task → fetch from store → mark QUEUED → LPUSH to Redis worker list.
  3. Workers BLPOP from that list.

Backoff retry:
  - On TaskUpdate(status=FAILED) with retries < max_retries, the API layer calls
    `schedule_retry`, which re-pushes with run_at = now + exponential delay.
"""
import asyncio
import json
import time
from typing import Optional

from .priority_queue import PriorityQueue
from ..core.config import settings
from ..core.redis_client import get_redis
from ..core.logging import setup_logging
from ..db.store import task_store
from ..db.audit import record_event
from ..models.schemas import Task, TaskStatus

log = setup_logging("scheduler")


class SchedulerService:
    def __init__(self) -> None:
        self.pq = PriorityQueue()
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    # ---------- Public API ----------

    async def schedule(self, task: Task) -> None:
        """Persist task and add to heap."""
        await task_store.save(task)
        run_at_epoch = task.run_at.timestamp()
        self.pq.push(task.id, task.priority, run_at_epoch)
        await record_event(task.id, "scheduled",
                           details={"priority": task.priority, "run_at": task.run_at.isoformat()})
        log.info(
            "scheduled",
            extra={"task_id": task.id, "priority": task.priority, "run_at": task.run_at.isoformat()},
        )

    async def schedule_retry(self, task: Task) -> None:
        """
        Re-schedule a failed task with EXPONENTIAL BACKOFF.

        delay = min(BASE_BACKOFF * 2^(retries-1), MAX_BACKOFF)

        Why exponential:
          - Linear (e.g. retry every 10s) hammers a struggling downstream service.
          - Exponential gives the dependency time to recover.
          - Capping at MAX_BACKOFF prevents waits of hours/days for high-retry tasks.
        """
        delay = min(
            settings.BASE_BACKOFF_SECONDS * (2 ** max(0, task.retries - 1)),
            settings.MAX_BACKOFF_SECONDS,
        )
        run_at_epoch = time.time() + delay
        task.status = TaskStatus.RETRYING
        await task_store.save(task)
        self.pq.push(task.id, task.priority, run_at_epoch)
        await record_event(task.id, "retry_scheduled",
                           details={"retries": task.retries, "delay_s": delay})
        log.info(
            "retry_scheduled",
            extra={"task_id": task.id, "retries": task.retries, "delay_s": delay},
        )

    # ---------- Loop ----------

    async def start(self) -> None:
        """Start the background dispatcher loop."""
        self._stop.clear()
        self._task = asyncio.create_task(self._run())
        log.info("scheduler_started")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await self._task

    async def _run(self) -> None:
        r = await get_redis()
        while not self._stop.is_set():
            try:
                # Sleep precisely until the next task is due, but bounded so we
                # remain responsive to new schedule() calls (which may insert
                # an earlier task than the current root).
                next_run = self.pq.peek_next_run_time()
                if next_run is None:
                    sleep_for = settings.SCHEDULER_TICK_SECONDS
                else:
                    sleep_for = max(0.0, min(next_run - time.time(), settings.SCHEDULER_TICK_SECONDS))
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)

                # Drain everything ready in this tick (batch dispatch)
                dispatched = 0
                while True:
                    task_id = self.pq.pop_ready()
                    if task_id is None:
                        break
                    await self._dispatch(r, task_id)
                    dispatched += 1
                if dispatched:
                    log.info("dispatched_batch", extra={"count": dispatched})

            except Exception as e:                    # never let the loop die
                log.exception("scheduler_loop_error", extra={"error": str(e)})
                await asyncio.sleep(1.0)

    async def _dispatch(self, r, task_id: str) -> None:
        """Move a task from heap → Redis worker queue."""
        task = await task_store.get(task_id)
        if task is None:
            log.warning("task_missing_at_dispatch", extra={"task_id": task_id})
            return
        # Idempotency — refuse to redispatch a task already running/done
        if task.status in (TaskStatus.RUNNING, TaskStatus.COMPLETED):
            return
        task.status = TaskStatus.QUEUED
        await task_store.save(task)

        # SADD to running set serves as DEDUPLICATION GUARD.
        # If a duplicate dispatch races, only one BLPOP will succeed AND pass the SADD check.
        await r.sadd(settings.REDIS_RUNNING_SET, task.id)
        # FIFO worker queue — workers BLPOP from the head
        await r.lpush(settings.REDIS_QUEUE_KEY, task.to_redis_payload())
        await record_event(task.id, "queued")


scheduler_service = SchedulerService()
