"""
Worker — long-running process that consumes from Redis queue and executes tasks.

Run multiple of these (one per CPU core, or in containers) for horizontal scaling.

Concurrency model:
  Each worker handles N concurrent tasks via asyncio.Semaphore. asyncio is ideal
  for I/O-bound tasks (API calls, DB queries, file ops). For CPU-bound work, swap
  to multiprocessing.Pool inside the handler.

Fault tolerance:
  - HEARTBEAT: every 5s the worker SET <hb:worker_id> with TTL=30s. If a worker
    crashes, the key expires and the supervisor (separate process) reaps any
    tasks left in REDIS_RUNNING_SET that belong to the dead worker.
  - DEDUPLICATION: SREM from running set after completion ensures the same task
    can't be re-dispatched accidentally.
"""
import asyncio
import os
import socket
import time
import uuid
from datetime import datetime

from .handlers import get_handler
from ..core.config import settings
from ..core.redis_client import get_redis, close_redis
from ..core.logging import setup_logging
from ..db.store import task_store
from ..db.audit import record_event, record_terminal, init_db, close_db
from ..models.schemas import Task, TaskStatus, TaskUpdate

log = setup_logging("worker")


class Worker:
    def __init__(self, concurrency: int = 4) -> None:
        self.worker_id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.concurrency = concurrency
        self.sem = asyncio.Semaphore(concurrency)
        self._stop = asyncio.Event()

    async def run(self) -> None:
        log.info("worker_starting", extra={"worker_id": self.worker_id, "concurrency": self.concurrency})
        await init_db()
        r = await get_redis()

        # Background heartbeat
        hb_task = asyncio.create_task(self._heartbeat(r))

        try:
            while not self._stop.is_set():
                # BLPOP blocks up to 1s; respects stop signal quickly.
                # Returns (queue_name, value) or None on timeout.
                res = await r.blpop(settings.REDIS_QUEUE_KEY, timeout=1)
                if res is None:
                    continue
                _, raw = res
                task = Task.from_redis_payload(raw)
                # Acquire semaphore — caps in-flight tasks per worker.
                await self.sem.acquire()
                asyncio.create_task(self._execute_with_release(r, task))
        finally:
            hb_task.cancel()
            await close_db()
            await close_redis()

    async def _execute_with_release(self, r, task: Task) -> None:
        try:
            await self._execute(r, task)
        finally:
            self.sem.release()

    async def _execute(self, r, task: Task) -> None:
        log.info("task_picked", extra={"task_id": task.id, "type": task.task_type, "worker_id": self.worker_id})
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.utcnow()
        task.worker_id = self.worker_id
        await task_store.save(task)
        await record_event(task.id, "started", worker_id=self.worker_id)

        handler = get_handler(task.task_type)
        try:
            result = await handler(task.payload)
            task.status = TaskStatus.COMPLETED
            task.result = result
            task.completed_at = datetime.utcnow()
            await task_store.save(task)
            await r.srem(settings.REDIS_RUNNING_SET, task.id)
            await record_event(task.id, "completed", worker_id=self.worker_id)
            await record_terminal(task)
            await self._publish_update(r, task)
            log.info("task_completed", extra={"task_id": task.id})
        except Exception as e:
            task.retries += 1
            task.last_error = f"{type(e).__name__}: {e}"
            await r.srem(settings.REDIS_RUNNING_SET, task.id)
            if task.retries <= task.max_retries:
                # Hand back to scheduler via a "retry channel" the API listens on.
                # Simplest implementation: push to a retry list the scheduler drains.
                task.status = TaskStatus.RETRYING
                await task_store.save(task)
                await r.lpush("itss:retry_queue", task.to_redis_payload())
                await record_event(task.id, "retrying", worker_id=self.worker_id,
                                   details={"error": task.last_error, "retries": task.retries})
                log.warning("task_failed_will_retry", extra={"task_id": task.id, "retries": task.retries, "error": task.last_error})
            else:
                task.status = TaskStatus.FAILED
                task.completed_at = datetime.utcnow()
                await task_store.save(task)
                await record_event(task.id, "failed_permanently", worker_id=self.worker_id,
                                   details={"error": task.last_error})
                await record_terminal(task)
                log.error("task_failed_permanently", extra={"task_id": task.id, "error": task.last_error})
            await self._publish_update(r, task)

    async def _publish_update(self, r, task: Task) -> None:
        """Pub/sub for live frontend updates."""
        update = TaskUpdate(
            task_id=task.id,
            status=task.status,
            result=task.result,
            error=task.last_error,
            worker_id=task.worker_id,
        )
        await r.publish(settings.REDIS_RESULT_CHANNEL, update.model_dump_json())

    async def _heartbeat(self, r) -> None:
        key = f"{settings.REDIS_HEARTBEAT_PREFIX}{self.worker_id}"
        while not self._stop.is_set():
            try:
                await r.set(key, str(time.time()), ex=settings.WORKER_HEARTBEAT_TTL)
            except Exception:
                pass
            await asyncio.sleep(settings.WORKER_HEARTBEAT_TTL // 3)

    def stop(self) -> None:
        self._stop.set()


async def main():
    concurrency = int(os.environ.get("WORKER_CONCURRENCY", "4"))
    w = Worker(concurrency=concurrency)
    await w.run()


if __name__ == "__main__":
    asyncio.run(main())
