"""
CronTicker — a long-running coroutine that:
  1. Wakes every CRON_TICK_SECONDS
  2. Asks the store for all crons whose next_fire is in the past (ZRANGEBYSCORE)
  3. For each due cron, atomically claims the fire slot via SET NX EX
     (so multiple replicas don't double-fire) and spawns a Task instance
  4. Re-computes next_fire_epoch and writes it back

The fencing lock key encodes the *scheduled* fire epoch (not now()), so that
even with clock skew across replicas they agree on which slot is being claimed.

Lock TTL = 2 * tick interval — long enough to outlive any pause-the-world
GC blip but short enough to self-clear without manual intervention.
"""
from __future__ import annotations
import asyncio
import time
from datetime import datetime

from .priority_queue import PriorityQueue
from .service import scheduler_service
from ..core.config import settings
from ..core.redis_client import get_redis
from ..core.logging import setup_logging
from ..db.cron_store import cron_store
from ..models.cron import CRON_FIRE_LOCK_PREFIX
from ..models.schemas import Task, TaskStatus
from ..ml.predictor import predictor

log = setup_logging("cron")


class CronTicker:
    def __init__(self, tick_seconds: float = 1.0) -> None:
        self.tick_seconds = tick_seconds
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._stop.clear()
        self._task = asyncio.create_task(self._run())
        log.info("cron_ticker_started", extra={"tick_s": self.tick_seconds})

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await self._task

    async def _run(self) -> None:
        r = await get_redis()
        lock_ttl = max(2, int(self.tick_seconds * 2))
        while not self._stop.is_set():
            try:
                await asyncio.sleep(self.tick_seconds)
                due = await cron_store.fetch_due()
                for cron_id, scheduled_fire in due:
                    # Fencing key encodes the *scheduled* slot — not now() —
                    # so all replicas race for the same key and only one wins.
                    lock_key = f"{CRON_FIRE_LOCK_PREFIX}{cron_id}:{int(scheduled_fire)}"
                    won = await r.set(lock_key, "1", nx=True, ex=lock_ttl)
                    if not won:
                        continue                     # another replica owns this fire
                    await self._fire(cron_id, scheduled_fire)
            except Exception as e:
                log.exception("cron_ticker_error", extra={"error": str(e)})
                await asyncio.sleep(1.0)

    async def _fire(self, cron_id: str, scheduled_fire: float) -> None:
        cron = await cron_store.get(cron_id)
        if cron is None or not cron.enabled:
            # Schedule entry is stale — clean up
            r = await get_redis()
            await r.zrem("itss:cron_schedule", cron_id)
            return

        # Decide priority: explicit > ML prediction
        priority = cron.priority if cron.priority is not None else predictor.predict(
            cron.task_type.value, cron.payload, datetime.utcnow()
        )

        # Spawn an instance — runs through the regular scheduler heap
        instance = Task(
            name=f"{cron.name} (cron)",
            task_type=cron.task_type,
            payload=cron.payload,
            priority=priority,
            run_at=datetime.utcfromtimestamp(scheduled_fire),
            max_retries=cron.max_retries,
            status=TaskStatus.PENDING,
        )
        await scheduler_service.schedule(instance)

        # Update template and re-arm — guard against catch-up storms by
        # rescheduling AFTER the scheduled_fire we just consumed (not after
        # now()). Otherwise a long-paused scheduler that wakes up to find
        # 100 missed fires would only fire one and skip the rest.
        cron.last_fired_at = datetime.utcnow()
        cron.fire_count += 1
        next_fire = await cron_store.reschedule(cron, after_epoch=scheduled_fire)

        log.info(
            "cron_fired",
            extra={
                "cron_id": cron_id,
                "instance_id": instance.id,
                "scheduled_for": scheduled_fire,
                "next_fire": next_fire,
            },
        )


cron_ticker = CronTicker(tick_seconds=1.0)
