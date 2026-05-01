"""
Retry drainer — a separate coroutine that listens to itss:retry_queue and
hands failed tasks back to the scheduler with exponential backoff.

Decoupling rationale:
  Workers must NOT call the scheduler directly (cross-process import / coupling).
  Instead they push to a Redis list; the backend consumes & re-schedules.
"""
import asyncio
from .service import scheduler_service
from ..core.config import settings
from ..core.redis_client import get_redis
from ..core.logging import setup_logging
from ..models.schemas import Task

log = setup_logging("retry_drainer")


async def drain_retries() -> None:
    r = await get_redis()
    while True:
        try:
            res = await r.blpop("itss:retry_queue", timeout=2)
            if res is None:
                continue
            _, raw = res
            task = Task.from_redis_payload(raw)
            await scheduler_service.schedule_retry(task)
        except Exception as e:
            log.exception("retry_drainer_error", extra={"error": str(e)})
            await asyncio.sleep(1.0)
