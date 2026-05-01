"""
CronStore — durable storage for cron templates plus the global fire schedule.

All ops are O(log n) on the schedule ZSET; O(1) on the per-cron HASH.
"""
import json
import time
from typing import Optional
from ..models.cron import (
    CronSchedule,
    CRON_KEY_PREFIX,
    CRON_SCHEDULE_KEY,
    CRON_INDEX_KEY,
)
from ..core.redis_client import get_redis


class CronStore:

    @staticmethod
    def _key(cron_id: str) -> str:
        return f"{CRON_KEY_PREFIX}{cron_id}"

    async def save(self, cron: CronSchedule) -> None:
        """Persist the cron and (re)set its position in the schedule ZSET."""
        r = await get_redis()
        next_fire = cron.next_fire_epoch() if cron.enabled else None
        async with r.pipeline(transaction=False) as pipe:
            pipe.set(self._key(cron.id), cron.model_dump_json())
            pipe.sadd(CRON_INDEX_KEY, cron.id)
            if cron.enabled and next_fire is not None:
                pipe.zadd(CRON_SCHEDULE_KEY, {cron.id: next_fire})
            else:
                # Disabled: yank from the schedule but keep the template
                pipe.zrem(CRON_SCHEDULE_KEY, cron.id)
            await pipe.execute()

    async def get(self, cron_id: str) -> Optional[CronSchedule]:
        r = await get_redis()
        raw = await r.get(self._key(cron_id))
        return CronSchedule.model_validate_json(raw) if raw else None

    async def list_all(self) -> list[CronSchedule]:
        r = await get_redis()
        ids = await r.smembers(CRON_INDEX_KEY)
        if not ids:
            return []
        raws = await r.mget([self._key(i) for i in ids])
        return [CronSchedule.model_validate_json(x) for x in raws if x]

    async def delete(self, cron_id: str) -> None:
        r = await get_redis()
        async with r.pipeline(transaction=False) as pipe:
            pipe.delete(self._key(cron_id))
            pipe.srem(CRON_INDEX_KEY, cron_id)
            pipe.zrem(CRON_SCHEDULE_KEY, cron_id)
            await pipe.execute()

    async def fetch_due(self, now_epoch: Optional[float] = None) -> list[tuple[str, float]]:
        """
        Return [(cron_id, scheduled_fire_epoch), ...] for all crons due now.
        O(log n + k) where k = number due. Caller is responsible for re-arming.
        """
        r = await get_redis()
        now_epoch = now_epoch if now_epoch is not None else time.time()
        # withscores returns the original fire epoch — we need it for the
        # fencing lock key so concurrent schedulers agree on the lock target.
        return await r.zrangebyscore(
            CRON_SCHEDULE_KEY, 0, now_epoch, withscores=True
        )

    async def reschedule(self, cron: CronSchedule, after_epoch: float) -> float:
        """Compute next fire time after `after_epoch` and update both stores."""
        next_fire = cron.next_fire_epoch(after=after_epoch)
        r = await get_redis()
        async with r.pipeline(transaction=False) as pipe:
            pipe.set(self._key(cron.id), cron.model_dump_json())
            pipe.zadd(CRON_SCHEDULE_KEY, {cron.id: next_fire})
            await pipe.execute()
        return next_fire


cron_store = CronStore()
