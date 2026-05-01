"""
TaskStore — abstraction over durable task state.

Implementation choice:
  - HASH MAP backed by Redis (`itss:task:{id}` -> JSON blob)
  - This gives O(1) lookups by ID and survives backend restarts

For full production you'd back this with PostgreSQL for durability + audit
history. We use Redis here for clarity; the interface is the same so swapping
the backend is mechanical.
"""
from typing import Optional
from ..models.schemas import Task
from ..core.redis_client import get_redis

TASK_KEY_PREFIX = "itss:task:"
TASK_INDEX_KEY = "itss:task_index"   # SET of all task IDs (for listing)


class TaskStore:
    """Hash map: task_id -> Task. O(1) get/set/delete."""

    @staticmethod
    def _key(task_id: str) -> str:
        return f"{TASK_KEY_PREFIX}{task_id}"

    async def save(self, task: Task) -> None:
        r = await get_redis()
        # Pipeline both writes — atomic enough for our needs (no cross-key txn required)
        async with r.pipeline(transaction=False) as pipe:
            pipe.set(self._key(task.id), task.to_redis_payload())
            pipe.sadd(TASK_INDEX_KEY, task.id)
            await pipe.execute()

    async def get(self, task_id: str) -> Optional[Task]:
        r = await get_redis()
        raw = await r.get(self._key(task_id))
        return Task.from_redis_payload(raw) if raw else None

    async def list_all(self, limit: int = 100) -> list[Task]:
        r = await get_redis()
        ids = await r.smembers(TASK_INDEX_KEY)
        if not ids:
            return []
        # Batch fetch with MGET — single round trip
        ids_list = list(ids)[:limit]
        keys = [self._key(tid) for tid in ids_list]
        raws = await r.mget(keys)
        tasks = [Task.from_redis_payload(r) for r in raws if r]
        # Most-recent first
        return sorted(tasks, key=lambda t: t.created_at, reverse=True)

    async def delete(self, task_id: str) -> None:
        r = await get_redis()
        async with r.pipeline(transaction=False) as pipe:
            pipe.delete(self._key(task_id))
            pipe.srem(TASK_INDEX_KEY, task_id)
            await pipe.execute()


task_store = TaskStore()
