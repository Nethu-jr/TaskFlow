"""
Single shared async Redis client. Connection pooling is handled by redis-py.
"""
import redis.asyncio as aioredis
from .config import settings

_redis: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    """Returns a process-wide Redis client. Lazy init for testability."""
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
            max_connections=50,         # tune based on worker concurrency
        )
    return _redis


async def close_redis() -> None:
    global _redis
    if _redis is not None:
        await _redis.close()
        _redis = None
