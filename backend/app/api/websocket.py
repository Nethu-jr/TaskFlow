"""
WebSocket bridge: Redis pub/sub -> connected browser clients.
Each client subscribes to itss:results and gets every TaskUpdate broadcast by workers.
"""
import asyncio
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from ..core.redis_client import get_redis
from ..core.config import settings
from ..core.logging import setup_logging

log = setup_logging("ws")
router = APIRouter()


@router.websocket("/ws/tasks")
async def task_stream(ws: WebSocket):
    await ws.accept()
    r = await get_redis()
    pubsub = r.pubsub()
    await pubsub.subscribe(settings.REDIS_RESULT_CHANNEL)
    log.info("ws_client_connected")
    try:
        async for msg in pubsub.listen():
            if msg["type"] != "message":
                continue
            try:
                await ws.send_text(msg["data"])
            except Exception:
                break
    except WebSocketDisconnect:
        pass
    finally:
        await pubsub.unsubscribe(settings.REDIS_RESULT_CHANNEL)
        await pubsub.close()
        log.info("ws_client_disconnected")
