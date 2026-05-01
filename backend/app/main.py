"""
FastAPI app entrypoint.

Lifespan:
  startup -> start scheduler loop, retry drainer
  shutdown -> stop loop, close redis
"""
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.routes import router as tasks_router, stats_router
from .api.websocket import router as ws_router
from .api.cron_routes import router as cron_router
from .api.history_routes import router as history_router
from .scheduler.service import scheduler_service
from .scheduler.retry_drainer import drain_retries
from .scheduler.cron_ticker import cron_ticker
from .core.redis_client import close_redis
from .core.logging import setup_logging
from .db.audit import init_db, close_db, drain_audit_buffer

log = setup_logging("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ---- startup ----
    await init_db()
    await scheduler_service.start()
    await cron_ticker.start()
    retry_task = asyncio.create_task(drain_retries())
    audit_drain_task = asyncio.create_task(drain_audit_buffer())
    log.info("api_started")
    yield
    # ---- shutdown ----
    await cron_ticker.stop()
    await scheduler_service.stop()
    retry_task.cancel()
    audit_drain_task.cancel()
    await close_db()
    await close_redis()
    log.info("api_stopped")


app = FastAPI(title="ITSS — Intelligent Task Scheduler", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],            # tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(tasks_router)
app.include_router(stats_router)
app.include_router(cron_router)
app.include_router(history_router)
app.include_router(ws_router)


@app.get("/health")
async def health():
    return {"status": "ok"}
