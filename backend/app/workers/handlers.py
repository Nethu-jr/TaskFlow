"""
Handler registry. Each task type maps to an executor function.
In real systems, these dispatch to actual business logic (email send, ETL job, etc.).
For demonstration, handlers simulate work and occasionally fail.
"""
import asyncio
import random
from typing import Any, Callable, Awaitable

Handler = Callable[[dict[str, Any]], Awaitable[Any]]
_registry: dict[str, Handler] = {}


def register(task_type: str):
    def deco(fn: Handler) -> Handler:
        _registry[task_type] = fn
        return fn
    return deco


def get_handler(task_type: str) -> Handler:
    return _registry.get(task_type, _registry["generic"])


# ---------- Built-in demo handlers ----------

@register("email")
async def send_email(payload: dict) -> dict:
    await asyncio.sleep(random.uniform(0.2, 0.8))
    return {"sent_to": payload.get("to"), "status": "delivered"}


@register("report")
async def generate_report(payload: dict) -> dict:
    # Reports take longer
    await asyncio.sleep(random.uniform(1.0, 3.0))
    if random.random() < 0.15:        # 15% transient failure rate
        raise RuntimeError("Report DB temporarily unavailable")
    return {"report_id": payload.get("id"), "rows": random.randint(100, 10_000)}


@register("data_sync")
async def data_sync(payload: dict) -> dict:
    await asyncio.sleep(random.uniform(0.5, 2.0))
    return {"synced": payload.get("source"), "records": random.randint(50, 500)}


@register("ml_inference")
async def ml_inference(payload: dict) -> dict:
    await asyncio.sleep(random.uniform(0.1, 0.5))
    return {"prediction": random.random(), "model": "demo-v1"}


@register("generic")
async def generic(payload: dict) -> dict:
    await asyncio.sleep(0.1)
    return {"echoed": payload}
