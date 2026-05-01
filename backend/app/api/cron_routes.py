"""
Cron management API.
  POST   /crons          create
  GET    /crons          list all
  GET    /crons/{id}     fetch one
  PATCH  /crons/{id}     enable/disable or update
  DELETE /crons/{id}     remove
"""
from typing import List
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from ..models.cron import CronCreate, CronSchedule
from ..db.cron_store import cron_store

router = APIRouter(prefix="/crons", tags=["crons"])


class CronPatch(BaseModel):
    enabled: bool | None = None
    priority: int | None = None


@router.post("", response_model=CronSchedule, status_code=status.HTTP_201_CREATED)
async def create_cron(req: CronCreate) -> CronSchedule:
    try:
        CronSchedule.validate_expr(req.cron_expr)
    except ValueError as e:
        raise HTTPException(400, str(e))
    cron = CronSchedule(**req.model_dump())
    await cron_store.save(cron)
    return cron


@router.get("", response_model=List[CronSchedule])
async def list_crons() -> List[CronSchedule]:
    return await cron_store.list_all()


@router.get("/{cron_id}", response_model=CronSchedule)
async def get_cron(cron_id: str) -> CronSchedule:
    cron = await cron_store.get(cron_id)
    if not cron:
        raise HTTPException(404, "Cron not found")
    return cron


@router.patch("/{cron_id}", response_model=CronSchedule)
async def patch_cron(cron_id: str, patch: CronPatch) -> CronSchedule:
    cron = await cron_store.get(cron_id)
    if not cron:
        raise HTTPException(404, "Cron not found")
    if patch.enabled is not None:
        cron.enabled = patch.enabled
    if patch.priority is not None:
        cron.priority = patch.priority
    await cron_store.save(cron)         # save() rewrites the schedule ZSET
    return cron


@router.delete("/{cron_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_cron(cron_id: str) -> None:
    await cron_store.delete(cron_id)
