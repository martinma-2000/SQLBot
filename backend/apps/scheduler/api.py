from __future__ import annotations

from typing import List

from fastapi import APIRouter, Request

router = APIRouter(tags=["scheduler"], prefix="/scheduler")


@router.get("/status")
async def status(request: Request):
    scheduler = getattr(request.app.state, "scheduler", None)
    running = bool(scheduler and scheduler.running)
    return {"running": running}


@router.get("/jobs")
async def jobs(request: Request) -> List[dict]:
    scheduler = getattr(request.app.state, "scheduler", None)
    if not scheduler:
        return []
    data = []
    for job in scheduler.get_jobs():
        data.append({
            "id": job.id,
            "name": job.name,
            "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
        })
    return data
