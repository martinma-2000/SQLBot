from __future__ import annotations

from typing import List

from fastapi import APIRouter, Request, HTTPException
import json
from common.core.config import settings
from apps.scheduler.scheduler import _exec_bi_excel_job, _exec_fetch_api_job

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


@router.post("/run")
async def run_job(request: Request):
    """
    手动触发一次指定的调度任务。

    请求体支持两种方式：
    - {"id": "job_id"}: 从配置中查找该任务并执行一次
    - {"conf": {...}}: 直接传入一次性任务配置并执行（不注册定时）
    - 可选 {"overrides": {...}}: 在执行前覆盖配置里的字段（例如 date_m、file_ids 等）
    """
    app = request.app
    data = await request.json()
    job_id = data.get("id")
    conf = data.get("conf")
    overrides = data.get("overrides") or {}

    if conf is None and job_id:
        raw = settings.API_FETCH_JOBS
        try:
            jobs = json.loads(raw) if raw else []
            for j in jobs:
                if j.get("id") == job_id:
                    conf = j
                    break
        except Exception:
            conf = None

    if not conf:
        raise HTTPException(status_code=400, detail="缺少有效的任务配置：请提供 id 或 conf")

    # 应用覆盖项
    if overrides:
        conf = {**conf, **overrides}

    jtype = (conf.get("type") or "").lower()
    if jtype in ("bi_excel", "bi_excel_process"):
        await _exec_bi_excel_job(app, conf)
    else:
        await _exec_fetch_api_job(app, conf)

    return {"status": "ok", "id": job_id or conf.get("id")}
