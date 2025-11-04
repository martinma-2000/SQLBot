from __future__ import annotations

from typing import Any
from collections import deque
from datetime import datetime, date, timedelta
import calendar
from zoneinfo import ZoneInfo
import os

from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from common.utils.utils import SQLBotLogUtil
from common.core.config import settings
import json
from apps.datasource.api.datasource import FetchApiRequest, fetch_excel_from_api


def _log_tick(app: FastAPI | None = None, source: str | None = None):
    SQLBotLogUtil.info("[APS] demo tick executed")
    try:
        if app is not None:
            events = getattr(app.state, "scheduler_events", None)
            if events is not None:
                tz = ZoneInfo("Asia/Shanghai")
                ts = datetime.now(tz)
                events.append({
                    "ts": ts.isoformat(),
                    "source": source or "demo_tick",
                    "message": "demo tick executed",
                })
    except Exception as e:
        SQLBotLogUtil.error(f"[APS] failed to record event: {e}")


def setup_scheduler(app: FastAPI) -> AsyncIOScheduler:
    """Initialize and start an AsyncIO scheduler, attach to app.state.

    - Uses Asia/Shanghai timezone by default.
    - Adds a demo interval job so you can immediately observe execution.
    """
    tz = ZoneInfo("Asia/Shanghai")
    scheduler = AsyncIOScheduler(timezone=tz)

    # Attach so API handlers can inspect/control
    app.state.scheduler = scheduler
    # In-memory event history (newest appended at tail)
    app.state.scheduler_events = deque(maxlen=200)

    # Heartbeat job: align frequency with first ingestion job (cron or interval); fallback to 1 minute
    heartbeat_trigger = IntervalTrigger(minutes=1, timezone=scheduler.timezone)
    try:
        raw = settings.API_FETCH_JOBS
        if raw:
            jobs = json.loads(raw)
            if isinstance(jobs, list) and jobs:
                job0 = jobs[0]
                if job0.get("cron"):
                    fields = job0["cron"].split()
                    if len(fields) == 5:
                        heartbeat_trigger = CronTrigger(
                            minute=fields[0],
                            hour=fields[1],
                            day=fields[2],
                            month=fields[3],
                            day_of_week=fields[4],
                            timezone=scheduler.timezone,
                        )
                elif job0.get("interval_minutes"):
                    heartbeat_trigger = IntervalTrigger(
                        minutes=int(job0["interval_minutes"]),
                        timezone=scheduler.timezone,
                    )
    except Exception as e:
        SQLBotLogUtil.error(f"[APS] failed to derive heartbeat interval: {e}")

    try:
        scheduler.add_job(
            _log_tick,
            trigger=heartbeat_trigger,
            id="demo_tick",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            kwargs={"app": app, "source": "demo_tick"},
        )
    except Exception as e:
        SQLBotLogUtil.error(f"[APS] failed to add heartbeat job: {e}")

    scheduler.start()
    SQLBotLogUtil.info("✅ APScheduler started")

    # Register API fetch jobs from settings (if any)
    try:
        register_api_fetch_jobs(app)
    except Exception as e:
        SQLBotLogUtil.error(f"[APS] failed to register API fetch jobs: {e}")

    # Daily cleanup job for processed Excel files
    try:
        def _cleanup_excel_dir():
            dir_path = settings.EXCEL_PATH
            retention_days = int(getattr(settings, "EXCEL_RETENTION_DAYS", 7))
            now = datetime.now(scheduler.timezone)
            removed = 0
            try:
                for fname in os.listdir(dir_path):
                    low = fname.lower()
                    if not (low.endswith("_processed.xlsx") or low.endswith("_processed.xls")):
                        continue
                    fpath = os.path.join(dir_path, fname)
                    try:
                        mtime = datetime.fromtimestamp(os.stat(fpath).st_mtime, tz=scheduler.timezone)
                        if now - mtime >= timedelta(days=retention_days):
                            os.remove(fpath)
                            removed += 1
                    except Exception:
                        # ignore individual file errors
                        continue
            except Exception as e:
                SQLBotLogUtil.error(f"[APS] cleanup failed: {e}")
                return
            SQLBotLogUtil.info(f"[APS] cleanup finished, removed {removed} files older than {retention_days} days")

        cleanup_trigger = CronTrigger(minute=0, hour=3, timezone=scheduler.timezone)
        scheduler.add_job(
            _cleanup_excel_dir,
            trigger=cleanup_trigger,
            id="cleanup_excel_files",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
        )
        SQLBotLogUtil.info("[APS] registered daily cleanup job for processed Excel files")
    except Exception as e:
        SQLBotLogUtil.error(f"[APS] failed to add cleanup job: {e}")
    return scheduler


def add_cron_demo_job(app: FastAPI, cron: str, job_id: str = "cron_demo") -> None:
    """Add a cron-based demo job with a standard Cron expression.

    Example cron: "*/5 * * * *" (every 5 minutes)
    """
    scheduler: AsyncIOScheduler = getattr(app.state, "scheduler", None)
    if not scheduler:
        raise RuntimeError("scheduler not initialized")

    try:
        fields = cron.split()
        if len(fields) != 5:
            raise ValueError("cron must have 5 fields: min hour dom mon dow")
        trigger = CronTrigger(
            minute=fields[0], hour=fields[1], day=fields[2], month=fields[3], day_of_week=fields[4],
            timezone=scheduler.timezone,
        )
        scheduler.add_job(
            _log_tick,
            trigger=trigger,
            id=job_id,
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            kwargs={"app": app, "source": job_id},
        )
        SQLBotLogUtil.info(f"[APS] added cron demo job '{job_id}' -> {cron}")
    except Exception as e:
        SQLBotLogUtil.error(f"[APS] failed to add cron demo job: {e}")


async def _exec_fetch_api_job(app: FastAPI, conf: dict):
    """Execute an API fetch ingestion job using existing datasource logic."""
    try:
        # Auto-compute monthly p_date_m (YYYY-MM) and date_m (YYYY-MM-DD, last day)
        tz = ZoneInfo("Asia/Shanghai")
        period_type = (conf.get("period_type") or "month").lower()
        p_date_m = conf.get("p_date_m")
        date_m = conf.get("date_m")
        period = conf.get("period")
        if period_type == "month" and (p_date_m is None or date_m is None):
            # Resolve target year/month
            now = datetime.now(tz)
            # Optional month offset: default to previous month for stable monthly ingestion
            month_offset = int(conf.get("month_offset", -1))

            # If explicit period provided, parse it; otherwise apply offset relative to now
            y, m = None, None
            if isinstance(period, str) and period:
                try:
                    s = period.strip()
                    if len(s) == 7 and s[4] == "-":  # YYYY-MM
                        y, m = int(s[:4]), int(s[5:7])
                    elif len(s) == 6 and s.isdigit():  # YYYYMM
                        y, m = int(s[:4]), int(s[4:6])
                    elif len(s) == 10 and s[4] == "-" and s[7] == "-":  # YYYY-MM-DD
                        y, m = int(s[:4]), int(s[5:7])
                    else:
                        y, m = None, None
                except Exception:
                    y, m = None, None
            if y is None or m is None:
                # Apply month offset relative to current month
                y, m = now.year, now.month
                # Compute shifted month/year
                total = y * 12 + (m - 1) + month_offset
                y = total // 12
                m = total % 12 + 1

            # Last day of target month
            last_day = calendar.monthrange(y, m)[1]
            p_date_m_auto = f"{y:04d}-{m:02d}"
            date_m_auto = f"{y:04d}-{m:02d}-{last_day:02d}"
            period_auto = f"{y:04d}{m:02d}"
            p_date_m = p_date_m or p_date_m_auto
            date_m = date_m or date_m_auto
            period = period or period_auto

        req = FetchApiRequest(
            endpoint=conf.get("endpoint"),
            method=conf.get("method", "GET"),
            date_m=date_m,
            p_date_m=p_date_m,
            period_type=conf.get("period_type"),
            period=period,
            headerKey=conf.get("headerKey"),
            headerValue=conf.get("headerValue"),
            cookieKey=conf.get("cookieKey"),
            cookieValue=conf.get("cookieValue"),
            paramKey=conf.get("paramKey"),
            paramValue=conf.get("paramValue"),
            timeout=int(conf.get("timeout", 30)),
            separator=conf.get("separator", "_")
        )
        # call route function directly (session unused in implementation)
        result = await fetch_excel_from_api(None, req)
        # record outcome
        events = getattr(app.state, "scheduler_events", None)
        if events is not None:
            tz = ZoneInfo("Asia/Shanghai")
            ts = datetime.now(tz)
            table = None
            try:
                sheets = result.get("sheets", []) if isinstance(result, dict) else []
                if sheets:
                    table = sheets[0].get("tableName")
            except Exception:
                pass
            events.append({
                "ts": ts.isoformat(),
                "source": conf.get("id", "api_fetch"),
                "message": f"ingested period {period} to {table or 'unknown_table'}",
            })
        SQLBotLogUtil.info(f"[APS] API fetch job '{conf.get('id')}' executed")
    except Exception as e:
        SQLBotLogUtil.error(f"[APS] API fetch job error: {e}")


def register_api_fetch_jobs(app: FastAPI) -> None:
    """Register ingestion jobs defined via settings.API_FETCH_JOBS (JSON)."""
    raw = settings.API_FETCH_JOBS
    if not raw:
        SQLBotLogUtil.info("[APS] no API_FETCH_JOBS configured; skipping")
        return
    try:
        jobs = json.loads(raw)
        if not isinstance(jobs, list):
            raise ValueError("API_FETCH_JOBS must be a JSON array")
    except Exception as e:
        raise RuntimeError(f"invalid API_FETCH_JOBS JSON: {e}")

    scheduler: AsyncIOScheduler = getattr(app.state, "scheduler", None)
    if not scheduler:
        raise RuntimeError("scheduler not initialized")

    for job in jobs:
        # Avoid computing default via hash(job) because dicts are unhashable.
        # Prefer provided id; otherwise generate a stable fallback from the JSON string.
        job_id = job.get("id") or f"api_fetch_{abs(hash(json.dumps(job, sort_keys=True)))}"
        try:
            if "cron" in job and job["cron"]:
                fields = job["cron"].split()
                if len(fields) != 5:
                    raise ValueError("cron must have 5 fields: min hour dom mon dow")
                trigger = CronTrigger(
                    minute=fields[0], hour=fields[1], day=fields[2], month=fields[3], day_of_week=fields[4],
                    timezone=scheduler.timezone,
                )
            elif "interval_minutes" in job and job["interval_minutes"]:
                trigger = IntervalTrigger(minutes=int(job["interval_minutes"]), timezone=scheduler.timezone)
            else:
                raise ValueError("job must define 'cron' or 'interval_minutes'")

            scheduler.add_job(
                _exec_fetch_api_job,
                trigger=trigger,
                id=job_id,
                replace_existing=True,
                coalesce=True,
                max_instances=1,
                kwargs={"app": app, "conf": job},
            )
            SQLBotLogUtil.info(f"[APS] registered API fetch job '{job_id}'")
        except Exception as e:
            SQLBotLogUtil.error(f"[APS] failed to register job '{job_id}': {e}")