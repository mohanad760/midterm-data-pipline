"""
Scheduled Jobs Module (Final Project - Phase 2)
Provides:
1. Two automated scheduled jobs:
   - refresh_materialized_views_job: Periodically refreshes Materialized Views incrementally.
   - generate_executive_report_job: Runs all 5 aggregation reports and saves an executive summary JSON.
2. Comprehensive execution logging in MongoDB collection 'job_logs' (start, end, duration, status, details).
3. Ability to trigger each job manually via Python function, CLI, or FastAPI endpoint.
4. BackgroundScheduler integration using APScheduler.
"""

import sys
import time
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional
from apscheduler.schedulers.background import BackgroundScheduler

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import settings
from src.mongo_setup import get_mongo_client, get_database
from src.materialized_views import refresh_all_materialized_views
from src.aggregations import run_all_aggregations

JOB_LOGS_COLLECTION = "job_logs"


def log_job_execution(
    job_name: str,
    started_at: datetime,
    ended_at: datetime,
    status: str,
    trigger_type: str = "MANUAL",
    details: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
    db=None
) -> str:
    """Logs job run details into MongoDB job_logs collection."""
    if db is None:
        client = get_mongo_client()
        db = get_database(client)

    duration = round((ended_at - started_at).total_seconds(), 3)
    record = {
        "job_name": job_name,
        "trigger_type": trigger_type,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "duration_seconds": duration,
        "status": status,
        "details": details or {},
        "error": error,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }
    res = db[JOB_LOGS_COLLECTION].insert_one(record)
    return str(res.inserted_id)


# -------------------------------------------------------------------------
# Job 1: Materialized Views Incremental Refresh
# -------------------------------------------------------------------------

def job_refresh_materialized_views(trigger_type: str = "MANUAL", db=None) -> Dict[str, Any]:
    """Scheduled Job 1: Refreshes all Materialized Views incrementally."""
    started_at = datetime.now(timezone.utc)
    status = "SUCCESS"
    err_msg = None
    result = {}

    try:
        result = refresh_all_materialized_views(incremental=True)
    except Exception as e:
        status = "FAILED"
        err_msg = str(e)
        result = {"error": err_msg}

    ended_at = datetime.now(timezone.utc)
    log_id = log_job_execution(
        job_name="refresh_materialized_views",
        started_at=started_at,
        ended_at=ended_at,
        status=status,
        trigger_type=trigger_type,
        details=result,
        error=err_msg,
        db=db
    )

    return {
        "job_name": "refresh_materialized_views",
        "log_id": log_id,
        "status": status,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "duration_seconds": round((ended_at - started_at).total_seconds(), 3),
        "result": result,
    }


# -------------------------------------------------------------------------
# Job 2: Generate Periodic Executive Report
# -------------------------------------------------------------------------

def job_generate_executive_report(trigger_type: str = "MANUAL", db=None) -> Dict[str, Any]:
    """Scheduled Job 2: Executes all 5 aggregations and saves reports/executive_summary.json."""
    started_at = datetime.now(timezone.utc)
    status = "SUCCESS"
    err_msg = None
    result = {}

    try:
        report_data = run_all_aggregations()
        reports_dir = Path(settings.REPORTS_DIR)
        reports_dir.mkdir(parents=True, exist_ok=True)
        summary_path = reports_dir / "executive_summary.json"

        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "trigger_type": trigger_type,
            "metrics": report_data,
        }
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)

        result = {
            "summary_file": str(summary_path),
            "reports_compiled": list(report_data.keys()),
            "top_city": report_data.get("sales_by_city", [{}])[0].get("city", "N/A"),
            "top_product": report_data.get("top_products", [{}])[0].get("product_name", "N/A"),
        }
    except Exception as e:
        status = "FAILED"
        err_msg = str(e)
        result = {"error": err_msg}

    ended_at = datetime.now(timezone.utc)
    log_id = log_job_execution(
        job_name="generate_executive_report",
        started_at=started_at,
        ended_at=ended_at,
        status=status,
        trigger_type=trigger_type,
        details=result,
        error=err_msg,
        db=db
    )

    return {
        "job_name": "generate_executive_report",
        "log_id": log_id,
        "status": status,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "duration_seconds": round((ended_at - started_at).total_seconds(), 3),
        "result": result,
    }


JOBS_REGISTRY = {
    "refresh_materialized_views": {
        "title": "Materialized Views Incremental Refresh",
        "description": "Periodically synchronizes new validated orders into daily_sales_summary and top_products_summary.",
        "interval_minutes": 60,
        "func": job_refresh_materialized_views,
    },
    "generate_executive_report": {
        "title": "Periodic Executive Aggregation Report",
        "description": "Compiles all 5 aggregation reports into reports/executive_summary.json.",
        "interval_minutes": 120,
        "func": job_generate_executive_report,
    },
}


def run_job_manually(job_name: str) -> Dict[str, Any]:
    """Manually triggers any registered job by name for oral discussion / testing."""
    if job_name not in JOBS_REGISTRY:
        raise ValueError(f"Unknown job '{job_name}'. Available: {list(JOBS_REGISTRY.keys())}")
    func = JOBS_REGISTRY[job_name]["func"]
    return func(trigger_type="MANUAL")


def get_job_logs(limit: int = 20, db=None) -> List[Dict[str, Any]]:
    """Retrieves recent job execution history from MongoDB job_logs."""
    if db is None:
        client = get_mongo_client()
        db = get_database(client)

    logs = list(
        db[JOB_LOGS_COLLECTION]
        .find({}, {"_id": 0})
        .sort("started_at", -1)
        .limit(limit)
    )
    return logs


# -------------------------------------------------------------------------
# Background Scheduler Manager
# -------------------------------------------------------------------------

_scheduler_instance: Optional[BackgroundScheduler] = None


def start_scheduler() -> BackgroundScheduler:
    """Initializes and starts the APScheduler background scheduler."""
    global _scheduler_instance
    if _scheduler_instance is not None and _scheduler_instance.running:
        return _scheduler_instance

    scheduler = BackgroundScheduler(daemon=True)

    # Schedule Job 1: every 60 minutes
    scheduler.add_job(
        func=lambda: job_refresh_materialized_views(trigger_type="SCHEDULED"),
        trigger="interval",
        minutes=JOBS_REGISTRY["refresh_materialized_views"]["interval_minutes"],
        id="refresh_materialized_views",
        replace_existing=True,
    )

    # Schedule Job 2: every 120 minutes
    scheduler.add_job(
        func=lambda: job_generate_executive_report(trigger_type="SCHEDULED"),
        trigger="interval",
        minutes=JOBS_REGISTRY["generate_executive_report"]["interval_minutes"],
        id="generate_executive_report",
        replace_existing=True,
    )

    scheduler.start()
    _scheduler_instance = scheduler
    print("[Scheduler] APScheduler BackgroundScheduler started successfully.")
    return scheduler


def stop_scheduler():
    """Stops the scheduler gracefully."""
    global _scheduler_instance
    if _scheduler_instance and _scheduler_instance.running:
        _scheduler_instance.shutdown(wait=False)
        _scheduler_instance = None
        print("[Scheduler] BackgroundScheduler stopped.")


if __name__ == "__main__":
    print("Testing manual execution of scheduled jobs:")
    res1 = run_job_manually("refresh_materialized_views")
    print(f"- Job 1 Result: {res1['status']} in {res1['duration_seconds']}s")

    res2 = run_job_manually("generate_executive_report")
    print(f"- Job 2 Result: {res2['status']} in {res2['duration_seconds']}s")

    logs = get_job_logs(limit=5)
    print(f"\nRecent Job Logs in MongoDB ({len(logs)} entries):")
    for log in logs:
        print(f"  [{log.get('started_at')}] {log.get('job_name')} | Status: {log.get('status')} | Duration: {log.get('duration_seconds')}s")
