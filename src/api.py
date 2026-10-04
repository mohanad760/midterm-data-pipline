"""
Unified FastAPI Interface Module (Final Project - Phase 2)
Provides standardized REST API for evaluation, testing, and operations:
- GET  /health: System and database health status
- POST /ingest: Run data pipeline ingestion & ELT (Midterm entry gate)
- POST /indexes: Create query optimization indexes
- GET  /queries: List available practical business queries
- GET  /queries/{name}: Execute a specific query
- GET  /queries/{name}/explain: Get executionStats plan for a query
- GET  /aggregations: List 5 aggregation reports
- GET  /aggregations/{name}: Execute a specific aggregation report
- POST /refresh-mv: Trigger incremental refresh of Materialized Views
- GET  /materialized-views/{name}: Inspect Materialized View records
- GET  /jobs: List scheduled jobs and execution history
- POST /jobs/{name}/run: Manually trigger a scheduled job
- Swagger docs at /docs
"""

import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query, Body, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import settings
from src.mongo_setup import get_mongo_client, get_database
from src.main import run_pipeline
from src.queries_and_indexes import (
    create_project_indexes,
    INDEX_DEFINITIONS,
    QUERIES_CATALOG,
    extract_explain_stats,
)
from src.aggregations import (
    AGGREGATIONS_CATALOG,
    report_sales_by_city,
    report_top_products,
    report_top_customers,
    report_sales_by_period,
    report_orders_by_status,
)
from src.materialized_views import (
    refresh_all_materialized_views,
    get_daily_sales_view,
    get_top_products_view,
    MV_DAILY_SALES,
    MV_TOP_PRODUCTS,
)
from src.scheduler import (
    JOBS_REGISTRY,
    run_job_manually,
    get_job_logs,
    start_scheduler,
    stop_scheduler,
)

SERVER_START_TIME = time.time()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan manager for background scheduler initialization."""
    print("[API] Starting background scheduler...")
    try:
        start_scheduler()
    except Exception as e:
        print(f"[API] Warning: could not start scheduler: {e}")
    yield
    print("[API] Stopping background scheduler...")
    stop_scheduler()


app = FastAPI(
    title="Big Data Hybrid Pipeline API",
    description="Unified execution and evaluation interface for Midterm and Final Big Data Project (Phase 2).",
    version="2.0.0",
    lifespan=lifespan,
)


# -------------------------------------------------------------------------
# Pydantic Request Models
# -------------------------------------------------------------------------

class IngestRequest(BaseModel):
    file_path: str = Field(
        default=settings.DEFAULT_SAMPLE_INPUT,
        description="Path to CSV input dataset"
    )
    engine: str = Field(
        default="auto",
        description="Execution engine ('auto', 'python_batch', or 'pyspark')"
    )
    batch_size: int = Field(
        default=settings.BATCH_SIZE,
        description="Batch size for bulk operations"
    )
    limit: Optional[int] = Field(
        default=None,
        description="Optional limit on rows to process (useful for testing)"
    )


class RefreshMVRequest(BaseModel):
    incremental: bool = Field(
        default=True,
        description="True for incremental delta sync using watermarks, False for full rebuild"
    )


# -------------------------------------------------------------------------
# 1. Health Endpoint
# -------------------------------------------------------------------------

@app.get("/health", tags=["System Health"], summary="Check system and MongoDB connection health")
def health_check():
    """Returns database connection status, collection document counts, and system uptime."""
    client = get_mongo_client()
    db = get_database(client)
    try:
        db.command("ping")
        db_status = "CONNECTED"
    except Exception as e:
        db_status = f"ERROR: {e}"

    raw_count = db[settings.RAW_COLLECTION].estimated_document_count()
    val_count = db[settings.VALIDATED_COLLECTION].count_documents({})
    quar_count = db[settings.QUARANTINE_COLLECTION].count_documents({})
    client.close()

    uptime = round(time.time() - SERVER_START_TIME, 2)
    return {
        "status": "HEALTHY" if db_status == "CONNECTED" else "DEGRADED",
        "database": {
            "name": settings.DATABASE_NAME,
            "status": db_status,
            "counts": {
                "orders_raw": raw_count,
                "orders_validated": val_count,
                "orders_quarantine": quar_count,
            },
        },
        "server": {
            "uptime_seconds": uptime,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    }


# -------------------------------------------------------------------------
# 2. Ingest Endpoint (Midterm Pipeline Gate)
# -------------------------------------------------------------------------

@app.post("/ingest", tags=["Pipeline Ingestion"], summary="Execute End-to-End Pipeline Ingestion & ELT")
def trigger_ingest(payload: IngestRequest = Body(...)):
    """
    Executes the Hybrid Pipeline (Python Batch or PySpark):
    1. Routes file by size.
    2. Loads raw data into orders_raw.
    3. Transforms, cleans, and applies idempotent upsert into orders_validated and orders_quarantine.
    4. Computes consistency and metrics.
    """
    input_file = Path(payload.file_path)
    if not input_file.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Input dataset not found at: {payload.file_path}"
        )

    engine_arg = None if payload.engine == "auto" else payload.engine
    try:
        metrics = run_pipeline(
            input_file=str(input_file),
            engine_override=engine_arg,
            batch_size=payload.batch_size,
            limit=payload.limit,
        )
        return {
            "status": "SUCCESS",
            "message": "Pipeline executed and metrics saved.",
            "metrics": metrics,
        }
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


# -------------------------------------------------------------------------
# 3. Indexes Endpoint
# -------------------------------------------------------------------------

@app.post("/indexes", tags=["Queries & Indexes"], summary="Create query optimization indexes")
def create_indexes():
    """Creates all defined project indexes (including Compound Index) on orders_validated."""
    results = create_project_indexes()
    return {
        "status": "SUCCESS",
        "message": f"Processed {len(results)} index definitions.",
        "indexes": results,
    }


# -------------------------------------------------------------------------
# 4. Queries Endpoints
# -------------------------------------------------------------------------

@app.get("/queries", tags=["Queries & Indexes"], summary="List available practical business queries")
def list_queries():
    """Lists available queries with metadata, parameters, and target indexes."""
    catalog = {}
    for k, v in QUERIES_CATALOG.items():
        catalog[k] = {
            "title": v["title"],
            "description": v["description"],
            "default_params": v["default_params"],
            "target_index": v.get("target_index"),
        }
    return {"queries_count": len(catalog), "queries": catalog}


@app.get("/queries/{name}", tags=["Queries & Indexes"], summary="Execute a specific practical query")
def execute_query(
    name: str,
    limit: int = Query(50, description="Max records to return"),
    city: Optional[str] = Query(None, description="City filter for orders_by_city_and_status"),
    order_status: Optional[str] = Query(None, alias="status", description="Status filter"),
    customer_id: Optional[str] = Query(None, description="Customer ID filter"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    min_amount: Optional[float] = Query(None, description="Minimum order amount"),
    payment_method: Optional[str] = Query(None, description="Payment method"),
    payment_status: Optional[str] = Query(None, description="Payment status"),
):
    """Executes the specified business query with optional filter parameters."""
    if name not in QUERIES_CATALOG:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Query '{name}' not found. Available: {list(QUERIES_CATALOG.keys())}"
        )

    client = get_mongo_client()
    db = get_database(client)

    try:
        if name == "orders_by_city_and_status":
            c = city or "صنعاء"
            s = order_status or "مكتمل"
            data = query_orders_by_city_and_status_dyn(c, s, limit, db)
        elif name == "orders_by_customer":
            cid = customer_id or "عميل-1"
            data = QUERIES_CATALOG[name]["func"](customer_id=cid, limit=limit, db=db)
        elif name == "orders_by_date_range":
            s_date = start_date or "2025-01-01"
            e_date = end_date or "2025-01-31"
            data = QUERIES_CATALOG[name]["func"](start_date=s_date, end_date=e_date, limit=limit, db=db)
        elif name == "high_value_orders":
            m_amt = min_amount if min_amount is not None else 300000.0
            data = QUERIES_CATALOG[name]["func"](min_amount=m_amt, limit=limit, db=db)
        elif name == "orders_by_payment_method":
            pm = payment_method or "محفظة إلكترونية"
            ps = payment_status or "مدفوع"
            data = QUERIES_CATALOG[name]["func"](payment_method=pm, payment_status=ps, limit=limit, db=db)
        else:
            data = QUERIES_CATALOG[name]["func"](limit=limit, db=db)

        return {
            "query_name": name,
            "title": QUERIES_CATALOG[name]["title"],
            "records_returned": len(data),
            "data": data,
        }
    finally:
        client.close()


def query_orders_by_city_and_status_dyn(city: str, order_status: str, limit: int, db):
    col = db[settings.VALIDATED_COLLECTION]
    filter_spec = {"city": city, "status": order_status}
    projection = {
        "_id": 0, "order_id": 1, "order_date": 1, "customer_name": 1,
        "city": 1, "status": 1, "total_amount": 1, "payment_method": 1
    }
    return list(col.find(filter_spec, projection).limit(limit))


@app.get("/queries/{name}/explain", tags=["Queries & Indexes"], summary="Explain executionStats plan for a query")
def explain_query(name: str):
    """Runs explain() executionStats on the given query using existing indexes."""
    if name not in QUERIES_CATALOG:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Query '{name}' not found. Available: {list(QUERIES_CATALOG.keys())}"
        )

    client = get_mongo_client()
    db = get_database(client)
    col = db[settings.VALIDATED_COLLECTION]

    sample_doc = col.find_one() or {}
    sample_city = sample_doc.get("city", "صنعاء")
    sample_status = sample_doc.get("status", "مكتمل")
    sample_cust = sample_doc.get("customer_id", "عميل-1")

    if name == "orders_by_city_and_status":
        cursor = col.find({"city": sample_city, "status": sample_status}).limit(100)
    elif name == "orders_by_customer":
        cursor = col.find({"customer_id": sample_cust}).sort("order_date", -1).limit(100)
    elif name == "high_value_orders":
        cursor = col.find({"total_amount": {"$gte": 200000.0}}).sort("total_amount", -1).limit(100)
    elif name == "orders_by_date_range":
        cursor = col.find({"order_date": {"$gte": "2025-01-01T00:00:00"}}).sort("order_date", 1).limit(100)
    else:
        cursor = col.find({}).limit(100)

    plan = cursor.explain()
    stats = extract_explain_stats(plan)
    client.close()

    return {
        "query_name": name,
        "title": QUERIES_CATALOG[name]["title"],
        "target_index": QUERIES_CATALOG[name].get("target_index"),
        "explain_summary": stats,
    }


# -------------------------------------------------------------------------
# 5. Aggregations Endpoints
# -------------------------------------------------------------------------

@app.get("/aggregations", tags=["Aggregation Reports"], summary="List available aggregation reports")
def list_aggregations():
    """Lists available 5 aggregation reports with descriptions."""
    catalog = {}
    for k, v in AGGREGATIONS_CATALOG.items():
        catalog[k] = {
            "title": v["title"],
            "description": v["description"],
            "default_params": v["default_params"],
        }
    return {"reports_count": len(catalog), "reports": catalog}


@app.get("/aggregations/{name}", tags=["Aggregation Reports"], summary="Execute a specific aggregation report")
def execute_aggregation(
    name: str,
    limit: int = Query(20, description="Max rows to return")
):
    """Executes a specific aggregation report on orders_validated."""
    if name not in AGGREGATIONS_CATALOG:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Aggregation '{name}' not found. Available: {list(AGGREGATIONS_CATALOG.keys())}"
        )

    client = get_mongo_client()
    db = get_database(client)
    try:
        func = AGGREGATIONS_CATALOG[name]["func"]
        if "limit" in AGGREGATIONS_CATALOG[name]["default_params"]:
            data = func(limit=limit, db=db)
        else:
            data = func(db=db)

        return {
            "report_name": name,
            "title": AGGREGATIONS_CATALOG[name]["title"],
            "count": len(data),
            "data": data,
        }
    finally:
        client.close()


# -------------------------------------------------------------------------
# 6. Materialized Views Endpoints
# -------------------------------------------------------------------------

@app.post("/refresh-mv", tags=["Materialized Views"], summary="Trigger incremental or full refresh of Materialized Views")
def trigger_refresh_mv(payload: RefreshMVRequest = Body(...)):
    """Refreshes daily_sales_summary and top_products_summary using watermarks."""
    res = refresh_all_materialized_views(incremental=payload.incremental)
    return {
        "status": "SUCCESS",
        "message": f"Materialized Views refreshed in {'INCREMENTAL' if payload.incremental else 'FULL'} mode.",
        "views": res,
    }


@app.get("/materialized-views/{name}", tags=["Materialized Views"], summary="Inspect records in a Materialized View")
def get_materialized_view(
    name: str,
    limit: int = Query(30, description="Max rows to return")
):
    """Returns stored pre-aggregated rows from daily_sales_summary or top_products_summary."""
    client = get_mongo_client()
    db = get_database(client)
    try:
        if name in ["daily_sales_summary", MV_DAILY_SALES]:
            data = get_daily_sales_view(limit=limit, db=db)
        elif name in ["top_products_summary", MV_TOP_PRODUCTS]:
            data = get_top_products_view(limit=limit, db=db)
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Materialized View '{name}' not found. Available: ['daily_sales_summary', 'top_products_summary']"
            )
        return {
            "view_name": name,
            "count": len(data),
            "data": data,
        }
    finally:
        client.close()


# -------------------------------------------------------------------------
# 7. Scheduled Jobs Endpoints
# -------------------------------------------------------------------------

@app.get("/jobs", tags=["Scheduled Jobs"], summary="List scheduled jobs and execution history")
def list_jobs(limit: int = Query(10, description="Max recent log entries")):
    """Lists registered scheduled jobs and recent execution history from MongoDB job_logs."""
    jobs_info = {}
    for k, v in JOBS_REGISTRY.items():
        jobs_info[k] = {
            "title": v["title"],
            "description": v["description"],
            "interval_minutes": v["interval_minutes"],
        }

    logs = get_job_logs(limit=limit)
    return {
        "registered_jobs": jobs_info,
        "recent_execution_logs": logs,
    }


@app.post("/jobs/{name}/run", tags=["Scheduled Jobs"], summary="Manually trigger a scheduled job")
def run_job(name: str):
    """Manually triggers any registered scheduled job for oral discussion or on-demand testing."""
    if name not in JOBS_REGISTRY:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{name}' not found. Available: {list(JOBS_REGISTRY.keys())}"
        )

    res = run_job_manually(name)
    return {
        "status": "SUCCESS",
        "message": f"Job '{name}' executed successfully.",
        "execution_summary": res,
    }


# -------------------------------------------------------------------------
# Main Entry Point for CLI
# -------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api:app", host="127.0.0.1", port=8000, reload=False)
