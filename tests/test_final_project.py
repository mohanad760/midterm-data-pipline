"""
Unit Tests for Final Project Phase 2
Tests:
1. Queries and Index definitions.
2. 5 Aggregation reports.
3. Materialized Views incremental refresh and queries.
4. Scheduled Jobs manual execution and logging.
5. FastAPI endpoints via TestClient.
"""

import pytest
from fastapi.testclient import TestClient
from src.api import app
from src.queries_and_indexes import (
    create_project_indexes,
    INDEX_DEFINITIONS,
    QUERIES_CATALOG,
    query_orders_by_city_and_status,
    query_orders_by_customer,
    query_high_value_orders,
)
from src.aggregations import (
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
)
from src.scheduler import (
    JOBS_REGISTRY,
    run_job_manually,
    get_job_logs,
)


@pytest.fixture(scope="module")
def api_client():
    return TestClient(app)


def test_index_definitions_count():
    assert len(INDEX_DEFINITIONS) >= 3
    compound_indexes = [idx for idx in INDEX_DEFINITIONS if idx["type"] == "compound"]
    assert len(compound_indexes) >= 1


def test_create_indexes():
    results = create_project_indexes()
    assert len(results) >= 3
    for r in results:
        assert r["status"] == "CREATED"


def test_queries_catalog_count():
    assert len(QUERIES_CATALOG) >= 5


def test_execute_queries():
    res1 = query_orders_by_city_and_status(limit=5)
    assert isinstance(res1, list)

    res2 = query_orders_by_customer(limit=5)
    assert isinstance(res2, list)

    res3 = query_high_value_orders(min_amount=10000.0, limit=5)
    assert isinstance(res3, list)


def test_aggregation_reports():
    city_rep = report_sales_by_city(limit=5)
    assert isinstance(city_rep, list)

    prod_rep = report_top_products(limit=5)
    assert isinstance(prod_rep, list)

    cust_rep = report_top_customers(limit=5)
    assert isinstance(cust_rep, list)

    period_rep = report_sales_by_period(limit=5)
    assert isinstance(period_rep, list)

    status_rep = report_orders_by_status()
    assert isinstance(status_rep, list)


def test_materialized_views_refresh():
    # Incremental refresh test
    res = refresh_all_materialized_views(incremental=True)
    assert "daily_sales_summary" in res
    assert "top_products_summary" in res
    assert res["daily_sales_summary"]["status"] in ["SUCCESS", "UP_TO_DATE"]
    assert res["top_products_summary"]["status"] in ["SUCCESS", "UP_TO_DATE"]

    # Verify view readers return records
    days = get_daily_sales_view(limit=5)
    assert isinstance(days, list)

    prods = get_top_products_view(limit=5)
    assert isinstance(prods, list)


def test_scheduled_jobs_manual_run():
    assert len(JOBS_REGISTRY) >= 2

    # Run Job 1 manually
    res1 = run_job_manually("refresh_materialized_views")
    assert res1["status"] == "SUCCESS"
    assert res1["job_name"] == "refresh_materialized_views"

    # Run Job 2 manually
    res2 = run_job_manually("generate_executive_report")
    assert res2["status"] == "SUCCESS"
    assert res2["job_name"] == "generate_executive_report"

    # Verify MongoDB job_logs has entries
    logs = get_job_logs(limit=5)
    assert len(logs) >= 2


def test_api_health_endpoint(api_client):
    r = api_client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] in ["HEALTHY", "DEGRADED"]
    assert "database" in data


def test_api_queries_endpoints(api_client):
    r = api_client.get("/queries")
    assert r.status_code == 200
    assert r.json()["queries_count"] >= 5

    r = api_client.get("/queries/orders_by_city_and_status?limit=5")
    assert r.status_code == 200

    r = api_client.get("/queries/orders_by_city_and_status/explain")
    assert r.status_code == 200
    assert "explain_summary" in r.json()


def test_api_aggregations_endpoints(api_client):
    r = api_client.get("/aggregations")
    assert r.status_code == 200
    assert r.json()["reports_count"] >= 5

    r = api_client.get("/aggregations/sales_by_city?limit=5")
    assert r.status_code == 200
    assert "data" in r.json()


def test_api_materialized_views_endpoints(api_client):
    r = api_client.post("/refresh-mv", json={"incremental": True})
    assert r.status_code == 200
    assert r.json()["status"] == "SUCCESS"

    r = api_client.get("/materialized-views/daily_sales_summary?limit=5")
    assert r.status_code == 200
    assert "data" in r.json()


def test_api_jobs_endpoints(api_client):
    r = api_client.get("/jobs")
    assert r.status_code == 200
    assert "registered_jobs" in r.json()

    r = api_client.post("/jobs/refresh_materialized_views/run")
    assert r.status_code == 200
    assert r.json()["status"] == "SUCCESS"
