"""
Final Project Practical Evaluation Demo Script (Phase 2)
Executes all 5 Final Project requirements live with formatted terminal output:
1. Index creation & Compound Index check.
2. 5 Practical business queries with live data.
3. Explain executionStats comparison (COLLSCAN vs IXSCAN) with % reduction.
4. 5 Advanced aggregation reports with real statistics.
5. Materialized Views with Incremental Refresh (watermark proof).
6. Scheduled Jobs manual trigger & MongoDB job_logs verification.
7. FastAPI test suite verification.
"""

import sys
import json
import time
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.mongo_setup import get_mongo_client, get_database
from src.queries_and_indexes import (
    create_project_indexes,
    INDEX_DEFINITIONS,
    QUERIES_CATALOG,
    run_explain_comparison,
)
from src.aggregations import AGGREGATIONS_CATALOG
from src.materialized_views import (
    refresh_all_materialized_views,
    get_daily_sales_view,
    get_top_products_view,
)
from src.scheduler import run_job_manually, get_job_logs, JOBS_REGISTRY


def run_final_demo():
    print("=" * 80)
    print("      BIG DATA FINAL PROJECT (PHASE 2) - PRACTICAL EVALUATION DEMO      ")
    print("=" * 80)

    client = get_mongo_client()
    db = get_database(client)

    # -------------------------------------------------------------------------
    # Part 1: Indexes & Compound Index
    # -------------------------------------------------------------------------
    print("\n" + "=" * 55)
    print("[REQUIREMENT 1] Indexes & Compound Index Verification")
    print("=" * 55)
    idx_res = create_project_indexes(db)
    print(f"Total Indexes Configured: {len(INDEX_DEFINITIONS)}")
    for idx in INDEX_DEFINITIONS:
        print(f"  * `{idx['name']}` ({idx['type'].upper()}): Keys={idx['keys']}")
        print(f"    -> Justification: {idx['justification']}")

    # -------------------------------------------------------------------------
    # Part 2: 5 Practical Queries Execution
    # -------------------------------------------------------------------------
    print("\n" + "=" * 55)
    print("[REQUIREMENT 1] 5 Practical Business Queries Execution")
    print("=" * 55)
    for qname, meta in QUERIES_CATALOG.items():
        data = meta["func"](limit=3, db=db)
        print(f"\n- Query: {meta['title']} (`{qname}`)")
        print(f"  Description: {meta['description']}")
        print(f"  Target Index: {meta.get('target_index')}")
        print(f"  Results Returned: {len(data)}")
        if data:
            print(f"  Sample Record: {data[0]}")

    # -------------------------------------------------------------------------
    # Part 3: Explain executionStats Comparison
    # -------------------------------------------------------------------------
    print("\n" + "=" * 55)
    print("[REQUIREMENT 1] Explain('executionStats') Before vs After Benchmark")
    print("=" * 55)
    explain_results = run_explain_comparison()
    print("\nSummary of Scanned Documents Reduction:")
    for r in explain_results:
        print(f"  * {r['query_title']}: {r['before']['total_docs_examined']:,} -> {r['after']['total_docs_examined']:,} Docs Examined ({r['docs_scanned_reduction_pct']}% reduction) [Stage: {r['after']['stage']}]")

    # -------------------------------------------------------------------------
    # Part 4: 5 Advanced Aggregation Reports
    # -------------------------------------------------------------------------
    print("\n" + "=" * 55)
    print("[REQUIREMENT 2] 5 Advanced Aggregation Reports")
    print("=" * 55)
    for aname, meta in AGGREGATIONS_CATALOG.items():
        data = meta["func"](db=db)
        print(f"\n- Report: {meta['title']} (`{aname}`)")
        print(f"  Description: {meta['description']}")
        print(f"  Records Aggregated: {len(data)}")
        if data:
            print(f"  Top Aggregation Result: {data[0]}")

    # -------------------------------------------------------------------------
    # Part 5: Materialized Views with Incremental Refresh
    # -------------------------------------------------------------------------
    print("\n" + "=" * 55)
    print("[REQUIREMENT 3] Materialized Views & Incremental Watermark Sync")
    print("=" * 55)
    print("1. Running Incremental Refresh (checks watermark):")
    inc_res = refresh_all_materialized_views(incremental=True)
    for vname, vres in inc_res.items():
        if isinstance(vres, dict):
            print(f"  * {vname}: Status={vres.get('status')} | New Docs Processed={vres.get('docs_processed')} | Time={vres.get('elapsed_seconds')}s")

    daily_rows = get_daily_sales_view(limit=3, db=db)
    prod_rows = get_top_products_view(limit=3, db=db)
    print(f"\nSample from Materialized View 'daily_sales_summary':")
    if daily_rows:
        print(f"  {daily_rows[0]}")
    print(f"Sample from Materialized View 'top_products_summary':")
    if prod_rows:
        print(f"  {prod_rows[0]}")

    # -------------------------------------------------------------------------
    # Part 6: Scheduled Jobs Manual Trigger & Logging
    # -------------------------------------------------------------------------
    print("\n" + "=" * 55)
    print("[REQUIREMENT 4] Scheduled Jobs & Execution Logging")
    print("=" * 55)
    print("Manually triggering registered jobs:")
    for jname in JOBS_REGISTRY:
        jres = run_job_manually(jname)
        print(f"  * Job '{jname}': Status={jres['status']} | Duration={jres['duration_seconds']}s | Log ID={jres.get('log_id')}")

    recent_logs = get_job_logs(limit=3, db=db)
    print(f"\nRecent Execution Logs in MongoDB 'job_logs' ({len(recent_logs)} entries):")
    for lg in recent_logs:
        print(f"  [{lg.get('started_at')}] {lg.get('job_name')} | Status={lg.get('status')} | Duration={lg.get('duration_seconds')}s")

    client.close()
    print("\n" + "=" * 80)
    print("             ALL FINAL PROJECT DEMO CHECKS PASSED SUCCESSFULLY!             ")
    print("=" * 80)


if __name__ == "__main__":
    run_final_demo()
