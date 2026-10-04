"""
Queries and Indexes Module (Final Project - Phase 2)
Provides:
1. 5 Practical business queries on orders_validated.
2. Index definitions including at least one Compound Index.
3. Automated explain('executionStats') comparison before and after indexes.
"""

import sys
import time
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from pymongo import MongoClient, ASCENDING, DESCENDING

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import settings
from src.mongo_setup import get_mongo_client, get_database


INDEX_DEFINITIONS = [
    {
        "name": "idx_city_status",
        "keys": [("city", ASCENDING), ("status", ASCENDING)],
        "type": "compound",
        "justification": "Compound index optimizes queries filtering by geographic location (city) and workflow state (status) together, avoiding memory sorting and full collection scans.",
    },
    {
        "name": "idx_customer_date",
        "keys": [("customer_id", ASCENDING), ("order_date", DESCENDING)],
        "type": "compound",
        "justification": "Compound index on customer_id and order_date allows instant retrieval of customer purchase history pre-sorted by date.",
    },
    {
        "name": "idx_order_date",
        "keys": [("order_date", ASCENDING)],
        "type": "single",
        "justification": "Single field index supports fast range queries for financial periods and daily reporting.",
    },
    {
        "name": "idx_total_amount",
        "keys": [("total_amount", DESCENDING)],
        "type": "single",
        "justification": "Single field index allows high-value order filtering and top-revenue sorting without in-memory sorting.",
    },
]


def create_project_indexes(db=None) -> List[Dict[str, Any]]:
    """Creates the defined indexes on orders_validated and returns creation results."""
    if db is None:
        client = get_mongo_client()
        db = get_database(client)

    col = db[settings.VALIDATED_COLLECTION]
    results = []

    for idx in INDEX_DEFINITIONS:
        name = idx["name"]
        keys = idx["keys"]
        try:
            created_name = col.create_index(keys, name=name, background=True)
            results.append({"name": created_name, "status": "CREATED", "type": idx["type"]})
        except Exception as e:
            results.append({"name": name, "status": "ERROR", "error": str(e)})

    return results


def drop_project_indexes(db=None):
    """Drops the custom project indexes (used during explain comparison)."""
    if db is None:
        client = get_mongo_client()
        db = get_database(client)

    col = db[settings.VALIDATED_COLLECTION]
    for idx in INDEX_DEFINITIONS:
        try:
            col.drop_index(idx["name"])
        except Exception:
            pass


# -------------------------------------------------------------------------
# 5 Practical Business Queries
# -------------------------------------------------------------------------

def query_orders_by_city_and_status(
    city: str = "صنعاء",
    status: str = "مكتمل",
    limit: int = 50,
    db=None
) -> List[Dict[str, Any]]:
    """Query 1: Filter orders by city and status (Uses compound index idx_city_status)."""
    if db is None:
        client = get_mongo_client()
        db = get_database(client)

    col = db[settings.VALIDATED_COLLECTION]
    filter_spec = {"city": city, "status": status}
    projection = {
        "_id": 0, "order_id": 1, "order_date": 1, "customer_name": 1,
        "city": 1, "status": 1, "total_amount": 1, "payment_method": 1
    }
    return list(col.find(filter_spec, projection).limit(limit))


def query_orders_by_customer(
    customer_id: str = "عميل-1",
    limit: int = 20,
    db=None
) -> List[Dict[str, Any]]:
    """Query 2: Order history for a customer sorted by order_date desc (Uses idx_customer_date)."""
    if db is None:
        client = get_mongo_client()
        db = get_database(client)

    col = db[settings.VALIDATED_COLLECTION]
    filter_spec = {"customer_id": customer_id}
    projection = {
        "_id": 0, "order_id": 1, "order_date": 1, "customer_id": 1,
        "customer_name": 1, "status": 1, "total_amount": 1
    }
    return list(col.find(filter_spec, projection).sort("order_date", DESCENDING).limit(limit))


def query_orders_by_date_range(
    start_date: str = "2025-01-01",
    end_date: str = "2025-01-31",
    limit: int = 50,
    db=None
) -> List[Dict[str, Any]]:
    """Query 3: Filter orders within a date range (Uses idx_order_date)."""
    if db is None:
        client = get_mongo_client()
        db = get_database(client)

    col = db[settings.VALIDATED_COLLECTION]
    filter_spec = {
        "order_date": {
            "$gte": f"{start_date}T00:00:00",
            "$lte": f"{end_date}T23:59:59"
        }
    }
    projection = {
        "_id": 0, "order_id": 1, "order_date": 1, "customer_name": 1,
        "city": 1, "total_amount": 1
    }
    return list(col.find(filter_spec, projection).sort("order_date", ASCENDING).limit(limit))


def query_high_value_orders(
    min_amount: float = 300000.0,
    limit: int = 50,
    db=None
) -> List[Dict[str, Any]]:
    """Query 4: Filter high-value VIP orders above threshold (Uses idx_total_amount)."""
    if db is None:
        client = get_mongo_client()
        db = get_database(client)

    col = db[settings.VALIDATED_COLLECTION]
    filter_spec = {"total_amount": {"$gte": min_amount}}
    projection = {
        "_id": 0, "order_id": 1, "customer_name": 1, "city": 1,
        "total_amount": 1, "payment_method": 1
    }
    return list(col.find(filter_spec, projection).sort("total_amount", DESCENDING).limit(limit))


def query_orders_by_payment_method(
    payment_method: str = "محفظة إلكترونية",
    payment_status: str = "مدفوع",
    limit: int = 50,
    db=None
) -> List[Dict[str, Any]]:
    """Query 5: Filter orders by payment method and payment verification status."""
    if db is None:
        client = get_mongo_client()
        db = get_database(client)

    col = db[settings.VALIDATED_COLLECTION]
    filter_spec = {"payment_method": payment_method, "payment_status": payment_status}
    projection = {
        "_id": 0, "order_id": 1, "order_date": 1, "customer_name": 1,
        "payment_method": 1, "payment_status": 1, "payment_amount": 1
    }
    return list(col.find(filter_spec, projection).limit(limit))


QUERIES_CATALOG = {
    "orders_by_city_and_status": {
        "title": "Orders by City and Status",
        "description": "Filters orders by city and workflow status (Compound Index: city + status)",
        "func": query_orders_by_city_and_status,
        "default_params": {"city": "صنعاء", "status": "مكتمل", "limit": 50},
        "target_index": "idx_city_status"
    },
    "orders_by_customer": {
        "title": "Customer Order History",
        "description": "Retrieves orders for a customer sorted by date descending (Compound Index: customer_id + order_date)",
        "func": query_orders_by_customer,
        "default_params": {"customer_id": "عميل-1", "limit": 20},
        "target_index": "idx_customer_date"
    },
    "orders_by_date_range": {
        "title": "Orders in Date Range",
        "description": "Filters orders placed within a given date period (Single Index: order_date)",
        "func": query_orders_by_date_range,
        "default_params": {"start_date": "2025-01-01", "end_date": "2025-01-31", "limit": 50},
        "target_index": "idx_order_date"
    },
    "high_value_orders": {
        "title": "High Value Orders",
        "description": "Filters high-value orders sorted by total amount descending (Single Index: total_amount)",
        "func": query_high_value_orders,
        "default_params": {"min_amount": 300000.0, "limit": 50},
        "target_index": "idx_total_amount"
    },
    "orders_by_payment_method": {
        "title": "Orders by Payment Method and Status",
        "description": "Filters orders by payment channel and settlement status",
        "func": query_orders_by_payment_method,
        "default_params": {"payment_method": "محفظة إلكترونية", "payment_status": "مدفوع", "limit": 50},
        "target_index": None
    },
}


# -------------------------------------------------------------------------
# Explain ExecutionStats Comparison (Before & After Indexes)
# -------------------------------------------------------------------------

def extract_explain_stats(explain_plan: Dict[str, Any]) -> Dict[str, Any]:
    """Helper to extract essential executionStats from MongoDB explain plan."""
    stats = explain_plan.get("executionStats", {})
    stages = stats.get("executionStages", {})

    stage_type = stages.get("stage", "UNKNOWN")
    # Dig into nested input stages if wrapped in LIMIT or FETCH
    current = stages
    stage_path = [stage_type]
    while "inputStage" in current and isinstance(current["inputStage"], dict):
        current = current["inputStage"]
        stage_path.append(current.get("stage", ""))
    
    full_stage = " -> ".join([s for s in stage_path if s])

    return {
        "stage": full_stage,
        "primary_stage": stage_path[-1] if stage_path else stage_type,
        "execution_time_ms": stats.get("executionTimeMillis", 0),
        "total_docs_examined": stats.get("totalDocsExamined", 0),
        "total_keys_examined": stats.get("totalKeysExamined", 0),
        "docs_returned": stats.get("nReturned", 0),
        "index_used": "IXSCAN" in full_stage,
    }


def run_explain_comparison(queries_to_test: List[str] = None) -> List[Dict[str, Any]]:
    """
    Executes explain('executionStats') for 3 queries BEFORE and AFTER index creation.
    Measures and documents execution time, scanned docs/keys, and stage (COLLSCAN vs IXSCAN).
    """
    if queries_to_test is None:
        queries_to_test = ["orders_by_city_and_status", "orders_by_customer", "high_value_orders"]

    client = get_mongo_client()
    db = get_database(client)
    col = db[settings.VALIDATED_COLLECTION]

    # Sample actual values from existing data to make sure queries return matches
    sample_doc = col.find_one() or {}
    sample_city = sample_doc.get("city", "صنعاء")
    sample_status = sample_doc.get("status", "مكتمل")
    sample_customer = sample_doc.get("customer_id", "عميل-1")

    comparison_results = []

    print("\n" + "=" * 70)
    print("   EXPLAIN('executionStats') BENCHMARK: BEFORE vs AFTER INDEXES   ")
    print("=" * 70)

    for qname in queries_to_test:
        meta = QUERIES_CATALOG[qname]
        target_idx_name = meta["target_index"]
        idx_def = next((i for i in INDEX_DEFINITIONS if i["name"] == target_idx_name), None)

        # 1. Drop target index to test BEFORE (COLLSCAN)
        if target_idx_name:
            try:
                col.drop_index(target_idx_name)
            except Exception:
                pass

        # Prepare cursor for explain
        if qname == "orders_by_city_and_status":
            filter_spec = {"city": sample_city, "status": sample_status}
            cursor_before = col.find(filter_spec).limit(100)
        elif qname == "orders_by_customer":
            filter_spec = {"customer_id": sample_customer}
            cursor_before = col.find(filter_spec).sort("order_date", DESCENDING).limit(100)
        elif qname == "high_value_orders":
            filter_spec = {"total_amount": {"$gte": 200000.0}}
            cursor_before = col.find(filter_spec).sort("total_amount", DESCENDING).limit(100)
        else:
            filter_spec = {}
            cursor_before = col.find(filter_spec).limit(100)

        # Explain BEFORE
        plan_before = cursor_before.explain()
        stats_before = extract_explain_stats(plan_before)

        # 2. Create target index to test AFTER (IXSCAN)
        if idx_def:
            col.create_index(idx_def["keys"], name=idx_def["name"])

        # Re-run cursor for AFTER
        if qname == "orders_by_city_and_status":
            cursor_after = col.find(filter_spec).limit(100)
        elif qname == "orders_by_customer":
            cursor_after = col.find(filter_spec).sort("order_date", DESCENDING).limit(100)
        elif qname == "high_value_orders":
            cursor_after = col.find(filter_spec).sort("total_amount", DESCENDING).limit(100)
        else:
            cursor_after = col.find(filter_spec).limit(100)

        plan_after = cursor_after.explain()
        stats_after = extract_explain_stats(plan_after)

        docs_reduction = 0
        if stats_before["total_docs_examined"] > 0:
            docs_reduction = round(
                (1 - (stats_after["total_docs_examined"] / stats_before["total_docs_examined"])) * 100, 2
            )

        item = {
            "query_name": qname,
            "query_title": meta["title"],
            "index_name": target_idx_name,
            "index_keys": idx_def["keys"] if idx_def else None,
            "index_type": idx_def["type"] if idx_def else "N/A",
            "justification": idx_def["justification"] if idx_def else "",
            "before": stats_before,
            "after": stats_after,
            "docs_scanned_reduction_pct": docs_reduction,
        }
        comparison_results.append(item)

        print(f"\n[Query: {meta['title']}]")
        print(f"- Index Tested: {target_idx_name} ({idx_def['type'] if idx_def else ''})")
        print(f"- BEFORE Index: Stage={stats_before['stage']} | Time={stats_before['execution_time_ms']}ms | DocsExamined={stats_before['total_docs_examined']:,}")
        print(f"- AFTER Index:  Stage={stats_after['stage']} | Time={stats_after['execution_time_ms']}ms | DocsExamined={stats_after['total_docs_examined']:,} | KeysExamined={stats_after['total_keys_examined']:,}")
        print(f"- Scanned Docs Reduction: {docs_reduction}%")
        print(f"- Justification: {item['justification']}")

    # Ensure all project indexes are created at the end
    create_project_indexes(db)
    client.close()

    # Save to reports/explain_results.json and .md
    save_explain_report(comparison_results)
    return comparison_results


def save_explain_report(results: List[Dict[str, Any]]):
    """Saves explain benchmark results to JSON and Markdown reports."""
    reports_dir = Path(settings.REPORTS_DIR)
    reports_dir.mkdir(parents=True, exist_ok=True)

    json_path = reports_dir / "explain_results.json"
    md_path = reports_dir / "explain_results.md"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    lines = [
        "# Query Performance & Explain Benchmark Report",
        "",
        f"**Date:** {time.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
        "## Summary Comparison (Before vs After Indexing)",
        "",
        "| Query | Target Index | Index Type | Before Stage | After Stage | Before Docs Scanned | After Docs Scanned | Docs Reduction |",
        "| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |",
    ]

    for r in results:
        lines.append(
            f"| **{r['query_title']}** | `{r['index_name']}` | {r['index_type']} | "
            f"`{r['before']['stage']}` | `{r['after']['stage']}` | "
            f"{r['before']['total_docs_examined']:,} | {r['after']['total_docs_examined']:,} | "
            f"**{r['docs_scanned_reduction_pct']}%** |"
        )

    lines.extend([
        "",
        "## Detailed Analysis & Index Justifications",
        ""
    ])

    for i, r in enumerate(results, 1):
        lines.extend([
            f"### {i}. {r['query_title']} (`{r['query_name']}`)",
            f"- **Target Index:** `{r['index_name']}` ({r['index_type']}) -> Keys: `{r['index_keys']}`",
            f"- **Why this index was chosen (Justification):** {r['justification']}",
            f"- **Performance Impact:**",
            f"  * Stage changed from full table scan (`{r['before']['stage']}`) to index tree scan (`{r['after']['stage']}`).",
            f"  * Total documents examined dropped from **{r['before']['total_docs_examined']:,}** to **{r['after']['total_docs_examined']:,}** ({r['docs_scanned_reduction_pct']}% reduction).",
            f"  * Index keys examined: **{r['after']['total_keys_examined']:,}**.",
            ""
        ])

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"\n[Explain Report] Successfully generated:")
    print(f"- JSON: {json_path}")
    print(f"- Markdown: {md_path}")


if __name__ == "__main__":
    create_project_indexes()
    run_explain_comparison()
