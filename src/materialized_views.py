"""
Materialized Views Module (Final Project - Phase 2)
Provides 2 Materialized Views with Incremental Refresh Mechanism:
1. daily_sales_summary (mv_daily_sales_summary):
   Aggregates total_sales, order_count, and avg_order_value per calendar day.
2. top_products_summary (mv_top_products_summary):
   Aggregates total_quantity, total_revenue, and order_appearances per product.

Incremental Logic:
- Stores sync watermark (last_synced_at) in 'mv_sync_metadata'.
- Queries only new/updated records in 'orders_validated' where validated_at > last_synced_at.
- Applies atomic upserts with $inc for additive metrics, preventing full table recomputation.
"""

import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional
from pymongo import UpdateOne, ASCENDING, DESCENDING

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import settings
from src.mongo_setup import get_mongo_client, get_database

MV_DAILY_SALES = "mv_daily_sales_summary"
MV_TOP_PRODUCTS = "mv_top_products_summary"
MV_METADATA = "mv_sync_metadata"


def get_watermark(db, view_name: str) -> Optional[str]:
    """Retrieves the last sync watermark timestamp for a given materialized view."""
    meta = db[MV_METADATA].find_one({"_id": view_name})
    if meta:
        return meta.get("last_synced_at")
    return None


def update_watermark(db, view_name: str, last_synced_at: str, processed_count: int):
    """Updates the watermark and sync statistics."""
    db[MV_METADATA].update_one(
        {"_id": view_name},
        {
            "$set": {
                "last_synced_at": last_synced_at,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
            "$inc": {
                "total_records_processed": processed_count,
                "sync_cycles_count": 1,
            },
        },
        upsert=True
    )


# -------------------------------------------------------------------------
# Materialized View 1: daily_sales_summary
# -------------------------------------------------------------------------

def refresh_daily_sales_summary(incremental: bool = True, db=None) -> Dict[str, Any]:
    """
    Refreshes the daily_sales_summary Materialized View.
    Supports both incremental refresh (using watermark) and full rebuild.
    """
    if db is None:
        client = get_mongo_client()
        db = get_database(client)

    start_time = time.time()
    val_col = db[settings.VALIDATED_COLLECTION]
    mv_col = db[MV_DAILY_SALES]

    # Ensure index on date
    mv_col.create_index([("date", ASCENDING)], unique=True, background=True)

    match_stage = {}
    last_synced = None
    if incremental:
        last_synced = get_watermark(db, "daily_sales_summary")
        if last_synced:
            match_stage = {"validated_at": {"$gt": last_synced}}
    else:
        # Full refresh: clear target collection and watermark
        mv_col.delete_many({})
        db[MV_METADATA].delete_one({"_id": "daily_sales_summary"})

    # Check how many docs to process
    docs_to_process = val_col.count_documents(match_stage)
    if docs_to_process == 0:
        return {
            "view_name": "daily_sales_summary",
            "mode": "incremental" if incremental else "full",
            "docs_processed": 0,
            "status": "UP_TO_DATE",
            "elapsed_seconds": round(time.time() - start_time, 3),
        }

    # Pipeline to aggregate daily sales for new slice
    pipeline = []
    if match_stage:
        pipeline.append({"$match": match_stage})

    pipeline.extend([
        {
            "$project": {
                "order_day": {"$substrCP": ["$order_date", 0, 10]},
                "total_amount": 1,
                "validated_at": 1,
            }
        },
        {
            "$group": {
                "_id": "$order_day",
                "slice_sales": {"$sum": "$total_amount"},
                "slice_count": {"$sum": 1},
                "max_validated_at": {"$max": "$validated_at"},
            }
        },
    ])

    cursor = val_col.aggregate(pipeline)
    operations = []
    max_val_at = last_synced

    for row in cursor:
        order_day = row["_id"]
        if not order_day or len(order_day) < 10:
            continue
        slice_sales = row["slice_sales"]
        slice_count = row["slice_count"]
        row_max_v = row.get("max_validated_at")
        if row_max_v and (max_val_at is None or row_max_v > max_val_at):
            max_val_at = row_max_v

        # Atomic Upsert using $inc
        op = UpdateOne(
            {"date": order_day},
            {
                "$inc": {
                    "total_sales": slice_sales,
                    "order_count": slice_count,
                },
                "$set": {
                    "last_refreshed_at": datetime.now(timezone.utc).isoformat(),
                },
            },
            upsert=True
        )
        operations.append(op)

    if operations:
        mv_col.bulk_write(operations, ordered=False)

        # Update average order value for touched days
        # avg_order_value = total_sales / order_count
        recalc_pipeline = [
            {
                "$project": {
                    "avg_order_value": {
                        "$cond": [
                            {"$gt": ["$order_count", 0]},
                            {"$round": [{"$divide": ["$total_sales", "$order_count"]}, 2]},
                            0
                        ]
                    }
                }
            }
        ]
        # Quick update of average for all days
        for doc in mv_col.find({}, {"_id": 1, "total_sales": 1, "order_count": 1}):
            cnt = doc.get("order_count", 0)
            sales = doc.get("total_sales", 0.0)
            avg_val = round(sales / cnt, 2) if cnt > 0 else 0.0
            mv_col.update_one({"_id": doc["_id"]}, {"$set": {"avg_order_value": avg_val, "total_sales": round(sales, 2)}})

    if max_val_at:
        update_watermark(db, "daily_sales_summary", max_val_at, docs_to_process)

    elapsed = time.time() - start_time
    return {
        "view_name": "daily_sales_summary",
        "mode": "incremental" if incremental else "full",
        "docs_processed": docs_to_process,
        "days_updated": len(operations),
        "total_days_in_view": mv_col.count_documents({}),
        "status": "SUCCESS",
        "elapsed_seconds": round(elapsed, 3),
    }


# -------------------------------------------------------------------------
# Materialized View 2: top_products_summary
# -------------------------------------------------------------------------

def refresh_top_products_summary(incremental: bool = True, db=None) -> Dict[str, Any]:
    """
    Refreshes the top_products_summary Materialized View.
    Supports both incremental refresh (using watermark) and full rebuild.
    """
    if db is None:
        client = get_mongo_client()
        db = get_database(client)

    start_time = time.time()
    val_col = db[settings.VALIDATED_COLLECTION]
    mv_col = db[MV_TOP_PRODUCTS]

    # Ensure index on product_name
    mv_col.create_index([("product_name", ASCENDING)], unique=True, background=True)
    mv_col.create_index([("total_revenue", DESCENDING)], background=True)

    match_stage = {}
    last_synced = None
    if incremental:
        last_synced = get_watermark(db, "top_products_summary")
        if last_synced:
            match_stage = {"validated_at": {"$gt": last_synced}}
    else:
        # Full refresh: clear target collection and watermark
        mv_col.delete_many({})
        db[MV_METADATA].delete_one({"_id": "top_products_summary"})

    docs_to_process = val_col.count_documents(match_stage)
    if docs_to_process == 0:
        return {
            "view_name": "top_products_summary",
            "mode": "incremental" if incremental else "full",
            "docs_processed": 0,
            "status": "UP_TO_DATE",
            "elapsed_seconds": round(time.time() - start_time, 3),
        }

    pipeline = []
    if match_stage:
        pipeline.append({"$match": match_stage})

    pipeline.extend([
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": "$items.name",
                "sku": {"$first": "$items.sku"},
                "slice_qty": {"$sum": "$items.qty"},
                "slice_revenue": {"$sum": "$items.total"},
                "slice_appearances": {"$sum": 1},
                "max_validated_at": {"$max": "$validated_at"},
            }
        },
    ])

    cursor = val_col.aggregate(pipeline)
    operations = []
    max_val_at = last_synced

    for row in cursor:
        product_name = row["_id"]
        sku = row.get("sku")
        slice_qty = row["slice_qty"]
        slice_rev = row["slice_revenue"]
        slice_app = row["slice_appearances"]
        row_max_v = row.get("max_validated_at")
        if row_max_v and (max_val_at is None or row_max_v > max_val_at):
            max_val_at = row_max_v

        op = UpdateOne(
            {"product_name": product_name},
            {
                "$inc": {
                    "total_quantity": slice_qty,
                    "total_revenue": slice_rev,
                    "order_appearances": slice_app,
                },
                "$set": {
                    "sku": sku,
                    "last_refreshed_at": datetime.now(timezone.utc).isoformat(),
                },
            },
            upsert=True
        )
        operations.append(op)

    if operations:
        mv_col.bulk_write(operations, ordered=False)

    if max_val_at:
        update_watermark(db, "top_products_summary", max_val_at, docs_to_process)

    elapsed = time.time() - start_time
    return {
        "view_name": "top_products_summary",
        "mode": "incremental" if incremental else "full",
        "docs_processed": docs_to_process,
        "products_updated": len(operations),
        "total_products_in_view": mv_col.count_documents({}),
        "status": "SUCCESS",
        "elapsed_seconds": round(elapsed, 3),
    }


def refresh_all_materialized_views(incremental: bool = True) -> Dict[str, Any]:
    """Refreshes all configured Materialized Views."""
    client = get_mongo_client()
    db = get_database(client)

    print("\n" + "=" * 70)
    print(f"   REFRESHING MATERIALIZED VIEWS (Mode: {'INCREMENTAL' if incremental else 'FULL'})   ")
    print("=" * 70)

    res1 = refresh_daily_sales_summary(incremental=incremental, db=db)
    print(f"- {res1['view_name']}: {res1['status']} | Processed: {res1.get('docs_processed', 0)} docs | Elapsed: {res1['elapsed_seconds']}s")

    res2 = refresh_top_products_summary(incremental=incremental, db=db)
    print(f"- {res2['view_name']}: {res2['status']} | Processed: {res2.get('docs_processed', 0)} docs | Elapsed: {res2['elapsed_seconds']}s")

    client.close()
    return {
        "daily_sales_summary": res1,
        "top_products_summary": res2,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# -------------------------------------------------------------------------
# Readers for Materialized Views
# -------------------------------------------------------------------------

def get_daily_sales_view(limit: int = 30, db=None) -> List[Dict[str, Any]]:
    """Returns rows from mv_daily_sales_summary sorted by date."""
    if db is None:
        client = get_mongo_client()
        db = get_database(client)
    return list(db[MV_DAILY_SALES].find({}, {"_id": 0}).sort("date", ASCENDING).limit(limit))


def get_top_products_view(limit: int = 15, db=None) -> List[Dict[str, Any]]:
    """Returns rows from mv_top_products_summary sorted by revenue descending."""
    if db is None:
        client = get_mongo_client()
        db = get_database(client)
    return list(db[MV_TOP_PRODUCTS].find({}, {"_id": 0}).sort("total_revenue", DESCENDING).limit(limit))


if __name__ == "__main__":
    refresh_all_materialized_views(incremental=False)
