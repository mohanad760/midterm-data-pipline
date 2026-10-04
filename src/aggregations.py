"""
Aggregation Reports Module (Final Project - Phase 2)
Provides 5 independent, robust MongoDB aggregation pipelines:
1. sales_by_city: Revenue and order volume per city.
2. top_products: Top-selling products by quantity and revenue (unwinds items).
3. top_customers: Highest spending customers and order frequency.
4. sales_by_period: Daily sales trends, order volume, and daily averages.
5. orders_by_status: Workflow distribution of orders and revenue across statuses.
"""

import sys
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from pymongo import ASCENDING, DESCENDING

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import settings
from src.mongo_setup import get_mongo_client, get_database


def report_sales_by_city(limit: int = 20, db=None) -> List[Dict[str, Any]]:
    """
    Aggregation Report 1: Sales by City
    Calculates total revenue, order count, average order value, and min/max order per city.
    """
    if db is None:
        client = get_mongo_client()
        db = get_database(client)

    pipeline = [
        {
            "$group": {
                "_id": "$city",
                "total_revenue": {"$sum": "$total_amount"},
                "order_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"},
                "min_order_value": {"$min": "$total_amount"},
                "max_order_value": {"$max": "$total_amount"},
            }
        },
        {"$sort": {"total_revenue": DESCENDING}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "city": "$_id",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "order_count": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "min_order_value": {"$round": ["$min_order_value", 2]},
                "max_order_value": {"$round": ["$max_order_value", 2]},
            }
        },
    ]

    return list(db[settings.VALIDATED_COLLECTION].aggregate(pipeline))


def report_top_products(limit: int = 15, db=None) -> List[Dict[str, Any]]:
    """
    Aggregation Report 2: Top Selling Products
    Unwinds the items array, aggregates total quantity sold and revenue generated per product.
    """
    if db is None:
        client = get_mongo_client()
        db = get_database(client)

    pipeline = [
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": "$items.name",
                "sku": {"$first": "$items.sku"},
                "total_quantity_sold": {"$sum": "$items.qty"},
                "total_revenue": {"$sum": "$items.total"},
                "order_appearances": {"$sum": 1},
                "avg_unit_price": {"$avg": "$items.unit_price"},
            }
        },
        {"$sort": {"total_revenue": DESCENDING}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "product_name": "$_id",
                "sku": 1,
                "total_quantity_sold": 1,
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "order_appearances": 1,
                "avg_unit_price": {"$round": ["$avg_unit_price", 2]},
            }
        },
    ]

    return list(db[settings.VALIDATED_COLLECTION].aggregate(pipeline))


def report_top_customers(limit: int = 15, db=None) -> List[Dict[str, Any]]:
    """
    Aggregation Report 3: Top Customers
    Identifies top spending clients, their order counts, and favorite payment methods.
    """
    if db is None:
        client = get_mongo_client()
        db = get_database(client)

    pipeline = [
        {
            "$group": {
                "_id": "$customer_id",
                "customer_name": {"$first": "$customer_name"},
                "city": {"$first": "$city"},
                "total_spent": {"$sum": "$total_amount"},
                "order_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"},
                "last_order_date": {"$max": "$order_date"},
            }
        },
        {"$sort": {"total_spent": DESCENDING}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "customer_id": "$_id",
                "customer_name": 1,
                "city": 1,
                "total_spent": {"$round": ["$total_spent", 2]},
                "order_count": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "last_order_date": 1,
            }
        },
    ]

    return list(db[settings.VALIDATED_COLLECTION].aggregate(pipeline))


def report_sales_by_period(limit: int = 30, db=None) -> List[Dict[str, Any]]:
    """
    Aggregation Report 4: Sales by Period (Daily Sales Trend)
    Groups orders by calendar date (extracted from ISO order_date string) to analyze daily revenue.
    """
    if db is None:
        client = get_mongo_client()
        db = get_database(client)

    pipeline = [
        {
            "$project": {
                "order_day": {"$substrCP": ["$order_date", 0, 10]},
                "total_amount": 1,
                "status": 1,
            }
        },
        {
            "$group": {
                "_id": "$order_day",
                "daily_revenue": {"$sum": "$total_amount"},
                "order_count": {"$sum": 1},
                "avg_daily_order": {"$avg": "$total_amount"},
            }
        },
        {"$sort": {"_id": ASCENDING}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "date": "$_id",
                "daily_revenue": {"$round": ["$daily_revenue", 2]},
                "order_count": 1,
                "avg_daily_order": {"$round": ["$avg_daily_order", 2]},
            }
        },
    ]

    return list(db[settings.VALIDATED_COLLECTION].aggregate(pipeline))


def report_orders_by_status(db=None) -> List[Dict[str, Any]]:
    """
    Aggregation Report 5: Orders Distribution by Status
    Groups orders by workflow status and calculates volume, revenue, and delivery cost per status.
    """
    if db is None:
        client = get_mongo_client()
        db = get_database(client)

    pipeline = [
        {
            "$group": {
                "_id": "$status",
                "order_count": {"$sum": 1},
                "total_revenue": {"$sum": "$total_amount"},
                "total_delivery_fees": {"$sum": "$delivery_cost"},
                "avg_order_value": {"$avg": "$total_amount"},
            }
        },
        {"$sort": {"order_count": DESCENDING}},
        {
            "$project": {
                "_id": 0,
                "status": "$_id",
                "order_count": 1,
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "total_delivery_fees": {"$round": ["$total_delivery_fees", 2]},
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
            }
        },
    ]

    return list(db[settings.VALIDATED_COLLECTION].aggregate(pipeline))


AGGREGATIONS_CATALOG = {
    "sales_by_city": {
        "title": "Sales by City",
        "description": "Total revenue, order count, and average order value per city",
        "func": report_sales_by_city,
        "default_params": {"limit": 20},
    },
    "top_products": {
        "title": "Top Selling Products",
        "description": "Top products by quantity sold and gross revenue (unwinds items)",
        "func": report_top_products,
        "default_params": {"limit": 15},
    },
    "top_customers": {
        "title": "Top Customers",
        "description": "Highest spending customers with order count and frequency",
        "func": report_top_customers,
        "default_params": {"limit": 15},
    },
    "sales_by_period": {
        "title": "Sales by Daily Period",
        "description": "Daily revenue trends, order counts, and daily basket size",
        "func": report_sales_by_period,
        "default_params": {"limit": 30},
    },
    "orders_by_status": {
        "title": "Orders by Status Distribution",
        "description": "Distribution of orders, total value, and delivery fees per workflow status",
        "func": report_orders_by_status,
        "default_params": {},
    },
}


def run_all_aggregations() -> Dict[str, Any]:
    """Runs all 5 aggregation reports and prints a summary."""
    client = get_mongo_client()
    db = get_database(client)

    print("=" * 70)
    print("           EXECUTING 5 ADVANCED AGGREGATION REPORTS           ")
    print("=" * 70)

    summary = {}
    for name, meta in AGGREGATIONS_CATALOG.items():
        print(f"\n[Report: {meta['title']}]")
        data = meta["func"](db=db)
        summary[name] = data
        print(f"- Records Returned: {len(data)}")
        if data:
            print(f"- Sample Row: {data[0]}")

    client.close()
    return summary


if __name__ == "__main__":
    run_all_aggregations()
