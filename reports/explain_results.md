# Query Performance & Explain Benchmark Report

**Date:** 2026-10-04 20:20:18 UTC

## Summary Comparison (Before vs After Indexing)

| Query | Target Index | Index Type | Before Stage | After Stage | Before Docs Scanned | After Docs Scanned | Docs Reduction |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Orders by City and Status** | `idx_city_status` | compound | `LIMIT -> COLLSCAN` | `LIMIT -> FETCH -> IXSCAN` | 6,199 | 100 | **98.39%** |
| **Customer Order History** | `idx_customer_date` | compound | `LIMIT -> FETCH -> IXSCAN` | `LIMIT -> FETCH -> IXSCAN` | 89,937 | 1 | **100.0%** |
| **High Value Orders** | `idx_total_amount` | single | `SORT -> COLLSCAN` | `LIMIT -> FETCH -> IXSCAN` | 89,937 | 100 | **99.89%** |

## Detailed Analysis & Index Justifications

### 1. Orders by City and Status (`orders_by_city_and_status`)
- **Target Index:** `idx_city_status` (compound) -> Keys: `[('city', 1), ('status', 1)]`
- **Why this index was chosen (Justification):** Compound index optimizes queries filtering by geographic location (city) and workflow state (status) together, avoiding memory sorting and full collection scans.
- **Performance Impact:**
  * Stage changed from full table scan (`LIMIT -> COLLSCAN`) to index tree scan (`LIMIT -> FETCH -> IXSCAN`).
  * Total documents examined dropped from **6,199** to **100** (98.39% reduction).
  * Index keys examined: **100**.

### 2. Customer Order History (`orders_by_customer`)
- **Target Index:** `idx_customer_date` (compound) -> Keys: `[('customer_id', 1), ('order_date', -1)]`
- **Why this index was chosen (Justification):** Compound index on customer_id and order_date allows instant retrieval of customer purchase history pre-sorted by date.
- **Performance Impact:**
  * Stage changed from full table scan (`LIMIT -> FETCH -> IXSCAN`) to index tree scan (`LIMIT -> FETCH -> IXSCAN`).
  * Total documents examined dropped from **89,937** to **1** (100.0% reduction).
  * Index keys examined: **1**.

### 3. High Value Orders (`high_value_orders`)
- **Target Index:** `idx_total_amount` (single) -> Keys: `[('total_amount', -1)]`
- **Why this index was chosen (Justification):** Single field index allows high-value order filtering and top-revenue sorting without in-memory sorting.
- **Performance Impact:**
  * Stage changed from full table scan (`SORT -> COLLSCAN`) to index tree scan (`LIMIT -> FETCH -> IXSCAN`).
  * Total documents examined dropped from **89,937** to **100** (99.89% reduction).
  * Index keys examined: **100**.
