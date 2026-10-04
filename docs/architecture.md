# Architecture & Technical Design Document

## 1. Executive Summary
This project implements a high-performance **Hybrid Big Data Pipeline** following the modern **ELT (Extract, Load, Transform)** paradigm. It is engineered to process messy, multi-gigabyte e-commerce order records with resilient fault tolerance, automated routing, data quality cleansing, audit trail tracking, quarantine isolation, and guaranteed idempotency.

---

## 2. Architectural Flow Diagram

```
                 +-------------------------------------+
                 |  Incoming CSV Dataset               |
                 |  (orders_huge_mixed_quality.csv)    |
                 +------------------+------------------+
                                    |
                                    v
                     +------------------------------+
                     | File Router:                 |
                     | File Size <= 200 MB?         |
                     +--------------+---------------+
                                    |
                    +---------------+---------------+
                    |                               |
              [ YES ]                             [ NO ]
                    v                               v
    +-------------------------------+   +-------------------------------+
    | Python Batch Loader           |   | Apache Spark (PySpark)        |
    | - Streaming chunked reading   |   | - Distributed DataFrame API   |
    | - Low RAM & Zero JVM overhead |   | - Explicit String Schema      |
    | - Fast insert_many bulk ops   |   | - Parallel foreachPartition   |
    +---------------+---------------+   +---------------+---------------+
                    |                               |
                    +---------------+---------------+
                                    |
                                    v
                 +-------------------------------------+
                 | Raw Storage Layer (MongoDB)         |
                 | Collection: orders_raw              |
                 | - Schema-less ingest (accepts all)  |
                 | - Tracks run_id, row_no, timestamp  |
                 +------------------+------------------+
                                    |
                                    v
                 +-------------------------------------+
                 | ELT Transformation & Data Quality   |
                 | - 8+ Cleaning Rules                 |
                 | - Audit Trail generation            |
                 | - Unrecoverable error classification|
                 +------------------+------------------+
                                    |
                    +---------------+---------------+
                    |                               |
            [ Valid / Corrected ]             [ Unrecoverable ]
                    v                               v
    +-------------------------------+   +-------------------------------+
    | Collection: orders_validated  |   | Collection: orders_quarantine |
    | - Stable Key: order_id        |   | - Preserves raw_record        |
    | - Unique Index on order_id    |   | - error_codes & error_details |
    | - Idempotent Upsert ($set)    |   | - No silent loss of data      |
    +-------------------------------+   +-------------------------------+
                                    |
                                    v
                 +-------------------------------------+
                 | Performance & Audit Reporting       |
                 | reports/results.json & results.md   |
                 +-------------------------------------+
```

---

## 3. Key Design Decisions

### 3.1 Routing Threshold Justification (200 MB)
- **Why 200 MB?**
  - Small files ($\le 200\text{ MB}$): Spark involves JVM startup overhead, distributed scheduling, and inter-process communication latency (~15–30 seconds baseline). Python Batch with streaming `csv.DictReader` runs in $< 5$ seconds with $< 50\text{ MB}$ RAM footprint.
  - Large files ($> 200\text{ MB}$ up to multi-GB): Single-thread Python cannot saturate multi-core CPUs and faces potential memory limits. PySpark parallelizes data across worker partitions, scaling linearly with available CPU cores.

### 3.2 ELT vs ETL Principle
- **ELT:** All incoming records—regardless of corruption—are stored immediately into `orders_raw`.
- This ensures an unalterable historical audit trail and enables reprocessing without re-ingesting external files.

### 3.3 Idempotency & Upsert Architecture
- Primary Business Key: `order_id`.
- A **Unique Index** is enforced on `order_id` in `orders_validated`.
- Data writing uses MongoDB bulk `UpdateOne({"order_id": order_id}, {"$set": doc}, upsert=True)`.
- If a pipeline run is restarted or receives duplicated records, no duplicate keys are created, and business state remains stable.

### 3.4 Strict Mathematical Consistency Formula
Every pipeline run guarantees:
$$\text{run\_raw\_count} = \text{run\_valid\_count} + \text{run\_corrected\_count} + \text{run\_quarantine\_count}$$
Zero record drop policy is enforced.
