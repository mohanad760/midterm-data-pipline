import argparse
import sys
import time
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings
from src.mongo_setup import init_database_schema, get_mongo_client, get_database
from src.file_router import route_file
from src.batch_loader import load_raw_batch
from src.spark_loader import load_raw_spark
from src.elt_pipeline import run_elt_pipeline
from src.metrics import save_run_metrics


def run_pipeline(
    input_file: str,
    engine_override: str = None,
    batch_size: int = settings.BATCH_SIZE,
    threshold_mb: float = settings.SMALL_FILE_THRESHOLD_MB,
    limit: int = None,
):
    """
    Main Orchestrator for the Hybrid Data Pipeline:
    1. Routes input file by size (Python Batch vs PySpark).
    2. Ingests all raw rows into orders_raw (Raw Layer).
    3. Transforms, Cleans, and Classifies rows into orders_validated (via Idempotent Upsert) and orders_quarantine.
    4. Computes end-to-end metrics and writes reports/results.json.
    """
    overall_start = time.time()
    input_path = Path(input_file)

    if not input_path.exists():
        raise FileNotFoundError(f"Input dataset not found at: {input_file}")

    print("=" * 70)
    print("      HYBRID DATA PIPELINE (Python Batch + Apache Spark + MongoDB)     ")
    print("=" * 70)

    # Step 1: Ensure MongoDB Collections and Indexes exist
    client = get_mongo_client()
    db = get_database(client)
    init_database_schema(db)
    client.close()

    # Step 2: Route file to appropriate engine
    auto_engine, file_size_mb, justification = route_file(str(input_path), threshold_mb)
    engine = engine_override if engine_override else auto_engine

    print(f"\n[Step 1: Engine Selection]")
    print(f"- File: {input_path.name}")
    print(f"- Size: {file_size_mb:.2f} MB")
    print(f"- Threshold: {threshold_mb:.2f} MB")
    print(f"- Selected Engine: {engine.upper()} {'(Manual Override)' if engine_override else '(Auto Selected)'}")
    print(f"- Justification: {justification}")

    # Step 3: Raw Ingestion Layer
    print(f"\n[Step 2: Raw Data Ingestion -> {settings.RAW_COLLECTION}]")
    if engine == "pyspark":
        raw_res = load_raw_spark(str(input_path), max_rows=limit)
    else:
        raw_res = load_raw_batch(str(input_path), batch_size=batch_size)

    run_id = raw_res["run_id"]
    raw_loaded = raw_res["raw_loaded"]

    # Step 4: ELT Transformation, Cleaning & Classification
    print(f"\n[Step 3: ELT Transformation, Cleaning & Classification]")
    elt_res = run_elt_pipeline(run_id, batch_size=batch_size)

    # Step 5: End-to-End Metrics Aggregation
    overall_elapsed = time.time() - overall_start
    overall_throughput = raw_loaded / overall_elapsed if overall_elapsed > 0 else 0

    full_metrics = {
        "run_id": run_id,
        "file_name": input_path.name,
        "file_size_mb": round(file_size_mb, 2),
        "engine_used": engine,
        "rows_read": raw_res["rows_read"],
        "raw_loaded": raw_loaded,
        "valid_count": elt_res["valid_count"],
        "corrected_count": elt_res["corrected_count"],
        "quarantine_count": elt_res["quarantine_count"],
        "elapsed_seconds": round(overall_elapsed, 2),
        "throughput": round(overall_throughput, 2),
        "batch_size": batch_size,
        "partitions": raw_res.get("partitions", 1),
        "error_case_counts": elt_res["error_case_counts"],
        "inserted_count": elt_res["inserted_count"],
        "updated_count": elt_res["updated_count"],
        "unchanged_count": elt_res["unchanged_count"],
        "is_consistent": elt_res["is_consistent"],
    }

    # Save to reports/results.json
    save_run_metrics(full_metrics)

    print("=" * 70)
    print("                    PIPELINE EXECUTION SUMMARY                    ")
    print("=" * 70)
    print(f"Run ID:             {run_id}")
    print(f"Engine:             {engine}")
    print(f"File:               {input_path.name} ({file_size_mb:.2f} MB)")
    print(f"Raw Loaded:         {raw_loaded:,}")
    print(f"Valid Records:      {elt_res['valid_count']:,}")
    print(f"Corrected Records:  {elt_res['corrected_count']:,}")
    print(f"Quarantined Records:{elt_res['quarantine_count']:,}")
    print(f"Upsert Inserts:     {elt_res['inserted_count']:,}")
    print(f"Upsert Updates:     {elt_res['updated_count']:,}")
    print(f"Upsert Unchanged:   {elt_res['unchanged_count']:,}")
    print(f"Total Time:         {overall_elapsed:.2f} seconds")
    print(f"Throughput:         {overall_throughput:,.0f} rows/second")
    print(f"Consistency Check:  {'PASSED' if elt_res['is_consistent'] else 'FAILED'}")
    print("=" * 70)

    return full_metrics


def main():
    parser = argparse.ArgumentParser(description="Hybrid Big Data Pipeline Runner")
    parser.add_argument(
        "--input",
        type=str,
        default=settings.DEFAULT_SAMPLE_INPUT,
        help="Path to CSV input data file."
    )
    parser.add_argument(
        "--engine",
        type=str,
        choices=["auto", "python_batch", "pyspark"],
        default="auto",
        help="Execution engine override."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=settings.BATCH_SIZE,
        help="Batch size for MongoDB bulk operations."
    )
    parser.add_argument(
        "--threshold-mb",
        type=float,
        default=settings.SMALL_FILE_THRESHOLD_MB,
        help="Threshold in MB to switch from Python Batch to PySpark."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of rows (useful when testing large files)."
    )

    args = parser.parse_args()
    engine_arg = None if args.engine == "auto" else args.engine

    run_pipeline(
        input_file=args.input,
        engine_override=engine_arg,
        batch_size=args.batch_size,
        threshold_mb=args.threshold_mb,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
