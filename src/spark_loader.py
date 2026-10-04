import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, monotonically_increasing_id, current_timestamp, struct

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import settings


CSV_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_phone", StringType(), True),
    StructField("customer_email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("district", StringType(), True),
    StructField("delivery_type", StringType(), True),
    StructField("delivery_cost", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("items_json", StringType(), True),
])


def get_spark_session() -> SparkSession:
    """Initialize SparkSession with optimized single-machine/multi-core configuration."""
    spark = (
        SparkSession.builder
        .appName(settings.SPARK_APP_NAME)
        .master(settings.SPARK_MASTER)
        .config("spark.driver.memory", settings.SPARK_DRIVER_MEMORY)
        .config("spark.driver.maxResultSize", "4g")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.shuffle.partitions", settings.SPARK_SHUFFLE_PARTITIONS)
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.ui.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def make_partition_writer(mongo_uri: str, db_name: str, coll_name: str, batch_size: int, counter=None):
    """Factory creating partition writer for PyMongo bulk write."""
    def write_partition(partition_rows):
        from pymongo import MongoClient

        client = MongoClient(
            mongo_uri,
            maxPoolSize=getattr(settings, "MONGODB_MAX_POOL_SIZE", 200),
            socketTimeoutMS=getattr(settings, "MONGODB_SOCKET_TIMEOUT_MS", 600000),
            serverSelectionTimeoutMS=5000,
        )
        try:
            col = client[db_name][coll_name]
            buffer = []
            for row in partition_rows:
                # Convert row into dictionary
                row_dict = row.asDict(recursive=True)
                buffer.append(row_dict)
                if len(buffer) >= batch_size:
                    for attempt in range(3):
                        try:
                            col.insert_many(buffer, ordered=False)
                            break
                        except Exception as e:
                            if attempt == 2:
                                raise
                            import time
                            time.sleep(2 ** attempt)
                    if counter is not None:
                        counter.add(len(buffer))
                    buffer = []
            if buffer:
                for attempt in range(3):
                    try:
                        col.insert_many(buffer, ordered=False)
                        break
                    except Exception as e:
                        if attempt == 2:
                            raise
                        import time
                        time.sleep(2 ** attempt)
                if counter is not None:
                    counter.add(len(buffer))
        finally:
            client.close()

    return write_partition


def load_raw_spark(file_path: str, run_id: str = None, max_rows: int = None) -> Dict[str, Any]:
    """
    Executes PySpark Parallel Loading:
    - Reads large CSV using explicit string schema without inferSchema overhead.
    - Adds ELT metadata columns.
    - Writes partitions in parallel to MongoDB orders_raw.
    - Tracks partitions, elapsed seconds, throughput.
    """
    if run_id is None:
        run_id = f"spark_{uuid.uuid4().hex[:12]}"

    print(f"\n[PySpark Loader] Starting Parallel Raw Ingestion")
    print(f"- Run ID: {run_id}")
    print(f"- Source File: {file_path}")
    print(f"- Master: {settings.SPARK_MASTER}")

    start_time = time.time()
    spark = get_spark_session()

    try:
        # Read CSV with explicit schema
        df = (
            spark.read
            .option("header", "true")
            .option("escape", '"')
            .option("multiLine", "false")
            .schema(CSV_SCHEMA)
            .csv(file_path)
        )

        if max_rows is not None:
            df = df.limit(max_rows)

        input_partitions = df.rdd.getNumPartitions()
        print(f"- Input Partitions: {input_partitions}")

        # Assemble raw_record struct containing all CSV columns as original strings
        raw_struct = struct(*[col(f.name) for f in CSV_SCHEMA.fields])

        # Add ELT metadata
        prepared_df = (
            df
            .withColumn("_row_id", monotonically_increasing_id())
            .withColumn("run_id", lit(run_id))
            .withColumn("source_file", lit(str(file_path)))
            .withColumn("source_row_number", col("_row_id") + 1)
            .withColumn("ingested_at", lit(datetime.now(timezone.utc).isoformat()))
            .withColumn("engine_used", lit("pyspark"))
            .withColumn("raw_record", raw_struct)
            .drop("_row_id")
        )

        # Use Spark accumulator for instant and accurate inserted row count
        row_counter = spark.sparkContext.accumulator(0)

        # Write in parallel across partitions
        writer = make_partition_writer(
            settings.MONGODB_URI,
            settings.DATABASE_NAME,
            settings.RAW_COLLECTION,
            settings.SPARK_BATCH_SIZE,
            counter=row_counter
        )

        print("- Writing partitions in parallel to MongoDB...")
        prepared_df.foreachPartition(writer)

        elapsed = time.time() - start_time
        inserted_count = row_counter.value

        throughput = inserted_count / elapsed if elapsed > 0 else 0

        print(f"[PySpark Loader] Ingestion Complete")
        print(f"- Total Rows Ingested: {inserted_count:,}")
        print(f"- Partitions: {input_partitions}")
        print(f"- Elapsed Time: {elapsed:.2f} seconds")
        return {
            "run_id": run_id,
            "engine_used": "pyspark",
            "file_name": Path(file_path).name,
            "file_size_mb": round(Path(file_path).stat().st_size / (1024 * 1024), 2),
            "rows_read": inserted_count,
            "raw_loaded": inserted_count,
            "partitions": input_partitions,
            "elapsed_seconds": round(elapsed, 2),
            "throughput": round(throughput, 2),
            "batch_size": settings.SPARK_BATCH_SIZE,
            "errors": [],
        }

    finally:
        spark.stop()


if __name__ == "__main__":
    import argparse
    from src.metrics import save_run_metrics

    parser = argparse.ArgumentParser(description="PySpark Raw Ingestion for Large File")
    parser.add_argument(
        "--input",
        type=str,
        default=settings.DEFAULT_LARGE_INPUT,
        help="Path to large CSV file"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional row limit"
    )
    args = parser.parse_args()

    print(f"Starting PySpark loader on: {args.input}")
    res = load_raw_spark(args.input, max_rows=args.limit)
    save_run_metrics(res)
    print("PySpark run complete and metrics saved!")

