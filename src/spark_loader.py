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
        .config("spark.sql.shuffle.partitions", settings.SPARK_SHUFFLE_PARTITIONS)
        .config("spark.ui.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def make_partition_writer(mongo_uri: str, db_name: str, coll_name: str, batch_size: int):
    """Factory creating partition writer for PyMongo bulk write."""
    def write_partition(partition_rows):
        from pymongo import MongoClient

        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        try:
            col = client[db_name][coll_name]
            buffer = []
            for row in partition_rows:
                # Convert row into dictionary
                row_dict = row.asDict(recursive=True)
                buffer.append(row_dict)
                if len(buffer) >= batch_size:
                    col.insert_many(buffer, ordered=False)
                    buffer = []
            if buffer:
                col.insert_many(buffer, ordered=False)
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

        # Write in parallel across partitions
        writer = make_partition_writer(
            settings.MONGODB_URI,
            settings.DATABASE_NAME,
            settings.RAW_COLLECTION,
            settings.SPARK_BATCH_SIZE
        )

        print("- Writing partitions in parallel to MongoDB...")
        prepared_df.foreachPartition(writer)

        elapsed = time.time() - start_time

        # Check total inserted for this run_id
        from src.mongo_setup import get_mongo_client, get_database
        mclient = get_mongo_client()
        mdb = get_database(mclient)
        inserted_count = mdb[settings.RAW_COLLECTION].count_documents({"run_id": run_id})
        mclient.close()

        throughput = inserted_count / elapsed if elapsed > 0 else 0

        print(f"[PySpark Loader] Ingestion Complete")
        print(f"- Total Rows Ingested: {inserted_count:,}")
        print(f"- Partitions: {input_partitions}")
        print(f"- Elapsed Time: {elapsed:.2f} seconds")
        print(f"- Throughput: {throughput:.0f} rows/sec\n")

        return {
            "run_id": run_id,
            "engine_used": "pyspark",
            "rows_read": inserted_count,
            "raw_loaded": inserted_count,
            "partitions": input_partitions,
            "elapsed_seconds": round(elapsed, 2),
            "throughput": round(throughput, 2),
            "batch_size": settings.SPARK_BATCH_SIZE,
            "errors": [],
        }

    finally:
        # Note: We keep SparkSession or stop it safely in main
        pass


if __name__ == "__main__":
    load_raw_spark(settings.DEFAULT_SAMPLE_INPUT, max_rows=10000)
