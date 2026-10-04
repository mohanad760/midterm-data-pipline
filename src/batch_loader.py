import csv
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Generator

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import settings
from src.mongo_setup import get_mongo_client, get_database


def stream_csv_chunks(file_path: str, chunk_size: int = settings.BATCH_SIZE) -> Generator:
    """
    Reads CSV line by line in a streaming fashion using csv.DictReader.
    Yields batches of raw dictionaries without loading the full file into RAM.
    """
    with open(file_path, mode="r", encoding="utf-8-sig", errors="replace") as fin:
        reader = csv.DictReader(fin)
        batch = []
        for row_num, row in enumerate(reader, start=1):
            batch.append((row_num, row))
            if len(batch) >= chunk_size:
                yield batch
                batch = []
        if batch:
            yield batch


def load_raw_batch(file_path: str, run_id: str = None, batch_size: int = settings.BATCH_SIZE) -> Dict[str, Any]:
    """
    Executes Python Batch Loading:
    - Streams raw records directly into orders_raw collection in MongoDB.
    - Tracks elapsed time, throughput, batch statistics, and errors.
    """
    if run_id is None:
        run_id = f"batch_{uuid.uuid4().hex[:12]}"

    client = get_mongo_client()
    db = get_database(client)
    raw_col = db[settings.RAW_COLLECTION]

    start_time = time.time()
    total_rows = 0
    total_inserted = 0
    batch_index = 0
    errors = []

    print(f"\n[Python Batch Loader] Starting Raw Ingestion")
    print(f"- Run ID: {run_id}")
    print(f"- Source File: {file_path}")
    print(f"- Target Collection: {settings.RAW_COLLECTION}")
    print(f"- Batch Size: {batch_size:,}")

    try:
        for chunk in stream_csv_chunks(file_path, batch_size):
            batch_index += 1
            chunk_start = time.time()

            documents = []
            for row_num, raw_row in chunk:
                total_rows += 1
                doc = {
                    "run_id": run_id,
                    "source_file": str(file_path),
                    "source_row_number": row_num,
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "engine_used": "python_batch",
                    "raw_record": raw_row,
                }
                documents.append(doc)

            try:
                result = raw_col.insert_many(documents, ordered=False)
                inserted_count = len(result.inserted_ids)
                total_inserted += inserted_count
            except Exception as batch_err:
                errors.append(f"Batch {batch_index} error: {batch_err}")
                print(f"  [ERROR] Batch {batch_index} insertion failure: {batch_err}")
                continue

            chunk_time = time.time() - chunk_start
            chunk_throughput = len(documents) / chunk_time if chunk_time > 0 else 0
            overall_elapsed = time.time() - start_time
            overall_throughput = total_inserted / overall_elapsed if overall_elapsed > 0 else 0

            if batch_index % 5 == 0 or len(chunk) < batch_size:
                print(
                    f"  Batch #{batch_index:03d} | Rows: {len(documents):,} | "
                    f"Elapsed: {overall_elapsed:.2f}s | Speed: {overall_throughput:.0f} rows/s"
                )

    finally:
        client.close()

    total_time = time.time() - start_time
    throughput = total_inserted / total_time if total_time > 0 else 0

    print(f"[Python Batch Loader] Finished Ingestion")
    print(f"- Total Rows Read: {total_rows:,}")
    print(f"- Total Raw Inserted: {total_inserted:,}")
    print(f"- Elapsed Time: {total_time:.2f} seconds")
    print(f"- Throughput: {throughput:.0f} rows/sec\n")

    return {
        "run_id": run_id,
        "engine_used": "python_batch",
        "rows_read": total_rows,
        "raw_loaded": total_inserted,
        "elapsed_seconds": round(total_time, 2),
        "throughput": round(throughput, 2),
        "batch_size": batch_size,
        "errors": errors,
    }


if __name__ == "__main__":
    load_raw_batch(settings.DEFAULT_SAMPLE_INPUT)
