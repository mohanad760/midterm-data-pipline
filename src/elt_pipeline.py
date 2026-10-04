import sys
import time
from collections import Counter
from typing import Dict, Any, List
from pathlib import Path
from pymongo import UpdateOne

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import settings
from src.mongo_setup import get_mongo_client, get_database, init_database_schema
from src.quality_rules import process_and_classify_record


def run_elt_pipeline(run_id: str, batch_size: int = settings.BATCH_SIZE) -> Dict[str, Any]:
    """
    Transforms data from orders_raw to orders_validated and orders_quarantine:
    1. Reads raw documents for given run_id.
    2. Applies Data Quality & Cleaning rules.
    3. Writes valid & corrected orders to orders_validated using Idempotent Upsert.
    4. Writes unrecoverable orders to orders_quarantine.
    5. Enforces consistency formula:
       run_raw_count = run_valid_count + run_corrected_count + run_quarantine_count
    """
    client = get_mongo_client()
    db = get_database(client)
    init_database_schema(db)

    raw_col = db[settings.RAW_COLLECTION]
    val_col = db[settings.VALIDATED_COLLECTION]
    quar_col = db[settings.QUARANTINE_COLLECTION]

    start_time = time.time()

    total_raw = raw_col.count_documents({"run_id": run_id})
    print(f"\n[ELT Pipeline] Processing Run: {run_id}")
    print(f"- Total Raw Records to process: {total_raw:,}")

    valid_count = 0
    corrected_count = 0
    quarantine_count = 0

    inserted_count = 0
    updated_count = 0
    unchanged_count = 0

    error_case_counts = Counter()

    val_operations: List[UpdateOne] = []
    quar_documents: List[Dict[str, Any]] = []

    processed = 0

    # Stream cursor to minimize RAM usage
    cursor = raw_col.find({"run_id": run_id}, no_cursor_timeout=True)

    try:
        for raw_doc in cursor:
            processed += 1
            raw_record = raw_doc.get("raw_record", {})

            result = process_and_classify_record(raw_record, run_id)

            if result["target"] == "quarantine":
                quarantine_count += 1
                for code in result.get("error_codes", []):
                    error_case_counts[code] += 1
                quar_documents.append(result)
            else:
                if result["quality_status"] == "corrected":
                    corrected_count += 1
                else:
                    valid_count += 1

                order_id = result["order_id"]
                # Exclude target internal flag
                doc_to_save = {k: v for k, v in result.items() if k != "target"}

                # Idempotent Upsert operation
                op = UpdateOne(
                    {"order_id": order_id},
                    {"$set": doc_to_save},
                    upsert=True
                )
                val_operations.append(op)

            # Flush validated upserts in chunks
            if len(val_operations) >= batch_size:
                bulk_res = val_col.bulk_write(val_operations, ordered=False)
                inserted_count += len(bulk_res.upserted_ids)
                updated_count += bulk_res.modified_count
                # Matched but not modified count
                unchanged = bulk_res.matched_count - bulk_res.modified_count
                unchanged_count += unchanged
                val_operations = []

            # Flush quarantine inserts in chunks
            if len(quar_documents) >= batch_size:
                quar_col.insert_many(quar_documents, ordered=False)
                quar_documents = []

            if processed % 20000 == 0:
                print(
                    f"  Processed {processed:,} / {total_raw:,} | "
                    f"Valid: {valid_count:,} | Corrected: {corrected_count:,} | Quarantined: {quarantine_count:,}"
                )

        # Final flush
        if val_operations:
            bulk_res = val_col.bulk_write(val_operations, ordered=False)
            inserted_count += len(bulk_res.upserted_ids)
            updated_count += bulk_res.modified_count
            unchanged = bulk_res.matched_count - bulk_res.modified_count
            unchanged_count += unchanged
            val_operations = []

        if quar_documents:
            quar_col.insert_many(quar_documents, ordered=False)
            quar_documents = []

    finally:
        cursor.close()
        client.close()

    elapsed = time.time() - start_time

    # Golden Consistency Verification
    sum_classified = valid_count + corrected_count + quarantine_count
    is_consistent = (total_raw == sum_classified)

    print(f"\n[ELT Pipeline] Processing Finished")
    print(f"- Total Raw Records: {total_raw:,}")
    print(f"- Valid Records: {valid_count:,}")
    print(f"- Corrected Records: {corrected_count:,}")
    print(f"- Quarantined Records: {quarantine_count:,}")
    print(f"- Idempotent Upsert Metrics: Inserted={inserted_count:,} | Updated={updated_count:,} | Unchanged={unchanged_count:,}")
    print(f"- Consistency Check: {total_raw} == {valid_count} + {corrected_count} + {quarantine_count} -> {'PASSED' if is_consistent else 'FAILED'}")
    print(f"- Elapsed Time: {elapsed:.2f} seconds\n")

    if not is_consistent:
        raise ValueError(
            f"Consistency violation: Raw {total_raw} != Classified {sum_classified}"
        )

    return {
        "run_id": run_id,
        "raw_loaded": total_raw,
        "valid_count": valid_count,
        "corrected_count": corrected_count,
        "quarantine_count": quarantine_count,
        "inserted_count": inserted_count,
        "updated_count": updated_count,
        "unchanged_count": unchanged_count,
        "error_case_counts": dict(error_case_counts),
        "is_consistent": is_consistent,
        "transform_elapsed_seconds": round(elapsed, 2),
    }
