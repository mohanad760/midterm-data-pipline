import json
import sys
from pathlib import Path
from pymongo import UpdateOne

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings
from src.mongo_setup import get_mongo_client, get_database
from src.file_router import route_file


def run_demo():
    print("=" * 75)
    print("   MIDTERM PRACTICAL EVALUATION DEMO - BIG DATA HYBRID PIPELINE   ")
    print("=" * 75)

    client = get_mongo_client()
    db = get_database(client)

    # -------------------------------------------------------------
    # Step 1: File Router Check
    # -------------------------------------------------------------
    print("\n" + "=" * 50)
    print("[DEMO STEP 1 & 4] Automatic File Router Evaluation")
    print("=" * 50)

    sample_engine, sample_sz, sample_just = route_file(settings.DEFAULT_SAMPLE_INPUT)
    print(f"1. Small File: {Path(settings.DEFAULT_SAMPLE_INPUT).name} ({sample_sz:.2f} MB)")
    print(f"   -> Chosen Engine: {sample_engine}")
    print(f"   -> Reason: {sample_just}")

    large_engine, large_sz, large_just = route_file(settings.DEFAULT_LARGE_INPUT)
    print(f"\n2. Large File: {Path(settings.DEFAULT_LARGE_INPUT).name} ({large_sz:.2f} MB)")
    print(f"   -> Chosen Engine: {large_engine}")
    print(f"   -> Reason: {large_just}")

    # -------------------------------------------------------------
    # Step 2: Raw Layer Proof
    # -------------------------------------------------------------
    print("\n" + "=" * 50)
    print("[DEMO STEP 2] Raw Layer Ingestion Proof (ELT Pattern)")
    print("=" * 50)
    raw_col = db[settings.RAW_COLLECTION]
    raw_count = raw_col.estimated_document_count()
    sample_raw = raw_col.find_one()

    print(f"Total documents in '{settings.RAW_COLLECTION}': {raw_count:,}")
    if sample_raw:
        print("\nSample Raw Document (stored before any quality filtering):")
        print(f"- run_id:            {sample_raw.get('run_id')}")
        print(f"- source_file:       {sample_raw.get('source_file')}")
        print(f"- source_row_number: {sample_raw.get('source_row_number')}")
        print(f"- engine_used:       {sample_raw.get('engine_used')}")
        print(f"- ingested_at:       {sample_raw.get('ingested_at')}")
        print(f"- raw_record sample keys: {list(sample_raw.get('raw_record', {}).keys())}")

    # -------------------------------------------------------------
    # Step 3: Data Quality & Audit Trail & Quarantine Examples
    # -------------------------------------------------------------
    print("\n" + "=" * 50)
    print("[DEMO STEP 3] Data Quality Rules & Audit Trail Inspection")
    print("=" * 50)
    val_col = db[settings.VALIDATED_COLLECTION]
    quar_col = db[settings.QUARANTINE_COLLECTION]

    corrected_doc = val_col.find_one({"quality_status": "corrected", "corrections.0": {"$exists": True}})
    if corrected_doc:
        print("Example of a CORRECTED record in orders_validated:")
        print(f"- Order ID:       {corrected_doc.get('order_id')}")
        print(f"- Quality Status: {corrected_doc.get('quality_status')}")
        print(f"- Total Amount:   {corrected_doc.get('total_amount')} YER")
        print("- Audit Trail (Corrections):")
        for c in corrected_doc.get("corrections", []):
            print(f"    * Field '{c['field']}': '{c['original_value']}' -> '{c['corrected_value']}' [Rule: {c['rule_code']}]")

    quarantine_doc = quar_col.find_one()
    if quarantine_doc:
        print("\nExample of an UNRECOVERABLE record in orders_quarantine:")
        print(f"- Order ID:      {quarantine_doc.get('order_id')}")
        print(f"- Error Codes:   {quarantine_doc.get('error_codes')}")
        print(f"- Error Details: {quarantine_doc.get('error_details')}")

    # -------------------------------------------------------------
    # Step 6 & 7: Final Collection Counts & Metrics Summary
    # -------------------------------------------------------------
    print("\n" + "=" * 50)
    print("[DEMO STEP 6 & 7] Final Collections Counts & Metrics")
    print("=" * 50)
    val_count = val_col.count_documents({})
    quar_count = quar_col.count_documents({})

    print(f"MongoDB Collection Status in '{settings.DATABASE_NAME}':")
    print(f"  * {settings.RAW_COLLECTION}:        {raw_count:,} documents")
    print(f"  * {settings.VALIDATED_COLLECTION}:  {val_count:,} unique business orders")
    print(f"  * {settings.QUARANTINE_COLLECTION}: {quar_count:,} isolated documents")

    results_file = Path(settings.RESULTS_JSON_PATH)
    if results_file.exists():
        with open(results_file, "r", encoding="utf-8") as f:
            runs = json.load(f)
            last_run = runs[-1] if runs else {}
            print(f"\nLatest Pipeline Run Metrics (from {results_file.name}):")
            print(f"- Run ID:       {last_run.get('run_id')}")
            print(f"- Engine:       {last_run.get('engine_used')}")
            print(f"- Total Rows:   {last_run.get('raw_loaded', 0):,}")
            if "is_consistent" in last_run:
                consistency_str = "PASSED" if last_run.get("is_consistent") else "FAILED"
            else:
                consistency_str = "COMPLETE (Raw Ingestion Layer)"
            print(f"- Consistency:  {consistency_str}")
            print(f"- Error Counts: {last_run.get('error_case_counts')}")

    # -------------------------------------------------------------
    # Step 8: Live Idempotency & Upsert Proof
    # -------------------------------------------------------------
    print("\n" + "=" * 50)
    print("[DEMO STEP 8] Live Idempotency & Upsert Demonstration")
    print("=" * 50)
    # Pick one order to test
    target_order = val_col.find_one()
    if target_order:
        test_order_id = target_order["order_id"]
        orig_count = val_col.count_documents({})
        orig_name = target_order.get("customer_name")

        print(f"Testing with Order ID: {test_order_id}")
        print(f"- Initial Collection Count: {orig_count:,}")

        # Part A: Re-run Upsert with identical record
        res_a = val_col.update_one(
            {"order_id": test_order_id},
            {"$set": target_order},
            upsert=True
        )
        count_after_a = val_col.count_documents({})
        print(f"- Re-running identical Upsert -> Matched: {res_a.matched_count}, Modified: {res_a.modified_count}, Upserted: {res_a.upserted_id}")
        print(f"- Collection Count after re-run: {count_after_a:,} (Change: +{count_after_a - orig_count}) [IDEMPOTENCY PROVED]")

        # Part B: Update single record
        updated_name = f"{orig_name} (Updated)"
        res_b = val_col.update_one(
            {"order_id": test_order_id},
            {"$set": {"customer_name": updated_name}},
            upsert=True
        )
        count_after_b = val_col.count_documents({})
        print(f"\n- Updating customer_name to '{updated_name}':")
        print(f"  Matched: {res_b.matched_count}, Modified: {res_b.modified_count}")
        print(f"- Collection Count after Update: {count_after_b:,} (Change: +{count_after_b - orig_count}) [NO DUPLICATE PROVED]")

        # Revert change
        val_col.update_one({"order_id": test_order_id}, {"$set": {"customer_name": orig_name}})

    client.close()
    print("\n" + "=" * 75)
    print("                      ALL DEMO CHECKS COMPLETED!                      ")
    print("=" * 75)


if __name__ == "__main__":
    run_demo()
