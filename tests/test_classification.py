import pytest
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.quality_rules import process_and_classify_record


def get_base_valid_record():
    return {
        "order_id": "طلب-100001",
        "order_date": "2025-01-12T00:13:00",
        "status": "مؤكد",
        "customer_id": "عميل-1",
        "customer_name": "علي حسين",
        "customer_phone": "714876334",
        "customer_email": "user392083@example.com",
        "city": "لحج",
        "district": "القاهرة",
        "delivery_type": "عادي",
        "delivery_cost": "2000.0",
        "payment_method": "محفظة إلكترونية",
        "payment_status": "تم الدفع",
        "payment_amount": "546500.0",
        "currency": "YER",
        "total_amount": "546500.0",
        "items_json": '[{"sku":"SKU-1010","name":"هاتف سامسونج A54","qty":3,"unit_price":181500.0,"total":544500.0}]',
    }


def test_missing_order_id_goes_to_quarantine():
    rec = get_base_valid_record()
    rec["order_id"] = ""
    res = process_and_classify_record(rec, "run_test")
    assert res["target"] == "quarantine"
    assert "MISSING_ORDER_ID" in res["error_codes"]


def test_missing_customer_id_goes_to_quarantine():
    rec = get_base_valid_record()
    rec["customer_id"] = "   "
    res = process_and_classify_record(rec, "run_test")
    assert res["target"] == "quarantine"
    assert "MISSING_CUSTOMER_ID" in res["error_codes"]


def test_empty_items_goes_to_quarantine():
    rec = get_base_valid_record()
    rec["items_json"] = "[]"
    res = process_and_classify_record(rec, "run_test")
    assert res["target"] == "quarantine"
    assert "EMPTY_ITEMS" in res["error_codes"]


def test_ambiguous_negative_values():
    rec = get_base_valid_record()
    rec["items_json"] = '[{"sku":"SKU-1","name":"item","qty":-1,"unit_price":100,"total":-100}]'
    res = process_and_classify_record(rec, "run_test")
    assert res["target"] == "quarantine"
    assert "AMBIGUOUS_NEGATIVE_VALUE" in res["error_codes"]


def test_multiple_conflicting_errors():
    rec = get_base_valid_record()
    rec["order_id"] = ""
    rec["customer_id"] = ""
    rec["items_json"] = "broken_json"
    res = process_and_classify_record(rec, "run_test")
    assert res["target"] == "quarantine"
    assert "MULTIPLE_CONFLICTING_ERRORS" in res["error_codes"]
    assert "MISSING_ORDER_ID" in res["error_codes"]
    assert "MISSING_CUSTOMER_ID" in res["error_codes"]
    assert "CORRUPTED_ITEMS_JSON" in res["error_codes"]


def test_audit_trail_schema():
    rec = get_base_valid_record()
    rec["customer_email"] = "user392083@@example..com"
    rec["total_amount"] = "٥٤٦٥٠٠٫٠"
    res = process_and_classify_record(rec, "run_test")
    assert res["target"] == "validated"
    assert "corrections" in res
    assert len(res["corrections"]) >= 2
    for item in res["corrections"]:
        assert "field" in item
        assert "original_value" in item
        assert "corrected_value" in item
        assert "rule_code" in item
