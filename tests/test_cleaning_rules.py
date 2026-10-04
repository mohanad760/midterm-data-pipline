import pytest
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.quality_rules import (
    normalize_arabic_digits,
    parse_numeric_field,
    clean_phone,
    clean_email,
    parse_and_clean_date,
    validate_and_clean_items,
    process_and_classify_record,
)


def test_arabic_digits_normalization():
    res, changed = normalize_arabic_digits("٧٠٦٠٠٠٫٠")
    assert changed is True
    assert res == "706000.0"

    res2, changed2 = normalize_arabic_digits("12345")
    assert changed2 is False
    assert res2 == "12345"


def test_numeric_field_parsing_words_and_currency():
    corrections = []
    # Test words
    val = parse_numeric_field("خمسة آلاف", "delivery_cost", corrections)
    assert val == 5000.0
    assert any(c["rule_code"] == "WORD_NUMBER_CONVERTED" for c in corrections)

    # Test currency suffix
    corrections = []
    val = parse_numeric_field("54000.00 ريال", "payment_amount", corrections)
    assert val == 54000.0
    assert any(c["rule_code"] == "CURRENCY_STANDARDIZED" for c in corrections)

    # Test thousand commas
    corrections = []
    val = parse_numeric_field("135,000.00", "total_amount", corrections)
    assert val == 135000.0
    assert any(c["rule_code"] == "THOUSAND_SEPARATORS_REMOVED" for c in corrections)


def test_clean_phone():
    corrections = []
    # Test Yemen phone with spaces and country code
    res = clean_phone("+967 736278430", corrections)
    assert res == "+967736278430"
    assert len(corrections) == 1
    assert corrections[0]["rule_code"] == "PHONE_FORMAT_NORMALIZED"

    # Test invalid short phone
    corrections = []
    assert clean_phone("12345", corrections) is None


def test_clean_email():
    corrections = []
    # Repeated symbols
    res = clean_email("user819896@@example..com", corrections)
    assert res == "user819896@example.com"
    assert any(c["rule_code"] == "EMAIL_REPEATED_SYMBOLS" for c in corrections)

    # Impossible email
    assert clean_email("@@", []) is None


def test_parse_and_clean_date():
    corrections = []
    res = parse_and_clean_date("31-01-2025 13:54:00", corrections)
    assert res == "2025-01-31T13:54:00"
    assert corrections[0]["rule_code"] == "DATE_FORMAT_STANDARDIZED"

    # Date out of logical bounds
    assert parse_and_clean_date("1995-01-01 00:00:00", []) is None


def test_validate_and_clean_items():
    # Valid items
    valid_json = '[{"sku":"SKU-1008","name":"محول HDMI","qty":2,"unit_price":12000.0,"total":24000.0}]'
    items, err, total = validate_and_clean_items(valid_json)
    assert err is None
    assert len(items) == 1
    assert total == 24000.0

    # Corrupted JSON
    _, err_corrupt, _ = validate_and_clean_items("not-json")
    assert err_corrupt == "CORRUPTED_ITEMS_JSON"

    # Negative Quantity
    negative_json = '[{"sku":"SKU-1010","name":"هاتف سامسونج","qty":-2,"unit_price":180000.0,"total":360000.0}]'
    _, err_neg, _ = validate_and_clean_items(negative_json)
    assert err_neg == "AMBIGUOUS_NEGATIVE_VALUE"


def test_classification_valid_vs_corrected_vs_quarantine():
    # 1. Clean valid record
    clean_row = {
        "order_id": "طلب-100001",
        "order_date": "2025-01-12T00:13:00",
        "status": "مؤكد",
        "customer_id": "عميل-1",
        "customer_name": "علي حسين",
        "customer_phone": "+967714876334",
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
        "items_json": '[{"sku":"SKU-1010","name":"هاتف سامسونج A54","qty":3,"unit_price":181500.0,"total":544500.0}]'
    }
    res_clean = process_and_classify_record(clean_row, run_id="run_test_1")
    assert res_clean["target"] == "validated"
    assert res_clean["quality_status"] == "valid"
    assert len(res_clean["corrections"]) == 0

    # 2. Corrected record (Arabic numbers + email repeated symbol + status spaces)
    dirty_correctable = clean_row.copy()
    dirty_correctable["status"] = "  مؤكد  "
    dirty_correctable["total_amount"] = "٥٤٦٥٠٠٫٠"
    dirty_correctable["customer_email"] = "user392083@@example.com"
    res_corrected = process_and_classify_record(dirty_correctable, run_id="run_test_2")
    assert res_corrected["target"] == "validated"
    assert res_corrected["quality_status"] == "corrected"
    assert len(res_corrected["corrections"]) >= 2

    # 3. Quarantine record (Missing customer_id + corrupted items JSON)
    unrecoverable = clean_row.copy()
    unrecoverable["customer_id"] = ""
    unrecoverable["items_json"] = "not-json"
    res_quarantine = process_and_classify_record(unrecoverable, run_id="run_test_3")
    assert res_quarantine["target"] == "quarantine"
    assert "MISSING_CUSTOMER_ID" in res_quarantine["error_codes"]
    assert "CORRUPTED_ITEMS_JSON" in res_quarantine["error_codes"]
