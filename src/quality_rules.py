import json
import re
from datetime import datetime
from typing import Dict, Any, Tuple, List, Optional


# ============================================================
# Dictionaries and Allowed Values
# ============================================================

VALID_STATUSES = {
    "مؤكد",
    "قيد الانتظار",
    "قيد الشحن",
    "تم التسليم",
    "مرتجع",
    "ملغي",
}

STATUS_SYNONYMS = {
    "مكتمل": "تم التسليم",
    "مشحون": "قيد الشحن",
    "معتمد": "مؤكد",
    "ملغى": "ملغي",
    "تم الإلغاء": "ملغي",
    "جاري التوصيل": "قيد الشحن",
}

VALID_PAYMENT_STATUSES = {
    "تم الدفع",
    "بانتظار الدفع",
    "قيد الدفع",
}

PAYMENT_STATUS_SYNONYMS = {
    "مدفوع": "تم الدفع",
    "معلق": "بانتظار الدفع",
    "مكتمل الدفع": "تم الدفع",
    "انتظار": "بانتظار الدفع",
}

VALID_DELIVERY_TYPES = {
    "سريع",
    "عادي",
}

VALID_PAYMENT_METHODS = {
    "محفظة إلكترونية",
    "بطاقة",
    "نقدًا عند التسليم",
    "نقدا عند التسليم",
}

VALID_CURRENCIES = {
    "YER",
}

CURRENCY_SYNONYMS = {
    "ريال يمني": "YER",
    "ريال": "YER",
    "ر.ي": "YER",
    "yer": "YER",
}

ARABIC_TO_LATIN_DIGITS = str.maketrans({
    "٠": "0", "١": "1", "٢": "2", "٣": "3", "٤": "4",
    "٥": "5", "٦": "6", "٧": "7", "٨": "8", "٩": "9",
    "٫": ".", "،": ",",
})

WORD_NUMBERS = {
    "ألف": 1000.0,
    "الف": 1000.0,
    "ألفان": 2000.0,
    "الفان": 2000.0,
    "ثلاثة آلاف": 3000.0,
    "ثلاثة الاف": 3000.0,
    "أربعة آلاف": 4000.0,
    "اربعة الاف": 4000.0,
    "خمسة آلاف": 5000.0,
    "خمسة الاف": 5000.0,
    "ستة آلاف": 6000.0,
    "ستة الاف": 6000.0,
    "سبعة آلاف": 7000.0,
    "سبعة الاف": 7000.0,
    "ثمانية آلاف": 8000.0,
    "ثمانية الاف": 8000.0,
    "تسعة آلاف": 9000.0,
    "تسعة الاف": 9000.0,
    "عشرة آلاف": 10000.0,
    "عشرة الاف": 10000.0,
}

EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")


# ============================================================
# Audit Trail Helper
# ============================================================

def add_correction(corrections: List[Dict[str, Any]], field: str, orig_val: Any, new_val: Any, rule_code: str):
    """
    Append an audit log entry conforming strictly to the project specification:
    {
      "field": ...,
      "original_value": ...,
      "corrected_value": ...,
      "rule_code": ...
    }
    """
    corrections.append({
        "field": field,
        "original_value": str(orig_val) if orig_val is not None else "",
        "corrected_value": str(new_val) if new_val is not None else "",
        "rule_code": rule_code,
    })


# ============================================================
# Atomic Cleaning & Normalization Functions
# ============================================================

def normalize_arabic_digits(text: str) -> Tuple[str, bool]:
    """Convert Arabic-Indic numerals and decimal separator to Latin."""
    if not text or not isinstance(text, str):
        return text, False
    converted = text.translate(ARABIC_TO_LATIN_DIGITS)
    return converted, converted != text


def parse_numeric_field(value: Any, field_name: str, corrections: List[Dict[str, Any]]) -> Optional[float]:
    """
    Clean and parse a numeric field with support for:
    1. Arabic digits (٠-٩)
    2. Word numbers (ألفان, خمسة آلاف)
    3. Currency suffixes (ريال, ريال يمني)
    4. Thousand separators (125,000.00)
    """
    if value is None:
        return None

    orig_str = str(value).strip()
    curr_str = orig_str

    if not curr_str or curr_str in {"???", "null", "none", "nan"}:
        return None

    # Check for word numbers
    if curr_str in WORD_NUMBERS:
        parsed_val = WORD_NUMBERS[curr_str]
        add_correction(corrections, field_name, orig_str, parsed_val, "WORD_NUMBER_CONVERTED")
        return parsed_val

    # Convert Arabic numerals
    converted_digits, has_arabic = normalize_arabic_digits(curr_str)
    if has_arabic:
        add_correction(corrections, field_name, curr_str, converted_digits, "ARABIC_DIGITS_CONVERTED")
        curr_str = converted_digits

    # Remove currency words
    curr_cleaned = re.sub(r"\s*(?:ريال\s*يمني|ريال|YER|ر\.ي)\s*", "", curr_str, flags=re.IGNORECASE).strip()
    if curr_cleaned != curr_str:
        add_correction(corrections, field_name, curr_str, curr_cleaned, "CURRENCY_STANDARDIZED")
        curr_str = curr_cleaned

    # Remove thousand commas
    if "," in curr_str:
        no_commas = curr_str.replace(",", "")
        add_correction(corrections, field_name, curr_str, no_commas, "THOUSAND_SEPARATORS_REMOVED")
        curr_str = no_commas

    # Attempt float conversion
    try:
        float_val = float(curr_str)
        return float_val
    except ValueError:
        return None


def clean_phone(phone: Any, corrections: List[Dict[str, Any]]) -> Optional[str]:
    """
    Clean Yemen phone numbers:
    Remove spaces, dashes, country code +967 or 00967.
    Standardize to 9-digit format (e.g. 77xxxxxxx) or E.164 (+967...).
    """
    if not phone or not isinstance(phone, str):
        return None

    orig_phone = phone.strip()
    p = orig_phone

    # Remove spaces and dashes
    p_clean = re.sub(r"[\s\-]", "", p)

    # Normalize country code
    if p_clean.startswith("+967"):
        p_clean = p_clean[4:]
    elif p_clean.startswith("00967"):
        p_clean = p_clean[5:]
    elif p_clean.startswith("967"):
        p_clean = p_clean[3:]

    # Remove leading zero if present
    if p_clean.startswith("0") and len(p_clean) == 10:
        p_clean = p_clean[1:]

    # Validate 9 digits starting with 7
    if len(p_clean) == 9 and p_clean.startswith("7") and p_clean.isdigit():
        standardized = f"+967{p_clean}"
        if standardized != orig_phone:
            add_correction(corrections, "customer_phone", orig_phone, standardized, "PHONE_FORMAT_NORMALIZED")
        return standardized

    return None


def clean_email(email: Any, corrections: List[Dict[str, Any]]) -> Optional[str]:
    """
    Clean email address:
    Fix repeated symbols like @@ or ..
    Validate format.
    """
    if not email or not isinstance(email, str):
        return None

    orig_email = email.strip()
    em = orig_email

    # Collapse repeated @ and .
    em_fixed = re.sub(r"@+", "@", em)
    em_fixed = re.sub(r"\.+", ".", em_fixed)

    if em_fixed != orig_email:
        add_correction(corrections, "customer_email", orig_email, em_fixed, "EMAIL_REPEATED_SYMBOLS")
        em = em_fixed

    if EMAIL_REGEX.match(em):
        return em

    return None


def parse_and_clean_date(date_val: Any, corrections: List[Dict[str, Any]]) -> Optional[str]:
    """
    Parse multiple date formats into standard ISO 8601 (YYYY-MM-DDTHH:MM:SS):
    - YYYY-MM-DDTHH:MM:SS
    - DD-MM-YYYY HH:MM:SS
    - YYYY/MM/DD HH:MM:SS
    - YYYY-MM-DD HH:MM:SS
    Validates logical bounds.
    """
    if not date_val or not isinstance(date_val, str):
        return None

    orig_date = date_val.strip()
    # Normalize Arabic digits in date if any
    date_str, has_arabic = normalize_arabic_digits(orig_date)
    if has_arabic:
        add_correction(corrections, "order_date", orig_date, date_str, "ARABIC_DIGITS_CONVERTED")

    candidate_formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d",
    ]

    dt = None
    for fmt in candidate_formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            break
        except ValueError:
            continue

    if dt is None:
        return None

    # Validate reasonable date bounds (between 2020 and 2030)
    if not (2020 <= dt.year <= 2030):
        return None

    standard_iso = dt.strftime("%Y-%m-%dT%H:%M:%S")
    if standard_iso != orig_date:
        add_correction(corrections, "order_date", orig_date, standard_iso, "DATE_FORMAT_STANDARDIZED")

    return standard_iso


def validate_and_clean_items(items_raw: Any) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str], Optional[float]]:
    """
    Parse and validate items_json:
    - Returns (items_list, error_code, total_items_price)
    - Checks for JSON corruption, empty list, negative qty/price/total.
    """
    if items_raw is None:
        return None, "CORRUPTED_ITEMS_JSON", None

    items_str = str(items_raw).strip()
    if not items_str or items_str == "not-json":
        return None, "CORRUPTED_ITEMS_JSON", None

    try:
        items = json.loads(items_str)
    except Exception:
        return None, "CORRUPTED_ITEMS_JSON", None

    if not isinstance(items, list):
        return None, "CORRUPTED_ITEMS_JSON", None

    if len(items) == 0:
        return None, "EMPTY_ITEMS", None

    total_items_price = 0.0
    for item in items:
        if not isinstance(item, dict):
            return None, "CORRUPTED_ITEMS_JSON", None

        qty = item.get("qty")
        unit_price = item.get("unit_price")
        item_total = item.get("total")

        if qty is None or unit_price is None or item_total is None:
            return None, "UNKNOWN_PRICE", None

        try:
            qty_num = float(qty)
            unit_price_num = float(unit_price)
            item_total_num = float(item_total)
        except (ValueError, TypeError):
            return None, "UNKNOWN_PRICE", None

        # Check negative values
        if qty_num <= 0 or unit_price_num < 0 or item_total_num < 0:
            return None, "AMBIGUOUS_NEGATIVE_VALUE", None

        total_items_price += item_total_num

    return items, None, total_items_price


# ============================================================
# Main Record Processing & Classification Engine
# ============================================================

def process_and_classify_record(raw_dict: Dict[str, Any], run_id: str) -> Dict[str, Any]:
    """
    Transforms a single raw order record:
    1. Runs all cleaning & standardization rules.
    2. Collects audit trail of changes.
    3. Detects fatal/unrecoverable errors and assigns official error codes.
    4. Returns processed document ready for Validated or Quarantine collection.
    """
    corrections: List[Dict[str, Any]] = []
    error_codes: List[str] = []
    error_details: List[str] = []

    # 1. Order ID validation
    order_id = raw_dict.get("order_id")
    if not order_id or not str(order_id).strip():
        error_codes.append("MISSING_ORDER_ID")
        error_details.append("order_id is null or empty")
        order_id_clean = None
    else:
        order_id_clean = str(order_id).strip()
        if order_id_clean != str(order_id):
            add_correction(corrections, "order_id", order_id, order_id_clean, "WHITESPACE_TRIMMED")

    # 2. Customer ID validation
    customer_id = raw_dict.get("customer_id")
    if not customer_id or not str(customer_id).strip():
        error_codes.append("MISSING_CUSTOMER_ID")
        error_details.append("customer_id is null or empty")
        customer_id_clean = None
    else:
        customer_id_clean = str(customer_id).strip()
        if customer_id_clean != str(customer_id):
            add_correction(corrections, "customer_id", customer_id, customer_id_clean, "WHITESPACE_TRIMMED")

    # 3. Order Date validation
    order_date_raw = raw_dict.get("order_date")
    cleaned_date = parse_and_clean_date(order_date_raw, corrections)
    if cleaned_date is None:
        error_codes.append("INVALID_IMPOSSIBLE_DATE")
        error_details.append(f"Invalid or out-of-range date: {order_date_raw}")

    # 4. Status validation & mapping
    status_raw = raw_dict.get("status")
    status_clean = str(status_raw).strip() if status_raw else ""
    if status_raw and status_clean != str(status_raw):
        add_correction(corrections, "status", status_raw, status_clean, "WHITESPACE_TRIMMED")

    if status_clean in STATUS_SYNONYMS:
        mapped_status = STATUS_SYNONYMS[status_clean]
        add_correction(corrections, "status", status_clean, mapped_status, "STATUS_SYNONYM_MAPPED")
        status_clean = mapped_status
    elif status_clean not in VALID_STATUSES:
        error_codes.append("UNKNOWN_PRICE")
        error_details.append(f"Unrecognized status: {status_raw}")

    # 5. Customer Phone validation
    customer_phone_raw = raw_dict.get("customer_phone")
    cleaned_phone = clean_phone(customer_phone_raw, corrections)
    if cleaned_phone is None and customer_phone_raw:
        # If invalid phone format, check if it's corrupted
        error_codes.append("UNKNOWN_PRICE")
        error_details.append(f"Invalid phone number: {customer_phone_raw}")

    # 6. Customer Email validation
    customer_email_raw = raw_dict.get("customer_email")
    cleaned_email = clean_email(customer_email_raw, corrections)
    if cleaned_email is None and customer_email_raw:
        error_codes.append("UNKNOWN_PRICE")
        error_details.append(f"Invalid email: {customer_email_raw}")

    # 7. Delivery Type & Cost
    delivery_type_raw = raw_dict.get("delivery_type")
    delivery_type_clean = str(delivery_type_raw).strip() if delivery_type_raw else "عادي"
    if delivery_type_raw and delivery_type_clean != str(delivery_type_raw):
        add_correction(corrections, "delivery_type", delivery_type_raw, delivery_type_clean, "WHITESPACE_TRIMMED")

    delivery_cost_raw = raw_dict.get("delivery_cost")
    cleaned_delivery_cost = parse_numeric_field(delivery_cost_raw, "delivery_cost", corrections)
    if cleaned_delivery_cost is None:
        # Default or fallback if valid delivery type
        if delivery_type_clean == "سريع":
            cleaned_delivery_cost = 5000.0
            add_correction(corrections, "delivery_cost", delivery_cost_raw, 5000.0, "WORD_NUMBER_CONVERTED")
        elif delivery_type_clean == "عادي":
            cleaned_delivery_cost = 2000.0
            add_correction(corrections, "delivery_cost", delivery_cost_raw, 2000.0, "WORD_NUMBER_CONVERTED")
        else:
            error_codes.append("UNKNOWN_PRICE")
            error_details.append("Could not determine delivery cost")

    # 8. Items validation
    items_raw = raw_dict.get("items_json")
    items_parsed, items_err, total_items_price = validate_and_clean_items(items_raw)
    if items_err:
        error_codes.append(items_err)
        error_details.append(f"Items error: {items_err}")

    # 9. Payment Status & Method & Currency
    payment_status_raw = raw_dict.get("payment_status")
    pay_status_clean = str(payment_status_raw).strip() if payment_status_raw else ""
    if payment_status_raw and pay_status_clean != str(payment_status_raw):
        add_correction(corrections, "payment_status", payment_status_raw, pay_status_clean, "WHITESPACE_TRIMMED")

    if pay_status_clean in PAYMENT_STATUS_SYNONYMS:
        mapped_pay = PAYMENT_STATUS_SYNONYMS[pay_status_clean]
        add_correction(corrections, "payment_status", pay_status_clean, mapped_pay, "STATUS_SYNONYM_MAPPED")
        pay_status_clean = mapped_pay

    currency_raw = raw_dict.get("currency")
    curr_clean = str(currency_raw).strip() if currency_raw else "YER"
    if curr_clean in CURRENCY_SYNONYMS:
        mapped_curr = CURRENCY_SYNONYMS[curr_clean]
        if mapped_curr != curr_clean:
            add_correction(corrections, "currency", currency_raw, mapped_curr, "CURRENCY_STANDARDIZED")
        curr_clean = mapped_curr
    elif curr_clean not in VALID_CURRENCIES:
        error_codes.append("UNKNOWN_PRICE")
        error_details.append(f"Invalid currency: {currency_raw}")

    # 10. Total Amount Reconciliation
    total_amount_raw = raw_dict.get("total_amount")
    cleaned_total = parse_numeric_field(total_amount_raw, "total_amount", corrections)

    if cleaned_total is not None and cleaned_total < 0:
        error_codes.append("AMBIGUOUS_NEGATIVE_VALUE")
        error_details.append(f"Negative total amount: {cleaned_total}")

    # Reconcile if total missing or differs, but items and delivery are known
    if total_items_price is not None and cleaned_delivery_cost is not None:
        expected_total = total_items_price + cleaned_delivery_cost
        if cleaned_total is None or abs(cleaned_total - expected_total) > 1.0:
            add_correction(corrections, "total_amount", total_amount_raw, expected_total, "TOTAL_RECALCULATED")
            cleaned_total = expected_total

    if cleaned_total is None and "UNKNOWN_PRICE" not in error_codes:
        error_codes.append("UNKNOWN_PRICE")
        error_details.append("Could not determine or calculate total_amount")

    # Payment amount check
    payment_amount_raw = raw_dict.get("payment_amount")
    cleaned_payment = parse_numeric_field(payment_amount_raw, "payment_amount", corrections)
    if cleaned_payment is None and cleaned_total is not None:
        cleaned_payment = cleaned_total
        add_correction(corrections, "payment_amount", payment_amount_raw, cleaned_payment, "TOTAL_RECALCULATED")

    # Multi-error code aggregation if applicable
    if len(error_codes) > 1:
        if "MULTIPLE_CONFLICTING_ERRORS" not in error_codes:
            error_codes.insert(0, "MULTIPLE_CONFLICTING_ERRORS")

    # Final Classification
    if error_codes:
        # Quarantine Document
        return {
            "target": "quarantine",
            "run_id": run_id,
            "order_id": order_id_clean or str(raw_dict.get("order_id")),
            "error_codes": error_codes,
            "error_details": error_details,
            "raw_record": raw_dict,
            "quarantined_at": datetime.utcnow().isoformat(),
        }
    else:
        # Validated Document (Valid or Corrected)
        quality_status = "corrected" if len(corrections) > 0 else "valid"
        validated_doc = {
            "target": "validated",
            "run_id": run_id,
            "order_id": order_id_clean,
            "order_date": cleaned_date,
            "status": status_clean,
            "customer_id": customer_id_clean,
            "customer_name": str(raw_dict.get("customer_name", "")).strip(),
            "customer_phone": cleaned_phone,
            "customer_email": cleaned_email,
            "city": str(raw_dict.get("city", "")).strip(),
            "district": str(raw_dict.get("district", "")).strip(),
            "delivery_type": delivery_type_clean,
            "delivery_cost": cleaned_delivery_cost,
            "payment_method": str(raw_dict.get("payment_method", "")).strip(),
            "payment_status": pay_status_clean,
            "payment_amount": cleaned_payment,
            "currency": curr_clean,
            "total_amount": cleaned_total,
            "items": items_parsed,
            "quality_status": quality_status,
            "corrections": corrections,
            "validated_at": datetime.utcnow().isoformat(),
        }
        return validated_doc
