import json

from src.quality_rules import (
    normalize_amount,
    normalize_date,
    normalize_email,
    normalize_phone,
    evaluate_record,
)


def make_record():
    return {
        "order_id": "طلب-1",
        "order_date": "2025-01-17T04:50:00",
        "status": "مؤكد",
        "customer_id": "عميل-1",
        "customer_name": "محمد علي",
        "customer_phone": "777559764",
        "customer_email": "user@example.com",
        "city": "تعز",
        "district": "القاهرة",
        "delivery_type": "عادي",
        "delivery_cost": "2000.0",
        "payment_method": "نقدي",
        "payment_status": "تم الدفع",
        "payment_amount": "52000.0",
        "currency": "YER",
        "total_amount": "52000.0",
        "items_json": json.dumps(
            [
                {
                    "sku": "SKU-1",
                    "name": "منتج",
                    "qty": 1,
                    "unit_price": 50000.0,
                    "total": 50000.0,
                }
            ],
            ensure_ascii=False,
        ),
    }


def test_arabic_digits_are_converted():
    value, rules = normalize_amount("٧٠٦٠٠٠٫٠")

    assert value == 706000.0
    assert "ARABIC_DIGITS_TO_LATIN" in rules


def test_thousands_separator_is_removed():
    value, rules = normalize_amount("135,000.00")

    assert value == 135000.0
    assert "REMOVE_THOUSANDS_SEPARATOR" in rules


def test_currency_text_is_removed_from_amount():
    value, rules = normalize_amount("54000.00 ريال")

    assert value == 54000.0
    assert "REMOVE_CURRENCY_TEXT" in rules


def test_known_number_words_are_converted():
    value, rules = normalize_amount("ألفان")

    assert value == 2000.0
    assert "NUMBER_WORDS_TO_NUMERIC" in rules


def test_phone_is_standardized():
    phone, changed = normalize_phone("+967 777559764")

    assert phone == "777559764"
    assert changed is True


def test_repeated_email_symbols_are_fixed():
    email, changed = normalize_email(
        "user@@example..com"
    )

    assert email == "user@example.com"
    assert changed is True


def test_nonstandard_date_becomes_iso():
    value, changed = normalize_date(
        "17-01-2025 04:50:00"
    )

    assert value == "2025-01-17T04:50:00"
    assert changed is True


def test_total_is_recalculated_when_components_are_valid():
    record = make_record()

    record["total_amount"] = "60000.0"

    result = evaluate_record(record)

    assert result.quality_status == "corrected"
    assert result.record["total_amount"] == 52000.0

    rule_codes = {
        item["rule_code"]
        for item in result.corrections
    }

    assert "TOTAL_RECALCULATED" in rule_codes


def test_doctor_qty_string_is_corrected():
    record = make_record()

    items = json.loads(
        record["items_json"]
    )

    items[0]["qty"] = "1"

    record["items_json"] = json.dumps(
        items,
        ensure_ascii=False,
    )

    result = evaluate_record(
        record
    )

    assert (
        result.quality_status
        == "corrected"
    )

    assert (
        result.record["items"][0]["qty"]
        == 1
    )

    rules = {
        item["rule_code"]
        for item in result.corrections
    }

    assert (
        "QTY_STRING_TO_NUMERIC"
        in rules
    )


def test_doctor_missing_item_sku_is_quarantined():
    record = make_record()

    items = json.loads(
        record["items_json"]
    )

    items[0].pop(
        "sku"
    )

    record["items_json"] = json.dumps(
        items,
        ensure_ascii=False,
    )

    result = evaluate_record(
        record
    )

    assert (
        result.quality_status
        == "quarantined"
    )

    assert (
        "ITEM_SKU_MISSING"
        in result.error_codes
    )

