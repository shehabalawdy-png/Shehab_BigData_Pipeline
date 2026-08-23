import json

from src.quality_rules import evaluate_record


def make_valid_record():
    return {
        "order_id": "طلب-900001",
        "order_date": "2025-01-17T04:50:00",
        "status": "مؤكد",
        "customer_id": "عميل-900001",
        "customer_name": "محمد",
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


def test_clean_record_is_valid():
    result = evaluate_record(
        make_valid_record()
    )

    assert result.quality_status == "valid"
    assert result.error_codes == []
    assert result.corrections == []


def test_safe_changes_produce_corrected_status():
    record = make_valid_record()

    record["status"] = "  مؤكد  "
    record["currency"] = "ريال يمني"
    record["payment_status"] = "مدفوع"

    result = evaluate_record(record)

    assert result.quality_status == "corrected"
    assert result.error_codes == []
    assert len(result.corrections) >= 3


def test_missing_order_id_goes_to_quarantine():
    record = make_valid_record()

    record["order_id"] = ""

    result = evaluate_record(record)

    assert result.quality_status == "quarantined"
    assert "ID_ORDER_MISSING" in result.error_codes


def test_missing_customer_id_goes_to_quarantine():
    record = make_valid_record()

    record["customer_id"] = ""

    result = evaluate_record(record)

    assert result.quality_status == "quarantined"
    assert "ID_CUSTOMER_MISSING" in result.error_codes


def test_corrupted_items_json_goes_to_quarantine():
    record = make_valid_record()

    record["items_json"] = "not-json"

    result = evaluate_record(record)

    assert result.quality_status == "quarantined"
    assert "JSON_ITEMS_CORRUPTED" in result.error_codes


def test_negative_quantity_goes_to_quarantine():
    record = make_valid_record()

    record["items_json"] = json.dumps(
        [
            {
                "sku": "SKU-1",
                "name": "منتج",
                "qty": -2,
                "unit_price": 50000.0,
                "total": 50000.0,
            }
        ],
        ensure_ascii=False,
    )

    result = evaluate_record(record)

    assert result.quality_status == "quarantined"
    assert (
        "VALUE_NEGATIVE_AMBIGUOUS"
        in result.error_codes
    )


def test_impossible_date_goes_to_quarantine():
    record = make_valid_record()

    record["order_date"] = (
        "2025-19-45 99:70:00"
    )

    result = evaluate_record(record)

    assert result.quality_status == "quarantined"
    assert (
        "DATE_IMPOSSIBLE_INVALID"
        in result.error_codes
    )


def test_unknown_currency_goes_to_quarantine():
    record = make_valid_record()

    record["currency"] = "عملة غير معروفة"

    result = evaluate_record(record)

    assert result.quality_status == "quarantined"
    assert "CURRENCY_UNKNOWN" in result.error_codes
