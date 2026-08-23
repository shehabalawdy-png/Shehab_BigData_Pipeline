from src.hashing import business_record_hash


def test_business_hash_ignores_run_and_engine_metadata():
    base = {
        "order_id": "O-1",
        "customer_id": "C-1",
        "items": [{"sku": "S", "qty": 1, "total": 10.0}],
        "total_amount": 10.0,
    }
    a = {**base, "source_run_id": "A", "engine_used": "python_batch"}
    b = {**base, "source_run_id": "B", "engine_used": "pyspark"}
    assert business_record_hash(a) == business_record_hash(b)


def test_business_hash_changes_when_business_state_changes():
    a = {"order_id": "O-1", "customer_name": "A"}
    b = {"order_id": "O-1", "customer_name": "B"}
    assert business_record_hash(a) != business_record_hash(b)
