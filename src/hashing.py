import hashlib
import json


BUSINESS_FIELDS = (
    "order_id",
    "order_date",
    "status",
    "customer_id",
    "customer_name",
    "customer_phone",
    "customer_email",
    "city",
    "district",
    "delivery_type",
    "delivery_cost",
    "payment_method",
    "payment_status",
    "payment_amount",
    "currency",
    "total_amount",
    "items",
)


def canonical_business_payload(record: dict) -> dict:
    """Return only the final business state, independent of run/engine metadata."""
    return {field: record.get(field) for field in BUSINESS_FIELDS}


def business_record_hash(record: dict) -> str:
    payload = canonical_business_payload(record)
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
