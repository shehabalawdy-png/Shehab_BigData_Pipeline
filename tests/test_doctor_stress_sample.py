import csv
from collections import Counter
from pathlib import Path

from src.quality_rules import evaluate_record
from src.schema import validate_csv_header


PROJECT_ROOT = Path(__file__).resolve().parents[1]
STRESS_FILE = PROJECT_ROOT / "demo" / "doctor_stress_test.csv"


def _rows():
    with STRESS_FILE.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def test_doctor_stress_sample_schema_and_quality_profile():
    check = validate_csv_header(STRESS_FILE)
    assert check.is_valid

    counts = Counter()
    for row in _rows():
        counts[evaluate_record(row).quality_status] += 1

    assert counts == {
        "valid": 4,
        "corrected": 4,
        "quarantined": 8,
    }


def test_doctor_stress_sample_contains_duplicate_business_key():
    ids = [row["order_id"].strip() for row in _rows() if row["order_id"].strip()]
    counts = Counter(ids)

    duplicates = {order_id for order_id, count in counts.items() if count > 1}
    assert duplicates == {"ORD-DUP"}
    assert counts["ORD-DUP"] == 2
