import random

from src.quality_rules import evaluate_record
from tests.test_classification import make_valid_record


DIRTY_VALUES = [
    "", None, "   ", "٥٠٠٠", "not-json", "@@", "-10", "عملة؟",
    "2025-99-99", "ريال يمني", "مدفوع", "+967 777 559 764",
]


def test_quality_engine_never_crashes_on_dirty_csv_like_values():
    rng = random.Random(20260821)
    fields = list(make_valid_record())
    for _ in range(300):
        record = make_valid_record()
        for field in rng.sample(fields, rng.randint(1, min(6, len(fields)))):
            record[field] = rng.choice(DIRTY_VALUES)
        result = evaluate_record(record)
        assert result.quality_status in {"valid", "corrected", "quarantined"}
        assert isinstance(result.corrections, list)
        assert isinstance(result.error_codes, list)
        assert isinstance(result.error_details, list)


def test_official_conflicting_errors_marker_is_added():
    result = evaluate_record({})
    assert result.quality_status == "quarantined"
    assert "ERRORS_CONFLICTING_MULTIPLE" in result.error_codes
