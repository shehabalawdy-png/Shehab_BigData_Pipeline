import pytest

from src.quality_rules import evaluate_record, normalize_amount, normalize_date


def test_completely_dirty_record_is_quarantined_not_crashed():
    result = evaluate_record({})
    assert result.quality_status == "quarantined"
    assert "ID_ORDER_MISSING" in result.error_codes
    assert "ID_CUSTOMER_MISSING" in result.error_codes
    assert "JSON_ITEMS_CORRUPTED" in result.error_codes


def test_lowercase_yer_inside_amount_is_supported():
    value, rules = normalize_amount("54,000 yer")
    assert value == 54000.0
    assert "REMOVE_CURRENCY_TEXT" in rules


def test_date_only_can_be_normalized_safely():
    value, changed = normalize_date("17-01-2025")
    assert value == "2025-01-17T00:00:00"
    assert changed is True


def test_non_dictionary_record_is_rejected_clearly():
    with pytest.raises(TypeError, match="dictionary"):
        evaluate_record("not-a-record")
