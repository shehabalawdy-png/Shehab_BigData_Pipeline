from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPARK_ELT = (ROOT / "src" / "spark_elt_pipeline.py").read_text(encoding="utf-8")
SPARK_RULES = (ROOT / "src" / "spark_quality_rules.py").read_text(encoding="utf-8")


def test_large_quality_path_has_no_python_udf():
    assert "apply_native_quality" in SPARK_ELT
    assert "QUALITY_UDF" not in SPARK_ELT
    assert "F.udf(" not in SPARK_ELT
    assert "evaluate_record" not in SPARK_ELT
    assert "business_record_hash" not in SPARK_ELT
    assert "F.udf(" not in SPARK_RULES


def test_native_quality_keeps_required_assignment_error_codes():
    required = {
        "ID_ORDER_MISSING",
        "ID_CUSTOMER_MISSING",
        "DATE_IMPOSSIBLE_INVALID",
        "JSON_ITEMS_CORRUPTED",
        "ITEMS_EMPTY",
        "PRICE_UNKNOWN",
        "VALUE_NEGATIVE_AMBIGUOUS",
        "ID_ORDER_DUPLICATE",
        "ERRORS_CONFLICTING_MULTIPLE",
    }
    combined = SPARK_ELT + SPARK_RULES
    for code in required:
        assert code in combined


def test_native_quality_keeps_deterministic_correction_audit_codes():
    expected = {
        "TRIM_WHITESPACE",
        "DATE_TO_ISO",
        "NUMBER_WORDS_TO_NUMERIC",
        "ARABIC_DIGITS_TO_LATIN",
        "REMOVE_CURRENCY_TEXT",
        "REMOVE_THOUSANDS_SEPARATOR",
        "CURRENCY_TO_YER",
        "PHONE_STANDARDIZATION",
        "EMAIL_REPEATED_SYMBOLS",
        "PAYMENT_STATUS_SYNONYM",
        "TOTAL_RECALCULATED",
    }
    for code in expected:
        assert code in SPARK_RULES
