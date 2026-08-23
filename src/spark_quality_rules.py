"""Spark-native data-quality expressions for the large-file path.

This module mirrors the deterministic rules in ``src.quality_rules`` using
Spark SQL/DataFrame expressions only.  No Python UDF is used, so large runs do
not serialize every business row through a Python worker.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from src.quality_rules import (
    NUMBER_WORDS,
    PAYMENT_STATUS_ALIASES,
    VALID_PAYMENT_STATUSES,
    VALID_STATUSES,
)


ITEM_SCHEMA = StructType(
    [
        StructField("sku", StringType(), True),
        StructField("name", StringType(), True),
        StructField("qty", DoubleType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total", DoubleType(), True),
    ]
)

CORRECTION_SCHEMA = StructType(
    [
        StructField("field", StringType(), True),
        StructField("original_value", StringType(), True),
        StructField("corrected_value", StringType(), True),
        StructField("rule_code", StringType(), True),
        StructField("quality_status", StringType(), True),
    ]
)

TRIM_FIELDS = (
    "order_id",
    "status",
    "customer_id",
    "customer_name",
    "customer_phone",
    "customer_email",
    "city",
    "district",
    "delivery_type",
    "payment_method",
    "payment_status",
    "currency",
)

NUMERIC_FIELDS = (
    "delivery_cost",
    "payment_amount",
    "total_amount",
)

CRITICAL_CODES = (
    "ID_ORDER_MISSING",
    "ID_CUSTOMER_MISSING",
    "DATE_IMPOSSIBLE_INVALID",
    "JSON_ITEMS_CORRUPTED",
    "ITEMS_EMPTY",
    "PRICE_UNKNOWN",
    "VALUE_NEGATIVE_AMBIGUOUS",
    "CURRENCY_UNKNOWN",
    "CSV_ROW_SHAPE_INVALID",
)

ARABIC_NUMERIC_SOURCE = "٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹٫٬"
ARABIC_NUMERIC_TARGET = "01234567890123456789.,"


def _empty_strings():
    return F.from_json(F.lit("[]"), ArrayType(StringType()))


def _empty_corrections():
    return F.from_json(F.lit("[]"), ArrayType(CORRECTION_SCHEMA))


def _text(column):
    return column.cast("string")


def _maybe_correction(condition, field, original, corrected, rule_code):
    item = F.struct(
        F.lit(field).alias("field"),
        _text(original).alias("original_value"),
        _text(corrected).alias("corrected_value"),
        F.lit(rule_code).alias("rule_code"),
        F.lit("corrected").alias("quality_status"),
    )
    return F.when(condition, F.array(item)).otherwise(_empty_corrections())


def _error_detail_json(code, field, value, message):
    # Existing Spark quarantine stores error_details as Array[String].  Keep a
    # structured JSON string so field/value/message remain inspectable.
    return F.to_json(
        F.struct(
            F.lit(code).alias("code"),
            F.lit(field).alias("field"),
            _text(value).alias("value"),
            F.lit(message).alias("message"),
        ),
        options={"ignoreNullFields": "false"},
    )


def _maybe_error_code(condition, code):
    return F.when(condition, F.array(F.lit(code))).otherwise(_empty_strings())


def _maybe_error_detail(condition, code, field, value, message):
    return F.when(
        condition,
        F.array(_error_detail_json(code, field, value, message)),
    ).otherwise(_empty_strings())


def _concat_arrays(parts, empty_factory):
    if not parts:
        return empty_factory()
    result = parts[0]
    for part in parts[1:]:
        result = F.concat(result, part)
    return result


def _normalized_amount(raw_value):
    """Return Spark expressions matching normalize_amount's safe rules."""
    text = F.trim(raw_value.cast("string"))

    word_value = None
    word_condition = None
    for word, number in NUMBER_WORDS.items():
        condition = text == F.lit(word)
        word_condition = condition if word_condition is None else (word_condition | condition)
        word_value = (
            F.when(condition, F.lit(float(number)))
            if word_value is None
            else word_value.when(condition, F.lit(float(number)))
        )
    if word_value is None:
        word_condition = F.lit(False)
        word_value = F.lit(None).cast("double")
    else:
        word_value = word_value.otherwise(F.lit(None).cast("double"))

    translated = F.translate(text, ARABIC_NUMERIC_SOURCE, ARABIC_NUMERIC_TARGET)
    currency_removed = F.trim(
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(translated, "(?i)YER", ""),
                "ريال يمني",
                "",
            ),
            "ريال",
            "",
        )
    )
    comma_removed = F.regexp_replace(currency_removed, ",", "")

    # Spark 4.2 Column.try_cast returns NULL instead of raising under ANSI
    # mode, which matches the Python path's PRICE_UNKNOWN behavior.
    parsed_regular = comma_removed.try_cast("double")

    number = F.when(word_condition, word_value).otherwise(parsed_regular)
    parse_ok = number.isNotNull()

    rule_number_words = word_condition
    rule_arabic = (~word_condition) & parse_ok & (translated != text)
    rule_currency = (~word_condition) & parse_ok & (currency_removed != translated)
    rule_commas = (~word_condition) & parse_ok & (F.instr(currency_removed, ",") > 0)

    return {
        "number": number,
        "parse_ok": parse_ok,
        "rule_number_words": rule_number_words,
        "rule_arabic": rule_arabic,
        "rule_currency": rule_currency,
        "rule_commas": rule_commas,
    }


def _parsed_date(raw_value):
    original = F.trim(raw_value.cast("string"))
    translated = F.translate(original, ARABIC_NUMERIC_SOURCE, ARABIC_NUMERIC_TARGET)

    formats = (
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.S",
        "yyyy-MM-dd'T'HH:mm:ss.SS",
        "yyyy-MM-dd'T'HH:mm:ss.SSS",
        "yyyy-MM-dd'T'HH:mm:ss.SSSS",
        "yyyy-MM-dd'T'HH:mm:ss.SSSSS",
        "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
        "dd-MM-yyyy HH:mm:ss",
        "yyyy/MM/dd HH:mm:ss",
        "dd/MM/yyyy HH:mm:ss",
        "yyyy-MM-dd",
        "dd-MM-yyyy",
        "yyyy/MM/dd",
        "dd/MM/yyyy",
    )

    parsed = F.coalesce(
        *[
            F.try_to_timestamp(translated, F.lit(fmt))
            for fmt in formats
        ]
    )
    normalized = F.date_format(parsed, "yyyy-MM-dd'T'HH:mm:ss")
    return original, parsed, normalized


def _first_item_error(items):
    """Return the first item-level error code, preserving Python rule order."""
    return F.aggregate(
        items,
        F.lit(None).cast("string"),
        lambda acc, item: F.when(acc.isNotNull(), acc).otherwise(
            F.when(item.isNull(), F.lit("JSON_ITEMS_CORRUPTED"))
            .when(item["qty"].isNull(), F.lit("JSON_ITEMS_CORRUPTED"))
            .when(item["qty"] < 0, F.lit("VALUE_NEGATIVE_AMBIGUOUS"))
            .when(item["total"].isNull(), F.lit("JSON_ITEMS_CORRUPTED"))
            .when(item["total"] < 0, F.lit("JSON_ITEMS_CORRUPTED"))
            .otherwise(F.lit(None).cast("string"))
        ),
    )


def _stable_spark_hash(processed_record):
    """Deterministic business-state hash using Spark-native expressions only."""
    return F.sha2(
        F.to_json(
            processed_record,
            options={"ignoreNullFields": "false"},
        ),
        256,
    )


def apply_native_quality(dataframe):
    """Add quality/result columns without executing any Python UDF per row.

    Output columns used by ``spark_elt_pipeline``:
      _processed_record, _corrections, _base_error_codes,
      _base_error_details, _base_status, _record_hash
    """
    raw = {name: F.col(f"record_raw.{name}") for name in (
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
        "items_json",
    )}

    processed = dict(raw)
    corrections = []
    error_code_parts = []
    error_detail_parts = []

    # Rule 1: whitespace for the same fields as quality_rules.py.
    for field in TRIM_FIELDS:
        trimmed = F.trim(raw[field])
        corrections.append(
            _maybe_correction(
                raw[field].isNotNull() & (trimmed != raw[field]),
                field,
                raw[field],
                trimmed,
                "TRIM_WHITESPACE",
            )
        )
        processed[field] = trimmed

    # Required IDs are checked after trimming.
    order_missing = processed["order_id"].isNull() | (F.length(processed["order_id"]) == 0)
    customer_missing = processed["customer_id"].isNull() | (F.length(processed["customer_id"]) == 0)
    error_code_parts.extend([
        _maybe_error_code(order_missing, "ID_ORDER_MISSING"),
        _maybe_error_code(customer_missing, "ID_CUSTOMER_MISSING"),
    ])
    error_detail_parts.extend([
        _maybe_error_detail(
            order_missing,
            "ID_ORDER_MISSING",
            "order_id",
            processed["order_id"],
            "Order ID is required.",
        ),
        _maybe_error_detail(
            customer_missing,
            "ID_CUSTOMER_MISSING",
            "customer_id",
            processed["customer_id"],
            "Customer ID is required.",
        ),
    ])

    # Rule 2: date normalization.
    date_original, date_parsed, date_normalized = _parsed_date(raw["order_date"])
    date_valid = date_parsed.isNotNull()
    date_changed = date_valid & (date_normalized != date_original)
    corrections.append(
        _maybe_correction(
            date_changed,
            "order_date",
            raw["order_date"],
            date_normalized,
            "DATE_TO_ISO",
        )
    )
    date_invalid = ~date_valid
    error_code_parts.append(_maybe_error_code(date_invalid, "DATE_IMPOSSIBLE_INVALID"))
    error_detail_parts.append(
        _maybe_error_detail(
            date_invalid,
            "DATE_IMPOSSIBLE_INVALID",
            "order_date",
            raw["order_date"],
            "impossible or unsupported date",
        )
    )
    processed["order_date"] = F.when(date_valid, date_normalized).otherwise(raw["order_date"])

    # Rules 3-6: numeric normalization.
    numeric_results = {}
    for field in NUMERIC_FIELDS:
        info = _normalized_amount(raw[field])
        numeric_results[field] = info
        processed[field] = info["number"]

        corrections.extend([
            _maybe_correction(
                info["rule_number_words"], field, raw[field], info["number"], "NUMBER_WORDS_TO_NUMERIC"
            ),
            _maybe_correction(
                info["rule_arabic"], field, raw[field], info["number"], "ARABIC_DIGITS_TO_LATIN"
            ),
            _maybe_correction(
                info["rule_currency"], field, raw[field], info["number"], "REMOVE_CURRENCY_TEXT"
            ),
            _maybe_correction(
                info["rule_commas"], field, raw[field], info["number"], "REMOVE_THOUSANDS_SEPARATOR"
            ),
        ])

        price_unknown = ~info["parse_ok"]
        negative = info["parse_ok"] & (info["number"] < 0)
        error_code_parts.extend([
            _maybe_error_code(price_unknown, "PRICE_UNKNOWN"),
            _maybe_error_code(negative, "VALUE_NEGATIVE_AMBIGUOUS"),
        ])
        error_detail_parts.extend([
            _maybe_error_detail(
                price_unknown,
                "PRICE_UNKNOWN",
                field,
                raw[field],
                "cannot parse numeric value",
            ),
            _maybe_error_detail(
                negative,
                "VALUE_NEGATIVE_AMBIGUOUS",
                field,
                info["number"],
                "Negative money value cannot be corrected safely.",
            ),
        ])

    # Rule 7: currency normalization. Uses already-trimmed value, matching the
    # Python rule ordering.
    currency_original = processed["currency"]
    currency_upper = F.upper(F.coalesce(currency_original, F.lit("")))
    currency_is_yer = currency_upper == F.lit("YER")
    currency_arabic = F.coalesce(
        currency_original.isin("ريال يمني", "ريال", "ر.ي"),
        F.lit(False),
    )
    currency_valid = currency_is_yer | currency_arabic
    processed["currency"] = F.when(currency_valid, F.lit("YER")).otherwise(currency_original)
    corrections.append(
        _maybe_correction(
            currency_arabic,
            "currency",
            currency_original,
            F.lit("YER"),
            "CURRENCY_TO_YER",
        )
    )
    currency_unknown = ~currency_valid
    error_code_parts.append(_maybe_error_code(currency_unknown, "CURRENCY_UNKNOWN"))
    error_detail_parts.append(
        _maybe_error_detail(
            currency_unknown,
            "CURRENCY_UNKNOWN",
            "currency",
            currency_original,
            "Currency cannot be identified safely.",
        )
    )

    # Rule 8: phone normalization.
    phone_original = processed["customer_phone"]
    phone_translated = F.translate(
        F.coalesce(phone_original, F.lit("")),
        ARABIC_NUMERIC_SOURCE,
        ARABIC_NUMERIC_TARGET,
    )
    phone_digits = F.regexp_replace(phone_translated, r"[^0-9]", "")
    phone_digits = F.when(
        (F.length(phone_digits) == 12) & phone_digits.startswith("967"),
        F.substring(phone_digits, 4, 9),
    ).otherwise(phone_digits)
    phone_valid = (F.length(phone_digits) == 9) & phone_digits.startswith("7")
    phone_changed = phone_valid & (phone_digits != phone_original)
    processed["customer_phone"] = F.when(phone_valid, phone_digits).otherwise(phone_original)
    corrections.append(
        _maybe_correction(
            phone_changed,
            "customer_phone",
            phone_original,
            phone_digits,
            "PHONE_STANDARDIZATION",
        )
    )
    phone_invalid = ~phone_valid
    error_code_parts.append(_maybe_error_code(phone_invalid, "PHONE_INVALID"))
    error_detail_parts.append(
        _maybe_error_detail(
            phone_invalid,
            "PHONE_INVALID",
            "customer_phone",
            phone_original,
            "invalid phone",
        )
    )

    # Rule 9: email repeated symbols.
    email_original = processed["customer_email"]
    email_fixed = F.regexp_replace(
        F.regexp_replace(F.coalesce(email_original, F.lit("")), r"@{2,}", "@"),
        r"\.{2,}",
        ".",
    )
    email_valid = email_fixed.rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    email_changed = email_valid & (email_fixed != email_original)
    processed["customer_email"] = F.when(email_valid, email_fixed).otherwise(email_original)
    corrections.append(
        _maybe_correction(
            email_changed,
            "customer_email",
            email_original,
            email_fixed,
            "EMAIL_REPEATED_SYMBOLS",
        )
    )
    email_invalid = ~email_valid
    error_code_parts.append(_maybe_error_code(email_invalid, "EMAIL_INVALID"))
    error_detail_parts.append(
        _maybe_error_detail(
            email_invalid,
            "EMAIL_INVALID",
            "customer_email",
            email_original,
            "invalid email",
        )
    )

    # Status dictionary validation.
    status_invalid = processed["status"].isNull() | (~processed["status"].isin(*sorted(VALID_STATUSES)))
    error_code_parts.append(_maybe_error_code(status_invalid, "STATUS_UNKNOWN"))
    error_detail_parts.append(
        _maybe_error_detail(
            status_invalid,
            "STATUS_UNKNOWN",
            "status",
            processed["status"],
            "Order status is outside the allowed dictionary.",
        )
    )

    # Rule 10: payment status aliases.
    payment_original = processed["payment_status"]
    alias_condition = F.lit(False)
    alias_value = payment_original
    for alias, normalized in PAYMENT_STATUS_ALIASES.items():
        condition = F.coalesce(
            payment_original == F.lit(alias),
            F.lit(False),
        )
        alias_condition = alias_condition | condition
        alias_value = F.when(condition, F.lit(normalized)).otherwise(alias_value)
    processed["payment_status"] = alias_value
    corrections.append(
        _maybe_correction(
            alias_condition,
            "payment_status",
            payment_original,
            alias_value,
            "PAYMENT_STATUS_SYNONYM",
        )
    )
    payment_unknown = (~alias_condition) & (
        payment_original.isNull()
        | (~payment_original.isin(*sorted(VALID_PAYMENT_STATUSES)))
    )
    error_code_parts.append(_maybe_error_code(payment_unknown, "PAYMENT_STATUS_UNKNOWN"))
    error_detail_parts.append(
        _maybe_error_detail(
            payment_unknown,
            "PAYMENT_STATUS_UNKNOWN",
            "payment_status",
            payment_original,
            "Payment status is outside the allowed dictionary.",
        )
    )

    # Items validation. Typed from_json keeps this entirely in Spark/JVM.
    raw_items = raw["items_json"]
    items = F.from_json(raw_items, ArrayType(ITEM_SCHEMA), {"mode": "PERMISSIVE"})
    items_parse_failed = items.isNull()
    items_empty = (~items_parse_failed) & (F.size(items) == 0)
    item_error = F.when(items_parse_failed | items_empty, F.lit(None).cast("string")).otherwise(
        _first_item_error(items)
    )
    negative_qty = item_error == F.lit("VALUE_NEGATIVE_AMBIGUOUS")
    item_corrupted = items_parse_failed | (item_error == F.lit("JSON_ITEMS_CORRUPTED"))
    items_valid = (~items_parse_failed) & (~items_empty) & item_error.isNull()

    error_code_parts.extend([
        _maybe_error_code(items_empty, "ITEMS_EMPTY"),
        _maybe_error_code(negative_qty, "VALUE_NEGATIVE_AMBIGUOUS"),
        _maybe_error_code(item_corrupted, "JSON_ITEMS_CORRUPTED"),
    ])
    error_detail_parts.extend([
        _maybe_error_detail(
            items_empty,
            "ITEMS_EMPTY",
            "items_json",
            raw_items,
            "items list is empty",
        ),
        _maybe_error_detail(
            negative_qty,
            "VALUE_NEGATIVE_AMBIGUOUS",
            "items_json",
            raw_items,
            "negative item quantity",
        ),
        _maybe_error_detail(
            item_corrupted,
            "JSON_ITEMS_CORRUPTED",
            "items_json",
            raw_items,
            "items_json or item structure is invalid",
        ),
    ])

    processed["items"] = F.when(items_valid, items).otherwise(F.lit(None).cast(ArrayType(ITEM_SCHEMA)))

    # Rule 11: total recalculation.
    item_total_sum = F.aggregate(
        items,
        F.lit(0.0),
        lambda acc, item: acc + item["total"],
    )
    calculated_total = item_total_sum + processed["delivery_cost"]
    current_total = processed["total_amount"]
    can_recalculate = (
        items_valid
        & numeric_results["delivery_cost"]["parse_ok"]
        & numeric_results["total_amount"]["parse_ok"]
    )
    total_changed = can_recalculate & (F.abs(current_total - calculated_total) > F.lit(0.01))
    corrections.append(
        _maybe_correction(
            total_changed,
            "total_amount",
            current_total,
            calculated_total,
            "TOTAL_RECALCULATED",
        )
    )
    processed["total_amount"] = F.when(total_changed, calculated_total).otherwise(current_total)

    # Build arrays after all deterministic rule checks.
    correction_array = _concat_arrays(corrections, _empty_corrections)
    error_codes = F.array_distinct(_concat_arrays(error_code_parts, _empty_strings))
    error_details = _concat_arrays(error_detail_parts, _empty_strings)

    critical_literals = F.array(*[F.lit(code) for code in CRITICAL_CODES])
    multiple_critical = F.size(F.array_intersect(error_codes, critical_literals)) >= 2
    error_codes = F.when(
        multiple_critical,
        F.array_union(error_codes, F.array(F.lit("ERRORS_CONFLICTING_MULTIPLE"))),
    ).otherwise(error_codes)
    error_details = F.when(
        multiple_critical,
        F.concat(
            error_details,
            F.array(
                _error_detail_json(
                    "ERRORS_CONFLICTING_MULTIPLE",
                    "_record",
                    F.lit(None).cast("string"),
                    "Multiple independent critical errors prevent safe correction.",
                )
            ),
        ),
    ).otherwise(error_details)

    base_status = (
        F.when(F.size(error_codes) > 0, F.lit("quarantined"))
        .when(F.size(correction_array) > 0, F.lit("corrected"))
        .otherwise(F.lit("valid"))
    )

    processed_record = F.struct(
        processed["order_id"].alias("order_id"),
        processed["order_date"].alias("order_date"),
        processed["status"].alias("status"),
        processed["customer_id"].alias("customer_id"),
        processed["customer_name"].alias("customer_name"),
        processed["customer_phone"].alias("customer_phone"),
        processed["customer_email"].alias("customer_email"),
        processed["city"].alias("city"),
        processed["district"].alias("district"),
        processed["delivery_type"].alias("delivery_type"),
        processed["delivery_cost"].alias("delivery_cost"),
        processed["payment_method"].alias("payment_method"),
        processed["payment_status"].alias("payment_status"),
        processed["payment_amount"].alias("payment_amount"),
        processed["currency"].alias("currency"),
        processed["total_amount"].alias("total_amount"),
        processed["items"].alias("items"),
    )

    return (
        dataframe
        .withColumn("_processed_record", processed_record)
        .withColumn("_corrections", correction_array)
        .withColumn("_base_error_codes", error_codes)
        .withColumn("_base_error_details", error_details)
        .withColumn("_base_status", base_status)
        .withColumn("_record_hash", _stable_spark_hash(F.col("_processed_record")))
    )
