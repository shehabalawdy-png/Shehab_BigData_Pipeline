import json
import re
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime


# ---------------------------------------------------------
# Quality result
# ---------------------------------------------------------

@dataclass
class QualityResult:
    quality_status: str
    record: dict
    corrections: list
    error_codes: list
    error_details: list


# ---------------------------------------------------------
# Dictionaries and constants
# ---------------------------------------------------------

ARABIC_TRANSLATION = str.maketrans(
    {
        "٠": "0",
        "١": "1",
        "٢": "2",
        "٣": "3",
        "٤": "4",
        "٥": "5",
        "٦": "6",
        "٧": "7",
        "٨": "8",
        "٩": "9",
        "۰": "0",
        "۱": "1",
        "۲": "2",
        "۳": "3",
        "۴": "4",
        "۵": "5",
        "۶": "6",
        "۷": "7",
        "۸": "8",
        "۹": "9",
        "٫": ".",
        "٬": ",",
    }
)


NUMBER_WORDS = {
    "ألفان": 2000.0,
    "الفان": 2000.0,
    "خمسة آلاف": 5000.0,
    "خمسة الاف": 5000.0,
}


VALID_STATUSES = {
    "مرتجع",
    "مؤكد",
    "ملغي",
    "قيد الشحن",
    "قيد الانتظار",
    "تم التسليم",
}


PAYMENT_STATUS_ALIASES = {
    "مدفوع": "تم الدفع",
}


VALID_PAYMENT_STATUSES = {
    "تم الدفع",
    "بانتظار الدفع",
    "قيد الدفع",
}


EMAIL_PATTERN = re.compile(
    r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
)


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def add_correction(
    corrections,
    field,
    original_value,
    corrected_value,
    rule_code,
):
    corrections.append(
        {
            "field": field,
            "original_value": original_value,
            "corrected_value": corrected_value,
            "rule_code": rule_code,
            "quality_status": "corrected",
        }
    )


def add_error(
    error_codes,
    error_details,
    code,
    field,
    value,
    message,
):
    if code not in error_codes:
        error_codes.append(code)

    error_details.append(
        {
            "code": code,
            "field": field,
            "value": value,
            "message": message,
        }
    )


def trim_value(
    record,
    field,
    corrections,
):
    value = record.get(field)

    if not isinstance(value, str):
        return

    trimmed = value.strip()

    if trimmed != value:
        add_correction(
            corrections,
            field,
            value,
            trimmed,
            "TRIM_WHITESPACE",
        )

        record[field] = trimmed


# ---------------------------------------------------------
# Numeric normalization
# ---------------------------------------------------------

def normalize_amount(
    value,
):
    original = value

    if value is None:
        raise ValueError("missing numeric value")

    text = str(value).strip()

    if not text:
        raise ValueError("empty numeric value")

    if text in NUMBER_WORDS:
        return (
            NUMBER_WORDS[text],
            ["NUMBER_WORDS_TO_NUMERIC"],
        )

    rules = []

    translated = text.translate(
        ARABIC_TRANSLATION
    )

    if translated != text:
        rules.append(
            "ARABIC_DIGITS_TO_LATIN"
        )

    text = translated

    currency_removed = re.sub(
        r"(?i)YER",
        "",
        text,
    )
    currency_removed = (
        currency_removed
        .replace("ريال يمني", "")
        .replace("ريال", "")
        .strip()
    )

    if currency_removed != text:
        rules.append(
            "REMOVE_CURRENCY_TEXT"
        )

    text = currency_removed

    if "," in text:
        text = text.replace(",", "")

        rules.append(
            "REMOVE_THOUSANDS_SEPARATOR"
        )

    try:
        number = float(text)

    except ValueError as exc:
        raise ValueError(
            f"cannot parse numeric value: {original}"
        ) from exc

    return number, rules


# ---------------------------------------------------------
# Phone
# ---------------------------------------------------------

def normalize_phone(
    value,
):
    if value is None:
        raise ValueError("missing phone")

    original = str(value)
    text = original.translate(
        ARABIC_TRANSLATION
    )

    digits = re.sub(
        r"[^\d]",
        "",
        text,
    )

    if (
        len(digits) == 12
        and digits.startswith("967")
    ):
        digits = digits[3:]

    if (
        len(digits) != 9
        or not digits.startswith("7")
    ):
        raise ValueError(
            f"invalid phone: {original}"
        )

    changed = digits != original

    return digits, changed


# ---------------------------------------------------------
# Email
# ---------------------------------------------------------

def normalize_email(
    value,
):
    if value is None:
        raise ValueError("missing email")

    original = str(value)
    text = original.strip()

    fixed = re.sub(
        r"@{2,}",
        "@",
        text,
    )

    fixed = re.sub(
        r"\.{2,}",
        ".",
        fixed,
    )

    if not EMAIL_PATTERN.fullmatch(fixed):
        raise ValueError(
            f"invalid email: {original}"
        )

    return fixed, fixed != original


# ---------------------------------------------------------
# Date
# ---------------------------------------------------------

def normalize_date(
    value,
):
    if value is None:
        raise ValueError("missing date")

    original = str(value).strip()
    translated = original.translate(ARABIC_TRANSLATION)

    formats = (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%d-%m-%Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%Y/%m/%d",
        "%d/%m/%Y",
    )

    for fmt in formats:
        try:
            parsed = datetime.strptime(
                translated,
                fmt,
            )

            normalized = parsed.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )

            return (
                normalized,
                normalized != original,
            )

        except ValueError:
            continue

    raise ValueError(
        f"impossible or unsupported date: {original}"
    )


# ---------------------------------------------------------
# Items JSON
# ---------------------------------------------------------

def parse_items(
    value,
    corrections=None,
):
    try:
        items = json.loads(value)

    except Exception as exc:
        raise ValueError(
            "items_json cannot be parsed"
        ) from exc

    if not isinstance(items, list):
        raise ValueError(
            "items_json is not a list"
        )

    if not items:
        raise ValueError(
            "items list is empty"
        )

    for index, item in enumerate(items):

        if not isinstance(item, dict):
            raise ValueError(
                "item is not an object"
            )

        # SKU is required.
        sku = str(
            item.get("sku")
            or ""
        ).strip()

        if not sku:
            raise ValueError(
                "missing item sku"
            )

        item["sku"] = sku

        # Numeric quantity stored as a JSON string is safely correctable.
        qty = item.get("qty")

        if isinstance(qty, str):

            original_qty = qty
            qty_text = qty.strip()

            try:
                qty_number = float(
                    qty_text
                )

            except ValueError as exc:
                raise ValueError(
                    "item quantity is invalid"
                ) from exc

            if qty_number.is_integer():
                corrected_qty = int(
                    qty_number
                )
            else:
                corrected_qty = (
                    qty_number
                )

            item["qty"] = (
                corrected_qty
            )

            qty = corrected_qty

            if corrections is not None:
                add_correction(
                    corrections,
                    f"items[{index}].qty",
                    original_qty,
                    corrected_qty,
                    "QTY_STRING_TO_NUMERIC",
                )

        if (
            isinstance(qty, bool)
            or not isinstance(
                qty,
                (int, float),
            )
        ):
            raise ValueError(
                "item quantity is invalid"
            )

        if qty < 0:
            raise ValueError(
                "negative item quantity"
            )

        try:
            item_total = float(
                item["total"]
            )

        except (
            KeyError,
            TypeError,
            ValueError,
        ) as exc:
            raise ValueError(
                "item total is invalid"
            ) from exc

        if item_total < 0:
            raise ValueError(
                "negative item total"
            )

        item["total"] = (
            item_total
        )

    return items


# ---------------------------------------------------------
# Main evaluation
# ---------------------------------------------------------

def evaluate_record(
    raw_record,
):
    if not isinstance(raw_record, dict):
        raise TypeError("raw_record must be a dictionary.")

    record = deepcopy(raw_record)

    corrections = []
    error_codes = []
    error_details = []

    # A malformed CSV row must still survive Raw loading, but it cannot be
    # promoted to Validated because its column mapping is ambiguous.
    extra_values = record.pop("_raw_extra_values", None)
    if extra_values:
        add_error(
            error_codes,
            error_details,
            "CSV_ROW_SHAPE_INVALID",
            "_raw_extra_values",
            extra_values,
            "CSV row contains more cells than the validated header.",
        )

    # -----------------------------------------------------
    # Rule 1: whitespace
    # -----------------------------------------------------

    fields_to_trim = (
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

    for field in fields_to_trim:
        trim_value(
            record,
            field,
            corrections,
        )

    # -----------------------------------------------------
    # Required IDs
    # -----------------------------------------------------

    if not record.get("order_id"):
        add_error(
            error_codes,
            error_details,
            "ID_ORDER_MISSING",
            "order_id",
            record.get("order_id"),
            "Order ID is required.",
        )

    if not record.get("customer_id"):
        add_error(
            error_codes,
            error_details,
            "ID_CUSTOMER_MISSING",
            "customer_id",
            record.get("customer_id"),
            "Customer ID is required.",
        )

    # -----------------------------------------------------
    # Rule 2: date normalization
    # -----------------------------------------------------

    original_date = record.get(
        "order_date"
    )

    try:
        normalized_date, changed = (
            normalize_date(
                original_date
            )
        )

        record["order_date"] = (
            normalized_date
        )

        if changed:
            add_correction(
                corrections,
                "order_date",
                original_date,
                normalized_date,
                "DATE_TO_ISO",
            )

    except ValueError as error:
        add_error(
            error_codes,
            error_details,
            "DATE_IMPOSSIBLE_INVALID",
            "order_date",
            original_date,
            str(error),
        )

    # -----------------------------------------------------
    # Rules 3, 4, 5, 6:
    # Arabic digits, separators, currency text,
    # numeric words
    # -----------------------------------------------------

    numeric_fields = (
        "delivery_cost",
        "payment_amount",
        "total_amount",
    )

    for field in numeric_fields:

        original_value = record.get(
            field
        )

        try:
            number, rule_codes = (
                normalize_amount(
                    original_value
                )
            )

            record[field] = number

            for rule_code in rule_codes:
                add_correction(
                    corrections,
                    field,
                    original_value,
                    number,
                    rule_code,
                )

            if number < 0:
                add_error(
                    error_codes,
                    error_details,
                    "VALUE_NEGATIVE_AMBIGUOUS",
                    field,
                    number,
                    "Negative money value cannot be corrected safely.",
                )

        except ValueError as error:
            add_error(
                error_codes,
                error_details,
                "PRICE_UNKNOWN",
                field,
                original_value,
                str(error),
            )

    # -----------------------------------------------------
    # Rule 7: currency normalization
    # -----------------------------------------------------

    original_currency = record.get(
        "currency"
    )

    currency = str(
        original_currency or ""
    ).strip()

    if currency.upper() == "YER":
        record["currency"] = "YER"

    elif currency in {"ريال يمني", "ريال", "ر.ي"}:
        record["currency"] = "YER"

        add_correction(
            corrections,
            "currency",
            original_currency,
            "YER",
            "CURRENCY_TO_YER",
        )

    else:
        add_error(
            error_codes,
            error_details,
            "CURRENCY_UNKNOWN",
            "currency",
            original_currency,
            "Currency cannot be identified safely.",
        )

    # -----------------------------------------------------
    # Rule 8: phone normalization
    # -----------------------------------------------------

    original_phone = record.get(
        "customer_phone"
    )

    try:
        phone, changed = normalize_phone(
            original_phone
        )

        record["customer_phone"] = phone

        if changed:
            add_correction(
                corrections,
                "customer_phone",
                original_phone,
                phone,
                "PHONE_STANDARDIZATION",
            )

    except ValueError as error:
        add_error(
            error_codes,
            error_details,
            "PHONE_INVALID",
            "customer_phone",
            original_phone,
            str(error),
        )

    # -----------------------------------------------------
    # Rule 9: email repeated symbols
    # -----------------------------------------------------

    original_email = record.get(
        "customer_email"
    )

    try:
        email, changed = normalize_email(
            original_email
        )

        record["customer_email"] = email

        if changed:
            add_correction(
                corrections,
                "customer_email",
                original_email,
                email,
                "EMAIL_REPEATED_SYMBOLS",
            )

    except ValueError as error:
        add_error(
            error_codes,
            error_details,
            "EMAIL_INVALID",
            "customer_email",
            original_email,
            str(error),
        )

    # -----------------------------------------------------
    # Status validation
    # -----------------------------------------------------

    status = record.get("status")

    if status not in VALID_STATUSES:
        add_error(
            error_codes,
            error_details,
            "STATUS_UNKNOWN",
            "status",
            status,
            "Order status is outside the allowed dictionary.",
        )

    # -----------------------------------------------------
    # Rule 10: payment status aliases
    # -----------------------------------------------------

    payment_status = record.get(
        "payment_status"
    )

    if payment_status in PAYMENT_STATUS_ALIASES:

        normalized_payment_status = (
            PAYMENT_STATUS_ALIASES[
                payment_status
            ]
        )

        record["payment_status"] = (
            normalized_payment_status
        )

        add_correction(
            corrections,
            "payment_status",
            payment_status,
            normalized_payment_status,
            "PAYMENT_STATUS_SYNONYM",
        )

    elif (
        payment_status
        not in VALID_PAYMENT_STATUSES
    ):
        add_error(
            error_codes,
            error_details,
            "PAYMENT_STATUS_UNKNOWN",
            "payment_status",
            payment_status,
            "Payment status is outside the allowed dictionary.",
        )

    # -----------------------------------------------------
    # Items validation
    # -----------------------------------------------------

    items_are_valid = False

    try:
        items = parse_items(
            record.get(
                "items_json",
                "",
            ),
            corrections=corrections,
        )

        record["items"] = items
        items_are_valid = True

    except ValueError as error:

        message = str(error)

        if "empty" in message:
            error_code = "ITEMS_EMPTY"

        elif "missing item sku" in message:
            error_code = (
                "ITEM_SKU_MISSING"
            )

        elif "negative item quantity" in message:
            error_code = (
                "VALUE_NEGATIVE_AMBIGUOUS"
            )

        else:
            error_code = (
                "JSON_ITEMS_CORRUPTED"
            )

        add_error(
            error_codes,
            error_details,
            error_code,
            "items_json",
            record.get("items_json"),
            message,
        )

    # -----------------------------------------------------
    # Rule 11: total recalculation
    # -----------------------------------------------------

    if (
        items_are_valid
        and isinstance(
            record.get("delivery_cost"),
            (int, float),
        )
        and isinstance(
            record.get("total_amount"),
            (int, float),
        )
    ):
        calculated_total = (
            sum(
                float(item["total"])
                for item in record["items"]
            )
            + float(
                record["delivery_cost"]
            )
        )

        current_total = float(
            record["total_amount"]
        )

        if (
            abs(
                current_total
                - calculated_total
            )
            > 0.01
        ):
            record["total_amount"] = (
                calculated_total
            )

            add_correction(
                corrections,
                "total_amount",
                current_total,
                calculated_total,
                "TOTAL_RECALCULATED",
            )

    # Do not store JSON text as the final usable value.
    # Raw still preserves the untouched original.
    if "items" in record:
        record.pop(
            "items_json",
            None,
        )

    # -----------------------------------------------------
    # Official conflict marker for records with several independent
    # non-correctable problems. The original codes are kept as well.
    # -----------------------------------------------------
    critical_codes = {
        "ID_ORDER_MISSING",
        "ID_CUSTOMER_MISSING",
        "DATE_IMPOSSIBLE_INVALID",
        "JSON_ITEMS_CORRUPTED",
        "ITEMS_EMPTY",
        "ITEM_SKU_MISSING",
        "PRICE_UNKNOWN",
        "VALUE_NEGATIVE_AMBIGUOUS",
        "CURRENCY_UNKNOWN",
        "CSV_ROW_SHAPE_INVALID",
    }
    critical_count = sum(code in critical_codes for code in error_codes)
    if critical_count >= 2 and "ERRORS_CONFLICTING_MULTIPLE" not in error_codes:
        add_error(
            error_codes,
            error_details,
            "ERRORS_CONFLICTING_MULTIPLE",
            "_record",
            None,
            "Multiple independent critical errors prevent safe correction.",
        )

    # -----------------------------------------------------
    # Final classification
    # -----------------------------------------------------

    if error_codes:
        quality_status = "quarantined"

    elif corrections:
        quality_status = "corrected"

    else:
        quality_status = "valid"

    return QualityResult(
        quality_status=quality_status,
        record=record,
        corrections=corrections,
        error_codes=error_codes,
        error_details=error_details,
    )
