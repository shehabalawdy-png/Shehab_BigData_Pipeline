import csv
from dataclasses import dataclass
from pathlib import Path


RAW_COLUMNS = (
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
)


class CsvSchemaError(ValueError):
    """Raised when an input CSV is incompatible with the orders schema."""


@dataclass(frozen=True)
class CsvHeaderCheck:
    path: Path
    columns: tuple[str, ...]
    expected_columns: tuple[str, ...]

    @property
    def is_valid(self) -> bool:
        return set(self.columns) == set(self.expected_columns)

    @property
    def reordered(self) -> bool:
        return self.columns != self.expected_columns


def read_csv_header(input_file) -> tuple[Path, tuple[str, ...]]:
    """Read only the header; the dataset itself is never loaded into memory."""
    path = Path(input_file).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Input CSV not found: {path}")
    if not path.is_file():
        raise ValueError(f"Input path is not a file: {path}")
    if path.stat().st_size == 0:
        raise CsvSchemaError(f"Input CSV is empty: {path}")

    try:
        with path.open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.reader(csv_file)
            header = next(reader, None)
    except UnicodeDecodeError as exc:
        raise CsvSchemaError(
            "CSV must be UTF-8 encoded (UTF-8 or UTF-8 with BOM)."
        ) from exc

    if not header:
        raise CsvSchemaError("CSV header is missing.")

    columns = tuple(column.strip() for column in header)
    if any(not column for column in columns):
        raise CsvSchemaError("CSV header contains an empty column name.")

    duplicates = sorted({c for c in columns if columns.count(c) > 1})
    if duplicates:
        raise CsvSchemaError(
            "CSV header contains duplicate columns: " + ", ".join(duplicates)
        )

    return path, columns


def validate_csv_header(input_file) -> CsvHeaderCheck:
    """Validate required columns while allowing a harmless column reordering.

    The project still uses an explicit StringType Spark schema. When the header
    order differs, the Spark loader builds that explicit schema in the incoming
    order and then selects the canonical RAW_COLUMNS order by name.
    """
    path, columns = read_csv_header(input_file)
    expected = tuple(RAW_COLUMNS)

    missing = [column for column in expected if column not in columns]
    extra = [column for column in columns if column not in expected]
    if missing or extra:
        details = []
        if missing:
            details.append("missing=" + ", ".join(missing))
        if extra:
            details.append("extra=" + ", ".join(extra))
        raise CsvSchemaError(
            "CSV schema does not match the required order schema ("
            + "; ".join(details)
            + ")."
        )

    return CsvHeaderCheck(
        path=path,
        columns=columns,
        expected_columns=expected,
    )


def row_to_raw_record(header: tuple[str, ...], row: list[str]) -> tuple[dict, str | None]:
    """Map a CSV row safely while preserving extra cells for quarantine."""
    record = {name: row[i] if i < len(row) else None for i, name in enumerate(header)}
    issue = None
    if len(row) < len(header):
        issue = f"ROW_HAS_MISSING_CELLS expected={len(header)} actual={len(row)}"
    elif len(row) > len(header):
        record["_raw_extra_values"] = row[len(header):]
        issue = f"ROW_HAS_EXTRA_CELLS expected={len(header)} actual={len(row)}"
    return record, issue
