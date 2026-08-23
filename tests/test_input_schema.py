import csv
from pathlib import Path

import pytest

from src.schema import RAW_COLUMNS, CsvSchemaError, validate_csv_header


def write_csv(path: Path, header):
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerow(["x"] * len(header))


def test_exact_schema_passes(tmp_path):
    path = tmp_path / "valid.csv"
    write_csv(path, RAW_COLUMNS)
    result = validate_csv_header(path)
    assert result.is_valid is True
    assert result.reordered is False


def test_reordered_columns_are_accepted_safely(tmp_path):
    path = tmp_path / "reordered.csv"
    header = list(RAW_COLUMNS)
    header[0], header[1] = header[1], header[0]
    write_csv(path, header)
    result = validate_csv_header(path)
    assert result.is_valid is True
    assert result.reordered is True


def test_missing_column_fails_before_loading(tmp_path):
    path = tmp_path / "missing.csv"
    write_csv(path, RAW_COLUMNS[:-1])
    with pytest.raises(CsvSchemaError, match="missing="):
        validate_csv_header(path)


def test_extra_column_fails_before_loading(tmp_path):
    path = tmp_path / "extra.csv"
    write_csv(path, [*RAW_COLUMNS, "unexpected_column"])
    with pytest.raises(CsvSchemaError, match="extra="):
        validate_csv_header(path)


def test_duplicate_column_fails(tmp_path):
    path = tmp_path / "duplicate.csv"
    header = list(RAW_COLUMNS)
    header[-1] = header[0]
    write_csv(path, header)
    with pytest.raises(CsvSchemaError, match="duplicate columns"):
        validate_csv_header(path)
