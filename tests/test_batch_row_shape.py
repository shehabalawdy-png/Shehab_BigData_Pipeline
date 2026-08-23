from src.schema import row_to_raw_record
from src.schema import RAW_COLUMNS


def test_missing_cells_are_preserved_as_none_for_raw():
    row = ["x"] * (len(RAW_COLUMNS) - 2)
    record, issue = row_to_raw_record(RAW_COLUMNS, row)
    assert issue and "MISSING_CELLS" in issue
    assert record[RAW_COLUMNS[-1]] is None


def test_extra_cells_are_preserved_for_quarantine_instead_of_crashing():
    row = ["x"] * len(RAW_COLUMNS) + ["EXTRA-A", "EXTRA-B"]
    record, issue = row_to_raw_record(RAW_COLUMNS, row)
    assert issue and "EXTRA_CELLS" in issue
    assert record["_raw_extra_values"] == ["EXTRA-A", "EXTRA-B"]
