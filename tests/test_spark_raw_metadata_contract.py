from pathlib import Path


def test_spark_raw_loader_writes_assignment_row_field():
    source = (
        Path(__file__).resolve().parents[1] / "src" / "spark_loader.py"
    ).read_text(encoding="utf-8")
    assert '"number_row_source"' in source
    assert '"source_partition_id"' in source
    assert '"source_record_id"' in source


def test_spark_elt_reads_assignment_row_field():
    source = (
        Path(__file__).resolve().parents[1] / "src" / "spark_elt_pipeline.py"
    ).read_text(encoding="utf-8")
    assert '"number_row_source"' in source
    assert 'F.col("number_row_source")' in source
