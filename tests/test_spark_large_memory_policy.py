from pathlib import Path


def test_large_elt_avoids_raw_and_changes_dataframe_cache():
    source = (Path(__file__).resolve().parents[1] / "src" / "spark_elt_pipeline.py").read_text(encoding="utf-8")
    assert "raw_df = read_raw_run(" in source
    assert "raw_df = read_raw_run(\n            spark,\n            database,\n            run_id,\n        ).persist" not in source
    assert "spark.sql.inMemoryColumnarStorage.batchSize" in source
    assert "SPARK_CACHE_BATCH_SIZE" in source


def test_default_cache_batch_size_is_bounded():
    source = (Path(__file__).resolve().parents[1] / "config" / "settings.py").read_text(encoding="utf-8")
    assert 'os.getenv("SPARK_CACHE_BATCH_SIZE", "256")' in source


def test_default_large_elt_limits_local_parallelism():
    source = (Path(__file__).resolve().parents[1] / "config" / "settings.py").read_text(encoding="utf-8")
    assert 'SPARK_ELT_MASTER = os.getenv("SPARK_ELT_MASTER", "local[2]")' in source


def test_default_driver_heap_is_configured_for_large_spark_path():
    source = (Path(__file__).resolve().parents[1] / "config" / "settings.py").read_text(encoding="utf-8")
    assert 'SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "4g")' in source
    assert 'PYSPARK_SUBMIT_ARGS' in source
    assert '--driver-memory' in source
