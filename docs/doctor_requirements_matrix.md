# Doctor Requirements Matrix

| Requirement | Implementation | Viva proof |
|---|---|---|
| Reproducible small sample | `src/create_small_sample.py` | `python -m src.create_small_sample --input ... --rows 100000` |
| One automatic Router | `src/main.py` + `src/file_router.py` | small file -> Python Batch; large -> PySpark |
| Configurable threshold | `config/settings.py` (`200 MB`) + `--threshold-mb` | Router prints size, threshold, engine, reason |
| Python streaming Batch | `src/batch_loader.py` | `csv.reader`, bounded list, `insert_many`, batch metrics |
| PySpark large loader | `src/spark_loader.py` | SparkSession, DataFrame API, explicit all-String schema, Mongo connector |
| No unjustified repartition | `src/spark_loader.py` | natural input partitions are printed |
| Raw-first ELT | Raw loaders + ELT modules | run `--raw-only`, show `orders_raw`, then run quality |
| Raw metadata | `id_run`, source, row/source id, timestamp, engine, `record_raw` | MongoDB proof cell in notebook |
| >= 8 correction rules | `src/quality_rules.py` | 11+ deterministic rules + tests |
| Audit Trail | `corrections[]` with field/original/corrected/rule code | show corrected record |
| Quarantine with reasons | `error_codes`, `error_details`, raw record + assignment aliases | show quarantined record |
| Stable business key | `id_order` | Mongo unique index |
| Idempotent Upsert | Python `ReplaceOne(..., upsert=True)` and Spark Mongo upsert | rerun same input -> no growth |
| Update without Duplicate | Upsert + record hash | `demo/doctor_update_test.csv` |
| Consistency equation | both ELT modules | Raw = Valid + Corrected + Quarantine |
| Required metrics | loaders/ELT + `reports/results.json` | notebook metrics cells |
| Tests | `tests/` | `python -m pytest tests -q` |
| Try/finally cleanup | Mongo/Spark modules | connections/contexts closed |
| Progress messages | Batch/Spark/ELT console output | live run |
| README/config/docs | root + `docs/` | deliver with repository |
| Spark UI | live Spark run | Jobs / Stages / Tasks / Partitions |
| Screenshots | `reports/screenshots/` checklist | capture on the viva machine |
| Advanced Path B (optional individual / mandatory 2-person group) | `src/incremental_loader.py` | Initial + Delta update/insert + same Delta rerun |
