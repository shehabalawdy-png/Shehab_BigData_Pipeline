# Architecture

```text
Provided Dirty CSV
        |
        v
Schema Preflight (header only, no data load)
        |
        v
File Router: size <= 200 MB ?
        |                         |
        v                         v
Python Batch                  PySpark
Streaming csv.reader          SparkSession + DataFrame API
insert_many batches           explicit StringType schema
        |                         |
        +-----------+-------------+
                    v
               orders_raw
                    |
                    v
          Quality + Transform
          /        |         \
       Valid    Corrected   Quarantined
          \        /           |
           v      v            v
      Idempotent Upsert   quarantine_orders
              |
              v
       orders_validated
              |
              v
       reports/results.json
```

## Separation of responsibilities

- `src/main.py`: one entry point and orchestration only.
- `src/schema.py`: input/header validation and safe CSV row mapping.
- `src/file_router.py`: engine decision only.
- `src/batch_loader.py`: small-file Raw loading only.
- `src/spark_loader.py`: large-file Raw loading only.
- `src/quality_rules.py`: deterministic cleaning/classification only.
- `src/elt_pipeline.py`: Python ELT/final writes.
- `src/spark_elt_pipeline.py`: distributed Spark ELT/final writes.
- `src/mongo_setup.py`: collections, validator, indexes.
- `src/metrics.py`: durable `reports/results.json` writes.
- `src/incremental_loader.py`: optional advanced Path B only.

## Raw-first guarantee

No quality rule runs before the source row has been stored in `orders_raw`.
A malformed row shape is preserved in Raw and then quarantined rather than
silently dropped.
