# Results Summary

## Final Verified Results

### Python Batch - Small File Proof

The small sample was generated reproducibly from the original large CSV using `src/create_small_sample.py`, without manually editing the dataset.

- File: `doctor_generated_sample.csv`
- File size: 41.77 MB
- Rows: 100,000
- Engine: Python Batch
- Read rows: 100,000
- Raw loaded: 100,000
- Batch size: 5,000
- Batches: 20
- Valid: 69,310
- Corrected: 18,122
- Quarantine: 12,568
- Classified total: 100,000
- Raw load time: 3.286 seconds
- Raw throughput: 30,430.46 rows/s
- ELT time: 33.325 seconds
- ELT throughput: 3,000.72 rows/s
- Consistency: PASS
- Pipeline: PASS

The Router selected Python Batch automatically because the 41.77 MB file size was less than or equal to the configurable 200 MB threshold.


---

### PySpark - Representative Large File Proof

The automatic Router was tested with a representative large file.

- File: `orders_large_representative.csv`
- File size: 230.12 MB
- Rows: 550,000
- Engine: PySpark
- Raw: 550,000
- Valid: 381,199
- Corrected: 99,430
- Quarantine: 69,371
- Classified total: 550,000
- Input partitions: 16
- Consistency: PASS
- Spark ELT: PASS
- Pipeline: PASS

The Router selected PySpark automatically because the file size exceeded the 200 MB threshold.

---

## Final 30-Million-Row Run

Final verified database:

```text
shehab_30m_final
```

Run ID:

```text
spark-20260822T044326Z-8a7259af
```

Input:

- File: `orders_huge_mixed_quality.csv`
- File size: 12650.32 MB
- Rows: 30,000,000
- Engine: PySpark
- Input partitions: 99

### Raw Load

- Loaded Raw: 30,000,000
- Time: 226.399 seconds
- Throughput: 132,509.38 rows/s
- Result: PASS

### Data Quality + ELT

- Raw: 30,000,000
- Valid: 20,785,463
- Corrected: 5,421,631
- Quarantine: 3,792,906
- Classified total: 30,000,000
- Validated collection: 26,207,094
- Mongo partitions: 459
- Shuffle partitions: 64
- Duplicate order IDs detected: 207,689
- ELT time: 7,656.417 seconds
- ELT throughput: 3,918.28 rows/s
- Consistency: PASS
- Spark ELT: PASS
- Pipeline: PASS

The final consistency equation is:

```text
30,000,000 =
20,785,463 Valid
+ 5,421,631 Corrected
+ 3,792,906 Quarantine
```

No source records were silently dropped.

---

## Idempotency Proof

The same 5-row input was processed twice.

- Raw Run 1: 5
- Raw Run 2: 5
- Raw history total: 10
- Validated documents: 2
- Unique `id_order`: 2
- Duplicate Validated IDs: 0

Result: PASS

Raw keeps historical runs, while final business records are not duplicated.

---

## Upsert Update Proof

The same business key was processed with an updated value:

```text
Original:
طلب-D1 -> محمد

Updated:
طلب-D1 -> محمد UPDATE TEST
```

Final result:

```text
طلب-D1 -> محمد UPDATE TEST
Document Count = 1
```

Result: PASS

The existing business record was updated instead of inserting a duplicate.

---

## Interpretation

Python Batch is suitable for smaller files because it has low startup overhead and processes the CSV using bounded streaming batches.

PySpark is used for larger files because it provides partitioned processing and Spark execution monitoring.

The project uses an automatic configurable Router with a default threshold of 200 MB:

```text
<= 200 MB -> Python Batch
>  200 MB -> PySpark
```
The default 200 MB threshold was selected as a practical balance between the two engines. For smaller files, Python Batch can stream records in bounded batches without loading the whole file into memory, while avoiding the startup and coordination overhead of Spark. For larger files, PySpark becomes more appropriate because it can process the input in parallel partitions and provides better scalability and execution monitoring. The threshold is configurable and can be changed according to the available hardware and workload.


The machine-readable final evidence is stored in:

```text
reports/results.json
```

Screenshot evidence is stored in:

```text
reports/screenshots/
```