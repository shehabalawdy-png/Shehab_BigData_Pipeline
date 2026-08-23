# Doctor Demo — Quick Commands

## 1) Tests
```powershell
python -m pytest .\tests -q
```

## 2) Small input -> automatic Python Batch
```powershell
python -m src.main --input ".\demo\doctor_test_sample.csv" --database "shehab_doctor_demo"
```

## 3) Raw-first proof
```powershell
python -m src.main --input ".\demo\doctor_test_sample.csv" --database "shehab_raw_demo" --raw-only
```
Then show `orders_raw` in Compass or the notebook before running ELT.

## 4) Large file Router proof — no writes
```powershell
python -m src.main --input "D:\Projects\big_data\orders_huge_mixed_quality.csv" --route-only
```
Expected engine: `pyspark`.

## 5) PySpark live proof — automatic Router only
Use any compatible CSV whose size is above the configured threshold (default 200 MB):
```powershell
python -m src.main --input "D:\path\large_orders.csv" --database "shehab_spark_demo"
```
The Router must choose `pyspark` automatically. Open `http://localhost:4040` while Spark is active and show Jobs / Stages / Tasks / Partitions.

## 6) Idempotency
Run the same small-file command twice in the same demo database. The second run must not increase business records in `orders_validated`.

## 7) Update without Duplicate
```powershell
python -m src.main --input ".\demo\doctor_update_test.csv" --database "shehab_doctor_demo"
```
For `طلب-D1`, expect `updated=1` and one final document only.

## 8) Notebook
Use `Shehab_BigData_Final_Viva.ipynb` for step-by-step execution of these same real modules.
