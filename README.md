# مشروع Hybrid Big Data Orders Pipeline

مشروع لمعالجة ملفات الطلبات الكبيرة والملوثة باستخدام:

- Python Batch
- Apache PySpark
- MongoDB
- Data Quality
- Quarantine
- Idempotency
- Upsert
- Metrics

المشروع يعمل بطريقة **Raw-first**، أي أن كل سجل يتم تخزينه أولًا في Raw قبل إجراء التنظيف أو التصحيح عليه.

---


## متطلبات التشغيل والإعداد

### المتطلبات

- Python 3.11
- Java 17
- MongoDB Server
- Apache PySpark 4.2.0
- MongoDB Spark Connector 11.1.0
- PowerShell أو Terminal

### تثبيت مكتبات Python

من داخل مجلد المشروع:

```powershell
pip install -r requirements.txt
```

يحتوي `requirements.txt` على المكتبات الأساسية المطلوبة للمشروع.

### تشغيل MongoDB

يجب أن يكون MongoDB Server يعمل قبل تشغيل الـPipeline.

الاتصال الافتراضي:

```text
mongodb://127.0.0.1:27017
```

يمكن تغيير إعدادات MongoDB من إعدادات المشروع أو متغيرات البيئة.

### إعدادات البيئة

يوجد ملف:

```text
.env.example
```

وهو يوضح الإعدادات القابلة للتغيير، ومنها:

- `SMALL_FILE_THRESHOLD_MB`
- `BATCH_SIZE`
- `SAMPLE_ROWS`
- `MONGO_URI`
- `MONGO_DATABASE`
- `SPARK_MASTER`
- `SPARK_ELT_MASTER`
- `SPARK_ELT_SHUFFLE_PARTITIONS`
- `SPARK_DRIVER_MEMORY`
- `SPARK_DRIVER_HOST`
- `SPARK_DRIVER_BIND_ADDRESS`
- `SPARK_CACHE_BATCH_SIZE`

يمكن تغيير هذه الإعدادات حسب الجهاز وبيئة التشغيل دون الحاجة إلى تعديل منطق المشروع.

---


## 1. التشغيل الرئيسي

الأمر الطبيعي للمشروع:

```powershell
python -m src.main --input "D:\path\orders.csv" --database "shehab_doctor_demo"
```

لا يتم اختيار المحرك يدويًا.

المشروع يختار المحرك تلقائيًا حسب حجم الملف:

```text
<= 200 MB  -> Python Batch
>  200 MB  -> PySpark
```

---

## 2. مسار البيانات

```text
CSV
↓
Schema Validation
↓
Automatic Router
↓
Python Batch / PySpark
↓
orders_raw
↓
Data Quality + ELT
↓
Valid / Corrected / Quarantine
↓
orders_validated / quarantine_orders
↓
Metrics
```

---

## 3. Schema المطلوبة

الملف يجب أن يحتوي على 17 عمودًا:

```text
order_id
order_date
status
customer_id
customer_name
customer_phone
customer_email
city
district
delivery_type
delivery_cost
payment_method
payment_status
payment_amount
currency
total_amount
items_json
```

يتم فحص الـSchema قبل بدء الكتابة إلى MongoDB.

---

## 4. Automatic Router

الحد الافتراضي:

```text
200 MB
```

قاعدة الاختيار:

```text
<= 200 MB  -> python_batch
>  200 MB  -> pyspark
```

ويطبع الـRouter:

- حجم الملف
- Threshold
- المحرك المختار
- سبب الاختيار

---

## 5. Python Batch

يستخدم تلقائيًا للملفات الصغيرة.

أهم خصائصه:

- Streaming CSV
- Bounded Batches
- `insert_many`
- لا يتم تحميل الملف كاملًا في RAM
- عرض تقدم كل Batch
- حساب Throughput
- Error Handling

---

## 6. PySpark

يستخدم تلقائيًا للملفات الأكبر من 200 MB.

أهم خصائصه:

- SparkSession
- DataFrame API
- Explicit StructType
- Raw fields as String
- MongoDB Spark Connector
- Partitions
- Throughput Metrics
- Spark UI
- بدون Pandas

إعدادات Spark الحالية:

```text
Raw Master         = local[*]
ELT Master         = local[2]
Driver Memory      = 4g
Driver Host        = 127.0.0.1
Bind Address       = 127.0.0.1
Shuffle Partitions = 64
Cache Batch Size   = 256
```

هذه الإعدادات موجودة داخل المشروع نفسه، ولا تحتاج أوامر PowerShell خاصة أثناء المناقشة.

---

## 7. Raw-first

كل سجل يصل أولًا إلى:

```text
orders_raw
```

ويحتوي Raw على معلومات مثل:

```text
id_run
file_source
number_row_source
at_ingested
engine_used
record_raw
```

بعد ذلك يبدأ Data Quality.

---

## 8. Data Quality

المشروع يحتوي على قواعد تصحيح Deterministic، منها:

1. إزالة المسافات الزائدة
2. تحويل الأرقام العربية والفارسية
3. Date Normalization
4. تنظيف القيم الرقمية
5. إزالة Thousands Separators
6. تحويل بعض الكلمات الرقمية
7. Currency Normalization
8. Phone Normalization
9. Email Correction
10. Payment Status Normalization
11. Total Recalculation عندما يكون ذلك آمنًا

كل تصحيح يحتفظ بـ Audit Trail:

```json
{
  "field": "...",
  "original_value": "...",
  "corrected_value": "...",
  "rule_code": "..."
}
```

---

## 9. Quarantine

السجلات التي لا يمكن تصحيحها بأمان لا يتم حذفها.

يتم نقلها إلى:

```text
quarantine_orders
```

مع سبب الخطأ والـRaw Record الأصلي.

أمثلة Error Codes:

```text
ID_ORDER_MISSING
ID_CUSTOMER_MISSING
DATE_IMPOSSIBLE_INVALID
JSON_ITEMS_CORRUPTED
ITEMS_EMPTY
PRICE_UNKNOWN
VALUE_NEGATIVE_AMBIGUOUS
ID_ORDER_DUPLICATE
ERRORS_CONFLICTING_MULTIPLE
```

---

## 10. MongoDB

Collections الرئيسية:

```text
orders_raw
orders_validated
quarantine_orders
```

`orders_validated` يحتوي على:

- JSON Schema Validation
- Unique Index على `id_order`
- Upsert

قاعدة الاتساق:

```text
run_raw_count =
run_valid_count +
run_corrected_count +
run_quarantine_count
```

---

## 11. Idempotency

تم اختبار نفس ملف الـ5 سجلات مرتين:

```text
Raw Run 1 = 5
Raw Run 2 = 5
Raw Total = 10
```

لكن النتيجة النهائية:

```text
Validated Documents     = 2
Unique id_order         = 2
Duplicate Validated IDs = 0
```

أي أن إعادة نفس البيانات لا تنشئ نسخًا تجارية مكررة.

---

## 12. Upsert Update

تم اختبار نفس `id_order` بقيمتين مختلفتين:

```text
النسخة الأولى:
طلب-D1 -> محمد

النسخة الثانية:
طلب-D1 -> محمد UPDATE TEST
```

والنتيجة النهائية:

```text
طلب-D1 -> محمد UPDATE TEST
Document Count = 1
```

أي تم تحديث السجل بدل إنشاء Duplicate.

---

## 13. اختبار 30 مليون سجل

تم تنفيذ المشروع على ملف حقيقي كبير:

```text
File     = orders_huge_mixed_quality.csv
Size     = 12650.32 MB
Rows     = 30,000,000
Engine   = PySpark
Database = shehab_30m_final
```

Run ID:

```text
spark-20260822T044326Z-8a7259af
```

نتيجة Raw:

```text
loaded_raw       = 30,000,000
input_partitions = 99
seconds_elapsed  = 226.399
throughput       = 132509.38 rows/s
SPARK RAW LOAD   = PASS
```

نتيجة Data Quality:

```text
Valid       = 20,785,463
Corrected   = 5,421,631
Quarantine  = 3,792,906
Total       = 30,000,000
```

MongoDB النهائي:

```text
orders_raw        = 30,000,000
orders_validated  = 26,207,094
quarantine_orders = 3,792,906
```

النتيجة النهائية:

```text
Consistency Check = PASS
SPARK ELT         = PASS
PIPELINE          = PASS
```

---

## 14. Metrics

نتيجة التشغيل النهائية محفوظة في:

```text
reports/results.json
```

وتحتوي على:

- File Size
- Engine
- Run ID
- Raw Count
- Valid Count
- Corrected Count
- Quarantine Count
- Throughput
- Elapsed Time
- Partitions
- Error Counts
- Inserted / Updated / Unchanged
- Consistency Status

---

## 15. Tests

أمر الاختبارات:

```powershell
python -m pytest -q -p no:cacheprovider
```

آخر نتيجة مؤكدة:

```text
48 passed
```

---

## 16. ملفات Demo

```text
demo/doctor_test_sample.csv
demo/doctor_stress_test.csv
demo/doctor_update_test.csv
```

تستخدم لإثبات:

- Valid
- Corrected
- Quarantine
- Idempotency
- Upsert

---

## 17. Notebook المناقشة

النسخة النهائية:

```text
Shehab_BigData_Final_Viva_V5.ipynb
```

الـNotebook عبارة عن Control Panel يستدعي ملفات المشروع الحقيقية الموجودة داخل `src/`، وليس تطبيقًا ثانيًا منفصلًا.

مع ملف جديد من الدكتور يتم تغيير مسار الملف وقاعدة الـDemo فقط، ويظل اختيار المحرك تلقائيًا.

---


## بنية المشروع

```text
Shehab_BigData_Pipeline/
│
├── config/
│   └── settings.py
│
├── data/
│   └── .gitkeep
│
├── demo/
│   ├── doctor_test_sample.csv
│   ├── doctor_stress_test.csv
│   └── doctor_update_test.csv
│
├── src/
│   ├── main.py
│   ├── file_router.py
│   ├── create_small_sample.py
│   ├── batch_loader.py
│   ├── spark_loader.py
│   ├── elt_pipeline.py
│   ├── spark_elt_pipeline.py
│   ├── quality_rules.py
│   ├── spark_quality_rules.py
│   ├── mongo_setup.py
│   ├── metrics.py
│   ├── schema.py
│   └── hashing.py
│
├── tests/
│   └── اختبارات قواعد الجودة والتصنيف والـRouter وSpark
│
├── reports/
│   ├── results.json
│   ├── results.md
│   └── screenshots/
│
├── docs/
│   ├── architecture.md
│   ├── doctor_requirements_matrix.md
│   └── advanced_path_b.md
│
├── Shehab_BigData_Final_Viva_V5.ipynb
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md


## 18. Evidence

### MongoDB Compass

```text
reports/screenshots/01_mongodb_compass/
```

يحتوي على:

```text
01_database_collections.png
02_raw_record.png
03_valid_record.png
04_corrected_audit_trail.png
05_quarantine_record.png
06_validated_unique_index.png
07_validated_schema_validation.png
```

### Spark UI

```text
reports/screenshots/02_spark_ui/
```

يحتوي على إثباتات:

- Jobs
- Active Job
- Stages
- Tasks
- Partitions
- Shuffle Read / Write

### Idempotency + Upsert

```text
reports/screenshots/03_idempotency_upsert/
```

يحتوي على:

```text
08_idempotency_proof.png
09_upsert_update_proof.png
```

---

## 19. ملفات التوثيق

```text
docs/architecture.md
docs/doctor_requirements_matrix.md
docs/advanced_path_b.md
reports/results.md
reports/results.json
```

---

## 20. Advanced Path B

موجود بشكل منفصل في:

```text
src/incremental_loader.py
docs/advanced_path_b.md
```

وهو اختياري للمشروع الفردي.

---

## 21. قاعدة المناقشة

مع أي ملف Orders CSV متوافق يعطيه الدكتور:

```powershell
python -m src.main --input "D:\path\doctor_orders.csv" --database "shehab_doctor_demo"
```

ثم المشروع ينفذ تلقائيًا:

```text
Schema Validation
↓
Automatic Router
↓
Python Batch أو PySpark
↓
Raw-first
↓
Data Quality
↓
Valid / Corrected / Quarantine
↓
Upsert
↓
Metrics
↓
Consistency Check
```

---

## 22. Phase 2 - Final Project Additions

الـ Final Project هو امتداد لنفس مشروع النصفي وفي نفس الـ Repository، ولم يتم إنشاء Pipeline جديد. تم إضافة متطلبات Phase 2 داخل `src/phase2/` مع إعادة استخدام مكونات Phase 1 الحالية.

### Queries

تم تنفيذ خمس Queries عملية داخل `src/phase2/queries.py`:

- `orders_by_city`
- `orders_by_status`
- `customer_orders`
- `orders_by_date_range`
- `high_value_orders`

### Indexes and Explain

تم إنشاء ثلاثة Compound Indexes داخل `src/phase2/indexes.py`:

- `idx_p2_city_total`
- `idx_p2_status_date`
- `idx_p2_customer_date`

تم استخدام `explain("executionStats")` قبل وبعد إنشاء الـ Indexes لقياس تأثيرها على الأداء.

نتائج المقارنة محفوظة في:

`reports/phase2_explain.json`

### Aggregations

تم تنفيذ خمس Aggregation Reports داخل `src/phase2/aggregations.py`:

- `sales_by_city`
- `orders_by_status`
- `payment_method_summary`
- `delivery_type_summary`
- `monthly_sales_summary`

تم اختبار الـ Aggregations فعليًا، والنتائج التجريبية محفوظة في:

`reports/phase2_aggregations_sample_test.json`

### Materialized Views

تم تنفيذ اثنين من الـ Materialized Views:

- `daily_sales_summary`
- `top_products_summary`

يتم إنشاء أو إعادة بناء الـ Materialized Views باستخدام:

`rebuild_materialized_views()`

كما يتم تحديثها بشكل Incremental باستخدام:

`apply_order_change()`

وتم ربط التحديث الـ Incremental مع:

`src/incremental_loader.py`

تم اختبار حالات Insert و Update و Replay / Idempotency بنجاح.

تقارير الاختبار محفوظة في:

- `reports/phase2_materialized_views_test.json`
- `reports/phase2_incremental_mv_integration_test.json`

### Scheduled Jobs

تم تنفيذ وظيفتين مجدولتين داخل `src/phase2/jobs.py`:

- `daily_sales_report` عند الساعة 01:00 UTC
- `top_products_report` عند الساعة 01:05 UTC

يتم تسجيل معلومات تشغيل الـ Jobs، ومنها:

- Start Time
- End Time
- Status
- Row Count
- Error

وذلك داخل Collection باسم:

`phase2_job_runs`

لتشغيل الـ Scheduler:

`python -m src.phase2.scheduler`

### FastAPI

تم إنشاء الـ API داخل:

`src/phase2/api.py`

لتشغيله:

`python -m uvicorn src.phase2.api:app --host 127.0.0.1 --port 8000`

واجهة Swagger متاحة على:

`http://127.0.0.1:8000/docs`

### Required API Endpoints

تم تنفيذ جميع الـ API Endpoints المطلوبة:

- `GET /health`
- `POST /ingest`
- `POST /indexes`
- `GET /queries`
- `GET /queries/{name}`
- `GET /aggregations`
- `GET /aggregations/{name}`
- `POST /refresh-mv`
- `GET /jobs`
- `POST /jobs/{name}/run`

تم التحقق من وجود جميع المسارات العشرة داخل FastAPI و Swagger.

### POST /ingest

المسار `POST /ingest` لا ينشئ Pipeline جديدًا، بل يعيد استخدام الـ Pipeline الموجود في Phase 1 من خلال:

`src.main.run_pipeline()`

تم اختبار هذا المسار فعليًا عبر HTTP وكانت النتيجة:

- API Status = success
- Engine = python_batch
- Raw Rows = 2
- Validated Rows = 2
- Quarantine Rows = 0
- Consistency Check = PASS

### Final Architecture

البنية النهائية للمشروع:

`Phase 1 Hybrid Pipeline → MongoDB Validated Data → Queries + Indexes → Aggregations → Materialized Views → Scheduled Jobs → FastAPI / Swagger`

Phase 2 لم يستبدل مشروع النصفي، بل أضاف متطلبات الـ Final Project إلى نفس النظام ونفس الـ Repository.