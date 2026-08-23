import argparse
import json
import os
import sys
import time
from pathlib import Path

from pymongo import MongoClient

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config.settings import (
    MONGO_DATABASE,
    MONGO_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    SPARK_APP_NAME,
    SPARK_MASTER,
    SPARK_LOCAL_DIR,
    SPARK_LOG_LEVEL,
    SPARK_DRIVER_MEMORY,
    SPARK_PYTHON_TEMP_DIR,
    SPARK_WAREHOUSE_DIR,
    SPARK_ELT_MASTER,
    SPARK_ELT_SHUFFLE_PARTITIONS,
    SPARK_CACHE_BATCH_SIZE,
    VALIDATED_COLLECTION,
    SPARK_DRIVER_HOST,
    SPARK_DRIVER_BIND_ADDRESS,
)

from src.schema import RAW_COLUMNS
from src.spark_quality_rules import apply_native_quality


SPARK_LOCAL = SPARK_LOCAL_DIR
SPARK_PYTHON_TEMP = SPARK_PYTHON_TEMP_DIR
SPARK_WAREHOUSE = SPARK_WAREHOUSE_DIR


RAW_RECORD_SCHEMA = StructType(
    [
        StructField(
            column,
            StringType(),
            True,
        )
        for column in RAW_COLUMNS
    ]
)


RAW_DOCUMENT_SCHEMA = StructType(
    [
        StructField(
            "id_run",
            StringType(),
            True,
        ),
        StructField(
            "file_source",
            StringType(),
            True,
        ),
        StructField(
            "number_row_source",
            LongType(),
            True,
        ),
        StructField(
            "source_partition_id",
            LongType(),
            True,
        ),
        StructField(
            "source_record_id",
            LongType(),
            True,
        ),
        StructField(
            "at_ingested",
            TimestampType(),
            True,
        ),
        StructField(
            "engine_used",
            StringType(),
            True,
        ),
        StructField(
            "record_raw",
            RAW_RECORD_SCHEMA,
            True,
        ),
    ]
)


EXISTING_VALIDATED_SCHEMA = StructType(
    [
        StructField(
            "id_order",
            StringType(),
            True,
        ),
        StructField(
            "record_hash",
            StringType(),
            True,
        ),
    ]
)


def prepare_paths():
    SPARK_LOCAL.mkdir(
        parents=True,
        exist_ok=True,
    )

    SPARK_PYTHON_TEMP.mkdir(
        parents=True,
        exist_ok=True,
    )

    SPARK_WAREHOUSE.mkdir(
        parents=True,
        exist_ok=True,
    )

    os.environ["TEMP"] = str(
        SPARK_PYTHON_TEMP
    )

    os.environ["TMP"] = str(
        SPARK_PYTHON_TEMP
    )

    os.environ[
        "PYSPARK_PYTHON"
    ] = sys.executable


def create_spark(
    shuffle_partitions=SPARK_ELT_SHUFFLE_PARTITIONS,
):
    prepare_paths()

    spark = (
        SparkSession.builder
        .master(
            SPARK_ELT_MASTER
        )
        .appName(
            f"{SPARK_APP_NAME}-ELT"
        )
        
        .config(
            "spark.driver.host",
            SPARK_DRIVER_HOST,
        )
        .config(
            "spark.driver.bindAddress",
            SPARK_DRIVER_BIND_ADDRESS,
        )
        
        .config(
            "spark.driver.memory",
            SPARK_DRIVER_MEMORY,
        )
        .config(
            "spark.local.dir",
            str(SPARK_LOCAL),
        )
        .config(
            "spark.sql.warehouse.dir",
            SPARK_WAREHOUSE.resolve().as_uri(),
        )
        .config(
            "spark.sql.shuffle.partitions",
            str(
                shuffle_partitions
            ),
        )
        .config(
            "spark.sql.inMemoryColumnarStorage.batchSize",
            str(SPARK_CACHE_BATCH_SIZE),
        )
        .config(
            "spark.python.worker.reuse",
            "true",
        )
        .config(
            "spark.pyspark.python",
            sys.executable,
        )
        .config(
            "spark.mongodb.read.connection.uri",
            MONGO_URI,
        )
        .config(
            "spark.mongodb.write.connection.uri",
            MONGO_URI,
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        SPARK_LOG_LEVEL
    )

    return spark


def read_raw_run(
    spark,
    database,
    run_id,
):
    pipeline = json.dumps(
        [
            {
                "$match": {
                    "id_run":
                        run_id
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "id_run": 1,
                    "file_source": 1,
                    "number_row_source": 1,
                    "source_partition_id": 1,
                    "source_record_id": 1,
                    "at_ingested": 1,
                    "engine_used": 1,
                    "record_raw": 1,
                }
            },
        ],
        ensure_ascii=False,
    )

    return (
        spark.read
        .format(
            "mongodb"
        )
        .schema(
            RAW_DOCUMENT_SCHEMA
        )
        .option(
            "database",
            database,
        )
        .option(
            "collection",
            RAW_COLLECTION,
        )
        .option(
            "aggregation.pipeline",
            pipeline,
        )
        .load()
    )


def read_existing_validated(
    spark,
    database,
):
    pipeline = json.dumps(
        [
            {
                "$project": {
                    "_id": 0,
                    "id_order": 1,
                    "record_hash": 1,
                }
            }
        ]
    )

    return (
        spark.read
        .format(
            "mongodb"
        )
        .schema(
            EXISTING_VALIDATED_SCHEMA
        )
        .option(
            "database",
            database,
        )
        .option(
            "collection",
            VALIDATED_COLLECTION,
        )
        .option(
            "aggregation.pipeline",
            pipeline,
        )
        .load()
    )


def write_mongodb(
    dataframe,
    database,
    collection,
    id_fields,
    max_batch_size,
):
    (
        dataframe.write
        .format(
            "mongodb"
        )
        .mode(
            "append"
        )
        .option(
            "database",
            database,
        )
        .option(
            "collection",
            collection,
        )
        .option(
            "operationType",
            "replace",
        )
        .option(
            "idFieldList",
            id_fields,
        )
        .option(
            "upsertDocument",
            "true",
        )
        .option(
            "ordered",
            "false",
        )
        .option(
            "maxBatchSize",
            str(
                max_batch_size
            ),
        )
        .save()
    )


def build_validated(
    classified,
):
    business_columns = [
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
        "items",
    ]

    result = (
        classified
        .filter(
            F.col(
                "_final_status"
            ).isin(
                "valid",
                "corrected",
            )
        )
        .select(
            *[
                F.col(
                    f"_processed_record.{column}"
                ).alias(
                    column
                )
                for column
                in business_columns
            ],
            F.col(
                "_final_status"
            ).alias(
                "quality_status"
            ),
            F.col(
                "_corrections"
            ).alias(
                "corrections"
            ),
            F.col(
                "id_run"
            ).alias(
                "source_run_id"
            ),
            F.coalesce(
                F.col("number_row_source"),
                F.col("source_record_id"),
            ).alias(
                "source_row_number"
            ),
            F.col(
                "source_record_id"
            ),
            F.col(
                "source_partition_id"
            ),
            F.col(
                "file_source"
            ).alias(
                "source_file"
            ),
            F.col(
                "engine_used"
            ),
            F.col(
                "_record_hash"
            ).alias(
                "record_hash"
            ),
        )
        .withColumn(
            "id_order",
            F.col(
                "order_id"
            ),
        )
    )

    result = result.withColumn(
        "processed_at",
        F.current_timestamp(),
    )

    return result


def build_quarantine(
    classified,
):
    return (
        classified
        .filter(
            F.col(
                "_final_status"
            )
            == "quarantined"
        )
        .select(
            F.col(
                "id_run"
            ).alias(
                "source_run_id"
            ),
            F.coalesce(
                F.col("number_row_source"),
                F.col("source_record_id"),
            ).alias(
                "source_row_number"
            ),
            F.col(
                "source_record_id"
            ),
            F.col(
                "source_partition_id"
            ),
            F.col(
                "file_source"
            ).alias(
                "source_file"
            ),
            F.col(
                "engine_used"
            ),
            F.lit(
                "quarantined"
            ).alias(
                "quality_status"
            ),
            F.col(
                "_final_error_codes"
            ).alias(
                "error_codes"
            ),
            F.col(
                "_final_error_details"
            ).alias(
                "error_details"
            ),
            F.col(
                "_final_error_codes"
            ).alias(
                "codes_error"
            ),
            F.col(
                "_final_error_details"
            ).alias(
                "details_error"
            ),
            F.col(
                "_corrections"
            ).alias(
                "corrections"
            ),
            F.col(
                "record_raw"
            ).alias(
                "raw_record"
            ),
            F.col(
                "record_raw"
            ).alias(
                "record_raw"
            ),
            F.col(
                "_processed_record"
            ).alias(
                "processed_record"
            ),
            F.current_timestamp()
            .alias(
                "processed_at"
            ),
        )
    )


def run_spark_elt(
    run_id,
    database=MONGO_DATABASE,
    write_batch_size=1000,
    shuffle_partitions=SPARK_ELT_SHUFFLE_PARTITIONS,
):
    started = time.perf_counter()

    spark = None
    raw_df = None
    duplicate_ids = None
    classified = None
    changes = None

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )

    try:
        spark = create_spark(
            shuffle_partitions=
                shuffle_partitions,
        )

        print(
            "Spark ELT Quality Pipeline"
        )
        print(
            "-" * 70
        )
        print(
            f"run_id             : "
            f"{run_id}"
        )
        print(
            f"database           : "
            f"{database}"
        )
        print(
            f"write batch size   : "
            f"{write_batch_size}"
        )
        print(
            f"shuffle partitions : "
            f"{shuffle_partitions}"
        )
        jvm_runtime = spark.sparkContext._jvm.java.lang.Runtime.getRuntime()
        print(
            "JVM max heap        : "
            f"{jvm_runtime.maxMemory() / 1024**3:.2f} GB"
        )

        # Do not cache the complete Raw collection. On very large runs the
        # SQL cache still builds columnar batches in JVM heap before writing
        # DISK_ONLY blocks, which can exhaust Java heap. Reading Raw again is
        # slower but keeps memory bounded and is safer for 30M+ rows.
        raw_df = read_raw_run(
            spark,
            database,
            run_id,
        )

        input_partitions = (
            raw_df.rdd
            .getNumPartitions()
        )

        print(
            f"Mongo partitions   : "
            f"{input_partitions}"
        )

        normalized_order_id = (
            F.trim(
                F.coalesce(
                    F.col(
                        "record_raw.order_id"
                    ),
                    F.lit(""),
                )
            )
        )

        duplicate_ids = (
            raw_df
            .select(
                normalized_order_id
                .alias(
                    "_duplicate_key"
                )
            )
            .filter(
                F.length(
                    F.col(
                        "_duplicate_key"
                    )
                ) > 0
            )
            .groupBy(
                "_duplicate_key"
            )
            .count()
            .filter(
                F.col(
                    "count"
                ) > 1
            )
            .select(
                "_duplicate_key"
            )
            .persist(
                StorageLevel.DISK_ONLY
            )
        )

        duplicate_count = (
            duplicate_ids.count()
        )

        print(
            "duplicate order IDs: "
            f"{duplicate_count}"
        )

        duplicate_markers = (
            duplicate_ids
            .withColumn(
                "_is_duplicate",
                F.lit(True),
            )
        )

        # Large-file quality rules are expressed entirely with Spark SQL /
        # DataFrame expressions.  This avoids sending tens of millions of rows
        # through a Python UDF worker, which is not stable at 30M-row scale.
        evaluated = apply_native_quality(
            raw_df.withColumn(
                "_order_key",
                normalized_order_id,
            )
        )

        evaluated = (
            evaluated
            .join(
                duplicate_markers,
                evaluated[
                    "_order_key"
                ]
                ==
                duplicate_markers[
                    "_duplicate_key"
                ],
                "left",
            )
            .drop(
                "_duplicate_key"
            )
        )

        final_codes = F.when(
            F.col(
                "_is_duplicate"
            ) == True,
            F.array_union(
                F.col(
                    "_base_error_codes"
                ),
                F.array(
                    F.lit(
                        "ID_ORDER_DUPLICATE"
                    )
                ),
            ),
        ).otherwise(
            F.col(
                "_base_error_codes"
            )
        )

        final_details = F.when(
            F.col(
                "_is_duplicate"
            ) == True,
            F.concat(
                F.col(
                    "_base_error_details"
                ),
                F.array(
                    F.lit(
                        "Duplicate business key: order_id"
                    )
                ),
            ),
        ).otherwise(
            F.col(
                "_base_error_details"
            )
        )

        final_status = F.when(
            F.col(
                "_is_duplicate"
            ) == True,
            F.lit(
                "quarantined"
            ),
        ).otherwise(
            F.col(
                "_base_status"
            )
        )

        classified = (
            evaluated
            .withColumn(
                "_final_status",
                final_status,
            )
            .withColumn(
                "_final_error_codes",
                final_codes,
            )
            .withColumn(
                "_final_error_details",
                final_details,
            )
            .persist(
                StorageLevel.DISK_ONLY
            )
        )

        status_rows = (
            classified
            .groupBy(
                "_final_status"
            )
            .count()
            .collect()
        )

        status_counts = {
            row[
                "_final_status"
            ]: row["count"]
            for row in status_rows
        }

        valid_count = (
            status_counts.get(
                "valid",
                0,
            )
        )

        corrected_count = (
            status_counts.get(
                "corrected",
                0,
            )
        )

        quarantine_count = (
            status_counts.get(
                "quarantined",
                0,
            )
        )

        raw_count = sum(
            status_counts.values()
        )

        error_rows = (
            classified
            .select(F.explode_outer("_final_error_codes").alias("error_code"))
            .filter(F.col("error_code").isNotNull())
            .groupBy("error_code")
            .count()
            .collect()
        )
        error_counts = {
            row["error_code"]: row["count"]
            for row in error_rows
        }

        classified_total = (
            valid_count
            + corrected_count
            + quarantine_count
        )

        if (
            raw_count
            != classified_total
        ):
            raise RuntimeError(
                "Consistency check failed: "
                f"raw={raw_count}, "
                f"classified="
                f"{classified_total}"
            )

        validated = build_validated(
            classified
        )

        existing = (
            read_existing_validated(
                spark,
                database,
            )
        )

        new_alias = (
            validated.alias("n")
        )

        old_alias = (
            existing.alias("e")
        )

        joined = (
            new_alias
            .join(
                old_alias,
                F.col(
                    "n.id_order"
                )
                ==
                F.col(
                    "e.id_order"
                ),
                "left",
            )
        )

        change_type = F.when(
            F.col(
                "e.id_order"
            ).isNull(),
            F.lit(
                "inserted"
            ),
        ).when(
            F.col(
                "e.record_hash"
            ).eqNullSafe(
                F.col(
                    "n.record_hash"
                )
            ),
            F.lit(
                "unchanged"
            ),
        ).otherwise(
            F.lit(
                "updated"
            )
        )

        validated_columns = (
            validated.columns
        )

        # Keep this as a normal DataFrame instead of caching another huge
        # 20M+ row intermediate. It is derived from the already disk-persisted
        # classified dataset, so recomputing the lightweight join is safer than
        # materializing a second large SQL cache.
        changes = (
            joined
            .withColumn(
                "_change_type",
                change_type,
            )
            .select(
                *[
                    F.col(
                        f"n.{column}"
                    ).alias(
                        column
                    )
                    for column
                    in validated_columns
                ],
                F.col(
                    "_change_type"
                ),
            )
        )

        change_rows = (
            changes
            .groupBy(
                "_change_type"
            )
            .count()
            .collect()
        )

        change_counts = {
            row[
                "_change_type"
            ]: row["count"]
            for row in change_rows
        }

        inserted = (
            change_counts.get(
                "inserted",
                0,
            )
        )

        updated = (
            change_counts.get(
                "updated",
                0,
            )
        )

        unchanged = (
            change_counts.get(
                "unchanged",
                0,
            )
        )

        changed_total = (
            inserted
            + updated
        )

        if changed_total > 0:
            to_write = (
                changes
                .filter(
                    F.col(
                        "_change_type"
                    )
                    != "unchanged"
                )
                .select(
                    *validated_columns
                )
            )

            write_mongodb(
                to_write,
                database,
                VALIDATED_COLLECTION,
                "id_order",
                write_batch_size,
            )

        quarantine = build_quarantine(
            classified
        )

        q_collection = (
            client[database]
            [QUARANTINE_COLLECTION]
        )

        q_before = (
            q_collection
            .count_documents(
                {
                    "source_run_id":
                        run_id
                }
            )
        )

        if quarantine_count > 0:
            write_mongodb(
                quarantine,
                database,
                QUARANTINE_COLLECTION,
                (
                    "source_run_id,"
                    "source_row_number"
                ),
                write_batch_size,
            )

        q_after = (
            q_collection
            .count_documents(
                {
                    "source_run_id":
                        run_id
                }
            )
        )

        quarantine_inserted = max(
            0,
            q_after - q_before,
        )

        quarantine_existing = max(
            0,
            quarantine_count
            - quarantine_inserted,
        )

        elapsed = (
            time.perf_counter()
            - started
        )

        throughput = (
            raw_count / elapsed
            if elapsed > 0
            else 0
        )

        print()
        print(
            "Spark ELT Summary"
        )
        print(
            "-" * 70
        )
        print(
            f"run_raw_count       : "
            f"{raw_count}"
        )
        print(
            f"run_valid_count     : "
            f"{valid_count}"
        )
        print(
            f"run_corrected_count : "
            f"{corrected_count}"
        )
        print(
            f"run_quarantine_count: "
            f"{quarantine_count}"
        )
        print(
            f"classified_total     : "
            f"{classified_total}"
        )
        print(
            f"inserted             : "
            f"{inserted}"
        )
        print(
            f"updated              : "
            f"{updated}"
        )
        print(
            f"unchanged            : "
            f"{unchanged}"
        )
        print(
            f"quarantine inserted  : "
            f"{quarantine_inserted}"
        )
        print(
            f"quarantine existing  : "
            f"{quarantine_existing}"
        )
        print(
            f"seconds_elapsed      : "
            f"{elapsed:.3f}"
        )
        print(
            f"throughput           : "
            f"{throughput:.2f} rows/s"
        )
        print(
            "consistency check    : "
            "PASS"
        )

        print()
        print("Top error codes")
        print("-" * 70)
        for code, count in sorted(
            error_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[:15]:
            print(f"{code}: {count}")

        print()
        print(
            "SPARK ELT = PASS"
        )

        return {
            "run_id": run_id,
            "database": database,
            "engine": "pyspark",
            "run_raw_count": raw_count,
            "run_valid_count": valid_count,
            "run_corrected_count": corrected_count,
            "run_quarantine_count": quarantine_count,
            "classified_total": classified_total,
            "inserted": inserted,
            "updated": updated,
            "unchanged": unchanged,
            "quarantine_inserted": quarantine_inserted,
            "quarantine_existing": quarantine_existing,
            "seconds_elapsed": round(elapsed, 3),
            "throughput": round(throughput, 2),
            "write_batch_size": write_batch_size,
            "input_partitions": input_partitions,
            "shuffle_partitions": shuffle_partitions,
            "duplicate_order_ids": duplicate_count,
            "error_counts": error_counts,
            "consistency_pass": True,
        }

    finally:
        if changes is not None:
            changes.unpersist()

        if classified is not None:
            classified.unpersist()

        if duplicate_ids is not None:
            duplicate_ids.unpersist()

        if raw_df is not None:
            raw_df.unpersist()

        if spark is not None:
            spark.stop()

        client.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Large-scale ELT quality "
            "pipeline using PySpark "
            "DataFrame API."
        )
    )

    parser.add_argument(
        "--run-id",
        required=True,
    )

    parser.add_argument(
        "--database",
        default=MONGO_DATABASE,
    )

    parser.add_argument(
        "--write-batch-size",
        type=int,
        default=1000,
    )

    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=SPARK_ELT_SHUFFLE_PARTITIONS,
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    run_spark_elt(
        run_id=args.run_id,
        database=args.database,
        write_batch_size=
            args.write_batch_size,
        shuffle_partitions=
            args.shuffle_partitions,
    )
