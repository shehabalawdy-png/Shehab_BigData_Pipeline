import argparse
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from config.settings import (
    INPUT_FILE,
    MONGO_DATABASE,
    MONGO_URI,
    RAW_COLLECTION,
    SPARK_APP_NAME,
    SPARK_MASTER,
    SPARK_LOCAL_DIR,
    SPARK_LOG_LEVEL,
    SPARK_DRIVER_MEMORY,
    SPARK_PYTHON_TEMP_DIR,
    SPARK_WAREHOUSE_DIR,
    SPARK_DRIVER_HOST,
    SPARK_DRIVER_BIND_ADDRESS,  
)


from src.schema import RAW_COLUMNS, validate_csv_header


def build_csv_schema(columns):
    """Build an explicit all-String Spark schema in the incoming header order."""
    return StructType(
        [StructField(column, StringType(), True) for column in columns]
    )


CSV_SCHEMA = build_csv_schema(RAW_COLUMNS)


SPARK_LOCAL = SPARK_LOCAL_DIR
SPARK_PYTHON_TEMP = SPARK_PYTHON_TEMP_DIR
SPARK_WAREHOUSE = SPARK_WAREHOUSE_DIR



def generate_run_id():
    timestamp = datetime.now(
        timezone.utc
    ).strftime(
        "%Y%m%dT%H%M%SZ"
    )

    short_id = uuid.uuid4().hex[:8]

    return (
        f"spark-{timestamp}-{short_id}"
    )


def prepare_local_paths():
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


def create_spark():
    prepare_local_paths()

    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(SPARK_APP_NAME)
        
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
            "spark.mongodb.write.connection.uri",
            MONGO_URI,
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        SPARK_LOG_LEVEL
    )

    return spark


def load_raw_with_pyspark(
    input_file,
    database=MONGO_DATABASE,
    collection=RAW_COLLECTION,
):
    source = Path(
        input_file
    ).resolve()

    if not source.exists():
        raise FileNotFoundError(
            f"Input file not found: {source}"
        )

    if not source.is_file():
        raise ValueError(
            f"Input path is not a file: {source}"
        )

    # Validate required columns before starting Spark or writing to MongoDB.
    # Reordered columns are safe: the explicit String schema follows the actual
    # header order, then the DataFrame is projected back to RAW_COLUMNS.
    header_check = validate_csv_header(source)
    incoming_schema = build_csv_schema(header_check.columns)

    run_id = generate_run_id()

    file_size_mb = (
        source.stat().st_size
        / (1024 * 1024)
    )

    spark = None

    started_at = time.perf_counter()

    try:
        spark = create_spark()

        print("PySpark Raw Loader")
        print("-" * 65)
        print(
            f"run_id      : {run_id}"
        )
        print(
            f"File        : {source}"
        )
        print(
            f"Size MB     : {file_size_mb:.2f}"
        )
        print(
            f"Database    : {database}"
        )
        print(
            f"Collection  : {collection}"
        )
        print(
            f"Spark       : {spark.version}"
        )

        source_df = (
            spark.read
            .option(
                "header",
                "true",
            )
            .option(
                "encoding",
                "UTF-8",
            )
            .option(
                "quote",
                '"',
            )
            .option(
                "escape",
                '"',
            )
            .option(
                "mode",
                "PERMISSIVE",
            )
            .schema(
                incoming_schema
            )
            .csv(
                str(source)
            )
            .select(*RAW_COLUMNS)
        )

        input_partitions = (
            source_df.rdd
            .getNumPartitions()
        )

        print(
            f"Input partitions: "
            f"{input_partitions}"
        )

        # No repartition() is used here.
        # Spark keeps its natural CSV input partitions.

        raw_record = F.struct(
            *[
                F.col(column).alias(
                    column
                )
                for column in RAW_COLUMNS
            ]
        )

        # Generate the distributed source record identifier once and reuse it
        # for the assignment-required number_row_source field.  We keep the
        # Spark partition/id metadata as well for distributed traceability.
        # This avoids an expensive global row_number() shuffle on large files.
        source_with_metadata = (
            source_df
            .withColumn(
                "_source_partition_id",
                F.spark_partition_id().cast("long"),
            )
            .withColumn(
                "_source_record_id",
                F.monotonically_increasing_id().cast("long"),
            )
        )

        output_df = (
            source_with_metadata
            .select(
                F.lit(
                    run_id
                ).alias(
                    "id_run"
                ),

                F.lit(
                    str(source)
                ).alias(
                    "file_source"
                ),

                F.col(
                    "_source_record_id"
                ).alias(
                    "number_row_source"
                ),

                F.col(
                    "_source_partition_id"
                ).alias(
                    "source_partition_id"
                ),

                F.col(
                    "_source_record_id"
                ).alias(
                    "source_record_id"
                ),

                F.current_timestamp()
                .alias(
                    "at_ingested"
                ),

                F.lit(
                    "pyspark"
                ).alias(
                    "engine_used"
                ),

                raw_record.alias(
                    "record_raw"
                ),
            )
        )

        (
            output_df.write
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
            .save()
        )

        client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=5000,
        )

        try:
            loaded_raw = (
                client[database][collection]
                .count_documents(
                    {
                        "id_run": run_id
                    }
                )
            )
        finally:
            client.close()

        # Raw loading does not filter or drop any CSV records.
        # Therefore every successfully loaded Raw document represents
        # one input row read from the source file.
        read_rows = loaded_raw


        elapsed = (
            time.perf_counter()
            - started_at
        )

        throughput = (
            loaded_raw / elapsed
            if elapsed > 0
            else 0
        )

        print()
        print("Spark Raw Load Summary")
        print("-" * 65)
        print(
            f"run_id          : {run_id}"
        )
        print(
            f"read_rows       : {read_rows}"
        )
        print(
            f"loaded_raw      : {loaded_raw}"
        )
        print(
            f"input_partitions: {input_partitions}"
        )
        print(
            f"seconds_elapsed : {elapsed:.3f}"
        )
        print(
            f"throughput      : "
            f"{throughput:.2f} rows/s"
        )

        if loaded_raw <= 0:
            raise RuntimeError(
                "Spark write produced zero MongoDB documents."
            )

        print(
            "SPARK RAW LOAD: PASS"
        )

        return {
            "run_id": run_id,
            "engine_used": "pyspark",
            "file_name": source.name,
            "file_size_mb": round(
                file_size_mb,
                2,
            ),
            "read_rows": read_rows,
            "loaded_raw": loaded_raw,
            "input_partitions":
                input_partitions,
            "seconds_elapsed": round(
                elapsed,
                3,
            ),
            "throughput": round(
                throughput,
                2,
            ),
        }

    finally:
        if spark is not None:
            spark.stop()


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Load CSV into MongoDB Raw "
            "using Apache Spark."
        )
    )

    parser.add_argument(
        "--input",
        default=str(INPUT_FILE),
    )

    parser.add_argument(
        "--database",
        default=MONGO_DATABASE,
    )

    parser.add_argument(
        "--collection",
        default=RAW_COLLECTION,
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    load_raw_with_pyspark(
        input_file=args.input,
        database=args.database,
        collection=args.collection,
    )
