import argparse
import csv
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from config.settings import (
    BATCH_SIZE,
    MONGO_CONNECT_TIMEOUT_MS,
    MONGO_DATABASE,
    MONGO_URI,
    RAW_COLLECTION,
    SMALL_SAMPLE_FILE,
)
from src.schema import row_to_raw_record, validate_csv_header


def generate_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"raw-{timestamp}-{uuid.uuid4().hex[:8]}"


def build_raw_document(
    raw_row: dict,
    run_id: str,
    source_file: Path,
    source_row_number: int,
    row_shape_issue: str | None = None,
) -> dict:
    document = {
        "id_run": run_id,
        "file_source": str(source_file),
        "number_row_source": source_row_number,
        "at_ingested": datetime.now(timezone.utc),
        "engine_used": "python_batch",
        "record_raw": raw_row,
    }
    if row_shape_issue:
        document["row_shape_issue"] = row_shape_issue
    return document


def _flush_batch(collection, batch: list[dict], batch_number: int) -> int:
    if not batch:
        return 0
    started = time.perf_counter()
    try:
        result = collection.insert_many(batch, ordered=False)
    except PyMongoError as exc:
        print(f"Batch {batch_number:03} FAILED: {type(exc).__name__}: {exc}")
        raise

    elapsed = time.perf_counter() - started
    inserted = len(result.inserted_ids)
    rate = inserted / elapsed if elapsed else 0.0
    print(
        f"Batch {batch_number:03} | rows={inserted} | "
        f"time={elapsed:.3f}s | rate={rate:.2f} rows/s"
    )
    return inserted


def load_raw_with_python_batch(
    input_file,
    batch_size: int = BATCH_SIZE,
    database: str = MONGO_DATABASE,
    collection: str = RAW_COLLECTION,
) -> dict:
    if batch_size <= 0:
        raise ValueError("Batch size must be greater than zero.")

    header_check = validate_csv_header(input_file)
    source = header_check.path
    header = header_check.columns
    run_id = generate_run_id()
    file_size_mb = source.stat().st_size / (1024 * 1024)

    read_rows = loaded_raw = batch_number = malformed_rows = 0
    started_at = time.perf_counter()
    client = None

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_CONNECT_TIMEOUT_MS)
        client.admin.command("ping")
        raw_collection = client[database][collection]

        print("Python Batch Raw Loader")
        print("-" * 60)
        print(f"id_run       : {run_id}")
        print(f"File         : {source}")
        print(f"Size         : {file_size_mb:.2f} MB")
        print(f"Database     : {database}")
        print(f"Collection   : {collection}")
        print(f"Batch size   : {batch_size}")
        print()

        batch: list[dict] = []
        with source.open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.reader(csv_file)
            next(reader, None)  # header already validated

            for source_row_number, row in enumerate(reader, start=2):
                read_rows += 1
                record, row_issue = row_to_raw_record(header, row)
                malformed_rows += int(row_issue is not None)
                batch.append(
                    build_raw_document(
                        record,
                        run_id,
                        source,
                        source_row_number,
                        row_issue,
                    )
                )
                if len(batch) >= batch_size:
                    batch_number += 1
                    loaded_raw += _flush_batch(raw_collection, batch, batch_number)
                    batch.clear()

            if batch:
                batch_number += 1
                loaded_raw += _flush_batch(raw_collection, batch, batch_number)

        elapsed = time.perf_counter() - started_at
        throughput = loaded_raw / elapsed if elapsed else 0.0
        mongo_count = raw_collection.count_documents({"id_run": run_id})
        if mongo_count != loaded_raw or loaded_raw != read_rows:
            raise RuntimeError(
                "Raw consistency check failed: "
                f"read_rows={read_rows}, loaded_raw={loaded_raw}, mongo_count={mongo_count}"
            )

        print("\nRaw Load Summary")
        print("-" * 60)
        print(f"id_run          : {run_id}")
        print(f"read_rows       : {read_rows}")
        print(f"loaded_raw      : {loaded_raw}")
        print(f"MongoDB count   : {mongo_count}")
        print(f"malformed_rows  : {malformed_rows}")
        print(f"batches         : {batch_number}")
        print(f"seconds_elapsed : {elapsed:.3f}")
        print(f"throughput      : {throughput:.2f} rows/s")

        return {
            "id_run": run_id,
            "file_name": source.name,
            "file_size_mb": round(file_size_mb, 2),
            "engine_used": "python_batch",
            "database": database,
            "collection": collection,
            "read_rows": read_rows,
            "loaded_raw": loaded_raw,
            "malformed_rows": malformed_rows,
            "batch_size": batch_size,
            "batch_count": batch_number,
            "seconds_elapsed": round(elapsed, 3),
            "throughput": round(throughput, 2),
        }
    finally:
        if client is not None:
            client.close()


def parse_args():
    parser = argparse.ArgumentParser(description="Stream a small CSV into MongoDB Raw.")
    parser.add_argument("--input", default=str(SMALL_SAMPLE_FILE))
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--database", default=MONGO_DATABASE)
    parser.add_argument("--collection", default=RAW_COLLECTION)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    load_raw_with_python_batch(args.input, args.batch_size, args.database, args.collection)
