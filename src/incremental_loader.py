"""Optional advanced Path B: incremental Delta loading with version handling.

Core pipeline behavior is unchanged. This module is a separate, documented
extension for the assignment's advanced incremental-loading path.
"""

import argparse
import csv
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from config.settings import (
    BATCH_SIZE,
    MONGO_CONNECT_TIMEOUT_MS,
    MONGO_DATABASE,
    MONGO_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
)
from src.quality_rules import evaluate_record
from src.hashing import business_record_hash
from src.schema import RAW_COLUMNS, CsvSchemaError, read_csv_header, row_to_raw_record
from src.phase2.materialized_views import apply_order_change

DELTA_COLUMNS = (*RAW_COLUMNS, "version")


def validate_delta_header(input_file):
    path, columns = read_csv_header(input_file)
    missing = [c for c in DELTA_COLUMNS if c not in columns]
    extra = [c for c in columns if c not in DELTA_COLUMNS]
    if missing or extra:
        details = []
        if missing:
            details.append("missing=" + ", ".join(missing))
        if extra:
            details.append("extra=" + ", ".join(extra))
        raise CsvSchemaError("Delta schema mismatch (" + "; ".join(details) + ").")
    return path, columns


def parse_version(value) -> int:
    try:
        version = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid version: {value!r}") from exc
    if version <= 0:
        raise ValueError("version must be a positive integer")
    return version


def decide_version_action(existing_version, existing_hash, incoming_version, incoming_hash):
    """Return insert/update/unchanged/stale/conflict without touching MongoDB."""
    if existing_version is None:
        return "inserted"
    if incoming_version < existing_version:
        return "stale"
    if incoming_version == existing_version:
        return "unchanged" if existing_hash == incoming_hash else "version_conflict"
    return "updated"


def run_incremental(input_file, database=MONGO_DATABASE, write_batch_size=BATCH_SIZE):
    # Heavy/database imports stay lazy so pure policy tests do not require a
    # running MongoDB/PyMongo environment.
    from pymongo import MongoClient
    from src.elt_pipeline import build_quarantine_document, build_validated_document
    from src.mongo_setup import configure_database

    if write_batch_size <= 0:
        raise ValueError("write_batch_size must be greater than zero")

    source, header = validate_delta_header(input_file)
    configure_database(database=database)
    run_id = f"delta-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    started = time.perf_counter()

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_CONNECT_TIMEOUT_MS)
    try:
        client.admin.command("ping")
        db = client[database]
        raw = db[RAW_COLLECTION]
        validated = db[VALIDATED_COLLECTION]
        quarantine = db[QUARANTINE_COLLECTION]

        # Phase 1: Raw-first. Every Delta row is stored before quality decisions.
        raw_docs = []
        read_rows = 0
        with source.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            next(reader, None)
            for row_number, row in enumerate(reader, start=2):
                read_rows += 1
                mapped, issue = row_to_raw_record(header, row)
                version_raw = mapped.pop("version", None)
                record_raw = {field: mapped.get(field) for field in RAW_COLUMNS}
                if mapped.get("_raw_extra_values"):
                    record_raw["_raw_extra_values"] = mapped["_raw_extra_values"]
                doc = {
                    "id_run": run_id,
                    "file_source": str(source),
                    "number_row_source": row_number,
                    "at_ingested": datetime.now(timezone.utc),
                    "engine_used": "python_incremental",
                    "delta_version_raw": version_raw,
                    "record_raw": record_raw,
                }
                if issue:
                    doc["row_shape_issue"] = issue
                raw_docs.append(doc)
                if len(raw_docs) >= write_batch_size:
                    raw.insert_many(raw_docs, ordered=False)
                    raw_docs.clear()
            if raw_docs:
                raw.insert_many(raw_docs, ordered=False)

        loaded_raw = raw.count_documents({"id_run": run_id})
        if loaded_raw != read_rows:
            raise RuntimeError(
                f"Delta Raw consistency failed: read={read_rows}, raw={loaded_raw}"
            )

        metrics = Counter()
        error_counts = Counter()
        cursor = raw.find({"id_run": run_id}).batch_size(write_batch_size)
        try:
            for raw_doc in cursor:
                quality = evaluate_record(raw_doc.get("record_raw", {}))
                try:
                    incoming_version = parse_version(raw_doc.get("delta_version_raw"))
                except ValueError as exc:
                    quality.quality_status = "quarantined"
                    quality.error_codes.append("VERSION_INVALID")
                    quality.error_details.append(
                        {
                            "code": "VERSION_INVALID",
                            "field": "version",
                            "value": raw_doc.get("delta_version_raw"),
                            "message": str(exc),
                        }
                    )

                if quality.quality_status == "quarantined":
                    metrics["quarantine"] += 1
                    error_counts.update(quality.error_codes)
                    qdoc = build_quarantine_document(quality, raw_doc)
                    qdoc["delta_version_raw"] = raw_doc.get("delta_version_raw")
                    quarantine.update_one(
                        {
                            "source_run_id": run_id,
                            "source_row_number": qdoc["source_row_number"],
                        },
                        {"$setOnInsert": qdoc},
                        upsert=True,
                    )
                    continue

                final_doc = build_validated_document(quality, raw_doc)
                final_doc["source_version"] = incoming_version
                final_doc["record_hash"] = business_record_hash(final_doc)
                existing = validated.find_one(
                    {"id_order": final_doc["id_order"]},
                    {
                        "record_hash": 1,
                        "source_version": 1,
                        "order_date": 1,
                        "total_amount": 1,
                        "delivery_cost": 1,
                        "items": 1,
                    },
                )
                action = decide_version_action(
                    None if existing is None else existing.get("source_version", 0),
                    None if existing is None else existing.get("record_hash"),
                    incoming_version,
                    final_doc["record_hash"],
                )

                if action in {"inserted", "updated"}:
                    validated.replace_one(
                        {"id_order": final_doc["id_order"]},
                        final_doc,
                        upsert=True,
                    )

                    # Phase 2:
                    # ????? ???Materialized Views ?????? ???.
                    # ?? ??? ??? Full Rebuild ??? ?? Delta.
                    apply_order_change(
                        old_order=(
                            existing
                            if action == "updated"
                            else None
                        ),
                        new_order=final_doc,
                        database=database,
                    )

                    metrics[action] += 1
                elif action == "unchanged":
                    metrics["unchanged"] += 1
                elif action == "stale":
                    metrics["unchanged"] += 1
                    metrics["stale_ignored"] += 1
                else:  # same version with different payload
                    metrics["quarantine"] += 1
                    error_counts["VERSION_CONFLICT_SAME_VERSION"] += 1
                    qdoc = build_quarantine_document(quality, raw_doc)
                    qdoc["error_codes"] = ["VERSION_CONFLICT_SAME_VERSION"]
                    qdoc["codes_error"] = qdoc["error_codes"]
                    qdoc["error_details"] = [
                        {
                            "code": "VERSION_CONFLICT_SAME_VERSION",
                            "field": "version",
                            "value": incoming_version,
                            "message": "Same id_order/version has a different payload.",
                        }
                    ]
                    qdoc["details_error"] = qdoc["error_details"]
                    quarantine.update_one(
                        {
                            "source_run_id": run_id,
                            "source_row_number": qdoc["source_row_number"],
                        },
                        {"$setOnInsert": qdoc},
                        upsert=True,
                    )
        finally:
            cursor.close()

        elapsed = time.perf_counter() - started
        classified = metrics["inserted"] + metrics["updated"] + metrics["unchanged"] + metrics["quarantine"]
        consistency_pass = classified == read_rows
        if not consistency_pass:
            raise RuntimeError(
                f"Delta consistency failed: raw={read_rows}, classified={classified}"
            )

        result = {
            "run_id": run_id,
            "mode": "incremental_delta",
            "database": database,
            "file_name": source.name,
            "read_rows": read_rows,
            "loaded_raw": loaded_raw,
            "inserted": metrics["inserted"],
            "updated": metrics["updated"],
            "unchanged": metrics["unchanged"],
            "stale_ignored": metrics["stale_ignored"],
            "quarantine": metrics["quarantine"],
            "error_counts": dict(error_counts),
            "seconds_elapsed": round(elapsed, 3),
            "throughput": round(read_rows / elapsed if elapsed else 0.0, 2),
            "consistency_pass": True,
        }

        print("Incremental Delta Summary")
        print("-" * 65)
        for key, value in result.items():
            print(f"{key:18}: {value}")
        return result
    finally:
        client.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Advanced Path B: process only new/updated Delta records by version."
    )
    parser.add_argument("--input", required=True, help="Delta CSV with the 17 base columns + version.")
    parser.add_argument("--database", default=MONGO_DATABASE)
    parser.add_argument("--write-batch-size", type=int, default=BATCH_SIZE)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_incremental(args.input, args.database, args.write_batch_size)
