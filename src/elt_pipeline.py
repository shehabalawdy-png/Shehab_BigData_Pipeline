import argparse
import hashlib
import json
import time
from collections import Counter
from datetime import datetime, timezone

from pymongo import (
    MongoClient,
    ReplaceOne,
    UpdateOne,
)

from config.settings import (
    BATCH_SIZE,
    MONGO_DATABASE,
    MONGO_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
)

from src.quality_rules import evaluate_record
from src.hashing import BUSINESS_FIELDS, business_record_hash


def stable_hash(document):
    serialized = json.dumps(
        document,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )

    return hashlib.sha256(
        serialized.encode("utf-8")
    ).hexdigest()


def get_latest_run_id(raw_collection):
    document = raw_collection.find_one(
        {},
        {"id_run": 1},
        sort=[("at_ingested", -1)],
    )

    if not document:
        raise RuntimeError(
            "orders_raw is empty."
        )

    return document["id_run"]


def find_duplicate_order_ids(
    raw_collection,
    run_id,
):
    counts = Counter()

    cursor = (
        raw_collection
        .find(
            {
                "id_run": run_id,
            },
            {
                "_id": 0,
                "record_raw.order_id": 1,
            },
        )
        .batch_size(5000)
    )

    try:
        for document in cursor:

            raw_record = (
                document.get(
                    "record_raw",
                    {},
                )
                or {}
            )

            order_id = str(
                raw_record.get(
                    "order_id",
                    "",
                )
                or ""
            ).strip()

            if order_id:
                counts[order_id] += 1

    finally:
        cursor.close()

    return {
        order_id
        for order_id, count
        in counts.items()
        if count > 1
    }


def build_validated_document(quality_result, raw_document):
    """Build one canonical final document independent of loader metadata."""
    record = quality_result.record
    document = {field: record.get(field) for field in BUSINESS_FIELDS}
    document.update(
        {
            "id_order": record.get("order_id"),
            "quality_status": quality_result.quality_status,
            "corrections": quality_result.corrections,
            "source_run_id": raw_document["id_run"],
            "source_row_number": raw_document.get(
                "number_row_source", raw_document.get("source_record_id")
            ),
            "source_record_id": raw_document.get("source_record_id"),
            "source_partition_id": raw_document.get("source_partition_id"),
            "source_file": raw_document.get("file_source"),
            "engine_used": raw_document.get("engine_used"),
        }
    )
    document["record_hash"] = business_record_hash(document)
    document["processed_at"] = datetime.now(timezone.utc)
    return document


def build_quarantine_document(quality_result, raw_document):
    error_codes = list(quality_result.error_codes)
    error_details = list(quality_result.error_details)
    raw_record = raw_document.get("record_raw")
    return {
        "source_run_id": raw_document["id_run"],
        "source_row_number": raw_document.get(
            "number_row_source", raw_document.get("source_record_id")
        ),
        "source_record_id": raw_document.get("source_record_id"),
        "source_partition_id": raw_document.get("source_partition_id"),
        "source_file": raw_document.get("file_source"),
        "engine_used": raw_document.get("engine_used"),
        "quality_status": "quarantined",
        # English names used by the code:
        "error_codes": error_codes,
        "error_details": error_details,
        "raw_record": raw_record,
        # Assignment-friendly aliases (same information):
        "codes_error": error_codes,
        "details_error": error_details,
        "record_raw": raw_record,
        "corrections": quality_result.corrections,
        "processed_record": quality_result.record,
        "processed_at": datetime.now(timezone.utc),
    }


def flush_validated(
    collection,
    documents,
    write_metrics,
):
    if not documents:
        return

    ids = [
        document["id_order"]
        for document in documents
    ]

    existing = {}

    cursor = collection.find(
        {
            "id_order": {
                "$in": ids,
            }
        },
        {
            "id_order": 1,
            "record_hash": 1,
        },
    )

    for document in cursor:
        existing[
            document["id_order"]
        ] = document.get(
            "record_hash"
        )

    operations = []

    for document in documents:

        order_id = document[
            "id_order"
        ]

        new_hash = document[
            "record_hash"
        ]

        if order_id not in existing:
            write_metrics[
                "inserted"
            ] += 1

            operations.append(
                ReplaceOne(
                    {
                        "id_order":
                            order_id
                    },
                    document,
                    upsert=True,
                )
            )

        elif (
            existing[order_id]
            == new_hash
        ):
            write_metrics[
                "unchanged"
            ] += 1

        else:
            write_metrics[
                "updated"
            ] += 1

            operations.append(
                ReplaceOne(
                    {
                        "id_order":
                            order_id
                    },
                    document,
                    upsert=True,
                )
            )

    if operations:
        collection.bulk_write(
            operations,
            ordered=False,
        )


def flush_quarantine(
    collection,
    documents,
    write_metrics,
):
    if not documents:
        return

    operations = []

    for document in documents:

        operations.append(
            UpdateOne(
                {
                    "source_run_id":
                        document[
                            "source_run_id"
                        ],
                    "source_row_number":
                        document[
                            "source_row_number"
                        ],
                },
                {
                    "$setOnInsert":
                        document
                },
                upsert=True,
            )
        )

    result = collection.bulk_write(
        operations,
        ordered=False,
    )

    write_metrics[
        "quarantine_inserted"
    ] += result.upserted_count

    write_metrics[
        "quarantine_existing"
    ] += result.matched_count


def run_elt(
    run_id=None,
    write_batch_size=BATCH_SIZE,
    database=MONGO_DATABASE,
):
    if write_batch_size <= 0:
        raise ValueError(
            "write_batch_size must be greater than zero."
        )

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )

    cursor = None
    started_at = time.perf_counter()

    try:
        client.admin.command(
            "ping"
        )

        db = client[
            database
        ]

        raw_collection = db[
            RAW_COLLECTION
        ]

        validated_collection = db[
            VALIDATED_COLLECTION
        ]

        quarantine_collection = db[
            QUARANTINE_COLLECTION
        ]

        if run_id is None:
            run_id = get_latest_run_id(
                raw_collection
            )

        raw_count = (
            raw_collection
            .count_documents(
                {
                    "id_run":
                        run_id
                }
            )
        )

        if raw_count == 0:
            raise RuntimeError(
                f"No raw documents found "
                f"for run_id={run_id}"
            )

        print("ELT Quality Pipeline")
        print("-" * 65)
        print(
            f"run_id           : {run_id}"
        )
        print(
            f"database         : {database}"
        )
        print(
            f"raw records      : {raw_count}"
        )
        print(
            f"write batch size : "
            f"{write_batch_size}"
        )

        print()
        print(
            "Scanning duplicate "
            "business keys..."
        )

        duplicate_ids = (
            find_duplicate_order_ids(
                raw_collection,
                run_id,
            )
        )

        print(
            "duplicate order IDs:",
            len(duplicate_ids),
        )
        print()

        counts = {
            "processed": 0,
            "valid": 0,
            "corrected": 0,
            "quarantined": 0,
        }

        write_metrics = {
            "inserted": 0,
            "updated": 0,
            "unchanged": 0,
            "quarantine_inserted": 0,
            "quarantine_existing": 0,
        }

        error_counts = Counter()

        validated_buffer = []
        quarantine_buffer = []

        cursor = (
            raw_collection
            .find(
                {
                    "id_run":
                        run_id
                }
            )
            .batch_size(
                write_batch_size
            )
        )

        for raw_document in cursor:

            counts[
                "processed"
            ] += 1

            raw_record = (
                raw_document.get(
                    "record_raw",
                    {},
                )
            )

            quality = evaluate_record(
                raw_record
            )

            normalized_order_id = str(
                quality.record.get(
                    "order_id",
                    ""
                )
                or ""
            ).strip()

            if (
                normalized_order_id
                and normalized_order_id
                in duplicate_ids
            ):
                if (
                    "ID_ORDER_DUPLICATE"
                    not in quality.error_codes
                ):
                    quality.error_codes.append(
                        "ID_ORDER_DUPLICATE"
                    )

                    quality.error_details.append(
                        {
                            "code":
                                "ID_ORDER_DUPLICATE",
                            "field":
                                "order_id",
                            "value":
                                normalized_order_id,
                            "message":
                                (
                                    "The same order ID "
                                    "appears more than once "
                                    "in this raw run."
                                ),
                        }
                    )

                quality.quality_status = (
                    "quarantined"
                )

            if (
                quality.quality_status
                == "quarantined"
            ):
                counts[
                    "quarantined"
                ] += 1

                for code in (
                    quality.error_codes
                ):
                    error_counts[
                        code
                    ] += 1

                quarantine_buffer.append(
                    build_quarantine_document(
                        quality,
                        raw_document,
                    )
                )

            else:
                counts[
                    quality.quality_status
                ] += 1

                validated_buffer.append(
                    build_validated_document(
                        quality,
                        raw_document,
                    )
                )

            if (
                len(validated_buffer)
                >= write_batch_size
            ):
                flush_validated(
                    validated_collection,
                    validated_buffer,
                    write_metrics,
                )

                validated_buffer.clear()

            if (
                len(quarantine_buffer)
                >= write_batch_size
            ):
                flush_quarantine(
                    quarantine_collection,
                    quarantine_buffer,
                    write_metrics,
                )

                quarantine_buffer.clear()

            if (
                counts["processed"]
                % 10000
                == 0
            ):
                print(
                    f"Processed "
                    f"{counts['processed']:,}"
                    f"/{raw_count:,} | "
                    f"valid="
                    f"{counts['valid']:,} | "
                    f"corrected="
                    f"{counts['corrected']:,} | "
                    f"quarantine="
                    f"{counts['quarantined']:,}"
                )

        flush_validated(
            validated_collection,
            validated_buffer,
            write_metrics,
        )

        flush_quarantine(
            quarantine_collection,
            quarantine_buffer,
            write_metrics,
        )

        elapsed = (
            time.perf_counter()
            - started_at
        )

        throughput = (
            counts["processed"]
            / elapsed
            if elapsed > 0
            else 0
        )

        final_classified = (
            counts["valid"]
            + counts["corrected"]
            + counts["quarantined"]
        )

        consistency_pass = (
            raw_count
            == final_classified
            == counts["processed"]
        )

        print()
        print(
            "ELT Summary"
        )
        print("-" * 65)

        print(
            f"run_raw_count      : "
            f"{raw_count}"
        )

        print(
            f"run_valid_count    : "
            f"{counts['valid']}"
        )

        print(
            f"run_corrected_count: "
            f"{counts['corrected']}"
        )

        print(
            f"run_quarantine_count: "
            f"{counts['quarantined']}"
        )

        print(
            f"classified_total    : "
            f"{final_classified}"
        )

        print(
            f"inserted            : "
            f"{write_metrics['inserted']}"
        )

        print(
            f"updated             : "
            f"{write_metrics['updated']}"
        )

        print(
            f"unchanged           : "
            f"{write_metrics['unchanged']}"
        )

        print(
            f"quarantine inserted : "
            f"{write_metrics['quarantine_inserted']}"
        )

        print(
            f"quarantine existing : "
            f"{write_metrics['quarantine_existing']}"
        )

        print(
            f"seconds_elapsed     : "
            f"{elapsed:.3f}"
        )

        print(
            f"throughput           : "
            f"{throughput:.2f} rows/s"
        )

        print(
            "consistency check    :",
            (
                "PASS"
                if consistency_pass
                else "FAIL"
            ),
        )

        print()
        print(
            "Top error codes"
        )
        print("-" * 65)

        for (
            code,
            count,
        ) in error_counts.most_common(
            15
        ):
            print(
                f"{code}: {count}"
            )

        if not consistency_pass:
            raise RuntimeError(
                "Run consistency check failed."
            )

        return {
            "run_id": run_id,
            "database": database,
            "run_raw_count": raw_count,
            "run_valid_count":
                counts["valid"],
            "run_corrected_count":
                counts["corrected"],
            "run_quarantine_count":
                counts["quarantined"],
            "classified_total":
                final_classified,
            "inserted":
                write_metrics["inserted"],
            "updated":
                write_metrics["updated"],
            "unchanged":
                write_metrics["unchanged"],
            "quarantine_inserted":
                write_metrics[
                    "quarantine_inserted"
                ],
            "quarantine_existing":
                write_metrics[
                    "quarantine_existing"
                ],
            "seconds_elapsed":
                round(elapsed, 3),
            "throughput":
                round(throughput, 2),
            "write_batch_size":
                write_batch_size,
            "error_counts":
                dict(error_counts),
            "consistency_pass":
                consistency_pass,
        }

    finally:
        if cursor is not None:
            cursor.close()

        client.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Transform and classify "
            "MongoDB raw orders."
        )
    )

    parser.add_argument(
        "--run-id",
        default=None,
        help=(
            "Raw run ID. "
            "If omitted, latest run is used."
        ),
    )

    parser.add_argument(
        "--database",
        default=MONGO_DATABASE,
        help="MongoDB database name.",
    )

    parser.add_argument(
        "--write-batch-size",
        type=int,
        default=BATCH_SIZE,
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    run_elt(
        run_id=args.run_id,
        write_batch_size=(
            args.write_batch_size
        ),
        database=args.database,
    )
