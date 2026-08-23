import argparse
from datetime import datetime, timezone
from pathlib import Path

from config.settings import (
    BATCH_SIZE,
    MONGO_DATABASE,
    SMALL_FILE_THRESHOLD_MB,
    SPARK_ELT_SHUFFLE_PARTITIONS,
)
from src.file_router import choose_engine, print_route
from src.metrics import save_runtime_run
from src.schema import validate_csv_header


def build_parser():
    parser = argparse.ArgumentParser(
        description=(
            "Run the complete hybrid orders pipeline: validate CSV, route by "
            "file size, load Raw, apply quality rules, and write final results."
        )
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the CSV file supplied for processing.",
    )
    parser.add_argument(
        "--database",
        default=MONGO_DATABASE,
        help="MongoDB database name. Use a separate demo DB during viva if desired.",
    )
    parser.add_argument(
        "--threshold-mb",
        type=int,
        default=SMALL_FILE_THRESHOLD_MB,
        help="Maximum file size handled by Python Batch. Larger files use PySpark.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help="Python Batch Raw insert size.",
    )
    parser.add_argument(
        "--write-batch-size",
        type=int,
        default=BATCH_SIZE,
        help="Final MongoDB write batch size.",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=SPARK_ELT_SHUFFLE_PARTITIONS,
        help="Spark ELT shuffle partitions for large-file processing.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate the CSV schema and exit without MongoDB writes.",
    )
    parser.add_argument(
        "--route-only",
        action="store_true",
        help="Validate, show the automatic Router decision, and exit without writes.",
    )
    parser.add_argument(
        "--raw-only",
        action="store_true",
        help="Stop after the Raw load. Useful for proving Raw-first ELT.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Re-raise exceptions with a full traceback.",
    )
    return parser


def _validate_runtime_args(args):
    if not args.database.strip():
        raise ValueError("Database name cannot be empty.")
    if args.threshold_mb <= 0:
        raise ValueError("--threshold-mb must be greater than zero.")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be greater than zero.")
    if args.write_batch_size <= 0:
        raise ValueError("--write-batch-size must be greater than zero.")
    if args.shuffle_partitions <= 0:
        raise ValueError("--shuffle-partitions must be greater than zero.")


def run_pipeline(args):
    _validate_runtime_args(args)

    source = Path(args.input).expanduser().resolve()
    started_at = datetime.now(timezone.utc)

    print("Hybrid Big Data Orders Pipeline")
    print("=" * 72)
    print(f"Input    : {source}")
    print(f"Database : {args.database}")
    print()

    header_check = validate_csv_header(source)
    print("CSV schema validation: PASS")
    print(f"Columns              : {len(header_check.columns)}")

    if args.validate_only:
        print("VALIDATE ONLY: PASS")
        return {
            "status": "validated_only",
            "file_name": source.name,
            "database": args.database,
            "schema_valid": True,
        }

    # The execution engine is ALWAYS chosen by the real file-size Router.
    # No manual engine override exists in the final project.
    decision = choose_engine(source, threshold_mb=args.threshold_mb)
    print()
    print_route(decision)

    base_report = {
        "started_at": started_at.isoformat(),
        "file_name": source.name,
        "file_path": str(source),
        "file_size_mb": decision.file_size_mb,
        "threshold_mb": decision.threshold_mb,
        "used_engine": decision.engine,
        "routing_mode": "automatic_file_size_router",
        "database": args.database,
        "schema_valid": True,
    }

    if args.route_only:
        report = {
            **base_report,
            "status": "route_only",
        }
        save_runtime_run(report)
        print("ROUTE ONLY: PASS")
        return report

    print()
    from src.mongo_setup import configure_database

    configure_database(database=args.database)

    if decision.engine == "python_batch":
        from src.batch_loader import load_raw_with_python_batch

        raw_metrics = load_raw_with_python_batch(
            input_file=source,
            batch_size=args.batch_size,
            database=args.database,
        )
    else:
        from src.spark_loader import load_raw_with_pyspark

        raw_metrics = load_raw_with_pyspark(
            input_file=source,
            database=args.database,
        )

    run_id = raw_metrics.get("id_run") or raw_metrics.get("run_id")
    if not run_id:
        raise RuntimeError("Raw loader did not return a run_id.")

    if args.raw_only:
        report = {
            **base_report,
            "status": "raw_only_pass",
            "run_id": run_id,
            "raw_load": raw_metrics,
        }
        save_runtime_run(report)
        print("RAW ONLY: PASS")
        return report

    if decision.engine == "python_batch":
        from src.elt_pipeline import run_elt

        quality_metrics = run_elt(
            run_id=run_id,
            write_batch_size=args.write_batch_size,
            database=args.database,
        )
    else:
        from src.spark_elt_pipeline import run_spark_elt

        quality_metrics = run_spark_elt(
            run_id=run_id,
            database=args.database,
            write_batch_size=args.write_batch_size,
            shuffle_partitions=args.shuffle_partitions,
        )

    finished_at = datetime.now(timezone.utc)
    report = {
        **base_report,
        "status": "pass",
        "run_id": run_id,
        "finished_at": finished_at.isoformat(),
        "raw_load": raw_metrics,
        "quality_and_final_load": quality_metrics,
    }
    result_path = save_runtime_run(report)

    print()
    print("=" * 72)
    print("PIPELINE: PASS")
    print(f"run_id         : {run_id}")
    print(f"engine         : {decision.engine}")
    print(f"results report : {result_path}")
    print("=" * 72)

    return report


def main():
    parser = build_parser()
    args = parser.parse_args()

    try:
        run_pipeline(args)
    except Exception as exc:
        print()
        print("PIPELINE: FAIL")
        print(f"{exc.__class__.__name__}: {exc}")
        if args.debug:
            raise
        raise SystemExit(1)


if __name__ == "__main__":
    main()
