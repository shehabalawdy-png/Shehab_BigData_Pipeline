import argparse
from dataclasses import dataclass
from pathlib import Path

from config.settings import INPUT_FILE, SMALL_FILE_THRESHOLD_MB


@dataclass(frozen=True)
class RouteDecision:
    file_path: Path
    file_size_mb: float
    threshold_mb: int
    engine: str
    reason: str


def choose_engine(file_path, threshold_mb=SMALL_FILE_THRESHOLD_MB):
    path = Path(file_path).expanduser().resolve()

    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    if not path.is_file():
        raise ValueError(f"Input path is not a file: {path}")

    if threshold_mb <= 0:
        raise ValueError("threshold_mb must be greater than zero.")

    size_mb = path.stat().st_size / (1024 * 1024)

    if size_mb <= threshold_mb:
        engine = "python_batch"
        reason = (
            f"File size {size_mb:.2f} MB is less than or equal "
            f"to the {threshold_mb} MB threshold."
        )
    else:
        engine = "pyspark"
        reason = (
            f"File size {size_mb:.2f} MB exceeds "
            f"the {threshold_mb} MB threshold."
        )

    return RouteDecision(
        file_path=path,
        file_size_mb=round(size_mb, 2),
        threshold_mb=threshold_mb,
        engine=engine,
        reason=reason,
    )


def print_route(decision):
    print("File Router Decision")
    print("-" * 55)
    print(f"File      : {decision.file_path}")
    print(f"Size      : {decision.file_size_mb:.2f} MB")
    print(f"Threshold : {decision.threshold_mb} MB")
    print(f"Engine    : {decision.engine}")
    print(f"Reason    : {decision.reason}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Choose Python Batch or PySpark from the CSV file size."
    )
    parser.add_argument(
        "--input",
        default=str(INPUT_FILE),
        help="CSV file to inspect.",
    )
    parser.add_argument(
        "--threshold-mb",
        type=int,
        default=SMALL_FILE_THRESHOLD_MB,
        help="Maximum file size for Python Batch.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    result = choose_engine(
        file_path=args.input,
        threshold_mb=args.threshold_mb,
    )
    print_route(result)
