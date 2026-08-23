import argparse
import csv
from pathlib import Path

from config.settings import (
    INPUT_FILE,
    SMALL_SAMPLE_FILE,
    SAMPLE_ROWS,
)
from src.schema import validate_csv_header


def create_sample(input_file, output_file, rows):
    source = Path(input_file)
    destination = Path(output_file)

    if not source.exists():
        raise FileNotFoundError(f"Source CSV not found: {source}")

    if rows <= 0:
        raise ValueError("Number of rows must be greater than zero.")

    validate_csv_header(source)

    destination.parent.mkdir(parents=True, exist_ok=True)

    written_rows = 0

    with source.open(
        "r",
        encoding="utf-8-sig",
        newline="",
        errors="replace",
    ) as src, destination.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as dst:

        reader = csv.reader(src)
        writer = csv.writer(dst)

        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("Source CSV is empty.")

        writer.writerow(header)

        for row in reader:
            writer.writerow(row)
            written_rows += 1

            if written_rows >= rows:
                break

    size_mb = destination.stat().st_size / (1024 * 1024)

    print("Small Sample Created")
    print("-" * 55)
    print(f"Source       : {source}")
    print(f"Output       : {destination}")
    print(f"Rows written : {written_rows}")
    print(f"Size         : {size_mb:.2f} MB")

    return destination


def parse_args():
    parser = argparse.ArgumentParser(
        description="Create a reproducible small CSV sample."
    )

    parser.add_argument(
        "--input",
        default=str(INPUT_FILE),
        help="Path to the original large CSV file.",
    )

    parser.add_argument(
        "--output",
        default=str(SMALL_SAMPLE_FILE),
        help="Destination of the generated sample.",
    )

    parser.add_argument(
        "--rows",
        type=int,
        default=SAMPLE_ROWS,
        help="Number of data rows to copy.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    create_sample(
        input_file=args.input,
        output_file=args.output,
        rows=args.rows,
    )
