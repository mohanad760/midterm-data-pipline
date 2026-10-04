import argparse
import sys
import time
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import settings


def create_sample(input_path: str, output_path: str, num_rows: int = 100000):
    """
    Extract a representative sample of `num_rows` from the large CSV file
    using streaming line-by-line reading to ensure constant minimal memory usage.
    """
    input_file = Path(input_path)
    output_file = Path(output_path)

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    print(f"Creating small sample from: {input_path}")
    print(f"Target row count: {num_rows:,}")
    print(f"Destination: {output_path}")

    start_time = time.time()
    rows_written = 0

    with open(input_file, mode="r", encoding="utf-8", errors="replace") as fin, \
         open(output_file, mode="w", encoding="utf-8", newline="") as fout:

        # Copy header
        header = fin.readline()
        if not header:
            raise ValueError("Input file is empty!")
        fout.write(header)

        for line in fin:
            fout.write(line)
            rows_written += 1

            if rows_written % 20000 == 0:
                print(f"Extracted {rows_written:,} / {num_rows:,} rows...")

            if rows_written >= num_rows:
                break

    elapsed = time.time() - start_time
    file_size_mb = output_file.stat().st_size / (1024 * 1024)

    print("Sample generation complete!")
    print(f"- Rows extracted: {rows_written:,}")
    print(f"- File size: {file_size_mb:.2f} MB")
    print(f"- Elapsed time: {elapsed:.2f} seconds")


def main():
    parser = argparse.ArgumentParser(description="Extract a small sample from the huge CSV dataset.")
    parser.add_argument(
        "--input",
        type=str,
        default=settings.DEFAULT_LARGE_INPUT,
        help="Path to the huge CSV file."
    )
    parser.add_argument(
        "--output",
        type=str,
        default=settings.DEFAULT_SAMPLE_INPUT,
        help="Path to write the sampled CSV file."
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=100000,
        help="Number of rows to extract (excluding header)."
    )

    args = parser.parse_args()
    create_sample(args.input, args.output, args.rows)


if __name__ == "__main__":
    main()
