import os
from pathlib import Path
from typing import Tuple
import sys

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import settings


def route_file(file_path: str, threshold_mb: float = None) -> Tuple[str, float, str]:
    """
    Examines file size and automatically selects the optimal execution engine:
    - python_batch: For files <= threshold_mb (default 200 MB).
      Zero JVM/cluster overhead, minimal latency, streaming line-by-line execution.
    - pyspark: For files > threshold_mb.
      Parallel distributed partition-based processing, scalable across multi-cores/nodes.

    Returns:
        (chosen_engine, file_size_mb, justification)
    """
    if threshold_mb is None:
        threshold_mb = settings.SMALL_FILE_THRESHOLD_MB

    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")

    file_size_bytes = path.stat().st_size
    file_size_mb = file_size_bytes / (1024 * 1024)

    if file_size_mb <= threshold_mb:
        engine = "python_batch"
        justification = (
            f"File size ({file_size_mb:.2f} MB) is within the small-file threshold ({threshold_mb:.2f} MB). "
            "Selected 'python_batch' engine for instant execution without Spark JVM startup latency, "
            "utilizing memory-efficient chunked streaming."
        )
    else:
        engine = "pyspark"
        justification = (
            f"File size ({file_size_mb:.2f} MB) exceeds the threshold ({threshold_mb:.2f} MB). "
            "Selected 'pyspark' distributed engine for parallel partitioned execution and multi-core scalability."
        )

    return engine, file_size_mb, justification


if __name__ == "__main__":
    test_files = [settings.DEFAULT_SAMPLE_INPUT, settings.DEFAULT_LARGE_INPUT]
    for tf in test_files:
        if Path(tf).exists():
            eng, sz, just = route_file(tf)
            print("=" * 60)
            print(f"File: {tf}")
            print(f"Size: {sz:.2f} MB")
            print(f"Engine: {eng}")
            print(f"Justification: {just}")
