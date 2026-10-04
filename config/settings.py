import os
from pathlib import Path

# Base Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"
DOCS_DIR = BASE_DIR / "docs"

# Ensure essential directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
DOCS_DIR.mkdir(parents=True, exist_ok=True)

# MongoDB Configuration
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DATABASE_NAME = os.getenv("DATABASE_NAME", "midterm_data_pipeline0")
RAW_COLLECTION = os.getenv("RAW_COLLECTION", "orders_raw")
VALIDATED_COLLECTION = os.getenv("VALIDATED_COLLECTION", "orders_validated")
QUARANTINE_COLLECTION = os.getenv("QUARANTINE_COLLECTION", "orders_quarantine")
MONGODB_MAX_POOL_SIZE = int(os.getenv("MONGODB_MAX_POOL_SIZE", "200"))
MONGODB_SOCKET_TIMEOUT_MS = int(os.getenv("MONGODB_SOCKET_TIMEOUT_MS", "600000"))

# Routing Configuration
# Threshold in MB to decide between Python Batch Loader and PySpark Loader
# Under 200 MB: Python Batch is fast, low overhead, low startup latency.
# Above 200 MB: Spark parallel distributed engine handles big data efficiently.
SMALL_FILE_THRESHOLD_MB = float(os.getenv("SMALL_FILE_THRESHOLD_MB", "200.0"))

# Processing Batch Sizes
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
SPARK_BATCH_SIZE = int(os.getenv("SPARK_BATCH_SIZE", "5000"))

# Spark Specific Configuration (Optimized for 16GB RAM & 8 Cores)
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "Midterm-Data-Pipeline-PySpark")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_SHUFFLE_PARTITIONS = os.getenv("SPARK_SHUFFLE_PARTITIONS", "16")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "8g")
SPARK_BATCH_SIZE = int(os.getenv("SPARK_BATCH_SIZE", "10000"))

# Default Data Paths
DEFAULT_LARGE_INPUT = str(DATA_DIR / "orders_huge_mixed_quality.csv")
DEFAULT_SAMPLE_INPUT = str(DATA_DIR / "orders_sample_100000.csv")
RESULTS_JSON_PATH = str(REPORTS_DIR / "results.json")
RESULTS_MD_PATH = str(REPORTS_DIR / "results.md")
