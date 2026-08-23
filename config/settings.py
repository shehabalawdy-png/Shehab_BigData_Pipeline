import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DEMO_DIR = PROJECT_ROOT / "demo"
REPORTS_DIR = PROJECT_ROOT / "reports"
SCREENSHOTS_DIR = REPORTS_DIR / "screenshots"
RUNTIME_DIR = PROJECT_ROOT / ".runtime"


def _env_path(name: str, default: Path) -> Path:
    return Path(os.getenv(name, str(default))).expanduser()


# Input/sample defaults. The main entry point still requires --input, so no
# machine-specific path is hidden inside the execution logic.
INPUT_FILE = _env_path(
    "BIGDATA_INPUT_FILE",
    DATA_DIR / "orders_huge_mixed_quality.csv",
)
SMALL_SAMPLE_FILE = _env_path(
    "SMALL_SAMPLE_FILE",
    DATA_DIR / "orders_small_sample.csv",
)

# File Router. 200 MB matches the assignment example and remains configurable.
SMALL_FILE_THRESHOLD_MB = int(os.getenv("SMALL_FILE_THRESHOLD_MB", "200"))
SAMPLE_ROWS = int(os.getenv("SAMPLE_ROWS", "100000"))

# Python Batch
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))

# MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "shehab_bigdata")
MONGO_CONNECT_TIMEOUT_MS = int(os.getenv("MONGO_CONNECT_TIMEOUT_MS", "5000"))
RAW_COLLECTION = os.getenv("RAW_COLLECTION", "orders_raw")
VALIDATED_COLLECTION = os.getenv("VALIDATED_COLLECTION", "orders_validated")
QUARANTINE_COLLECTION = os.getenv("QUARANTINE_COLLECTION", "quarantine_orders")

# Spark
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "ShehabHybridOrdersPipeline")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_ELT_MASTER = os.getenv("SPARK_ELT_MASTER", "local[2]")
SPARK_ELT_SHUFFLE_PARTITIONS = int(
    os.getenv("SPARK_ELT_SHUFFLE_PARTITIONS", "64")
)
SPARK_CACHE_BATCH_SIZE = int(
    os.getenv("SPARK_CACHE_BATCH_SIZE", "256")
)
SPARK_LOG_LEVEL = os.getenv("SPARK_LOG_LEVEL", "ERROR").upper()

# Spark JVM memory. The Windows viva machine has ~16 GB RAM; PySpark otherwise
# starts the local JVM with only ~1 GB heap, which is insufficient for the
# 30M-row MongoDB/Spark ELT path. Keep this configurable for other machines.
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "4g")


# Use a valid local address for Spark RPC.
# This avoids failures on Windows computer names containing characters
# that are invalid inside Spark RPC URLs, such as underscores.
SPARK_DRIVER_HOST = os.getenv("SPARK_DRIVER_HOST", "127.0.0.1")
SPARK_DRIVER_BIND_ADDRESS = os.getenv(
    "SPARK_DRIVER_BIND_ADDRESS",
    "127.0.0.1",
)

# Spark reads this environment variable when the local JVM starts.
os.environ.setdefault("SPARK_LOCAL_IP", SPARK_DRIVER_HOST)
# PYSPARK_SUBMIT_ARGS is consumed when PySpark launches the JVM. Preserve any
# existing connector/package arguments and inject driver memory only when the
# caller has not already supplied it. This keeps the normal `python -m src.main`
# command self-contained; no special Spark command is required during viva.
_existing_submit_args = os.getenv("PYSPARK_SUBMIT_ARGS", "").strip()
if "--driver-memory" not in _existing_submit_args:
    if _existing_submit_args:
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--driver-memory {SPARK_DRIVER_MEMORY} {_existing_submit_args}"
        )
    else:
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--driver-memory {SPARK_DRIVER_MEMORY} pyspark-shell"
        )
SPARK_TEMP_DIR = _env_path("SPARK_TEMP_DIR", RUNTIME_DIR / "spark")
SPARK_LOCAL_DIR = _env_path("SPARK_LOCAL_DIR", SPARK_TEMP_DIR / "local")
SPARK_PYTHON_TEMP_DIR = _env_path(
    "SPARK_PYTHON_TEMP_DIR", SPARK_TEMP_DIR / "python"
)
SPARK_WAREHOUSE_DIR = _env_path(
    "SPARK_WAREHOUSE_DIR", SPARK_TEMP_DIR / "warehouse"
)

# Reports
RESULTS_JSON = _env_path("RESULTS_JSON", REPORTS_DIR / "results.json")
