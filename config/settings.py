"""
Project settings.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

APP_NAME = "booking-etl"
MASTER = "local[*]"

# Driver memory needs PYSPARK_SUBMIT_ARGS (set in the job) to take effect.
DRIVER_MEMORY = "4g"
EXECUTOR_MEMORY = "4g"
SHUFFLE_PARTITIONS = "8"

INPUT_FILE = str(ROOT / "data" / "raw" / "bookings.jsonl")
DEVICE_MAPPING = str(ROOT / "src" / "mappings" / "device.json")
STATUS_MAPPING = str(ROOT / "src" / "mappings" / "booking_status.json")
REGION_MAPPING = str(ROOT / "src" / "mappings" / "country_region.json")

# Iceberg (Hadoop catalog) — table lives at data/warehouse/bookings (Parquet).
WAREHOUSE_DIR = str(ROOT / "data" / "warehouse")
ICEBERG_CATALOG = "local"
ICEBERG_TABLE = "bookings"
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
ICEBERG_FULL_TABLE_NAME = f"{ICEBERG_CATALOG}.{ICEBERG_TABLE}"
ICEBERG_MERGE_KEY = "transaction_id"
ICEBERG_SNAPSHOT_RETAIN_LAST = 5
