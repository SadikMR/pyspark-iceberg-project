"""
Project settings.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

APP_NAME = "booking-etl"
MASTER = "local[*]"

# Driver memory needs PYSPARK_SUBMIT_ARGS (set in main) to take effect.
DRIVER_MEMORY = "4g"
EXECUTOR_MEMORY = "4g"
SHUFFLE_PARTITIONS = "8"

INPUT_FILE = str(ROOT / "data" / "raw" / "bookings.jsonl")
DEVICE_MAPPING = str(ROOT / "src" / "mappings" / "device.json")
STATUS_MAPPING = str(ROOT / "src" / "mappings" / "booking_status.json")
REGION_MAPPING = str(ROOT / "src" / "mappings" / "country_region.json")
