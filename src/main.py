"""
Application entry point.
"""

import os

from config import settings

# Driver memory is applied at JVM start, so set this before SparkSession.
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    (
        f"--driver-memory {settings.DRIVER_MEMORY} "
        f"--executor-memory {settings.EXECUTOR_MEMORY} "
        "pyspark-shell"
    ),
)

from src.core.spark_session import SparkSessionFactory
from src.readers.jsonl_reader import JsonlReader
from src.readers.mapping_reader import MappingReader
from src.services.exchange_rates import ExchangeRateService
from src.transforms.bookings import BookingTransformer


def main() -> None:
    spark = SparkSessionFactory.create()

    try:
        reader = JsonlReader(spark)
        mapping_reader = MappingReader(spark)
        exchange_rates = ExchangeRateService(spark)
        transformer = BookingTransformer(exchange_rates)

        status_map = mapping_reader.read(settings.STATUS_MAPPING)
        device_map = mapping_reader.read(settings.DEVICE_MAPPING)
        region_map = mapping_reader.read(settings.REGION_MAPPING)

        df = reader.read(
            settings.INPUT_FILE,
            updated_from="2026-07-01",
            updated_to="2026-07-31",
        )
        df = transformer.transform(df, status_map, device_map, region_map)

        print("\n========== SCHEMA ==========")
        df.printSchema()

        print("\n========== COLUMNS ==========")
        print(df.columns)

        print("\n========== SAMPLE DATA ==========")
        df.show(20, truncate=False)

        print("\n========== ROW COUNT ==========")
        print(df.count())

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
