"""
Booking ETL job.

Industry Spark convention: runnable jobs live under src/jobs/.
(Use "pipelines/" for orchestrators like Airflow — not for the Spark entrypoint.)
"""

import os

from config import settings

# Driver memory + Iceberg jars must be available when the JVM starts.
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    (
        f"--driver-memory {settings.DRIVER_MEMORY} "
        f"--executor-memory {settings.EXECUTOR_MEMORY} "
        f"--packages {settings.ICEBERG_PACKAGE} "
        "pyspark-shell"
    ),
)

from src.core.spark_session import SparkSessionFactory
from src.readers.jsonl_reader import JsonlReader
from src.readers.mapping_reader import MappingReader
from src.services.exchange_rates import ExchangeRateService
from src.transforms.bookings import BookingTransformer
from src.writers.iceberg_writer import IcebergWriter


def run() -> None:
    """Read → transform → write Iceberg table."""

    spark = SparkSessionFactory.create()

    try:
        reader = JsonlReader(spark)
        mapping_reader = MappingReader(spark)
        exchange_rates = ExchangeRateService(spark)
        transformer = BookingTransformer(exchange_rates)
        writer = IcebergWriter(spark)

        status_map = mapping_reader.read(settings.STATUS_MAPPING)
        device_map = mapping_reader.read(settings.DEVICE_MAPPING)
        region_map = mapping_reader.read(settings.REGION_MAPPING)

        df = reader.read(
            settings.INPUT_FILE,
            updated_from="2026-07-01",
            updated_to="2026-07-31",
        )
        df = transformer.transform(df, status_map, device_map, region_map)

        writer.write(df)

        print(f"\nMerged into Iceberg table: {settings.ICEBERG_FULL_TABLE_NAME}")
        print(f"Location: {settings.WAREHOUSE_DIR}/{settings.ICEBERG_TABLE}")

        result = spark.table(settings.ICEBERG_FULL_TABLE_NAME)
        print("\n========== SCHEMA ==========")
        result.printSchema()

        print("\n========== SAMPLE DATA ==========")
        result.show(20, truncate=False)

        print("\n========== ROW COUNT ==========")
        print(result.count())

    finally:
        spark.stop()


if __name__ == "__main__":
    run()
