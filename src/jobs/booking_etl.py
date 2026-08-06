"""
Booking ETL job.
"""

from config import settings
from src.core.spark_session import SparkSessionFactory
from src.readers.jsonl_reader import JsonlReader
from src.readers.mapping_reader import MappingReader
from src.services.exchange_rates import ExchangeRateService
from src.services.exchange_rates_DAG_exec import ExchangeRateService as ExchangeRateServiceDAG
from src.transforms.bookings import BookingTransformer
from src.writers.iceberg_writer import IcebergWriter


class BookingEtlJob:
    """Read bookings → transform → MERGE INTO Iceberg."""

    def __init__(self, updated_from: str, updated_to: str) -> None:
        self.updated_from = updated_from
        self.updated_to = updated_to

    def run(self) -> None:
        spark = SparkSessionFactory.create()

        try:
            reader = JsonlReader(spark)
            mapping_reader = MappingReader(spark)

            # Flip these (and the return in BookingTransformer.transform) to compare FX modes.
            # Compare the single [runtime] fx_api line between runs.
            exchange_rates = ExchangeRateService(spark)
            # exchange_rates = ExchangeRateServiceDAG()

            transformer = BookingTransformer(exchange_rates)
            writer = IcebergWriter(spark)

            print(f"cron-name    = booking")
            print(f"updated_from = {self.updated_from}")
            print(f"updated_to   = {self.updated_to}")

            df = reader.read(
                settings.INPUT_FILE,
                updated_from=self.updated_from,
                updated_to=self.updated_to,
            )
            df = transformer.transform(
                df,
                mapping_reader.read(settings.STATUS_MAPPING),
                mapping_reader.read(settings.DEVICE_MAPPING),
                mapping_reader.read(settings.REGION_MAPPING),
            )
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
