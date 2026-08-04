"""
Application entry point.
"""

from __future__ import annotations

from config.settings import settings
from src.core.spark_session import SparkSessionFactory
from src.readers.jsonl_reader import JsonlReader
from src.services.booking.booking_transform_service import (
    BookingTransformService,
)


def main() -> None:
    """Run the booking transformation."""

    spark = SparkSessionFactory.create()

    try:
        reader = JsonlReader(spark)
        transformer = BookingTransformService()

        dataframe = reader.read(
            file_path=str(settings.paths.input_file),
            updated_from="2026-08-01",
            updated_to="2026-08-02",
        )

        dataframe = transformer.transform(dataframe)

        print("\n========== SCHEMA ==========")
        dataframe.printSchema()

        print("\n========== COLUMNS ==========")
        print(dataframe.columns)

        print("\n========== SAMPLE DATA ==========")
        dataframe.show(20, truncate=False)

        print("\n========== ROW COUNT ==========")
        print(dataframe.count())

    finally:
        spark.stop()


if __name__ == "__main__":
    main()