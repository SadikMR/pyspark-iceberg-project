"""
Migrate Iceberg bookings → PostgreSQL using a snapshot tracking table.
"""

import logging

from config import settings
from src.core.spark_session import SparkSessionFactory
from src.readers.iceberg_reader import IcebergReader
from src.writers.postgres_writer import PostgresWriter

logger = logging.getLogger(__name__)


class MigratePostgresJob:
    """Compare Iceberg snapshot to tracking table, then upsert if needed."""

    def __init__(self) -> None:
        self._iceberg_table_name = settings.ICEBERG_FULL_TABLE_NAME
        self._pipeline_name = settings.MIGRATE_PIPELINE_NAME

    def run(self) -> None:
        logger.info(
            "Starting migrate_postgres | iceberg_table=%s",
            self._iceberg_table_name,
        )

        spark = SparkSessionFactory.create()
        try:
            iceberg_reader = IcebergReader(spark, self._iceberg_table_name)

            with PostgresWriter(settings.POSTGRES_URL) as postgres_writer:
                postgres_writer.create_tables_if_not_exist()

                iceberg_snapshot_id = iceberg_reader.get_latest_snapshot_id()
                if iceberg_snapshot_id is None:
                    logger.info("No Iceberg snapshots — nothing to migrate")
                    return

                tracked_snapshot_id = postgres_writer.get_last_snapshot_id(
                    self._pipeline_name,
                )
                logger.info(
                    "Snapshots | tracked=%s iceberg=%s",
                    tracked_snapshot_id,
                    iceberg_snapshot_id,
                )

                if tracked_snapshot_id == iceberg_snapshot_id:
                    logger.info("Already up to date — skip")
                    return

                bookings = iceberg_reader.read_current_bookings()
                upserted_row_count = postgres_writer.upsert_bookings(bookings)
                postgres_writer.save_last_snapshot_id(
                    self._pipeline_name,
                    iceberg_snapshot_id,
                )
                postgres_writer.commit()

                logger.info(
                    "Migration done | upserted_rows=%s snapshot_id=%s",
                    upserted_row_count,
                    iceberg_snapshot_id,
                )
        finally:
            spark.stop()
