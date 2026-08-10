"""Migrate Iceberg bookings to PostgreSQL using snapshot tracking."""

from __future__ import annotations

import logging

from config import settings
from src.core.spark_session import SparkSessionFactory
from src.db import models  # noqa: F401
from src.db.database import Base
from src.db.database import create_database_engine
from src.db.database import create_session_factory
from src.readers.iceberg_reader import IcebergReader
from src.writers.migration_tracker import MigrationTracker
from src.writers.postgres_writer import PostgresWriter


logger = logging.getLogger(__name__)


class MigratePostgresJob:
    """Migrate new Iceberg snapshots to PostgreSQL."""

    def __init__(self) -> None:
        self._iceberg_table_name = settings.ICEBERG_FULL_TABLE_NAME

    def run(self) -> None:
        """Run the Iceberg to PostgreSQL migration."""

        logger.info(
            "Starting migrate_postgres | iceberg_table=%s",
            self._iceberg_table_name,
        )

        spark = SparkSessionFactory.create()
        engine = create_database_engine(settings.POSTGRES_URL)

        try:
            session_factory = create_session_factory(engine)

            # Importing the models registers them with Base.metadata.
            Base.metadata.create_all(engine)

            iceberg_reader = IcebergReader(
                spark,
                self._iceberg_table_name,
            )
            postgres_writer = PostgresWriter(session_factory)
            migration_tracker = MigrationTracker(session_factory)

            iceberg_snapshot_id = (
                iceberg_reader.get_latest_snapshot_id()
            )

            if iceberg_snapshot_id is None:
                logger.info(
                    "No Iceberg snapshots — nothing to migrate",
                )
                return

            tracked_snapshot_id = (
                migration_tracker.get_last_snapshot_id()
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

            upserted_row_count = postgres_writer.upsert_bookings(
                bookings,
            )

            migration_tracker.save_snapshot(
                iceberg_snapshot_id,
            )

            logger.info(
                "Migration done | upserted_rows=%s snapshot_id=%s",
                upserted_row_count,
                iceberg_snapshot_id,
            )

        finally:
            engine.dispose()
            spark.stop()