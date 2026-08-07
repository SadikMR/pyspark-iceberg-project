"""
Migration service.
"""

import logging

from sqlalchemy.orm import Session

from src.readers.iceberg_reader import IcebergReader
from src.readers.tracking_reader import TrackingReader
from src.writers.postgres_writer import PostgresWriter
from src.writers.tracking_writer import TrackingWriter

logger = logging.getLogger(__name__)


class MigrationService:
    """Migrates data from Iceberg to PostgreSQL."""

    def __init__(
        self,
        session: Session,
        iceberg_reader: IcebergReader,
        tracking_reader: TrackingReader,
        postgres_writer: PostgresWriter,
        tracking_writer: TrackingWriter,
        table_name: str,
    ) -> None:
        self._session = session
        self._iceberg_reader = iceberg_reader
        self._tracking_reader = tracking_reader
        self._postgres_writer = postgres_writer
        self._tracking_writer = tracking_writer
        self._table_name = table_name

    def run(self) -> None:
        """Execute the migration."""

        try:
            last_snapshot = self._tracking_reader.get_last_snapshot_id(
                self._table_name,
            )

            latest_snapshot = (
                self._iceberg_reader.get_latest_snapshot_id()
            )

            if latest_snapshot is None:
                logger.info("No Iceberg snapshots found.")
                return

            if last_snapshot is None:
                self._run_initial_load(latest_snapshot)
            elif last_snapshot != latest_snapshot:
                self._run_incremental_load(
                    last_snapshot,
                    latest_snapshot,
                )
            else:
                logger.info("No new snapshot available.")

            self._session.commit()

        except Exception:
            self._session.rollback()
            logger.exception("Migration failed.")
            raise

    def _run_initial_load(
        self,
        latest_snapshot: int,
    ) -> None:
        """Run the initial migration."""

        logger.info("Starting initial load.")

        dataframe = self._iceberg_reader.read_table()

        self._postgres_writer.upsert(dataframe)

        self._tracking_writer.save_snapshot(
            self._table_name,
            latest_snapshot,
        )

        logger.info(
            "Initial migration completed successfully."
        )

    def _run_incremental_load(
        self,
        start_snapshot: int,
        end_snapshot: int,
    ) -> None:
        """Run an incremental migration."""

        logger.info(
            "Migrating snapshots %s -> %s",
            start_snapshot,
            end_snapshot,
        )

        dataframe = self._iceberg_reader.read_changes(
            start_snapshot,
            end_snapshot,
        )

        self._postgres_writer.upsert(dataframe)

        self._tracking_writer.save_snapshot(
            self._table_name,
            end_snapshot,
        )

        logger.info(
            "Incremental migration completed successfully."
        )