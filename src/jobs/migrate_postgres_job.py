"""
Entry point for the PostgreSQL migration job.
"""

import logging

from src.core.spark_session import SparkSessionFactory
from src.db.database import SessionFactory
from src.db.database import engine
from src.db.initializer import initialize_database
from src.readers.iceberg_reader import IcebergReader
from src.readers.tracking_reader import TrackingReader
from src.services.migration_service import MigrationService
from src.writers.postgres_writer import PostgresWriter
from src.writers.tracking_writer import TrackingWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s",
)

logger = logging.getLogger(__name__)


class MigratePostgresJob:
    """Run the Iceberg-to-PostgreSQL migration."""

    def __init__(
        self,
        table_name: str = "local.bookings",
    ) -> None:
        self._table_name = table_name

    def run(self) -> None:
        """Execute the migration job."""

        logger.info(
            "Starting migration for %s",
            self._table_name,
        )

        spark = SparkSessionFactory.create()

        try:
            initialize_database(engine)

            with SessionFactory() as session:

                iceberg_reader = IcebergReader(
                    spark=spark,
                    table_name=self._table_name,
                )

                tracking_reader = TrackingReader(session)

                postgres_writer = PostgresWriter(session)

                tracking_writer = TrackingWriter(session)

                migration_service = MigrationService(
                    session=session,
                    iceberg_reader=iceberg_reader,
                    tracking_reader=tracking_reader,
                    postgres_writer=postgres_writer,
                    tracking_writer=tracking_writer,
                    table_name=self._table_name,
                )

                migration_service.run()

        finally:
            spark.stop()