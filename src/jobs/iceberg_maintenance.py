"""Iceberg table maintenance job."""

import logging

from src.core.spark_session import SparkSessionFactory
from src.writers.iceberg_writer import IcebergWriter

logger = logging.getLogger(__name__)


class IcebergMaintenanceJob:
    """Run maintenance operations on the Iceberg table."""

    def run(self) -> None:
        """Optimize the Iceberg table."""

        logger.info("Starting Iceberg table maintenance.")

        spark = SparkSessionFactory.create()

        try:
            writer = IcebergWriter(spark)
            writer.optimize()

            logger.info("Iceberg table maintenance completed.")

        finally:
            spark.stop()