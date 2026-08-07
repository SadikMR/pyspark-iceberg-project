"""
Iceberg table writer.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from config import settings

logger = logging.getLogger(__name__)


class IcebergWriter:
    """Writes booking data to an Iceberg table."""

    _TEMP_VIEW = "bookings_updates"

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark
        self._table = settings.ICEBERG_FULL_TABLE_NAME
        self._catalog = settings.ICEBERG_CATALOG

    def write(self, dataframe: DataFrame) -> None:
        """
        Write data into the Iceberg table using MERGE INTO.
        """
        self._create_table_if_not_exists(dataframe)
        self._merge(dataframe)

    def optimize(self) -> None:
        """
        Optimize the Iceberg table.
        """
        logger.info("Rewriting data files.")

        self._spark.sql(
            f"""
            CALL {self._catalog}.system.rewrite_data_files(
                table => '{self._table}'
            )
            """
        )

        logger.info("Expiring old snapshots.")

        self._spark.sql(
            f"""
            CALL {self._catalog}.system.expire_snapshots(
                table => '{self._table}',
                retain_last => {settings.ICEBERG_SNAPSHOT_RETAIN_LAST}
            )
            """
        )

        logger.info("Removing orphan files.")

        self._spark.sql(
            f"""
            CALL {self._catalog}.system.remove_orphan_files(
                table => '{self._table}'
            )
            """
        )

    def _create_table_if_not_exists(
        self,
        dataframe: DataFrame,
    ) -> None:
        """Create the Iceberg table if it does not exist."""

        if self._spark.catalog.tableExists(self._table):
            return

        logger.info("Creating Iceberg table: %s", self._table)

        (
            dataframe.limit(0)
            .writeTo(self._table)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
            .create()
        )

    def _merge(
        self,
        dataframe: DataFrame,
    ) -> None:
        """Merge booking data into the Iceberg table."""

        logger.info("Merging data into Iceberg table.")

        dataframe.createOrReplaceTempView(self._TEMP_VIEW)

        self._spark.sql(
            f"""
            MERGE INTO {self._table} AS target
            USING {self._TEMP_VIEW} AS source
            ON target.transaction_id = source.transaction_id

            WHEN MATCHED THEN
                UPDATE SET *

            WHEN NOT MATCHED THEN
                INSERT *
            """
        )