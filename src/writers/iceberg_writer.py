"""
Iceberg writer — MERGE INTO + table optimize.
"""

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from config import settings


class IcebergWriter:
    """Write bookings to Iceberg with MERGE INTO, then optimize the table."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.table = settings.ICEBERG_FULL_TABLE_NAME
        self.table_id = settings.ICEBERG_TABLE

    def write(self, df: DataFrame) -> None:
        if not self.spark.catalog.tableExists(self.table):
            (
                df.limit(0)
                .writeTo(self.table)
                .using("iceberg")
                .tableProperty("format-version", "2")
                .tableProperty("write.format.default", "parquet")
                .create()
            )

        df.createOrReplaceTempView("bookings_updates")
        self.spark.sql(
            f"""
            MERGE INTO {self.table} t
            USING bookings_updates s
            ON t.transaction_id = s.transaction_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )

        # Compact data files, then drop old snapshots.
        self.spark.sql(
            f"CALL {settings.ICEBERG_CATALOG}.system.rewrite_data_files("
            f"table => '{self.table_id}')"
        )
        self.spark.sql(
            f"CALL {settings.ICEBERG_CATALOG}.system.expire_snapshots("
            f"table => '{self.table_id}', "
            f"retain_last => {settings.ICEBERG_SNAPSHOT_RETAIN_LAST})"
        )
