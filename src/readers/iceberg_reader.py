"""
Iceberg reader — current table data and latest snapshot id.
"""

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


class IcebergReader:
    """Reads bookings and snapshot metadata from an Iceberg table."""

    def __init__(self, spark: SparkSession, table_name: str) -> None:
        self._spark = spark
        self._table_name = table_name

    def read_current_bookings(self) -> DataFrame:
        return self._spark.table(self._table_name)

    def get_latest_snapshot_id(self) -> int | None:
        snapshot_rows = self._spark.sql(
            f"""
            SELECT snapshot_id
            FROM {self._table_name}.snapshots
            ORDER BY committed_at DESC
            LIMIT 1
            """
        ).collect()

        if not snapshot_rows:
            return None
        return int(snapshot_rows[0]["snapshot_id"])
