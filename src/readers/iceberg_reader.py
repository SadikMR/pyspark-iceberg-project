"""
Iceberg table reader.
"""

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


class IcebergReader:
    """Reads data and metadata from an Iceberg table."""

    def __init__(
        self,
        spark: SparkSession,
        table_name: str,
    ) -> None:
        self._spark = spark
        self._table_name = table_name

    def read_table(self) -> DataFrame:
        """
        Read the latest state of the Iceberg table.
        """
        return self._spark.table(self._table_name)

    def get_latest_snapshot_id(self) -> int | None:
        """
        Return the latest Iceberg snapshot ID.

        Returns:
            Latest snapshot ID or None if the table has no snapshots.
        """
        snapshot_df = self._spark.sql(
            f"""
            SELECT snapshot_id
            FROM {self._table_name}.snapshots
            ORDER BY committed_at DESC
            LIMIT 1
            """
        )

        row = snapshot_df.first()

        if row is None:
            return None

        return int(row.snapshot_id)

    def read_changes(
        self,
        start_snapshot_id: int,
        end_snapshot_id: int,
    ) -> DataFrame:
        """
        Read rows changed between two snapshots.

        Returns:
            Spark DataFrame containing incremental changes.
        """
        return (
            self._spark.read.format("iceberg")
            .option("start-snapshot-id", start_snapshot_id)
            .option("end-snapshot-id", end_snapshot_id)
            .load(self._table_name)
        )