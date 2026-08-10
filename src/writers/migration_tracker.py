"""Track successfully migrated Iceberg snapshots."""

from __future__ import annotations

from pyspark.sql import SparkSession

from config import settings


class MigrationTracker:
    """Manage the Iceberg migration tracking table."""

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark
        self._table = (
            f"{settings.ICEBERG_CATALOG}.migration_tracking"
        )

    def create_table_if_not_exists(self) -> None:
        """Create the migration tracking table if needed."""

        self._spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table} (
                snapshot_id BIGINT,
                processed_at TIMESTAMP
            )
            USING iceberg
            """
        )

    def get_last_snapshot_id(self) -> int | None:
        """Return the most recently processed snapshot ID."""

        result = self._spark.sql(
            f"""
            SELECT snapshot_id
            FROM {self._table}
            ORDER BY processed_at DESC
            LIMIT 1
            """
        ).first()

        if result is None:
            return None

        return int(result["snapshot_id"])

    def save_snapshot(self, snapshot_id: int) -> None:
        """Record a successfully migrated snapshot."""

        self._spark.sql(
            f"""
            INSERT INTO {self._table}
            VALUES (
                {snapshot_id},
                current_timestamp()
            )
            """
        )