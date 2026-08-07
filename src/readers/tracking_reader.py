"""
Read migration tracking information from PostgreSQL.
"""

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models.migration_tracking import MigrationTracking


class TrackingReader:
    """Reads migration tracking information."""

    def __init__(
        self,
        session: Session,
    ) -> None:
        self._session = session

    def get_tracking(
        self,
        table_name: str,
    ) -> MigrationTracking | None:
        """Return the tracking record for the given table."""

        statement = (
            select(MigrationTracking)
            .where(MigrationTracking.table_name == table_name)
        )

        return self._session.scalar(statement)

    def get_last_snapshot_id(
        self,
        table_name: str,
    ) -> int | None:
        """Return the last migrated snapshot ID."""

        tracking = self.get_tracking(table_name)

        if tracking is None:
            return None

        return tracking.last_snapshot_id