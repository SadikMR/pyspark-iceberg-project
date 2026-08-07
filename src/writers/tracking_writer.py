"""
Write migration tracking information.
"""

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models.migration_tracking import MigrationTracking


class TrackingWriter:
    """Writes migration tracking information."""

    def __init__(
        self,
        session: Session,
    ) -> None:
        self._session = session

    def save_snapshot(
        self,
        table_name: str,
        snapshot_id: int,
    ) -> None:
        """
        Create or update the tracking record for a table.
        """

        statement = (
            select(MigrationTracking)
            .where(MigrationTracking.table_name == table_name)
        )

        tracking = self._session.scalar(statement)

        if tracking is None:
            tracking = MigrationTracking(
                table_name=table_name,
                last_snapshot_id=snapshot_id,
            )
            self._session.add(tracking)
            return

        tracking.last_snapshot_id = snapshot_id