"""Track migrated Iceberg snapshots in PostgreSQL."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from src.db.models import MigrationTracking


class MigrationTracker:
    """Manage the migration snapshot watermark."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def get_last_snapshot_id(self) -> int | None:
        """Return the most recently processed snapshot ID."""

        with self._session_factory() as session:
            return session.scalar(
                select(MigrationTracking.snapshot_id)
                .order_by(MigrationTracking.processed_at.desc())
                .limit(1)
            )

    def save_snapshot(self, snapshot_id: int) -> None:
        """Record a successfully migrated snapshot."""

        statement = insert(MigrationTracking).values(
            snapshot_id=snapshot_id,
        )

        with self._session_factory.begin() as session:
            session.execute(statement)