"""Write booking data to PostgreSQL."""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from src.db.models import Booking

logger = logging.getLogger(__name__)

UPSERT_BATCH_SIZE = 500


class PostgresWriter:
    """Write booking data to PostgreSQL."""

    def __init__(self,session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def upsert_bookings(self, bookings: DataFrame) -> int:
        """Insert or update bookings by transaction ID."""

        if "transaction_id" not in bookings.columns:
            raise ValueError(
                "Bookings DataFrame must include transaction_id",
            )

        rows_processed = 0
        batch: list[dict[str, Any]] = []

        with self._session_factory.begin() as session:
            for row in bookings.toLocalIterator():
                batch.append(row.asDict())
                rows_processed += 1

                if len(batch) >= UPSERT_BATCH_SIZE:
                    self._upsert_batch(session, batch)
                    batch.clear()

            if batch:
                self._upsert_batch(session, batch)

        logger.info(
            "Upserted %s booking rows into PostgreSQL",
            rows_processed,
        )

        return rows_processed

    @staticmethod
    def _upsert_batch(session: Session, rows: list[dict[str, Any]]) -> None:
        """Upsert one batch of bookings."""

        statement = insert(Booking).values(rows)

        update_values = {
            column.name: statement.excluded[column.name]
            for column in Booking.__table__.columns
            if column.name != "transaction_id"
        }

        statement = statement.on_conflict_do_update(
            index_elements=[Booking.transaction_id],
            set_=update_values,
        )

        session.execute(statement)