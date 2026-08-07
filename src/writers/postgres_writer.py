"""
Write booking records into PostgreSQL.
"""

import logging

from pyspark.sql import DataFrame
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from src.models.booking import Booking

logger = logging.getLogger(__name__)


class PostgresWriter:
    """Writes booking records into PostgreSQL."""

    _BATCH_SIZE = 500

    def __init__(
        self,
        session: Session,
    ) -> None:
        self._session = session

    def upsert(
        self,
        dataframe: DataFrame,
    ) -> None:
        """
        Upsert booking records into PostgreSQL.
        """

        batch: list[dict] = []
        row_count = 0

        for row in dataframe.toLocalIterator():
            batch.append(row.asDict(recursive=True))
            row_count += 1

            if len(batch) >= self._BATCH_SIZE:
                self._execute_batch(batch)
                batch.clear()

        if batch:
            self._execute_batch(batch)

        logger.info("Upserted %s rows into PostgreSQL", row_count)

    def _execute_batch(
        self,
        records: list[dict],
    ) -> None:
        """Execute a bulk UPSERT."""

        if not records:
            return

        statement = insert(Booking).values(records)

        update_columns = {
            column.name: statement.excluded[column.name]
            for column in Booking.__table__.columns
            if column.name != "transaction_id"
        }

        statement = statement.on_conflict_do_update(
            index_elements=["transaction_id"],
            set_=update_columns,
        )

        self._session.execute(statement)
        self._session.flush()