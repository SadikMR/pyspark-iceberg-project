"""
PostgreSQL writer — create tables, track snapshots, upsert bookings.

No ORM models. Rows are inserted with plain SQL (ON CONFLICT upsert).
"""

from __future__ import annotations

import logging
from datetime import date
from datetime import datetime
from decimal import Decimal
from typing import Any
from urllib.parse import urlparse
from urllib.parse import urlunparse

import psycopg
from psycopg.types.json import Jsonb
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

UPSERT_BATCH_SIZE = 500

CREATE_BOOKINGS_TABLE = """
CREATE TABLE IF NOT EXISTS bookings (
    transaction_id       TEXT PRIMARY KEY,
    conversion_key       TEXT,
    property_id          TEXT,
    status               TEXT,
    currency             TEXT,
    check_in_date        DATE,
    check_out_date       DATE,
    revenue              NUMERIC(18, 2),
    travel_purpose       TEXT,
    country_code         TEXT,
    site_key             TEXT,
    referral_property_id TEXT,
    device               TEXT,
    region               TEXT,
    revenue_usd          NUMERIC(18, 2)
)
"""

CREATE_TRACKING_TABLE = """
CREATE TABLE IF NOT EXISTS migration_tracking (
    pipeline_name      TEXT PRIMARY KEY,
    last_snapshot_id   BIGINT,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def normalize_connection_url(url: str) -> str:
    """Convert postgresql+psycopg://... to a plain psycopg DSN if needed."""

    if url.startswith("postgresql+"):
        parsed = urlparse(url)
        return urlunparse(
            ("postgresql", parsed.netloc, parsed.path, "", parsed.query, "")
        )
    return url


def to_postgres_value(value: Any) -> Any:
    """Convert a Spark cell value into something psycopg can bind."""

    if value is None:
        return None
    if isinstance(value, (str, int, float, bool, Decimal, date, datetime)):
        return value
    if isinstance(value, dict):
        return Jsonb(value)
    return str(value)


class PostgresWriter:
    """Writes booking data and migration watermarks to PostgreSQL."""

    def __init__(self, database_url: str) -> None:
        self._database_url = normalize_connection_url(database_url)
        self._connection: psycopg.Connection | None = None

    def __enter__(self) -> "PostgresWriter":
        self._connection = psycopg.connect(self._database_url)
        return self

    def __exit__(self, *args: object) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    @property
    def connection(self) -> psycopg.Connection:
        if self._connection is None:
            raise RuntimeError("PostgresWriter is not connected")
        return self._connection

    def create_tables_if_not_exist(self) -> None:
        self.connection.execute(CREATE_BOOKINGS_TABLE)
        self.connection.execute(CREATE_TRACKING_TABLE)
        self.connection.commit()

    def get_last_snapshot_id(self, pipeline_name: str) -> int | None:
        """Return the last Iceberg snapshot_id stored in migration_tracking."""

        result = self.connection.execute(
            """
            SELECT last_snapshot_id
            FROM migration_tracking
            WHERE pipeline_name = %s
            """,
            (pipeline_name,),
        ).fetchone()

        if result is None or result[0] is None:
            return None
        return int(result[0])

    def save_last_snapshot_id(self, pipeline_name: str, snapshot_id: int) -> None:
        """Upsert the watermark after a successful migration."""

        self.connection.execute(
            """
            INSERT INTO migration_tracking (pipeline_name, last_snapshot_id, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (pipeline_name) DO UPDATE
            SET last_snapshot_id = EXCLUDED.last_snapshot_id,
                updated_at = now()
            """,
            (pipeline_name, snapshot_id),
        )

    def upsert_bookings(self, bookings: DataFrame) -> int:
        """Insert or update bookings by transaction_id. Returns rows processed."""

        column_names = list(bookings.columns)
        if "transaction_id" not in column_names:
            raise ValueError("Bookings DataFrame must include transaction_id")

        columns_sql = ", ".join(column_names)
        placeholders_sql = ", ".join(["%s"] * len(column_names))
        update_sql = ", ".join(
            f"{name} = EXCLUDED.{name}"
            for name in column_names
            if name != "transaction_id"
        )
        upsert_sql = f"""
            INSERT INTO bookings ({columns_sql})
            VALUES ({placeholders_sql})
            ON CONFLICT (transaction_id) DO UPDATE SET {update_sql}
        """

        rows_processed = 0
        batch: list[tuple[Any, ...]] = []

        with self.connection.cursor() as cursor:
            for spark_row in bookings.toLocalIterator():
                batch.append(
                    tuple(to_postgres_value(spark_row[name]) for name in column_names)
                )
                rows_processed += 1

                if len(batch) >= UPSERT_BATCH_SIZE:
                    cursor.executemany(upsert_sql, batch)
                    batch.clear()

            if batch:
                cursor.executemany(upsert_sql, batch)

        logger.info("Upserted %s booking rows into PostgreSQL", rows_processed)
        return rows_processed

    def commit(self) -> None:
        self.connection.commit()
