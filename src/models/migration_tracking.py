"""
ORM model for migration tracking.
"""

from datetime import datetime

from sqlalchemy import BigInteger
from sqlalchemy import DateTime
from sqlalchemy import String
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.sql import func

from src.db.database import Base


class MigrationTracking(Base):
    """Tracks the latest Iceberg snapshot applied to PostgreSQL."""

    __tablename__ = "migration_tracking"

    table_name: Mapped[str] = mapped_column(
        String(100),
        primary_key=True,
    )

    last_snapshot_id: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )