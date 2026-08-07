"""
ORM model for migrated booking records.
"""

from datetime import date
from decimal import Decimal

from sqlalchemy import Date
from sqlalchemy import Numeric
from sqlalchemy import String
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

from src.db.database import Base


class Booking(Base):
    """Represents a booking record in PostgreSQL."""

    __tablename__ = "bookings"

    transaction_id: Mapped[str] = mapped_column(
        String(255),
        primary_key=True,
    )

    conversion_key: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )

    property_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )

    status: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
    )

    currency: Mapped[str | None] = mapped_column(
        String(10),
        nullable=True,
    )

    check_in_date: Mapped[date | None] = mapped_column(
        Date,
        nullable=True,
    )

    check_out_date: Mapped[date | None] = mapped_column(
        Date,
        nullable=True,
    )

    revenue: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 2),
        nullable=True,
    )

    travel_purpose: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
    )

    country_code: Mapped[str | None] = mapped_column(
        String(10),
        nullable=True,
    )

    site_key: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
    )

    referral_property_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )

    device: Mapped[str | None] = mapped_column(
        String(50),
        nullable=True,
    )

    region: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
    )

    revenue_usd: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 2),
        nullable=True,
    )