"""SQLAlchemy ORM models."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from sqlalchemy import Date
from sqlalchemy import Numeric
from sqlalchemy import Text
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

from src.db.database import Base


class Booking(Base):
    """Represent a booking record."""

    __tablename__ = "bookings"

    transaction_id: Mapped[str] = mapped_column(
        Text,
        primary_key=True,
    )
    conversion_key: Mapped[str | None] = mapped_column(Text)
    property_id: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str | None] = mapped_column(Text)
    currency: Mapped[str | None] = mapped_column(Text)
    check_in_date: Mapped[date | None] = mapped_column(Date)
    check_out_date: Mapped[date | None] = mapped_column(Date)
    revenue: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 2),
    )
    travel_purpose: Mapped[str | None] = mapped_column(Text)
    country_code: Mapped[str | None] = mapped_column(Text)
    site_key: Mapped[str | None] = mapped_column(Text)
    referral_property_id: Mapped[str | None] = mapped_column(Text)
    device: Mapped[str | None] = mapped_column(Text)
    region: Mapped[str | None] = mapped_column(Text)
    revenue_usd: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 2),
    )
