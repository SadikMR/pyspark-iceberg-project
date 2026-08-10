"""SQLAlchemy database configuration."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker


class Base(DeclarativeBase):
    """Base class for SQLAlchemy ORM models."""


def create_database_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy database engine."""
    return create_engine(database_url)


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Create a SQLAlchemy session factory."""
    return sessionmaker(bind=engine)