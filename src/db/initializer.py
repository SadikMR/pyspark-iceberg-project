"""
Initialize PostgreSQL tables using SQLAlchemy metadata.
"""

from sqlalchemy import Engine

import src.models
from src.db.database import Base


def initialize_database(engine: Engine) -> None:
    """Create ORM tables if they do not exist."""
    Base.metadata.create_all(bind=engine)