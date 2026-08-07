from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import sessionmaker

from config import settings


class Base(DeclarativeBase):
    """Base class for ORM models."""


engine = create_engine(
    settings.POSTGRES_URL,
    echo=False,
)

SessionFactory = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
)