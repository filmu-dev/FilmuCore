"""SQLAlchemy declarative metadata for filmu-python persistence."""

from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Declarative base used by ORM models and Alembic metadata."""
