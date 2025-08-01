from sqlalchemy import Column, DateTime, Integer
from sqlalchemy.sql import func
from src.models.base import Base


class StagingBase(Base):
    """Base class for all staging tables."""
    __abstract__ = True


class TimestampMixin:
    """Mixin that adds timestamp fields to models."""
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())