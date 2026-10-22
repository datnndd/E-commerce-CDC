"""OLTP table definitions. Add one mapped class per source table."""

from datetime import datetime

from sqlalchemy import DateTime, Index, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Customer(Base):
    __tablename__ = "customers"
    __table_args__ = (
        Index("ix_customers_customer_unique_id", "customer_unique_id"),
        Index("ix_customers_updated_at", "updated_at"),
    )

    customer_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    customer_unique_id: Mapped[str] = mapped_column(String(32), nullable=False)
    customer_zip_code_prefix: Mapped[str] = mapped_column(String(5), nullable=False)
    customer_city: Mapped[str] = mapped_column(Text, nullable=False)
    customer_state: Mapped[str] = mapped_column(String(2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
