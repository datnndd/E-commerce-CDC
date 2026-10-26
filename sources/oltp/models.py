"""Olist source tables and CDC timestamps for the local OLTP database."""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import BigInteger, Boolean, DateTime, Float, ForeignKey, Identity, Index, Integer, Numeric, String, Text, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class CdcWatermark(Base):
    __tablename__ = "cdc_watermark"

    table_name: Mapped[str] = mapped_column(String(63), primary_key=True)
    last_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_boundary_hash: Mapped[str | None] = mapped_column(String(64))


class Customer(Base):
    __tablename__ = "customers"
    __table_args__ = (
        Index("ix_customers_customer_unique_id", "customer_unique_id"),
        Index("ix_customers_zip", "customer_zip_code_prefix"),
        Index("ix_customers_updated_at", "updated_at"),
    )

    customer_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    customer_unique_id: Mapped[str] = mapped_column(String(32), nullable=False)
    customer_zip_code_prefix: Mapped[str] = mapped_column(String(5), nullable=False)
    customer_city: Mapped[str] = mapped_column(Text, nullable=False)
    customer_state: Mapped[str] = mapped_column(String(2), nullable=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    orders: Mapped[list["Order"]] = relationship(back_populates="customer")


class Geolocation(Base):
    __tablename__ = "geolocation"
    __table_args__ = (
        Index("ix_geolocation_zip", "geolocation_zip_code_prefix"),
        Index("ix_geolocation_updated_at", "updated_at"),
    )

    # The CSV has repeated ZIP prefixes and no row key.
    geolocation_id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    geolocation_zip_code_prefix: Mapped[str] = mapped_column(String(5), nullable=False)
    geolocation_lat: Mapped[float] = mapped_column(Float, nullable=False)
    geolocation_lng: Mapped[float] = mapped_column(Float, nullable=False)
    geolocation_city: Mapped[str] = mapped_column(Text, nullable=False)
    geolocation_state: Mapped[str] = mapped_column(String(2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Order(Base):
    __tablename__ = "orders"
    __table_args__ = (
        Index("ix_orders_customer_id", "customer_id"),
        Index("ix_orders_updated_at", "updated_at"),
    )

    order_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    customer_id: Mapped[str] = mapped_column(
        String(32), ForeignKey("customers.customer_id"), nullable=False
    )
    order_status: Mapped[str] = mapped_column(String(64), nullable=False)
    order_purchase_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    order_approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    order_delivered_carrier_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    order_delivered_customer_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    order_estimated_delivery_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    customer: Mapped[Customer] = relationship(back_populates="orders")
    items: Mapped[list["OrderItem"]] = relationship(back_populates="order")
    payments: Mapped[list["OrderPayment"]] = relationship(back_populates="order")
    reviews: Mapped[list["OrderReview"]] = relationship(back_populates="order")


class Product(Base):
    __tablename__ = "products"
    __table_args__ = (
        Index("ix_products_category", "product_category_name"),
        Index("ix_products_updated_at", "updated_at"),
    )

    product_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    product_category_name: Mapped[str | None] = mapped_column(String(100))
    # Olist spells these CSV headers "lenght".
    product_name_lenght: Mapped[int | None] = mapped_column(Integer)
    product_description_lenght: Mapped[int | None] = mapped_column(Integer)
    product_photos_qty: Mapped[int | None] = mapped_column(Integer)
    product_weight_g: Mapped[int | None] = mapped_column(Integer)
    product_length_cm: Mapped[int | None] = mapped_column(Integer)
    product_height_cm: Mapped[int | None] = mapped_column(Integer)
    product_width_cm: Mapped[int | None] = mapped_column(Integer)
    current_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    order_items: Mapped[list["OrderItem"]] = relationship(back_populates="product")


class Seller(Base):
    __tablename__ = "sellers"
    __table_args__ = (
        Index("ix_sellers_zip", "seller_zip_code_prefix"),
        Index("ix_sellers_updated_at", "updated_at"),
    )

    seller_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    seller_zip_code_prefix: Mapped[str] = mapped_column(String(5), nullable=False)
    seller_city: Mapped[str] = mapped_column(Text, nullable=False)
    seller_state: Mapped[str] = mapped_column(String(2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    order_items: Mapped[list["OrderItem"]] = relationship(back_populates="seller")


class OrderItem(Base):
    __tablename__ = "order_items"
    __table_args__ = (
        Index("ix_order_items_product_id", "product_id"),
        Index("ix_order_items_seller_id", "seller_id"),
        Index("ix_order_items_updated_at", "updated_at"),
    )

    order_id: Mapped[str] = mapped_column(
        String(32), ForeignKey("orders.order_id"), primary_key=True
    )
    order_item_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    product_id: Mapped[str] = mapped_column(
        String(32), ForeignKey("products.product_id"), nullable=False
    )
    seller_id: Mapped[str] = mapped_column(
        String(32), ForeignKey("sellers.seller_id"), nullable=False
    )
    shipping_limit_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    freight_value: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    order: Mapped[Order] = relationship(back_populates="items")
    product: Mapped[Product] = relationship(back_populates="order_items")
    seller: Mapped[Seller] = relationship(back_populates="order_items")


class OrderPayment(Base):
    __tablename__ = "order_payments"
    __table_args__ = (Index("ix_order_payments_updated_at", "updated_at"),)

    order_id: Mapped[str] = mapped_column(
        String(32), ForeignKey("orders.order_id"), primary_key=True
    )
    payment_sequential: Mapped[int] = mapped_column(Integer, primary_key=True)
    payment_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payment_installments: Mapped[int] = mapped_column(Integer, nullable=False)
    payment_value: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    order: Mapped[Order] = relationship(back_populates="payments")


class OrderReview(Base):
    __tablename__ = "order_reviews"
    __table_args__ = (
        Index("ix_order_reviews_order_id", "order_id"),
        Index("ix_order_reviews_review_id", "review_id"),
        Index("ix_order_reviews_updated_at", "updated_at"),
    )

    # review_id alone is not a reliable row key in the source data.
    review_row_id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    review_id: Mapped[str] = mapped_column(String(32), nullable=False)
    order_id: Mapped[str] = mapped_column(
        String(32), ForeignKey("orders.order_id"), nullable=False
    )
    review_score: Mapped[int] = mapped_column(Integer, nullable=False)
    review_comment_title: Mapped[str | None] = mapped_column(Text)
    review_comment_message: Mapped[str | None] = mapped_column(Text)
    review_creation_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    review_answer_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    order: Mapped[Order] = relationship(back_populates="reviews")


class CategoryNameTranslation(Base):
    __tablename__ = "product_category_name_translation"
    __table_args__ = (Index("ix_category_translation_updated_at", "updated_at"),)

    product_category_name: Mapped[str] = mapped_column(String(100), primary_key=True)
    product_category_name_english: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
