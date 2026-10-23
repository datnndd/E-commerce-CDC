"""Add product list price and observable customer soft deletion."""

from alembic import op
import sqlalchemy as sa


revision = "c682c10e1f1a"
down_revision = "b5e79439fda1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("customers", sa.Column("is_deleted", sa.Boolean(), server_default=sa.text("false"), nullable=False))
    op.add_column("customers", sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("products", sa.Column("current_price", sa.Numeric(12, 2), nullable=True))
    op.execute("""
        UPDATE products AS p SET current_price = latest.price
        FROM (
            SELECT DISTINCT ON (oi.product_id) oi.product_id, oi.price
            FROM order_items AS oi
            JOIN orders AS o ON o.order_id = oi.order_id
            ORDER BY oi.product_id, o.order_purchase_timestamp DESC, oi.order_id DESC, oi.order_item_id DESC
        ) AS latest
        WHERE p.product_id = latest.product_id
    """)


def downgrade() -> None:
    op.drop_column("products", "current_price")
    op.drop_column("customers", "deleted_at")
    op.drop_column("customers", "is_deleted")
