"""Create the Olist customers table.

Revision ID: 20261022_0001
Revises:
"""

from alembic import op
import sqlalchemy as sa


revision = "20261022_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "customers",
        sa.Column("customer_id", sa.String(length=32), primary_key=True),
        sa.Column("customer_unique_id", sa.String(length=32), nullable=False),
        sa.Column("customer_zip_code_prefix", sa.String(length=5), nullable=False),
        sa.Column("customer_city", sa.Text(), nullable=False),
        sa.Column("customer_state", sa.String(length=2), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_customers_customer_unique_id", "customers", ["customer_unique_id"]
    )
    op.create_index("ix_customers_updated_at", "customers", ["updated_at"])


def downgrade() -> None:
    op.drop_index("ix_customers_updated_at", table_name="customers")
    op.drop_index("ix_customers_customer_unique_id", table_name="customers")
    op.drop_table("customers")
