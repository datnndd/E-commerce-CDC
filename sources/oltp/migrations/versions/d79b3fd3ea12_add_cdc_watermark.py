"""Add an empty per-table CDC watermark control table."""

from alembic import op
import sqlalchemy as sa


revision = "d79b3fd3ea12"
down_revision = "b5e79439fda1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cdc_watermark",
        sa.Column("table_name", sa.String(length=63), primary_key=True),
        sa.Column("last_updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("cdc_watermark")
