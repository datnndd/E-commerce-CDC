"""Remember the newest timestamp group for idempotent Bronze polling."""

from alembic import op
import sqlalchemy as sa


revision = "6c4d9b7e2a11"
down_revision = "d79b3fd3ea12"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cdc_watermark", sa.Column("last_boundary_hash", sa.String(length=64)))


def downgrade() -> None:
    op.drop_column("cdc_watermark", "last_boundary_hash")
