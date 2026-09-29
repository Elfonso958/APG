"""add seatmap configuration

Revision ID: fa1b2c3d4e5f
Revises: f9a0b1c2d3e4
"""
from alembic import op
import sqlalchemy as sa


revision = "fa1b2c3d4e5f"
down_revision = "f9a0b1c2d3e4"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("app_config") as batch:
        batch.add_column(sa.Column("seatmap_config_json", sa.Text(), nullable=False, server_default="{}"))


def downgrade():
    with op.batch_alter_table("app_config") as batch:
        batch.drop_column("seatmap_config_json")
