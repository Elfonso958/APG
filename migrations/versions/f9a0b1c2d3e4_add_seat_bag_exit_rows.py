"""add seat-bag emergency exit row settings

Revision ID: f9a0b1c2d3e4
Revises: f8a9b0c1d2e3
"""
from alembic import op
import sqlalchemy as sa


revision = "f9a0b1c2d3e4"
down_revision = "f8a9b0c1d2e3"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("app_config") as batch:
        batch.add_column(sa.Column("seat_bag_exit_rows_json", sa.Text(), nullable=False, server_default="{}"))


def downgrade():
    with op.batch_alter_table("app_config") as batch:
        batch.drop_column("seat_bag_exit_rows_json")
