"""add configurable APG creation lead time

Revision ID: e3a7c9d1f204
Revises: b4c2d8e9f1a0
"""
from alembic import op
import sqlalchemy as sa


revision = "e3a7c9d1f204"
down_revision = "b4c2d8e9f1a0"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("app_config", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("apg_create_ahead_hours", sa.Integer(), nullable=False, server_default="48")
        )


def downgrade():
    with op.batch_alter_table("app_config", schema=None) as batch_op:
        batch_op.drop_column("apg_create_ahead_hours")
