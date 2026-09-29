"""add charter tail debug setting

Revision ID: 02c3d4e5f6a7
Revises: 01b2c3d4e5f6
"""
from alembic import op
import sqlalchemy as sa


revision = "02c3d4e5f6a7"
down_revision = "01b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("app_config", sa.Column("charter_tail_debug_enabled", sa.Boolean(), nullable=False, server_default=sa.false()))


def downgrade():
    op.drop_column("app_config", "charter_tail_debug_enabled")
