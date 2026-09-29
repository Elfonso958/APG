"""add charter planner registration settings

Revision ID: 00a1b2c3d4e5
Revises: ff6a7b8c9d0e
"""
from alembic import op
import sqlalchemy as sa

revision = "00a1b2c3d4e5"
down_revision = "ff6a7b8c9d0e"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("app_config", sa.Column("charter_planner_registrations_json", sa.Text(), nullable=False, server_default="[]"))


def downgrade():
    op.drop_column("app_config", "charter_planner_registrations_json")
