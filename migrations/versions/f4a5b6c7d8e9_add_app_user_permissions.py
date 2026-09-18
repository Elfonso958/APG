"""add granular APG user permissions

Revision ID: f4a5b6c7d8e9
Revises: a7b8c9d0e1f2
"""
from alembic import op
import sqlalchemy as sa


revision = "f4a5b6c7d8e9"
down_revision = "a7b8c9d0e1f2"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("app_users", sa.Column("permissions_json", sa.Text(), nullable=False, server_default="[]"))


def downgrade():
    op.drop_column("app_users", "permissions_json")
