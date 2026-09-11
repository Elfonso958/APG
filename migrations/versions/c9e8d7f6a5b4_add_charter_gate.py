"""add charter flight gate

Revision ID: c9e8d7f6a5b4
Revises: b8d7e6f5a4c3
Create Date: 2026-09-11 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "c9e8d7f6a5b4"
down_revision = "b8d7e6f5a4c3"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("charter_manifests", sa.Column("gate", sa.String(length=16), nullable=True))


def downgrade():
    op.drop_column("charter_manifests", "gate")
