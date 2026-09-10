"""add charter flight closure state

Revision ID: b8d7e6f5a4c3
Revises: e8b7c2d4f901
Create Date: 2026-09-11 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "b8d7e6f5a4c3"
down_revision = "e8b7c2d4f901"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("charter_manifests", sa.Column("closed_at", sa.DateTime(), nullable=True))
    op.add_column("charter_manifests", sa.Column("closure_email_sent_at", sa.DateTime(), nullable=True))


def downgrade():
    op.drop_column("charter_manifests", "closure_email_sent_at")
    op.drop_column("charter_manifests", "closed_at")
