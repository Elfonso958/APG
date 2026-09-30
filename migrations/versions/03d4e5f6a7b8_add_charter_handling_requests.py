"""add charter handling requests

Revision ID: 03d4e5f6a7b8
Revises: 02c3d4e5f6a7
"""
from alembic import op
import sqlalchemy as sa


revision = "03d4e5f6a7b8"
down_revision = "02c3d4e5f6a7"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "charter_handling_requests",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("charter_request_id", sa.Integer(), sa.ForeignKey("charter_requests.id"), nullable=False),
        sa.Column("airport", sa.String(length=8), nullable=False),
        sa.Column("provider_id", sa.Integer(), sa.ForeignKey("airport_handling_providers.id"), nullable=False),
        sa.Column("recipient_emails", sa.Text(), nullable=False, server_default=""),
        sa.Column("subject", sa.String(length=500), nullable=False, server_default=""),
        sa.Column("body", sa.Text(), nullable=False, server_default=""),
        sa.Column("status", sa.String(length=24), nullable=False, server_default="Not sent"),
        sa.Column("sent_at", sa.DateTime(), nullable=True),
        sa.Column("sent_by", sa.String(length=255), nullable=True),
        sa.Column("reply_log", sa.Text(), nullable=False, server_default="[]"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_charter_handling_requests_charter_request_id", "charter_handling_requests", ["charter_request_id"])
    op.create_index("ix_charter_handling_requests_airport", "charter_handling_requests", ["airport"])


def downgrade():
    op.drop_index("ix_charter_handling_requests_airport", table_name="charter_handling_requests")
    op.drop_index("ix_charter_handling_requests_charter_request_id", table_name="charter_handling_requests")
    op.drop_table("charter_handling_requests")
