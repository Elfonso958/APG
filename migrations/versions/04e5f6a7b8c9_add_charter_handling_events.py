"""add charter handling activity events

Revision ID: 04e5f6a7b8c9
Revises: 03d4e5f6a7b8
"""
from alembic import op
import sqlalchemy as sa

revision = "04e5f6a7b8c9"
down_revision = "03d4e5f6a7b8"
branch_labels = None
depends_on = None

def upgrade():
    op.create_table("charter_handling_events", sa.Column("id", sa.Integer(), primary_key=True), sa.Column("handling_request_id", sa.Integer(), sa.ForeignKey("charter_handling_requests.id"), nullable=False), sa.Column("event_type", sa.String(length=48), nullable=False), sa.Column("detail", sa.Text(), nullable=True), sa.Column("actor", sa.String(length=255), nullable=True), sa.Column("created_at", sa.DateTime(), nullable=False))
    op.create_index("ix_charter_handling_events_handling_request_id", "charter_handling_events", ["handling_request_id"])
    op.create_index("ix_charter_handling_events_created_at", "charter_handling_events", ["created_at"])

def downgrade():
    op.drop_index("ix_charter_handling_events_created_at", table_name="charter_handling_events")
    op.drop_index("ix_charter_handling_events_handling_request_id", table_name="charter_handling_events")
    op.drop_table("charter_handling_events")
