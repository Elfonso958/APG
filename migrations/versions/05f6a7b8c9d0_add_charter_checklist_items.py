"""add accommodation and transport checklist items

Revision ID: 05f6a7b8c9d0
Revises: 04e5f6a7b8c9
"""
from alembic import op
import sqlalchemy as sa

revision = "05f6a7b8c9d0"
down_revision = "04e5f6a7b8c9"
branch_labels = None
depends_on = None

def upgrade():
    op.create_table("charter_checklist_items", sa.Column("id", sa.Integer(), primary_key=True), sa.Column("charter_request_id", sa.Integer(), sa.ForeignKey("charter_requests.id"), nullable=False), sa.Column("item_type", sa.String(length=24), nullable=False), sa.Column("service_date", sa.String(length=16), nullable=False), sa.Column("location", sa.String(length=8), nullable=False), sa.Column("provider_name", sa.String(length=255)), sa.Column("contact", sa.String(length=255)), sa.Column("email_addresses", sa.Text()), sa.Column("details", sa.Text()), sa.Column("status", sa.String(length=24), nullable=False, server_default="Not sent"), sa.Column("event_log", sa.Text(), nullable=False, server_default="[]"), sa.Column("evidence_filename", sa.String(length=255)), sa.Column("evidence_data", sa.LargeBinary()), sa.Column("created_at", sa.DateTime(), nullable=False), sa.Column("updated_at", sa.DateTime(), nullable=False))
    op.create_index("ix_charter_checklist_items_charter_request_id", "charter_checklist_items", ["charter_request_id"])

def downgrade():
    op.drop_index("ix_charter_checklist_items_charter_request_id", table_name="charter_checklist_items")
    op.drop_table("charter_checklist_items")
