"""add multiple charter checklist evidence files

Revision ID: 06a7b8c9d0e1
Revises: 05f6a7b8c9d0
"""
from alembic import op
import sqlalchemy as sa


revision = "06a7b8c9d0e1"
down_revision = "05f6a7b8c9d0"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "charter_checklist_evidence",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("checklist_item_id", sa.Integer(), sa.ForeignKey("charter_checklist_items.id"), nullable=False),
        sa.Column("filename", sa.String(length=255), nullable=False),
        sa.Column("data", sa.LargeBinary(), nullable=False),
        sa.Column("uploaded_by", sa.String(length=255)),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_charter_checklist_evidence_checklist_item_id", "charter_checklist_evidence", ["checklist_item_id"])


def downgrade():
    op.drop_index("ix_charter_checklist_evidence_checklist_item_id", table_name="charter_checklist_evidence")
    op.drop_table("charter_checklist_evidence")
