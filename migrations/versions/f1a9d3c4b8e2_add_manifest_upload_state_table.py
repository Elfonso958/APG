"""add manifest upload state table

Revision ID: f1a9d3c4b8e2
Revises: c2f1b6a0f2a1
Create Date: 2026-03-12 11:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f1a9d3c4b8e2"
down_revision = "c2f1b6a0f2a1"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "manifest_upload_state",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("apg_plan_id", sa.Integer(), nullable=False),
        sa.Column("upload_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_doc_id", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index(
        "ix_manifest_upload_state_apg_plan_id",
        "manifest_upload_state",
        ["apg_plan_id"],
        unique=True,
    )


def downgrade():
    op.drop_index("ix_manifest_upload_state_apg_plan_id", table_name="manifest_upload_state")
    op.drop_table("manifest_upload_state")
