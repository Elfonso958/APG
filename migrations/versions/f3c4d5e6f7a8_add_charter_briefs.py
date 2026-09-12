"""add charter briefs

Revision ID: f3c4d5e6f7a8
Revises: f2b3c4d5e6f7
"""
from alembic import op
import sqlalchemy as sa


revision = "f3c4d5e6f7a8"
down_revision = "f2b3c4d5e6f7"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "charter_briefs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("reference", sa.String(length=48), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("charterer", sa.String(length=255), nullable=True),
        sa.Column("start_date", sa.Date(), nullable=True),
        sa.Column("end_date", sa.Date(), nullable=True),
        sa.Column("status", sa.String(length=24), nullable=False, server_default="Draft"),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("details_json", sa.Text(), nullable=False, server_default="{}"),
        sa.Column("published_at", sa.DateTime(), nullable=True),
        sa.Column("published_by", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_charter_briefs_reference", "charter_briefs", ["reference"], unique=True)
    op.create_index("ix_charter_briefs_start_date", "charter_briefs", ["start_date"])
    op.create_index("ix_charter_briefs_end_date", "charter_briefs", ["end_date"])


def downgrade():
    op.drop_index("ix_charter_briefs_end_date", table_name="charter_briefs")
    op.drop_index("ix_charter_briefs_start_date", table_name="charter_briefs")
    op.drop_index("ix_charter_briefs_reference", table_name="charter_briefs")
    op.drop_table("charter_briefs")
