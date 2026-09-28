"""add administrator managed Power BI API keys

Revision ID: f8a9b0c1d2e3
Revises: f7b8c9d0e1f2
"""

from alembic import op
import sqlalchemy as sa


revision = "f8a9b0c1d2e3"
down_revision = "f7b8c9d0e1f2"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "powerbi_api_keys",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("key_prefix", sa.String(length=16), nullable=False),
        sa.Column("key_hash", sa.String(length=64), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey("app_users.id"), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("revoked_at", sa.DateTime(), nullable=True),
    )
    op.create_index("ix_powerbi_api_keys_key_hash", "powerbi_api_keys", ["key_hash"], unique=True)


def downgrade():
    op.drop_index("ix_powerbi_api_keys_key_hash", table_name="powerbi_api_keys")
    op.drop_table("powerbi_api_keys")
