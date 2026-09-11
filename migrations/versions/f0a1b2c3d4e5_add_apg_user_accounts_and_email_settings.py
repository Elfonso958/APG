"""add APG user accounts and email settings

Revision ID: f0a1b2c3d4e5
Revises: c9e8d7f6a5b4
Create Date: 2026-09-11 18:10:00.000000
"""
from alembic import op
import sqlalchemy as sa


revision = "f0a1b2c3d4e5"
down_revision = "c9e8d7f6a5b4"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "app_users",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("display_name", sa.String(length=120), nullable=True),
        sa.Column("password_hash", sa.String(length=255), nullable=False),
        sa.Column("is_admin", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("last_login_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_app_users_email", "app_users", ["email"], unique=True)
    op.create_table(
        "email_settings",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("flight_operations_email", sa.String(length=1000), nullable=True),
        sa.Column("charter_closure_emails_enabled", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade():
    op.drop_table("email_settings")
    op.drop_index("ix_app_users_email", table_name="app_users")
    op.drop_table("app_users")
