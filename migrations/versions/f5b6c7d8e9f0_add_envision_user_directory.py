"""add Envision-backed APG user directory fields

Revision ID: f5b6c7d8e9f0
Revises: f4a5b6c7d8e9
"""

from alembic import op
import sqlalchemy as sa


revision = "f5b6c7d8e9f0"
down_revision = "f4a5b6c7d8e9"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("app_users") as batch:
        batch.add_column(sa.Column("auth_provider", sa.String(length=24), nullable=False, server_default="local"))
        batch.add_column(sa.Column("envision_username", sa.String(length=120), nullable=True))
        batch.add_column(sa.Column("envision_employee_id", sa.String(length=64), nullable=True))
        batch.add_column(sa.Column("directory_last_seen_at", sa.DateTime(), nullable=True))
        batch.create_index("ix_app_users_envision_username", ["envision_username"], unique=True)
        batch.create_index("ix_app_users_envision_employee_id", ["envision_employee_id"], unique=True)
    with op.batch_alter_table("app_config") as batch:
        batch.add_column(sa.Column("last_envision_user_sync_at", sa.DateTime(), nullable=True))


def downgrade():
    with op.batch_alter_table("app_config") as batch:
        batch.drop_column("last_envision_user_sync_at")
    with op.batch_alter_table("app_users") as batch:
        batch.drop_index("ix_app_users_envision_employee_id")
        batch.drop_index("ix_app_users_envision_username")
        batch.drop_column("directory_last_seen_at")
        batch.drop_column("envision_employee_id")
        batch.drop_column("envision_username")
        batch.drop_column("auth_provider")
