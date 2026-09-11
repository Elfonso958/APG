"""add configurable charter passenger weights

Revision ID: f2b3c4d5e6f7
Revises: f1a2b3c4d5e6
"""
from alembic import op
import sqlalchemy as sa


revision = "f2b3c4d5e6f7"
down_revision = "f1a2b3c4d5e6"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "app_config",
        sa.Column("charter_passenger_weights_json", sa.Text(), nullable=False, server_default="{}"),
    )


def downgrade():
    op.drop_column("app_config", "charter_passenger_weights_json")
