"""Add sync_flight_state table

Revision ID: c2f1b6a0f2a1
Revises: 7b15205458ea
Create Date: 2026-03-04 12:10:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c2f1b6a0f2a1"
down_revision = "7b15205458ea"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "sync_flight_state",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("envision_flight_id", sa.String(length=32), nullable=False),
        sa.Column("core_json", sa.Text(), nullable=True),
        sa.Column("fp", sa.String(length=128), nullable=True),
        sa.Column("apg_id", sa.Integer(), nullable=True),
        sa.Column("last_run_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_sync_flight_state_envision_flight_id", "sync_flight_state", ["envision_flight_id"], unique=True)


def downgrade():
    op.drop_index("ix_sync_flight_state_envision_flight_id", table_name="sync_flight_state")
    op.drop_table("sync_flight_state")
