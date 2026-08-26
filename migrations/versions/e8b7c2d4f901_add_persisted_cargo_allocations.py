"""add persisted cargo allocations

Revision ID: e8b7c2d4f901
Revises: d4e6f8a1b3c5
"""
from alembic import op
import sqlalchemy as sa

revision = "e8b7c2d4f901"
down_revision = "d4e6f8a1b3c5"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("flight_freight_allocations") as batch_op:
        batch_op.add_column(sa.Column("revision", sa.Integer(), nullable=False, server_default="1"))
    op.create_table(
        "flight_cargo_allocations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("envision_flight_id", sa.String(length=32), nullable=False),
        sa.Column("allocations_json", sa.Text(), nullable=False, server_default="[]"),
        sa.Column("atr_rows_json", sa.Text(), nullable=False, server_default="[]"),
        sa.Column("revision", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_flight_cargo_allocations_envision_flight_id", "flight_cargo_allocations", ["envision_flight_id"], unique=True)


def downgrade():
    op.drop_index("ix_flight_cargo_allocations_envision_flight_id", table_name="flight_cargo_allocations")
    op.drop_table("flight_cargo_allocations")
    with op.batch_alter_table("flight_freight_allocations") as batch_op:
        batch_op.drop_column("revision")
