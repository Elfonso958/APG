"""add flight freight allocations

Revision ID: d4e6f8a1b3c5
Revises: e3a7c9d1f204
"""
from alembic import op
import sqlalchemy as sa

revision = "d4e6f8a1b3c5"
down_revision = "e3a7c9d1f204"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("app_config") as batch_op:
        batch_op.add_column(sa.Column("seat_bag_tare_kg", sa.Float(), nullable=False, server_default="7.0"))
    op.create_table(
        "flight_freight_allocations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("envision_flight_id", sa.String(length=32), nullable=False),
        sa.Column("seats_json", sa.Text(), nullable=False, server_default="[]"),
        sa.Column("freight_kg", sa.Float(), nullable=False, server_default="0"),
        sa.Column("tare_kg", sa.Float(), nullable=False, server_default="7.0"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_flight_freight_allocations_envision_flight_id", "flight_freight_allocations", ["envision_flight_id"], unique=True)


def downgrade():
    op.drop_index("ix_flight_freight_allocations_envision_flight_id", table_name="flight_freight_allocations")
    op.drop_table("flight_freight_allocations")
    with op.batch_alter_table("app_config") as batch_op:
        batch_op.drop_column("seat_bag_tare_kg")
