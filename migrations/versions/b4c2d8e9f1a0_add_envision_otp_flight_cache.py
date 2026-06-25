"""add envision otp flight cache

Revision ID: b4c2d8e9f1a0
Revises: a9c4f2d8e1b3
Create Date: 2026-06-25 14:45:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "b4c2d8e9f1a0"
down_revision = "a9c4f2d8e1b3"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "envision_otp_flight_cache",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("envision_flight_id", sa.String(length=32), nullable=False),
        sa.Column("flight_date", sa.Date(), nullable=True),
        sa.Column("departure_scheduled", sa.DateTime(), nullable=True),
        sa.Column("reg", sa.String(length=16), nullable=True),
        sa.Column("row_json", sa.Text(), nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index(
        "ix_envision_otp_flight_cache_envision_flight_id",
        "envision_otp_flight_cache",
        ["envision_flight_id"],
        unique=True,
    )
    op.create_index(
        "ix_envision_otp_flight_cache_flight_date",
        "envision_otp_flight_cache",
        ["flight_date"],
        unique=False,
    )
    op.create_index(
        "ix_envision_otp_flight_cache_departure_scheduled",
        "envision_otp_flight_cache",
        ["departure_scheduled"],
        unique=False,
    )
    op.create_index(
        "ix_envision_otp_flight_cache_reg",
        "envision_otp_flight_cache",
        ["reg"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_envision_otp_flight_cache_reg", table_name="envision_otp_flight_cache")
    op.drop_index("ix_envision_otp_flight_cache_departure_scheduled", table_name="envision_otp_flight_cache")
    op.drop_index("ix_envision_otp_flight_cache_flight_date", table_name="envision_otp_flight_cache")
    op.drop_index("ix_envision_otp_flight_cache_envision_flight_id", table_name="envision_otp_flight_cache")
    op.drop_table("envision_otp_flight_cache")

