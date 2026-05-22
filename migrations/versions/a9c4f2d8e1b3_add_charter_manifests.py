"""add charter manifests

Revision ID: a9c4f2d8e1b3
Revises: f1a9d3c4b8e2
Create Date: 2026-05-19 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "a9c4f2d8e1b3"
down_revision = "f1a9d3c4b8e2"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "charter_manifests",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("envision_flight_id", sa.String(length=32), nullable=False),
        sa.Column("flight_no", sa.String(length=16), nullable=True),
        sa.Column("dep", sa.String(length=8), nullable=True),
        sa.Column("ades", sa.String(length=8), nullable=True),
        sa.Column("pax_json", sa.Text(), nullable=False, server_default="[]"),
        sa.Column("uploaded_filename", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index(
        "ix_charter_manifests_envision_flight_id",
        "charter_manifests",
        ["envision_flight_id"],
        unique=True,
    )
    op.create_index(
        "ix_charter_manifests_flight_no",
        "charter_manifests",
        ["flight_no"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_charter_manifests_flight_no", table_name="charter_manifests")
    op.drop_index("ix_charter_manifests_envision_flight_id", table_name="charter_manifests")
    op.drop_table("charter_manifests")
