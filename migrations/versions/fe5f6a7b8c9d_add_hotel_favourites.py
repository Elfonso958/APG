"""add saved hotel favourites for charter briefs

Revision ID: fe5f6a7b8c9d
Revises: fd4e5f6a7b8c
"""
from alembic import op
import sqlalchemy as sa


revision = "fe5f6a7b8c9d"
down_revision = "fd4e5f6a7b8c"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "hotel_favourites",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("google_place_id", sa.String(length=255), nullable=True),
        sa.Column("address", sa.Text(), nullable=True),
        sa.Column("phone", sa.String(length=100), nullable=True),
        sa.Column("website", sa.String(length=500), nullable=True),
        sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey("app_users.id"), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("google_place_id", name="uq_hotel_favourites_google_place_id"),
    )


def downgrade():
    op.drop_table("hotel_favourites")
