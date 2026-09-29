"""add manual APG flight links

Revision ID: fb2c3d4e5f6a
Revises: fa1b2c3d4e5f
"""
from alembic import op
import sqlalchemy as sa

revision = "fb2c3d4e5f6a"
down_revision = "fa1b2c3d4e5f"
branch_labels = None
depends_on = None

def upgrade():
    op.create_table("manual_apg_flight_links",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("envision_flight_id", sa.String(length=32), nullable=False),
        sa.Column("apg_plan_id", sa.Integer(), nullable=False),
        sa.Column("linked_by_user_id", sa.Integer(), sa.ForeignKey("app_users.id"), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_manual_apg_flight_links_envision_flight_id", "manual_apg_flight_links", ["envision_flight_id"], unique=True)
    op.create_index("ix_manual_apg_flight_links_apg_plan_id", "manual_apg_flight_links", ["apg_plan_id"], unique=True)

def downgrade():
    op.drop_index("ix_manual_apg_flight_links_apg_plan_id", table_name="manual_apg_flight_links")
    op.drop_index("ix_manual_apg_flight_links_envision_flight_id", table_name="manual_apg_flight_links")
    op.drop_table("manual_apg_flight_links")
