"""add manual DCS flight links

Revision ID: fc3d4e5f6a7b
Revises: fb2c3d4e5f6a
"""
from alembic import op
import sqlalchemy as sa

revision = "fc3d4e5f6a7b"
down_revision = "fb2c3d4e5f6a"
branch_labels = None
depends_on = None

def upgrade():
    op.create_table("manual_dcs_flight_links",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("envision_flight_id", sa.String(length=32), nullable=False),
        sa.Column("dcs_flight_json", sa.Text(), nullable=False),
        sa.Column("linked_by_user_id", sa.Integer(), sa.ForeignKey("app_users.id"), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_manual_dcs_flight_links_envision_flight_id", "manual_dcs_flight_links", ["envision_flight_id"], unique=True)

def downgrade():
    op.drop_index("ix_manual_dcs_flight_links_envision_flight_id", table_name="manual_dcs_flight_links")
    op.drop_table("manual_dcs_flight_links")
