"""add charter crew car settings

Revision ID: 07b8c9d0e1f2
Revises: 06a7b8c9d0e1
"""
from alembic import op
import sqlalchemy as sa

revision = "07b8c9d0e1f2"
down_revision = "06a7b8c9d0e1"
branch_labels = None
depends_on = None

def upgrade():
    op.add_column("app_config", sa.Column("charter_crew_cars_json", sa.Text(), nullable=False, server_default="{}"))

def downgrade():
    op.drop_column("app_config", "charter_crew_cars_json")
