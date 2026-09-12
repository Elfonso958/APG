"""add charter operations directory

Revision ID: a7b8c9d0e1f2
Revises: f3c4d5e6f7a8
Create Date: 2026-09-12
"""
from alembic import op
import sqlalchemy as sa

revision = "a7b8c9d0e1f2"
down_revision = "f3c4d5e6f7a8"
branch_labels = None
depends_on = None

def upgrade():
    with op.batch_alter_table("app_config") as batch:
        batch.add_column(sa.Column("catering_services_json", sa.Text(), nullable=False, server_default="[]"))
        batch.add_column(sa.Column("airport_handling_json", sa.Text(), nullable=False, server_default="[]"))

def downgrade():
    with op.batch_alter_table("app_config") as batch:
        batch.drop_column("airport_handling_json")
        batch.drop_column("catering_services_json")
