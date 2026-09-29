"""add charter request workflow

Revision ID: ff6a7b8c9d0e
Revises: fe5f6a7b8c9d
"""
from alembic import op
import sqlalchemy as sa

revision = "ff6a7b8c9d0e"
down_revision = "fe5f6a7b8c9d"
branch_labels = None
depends_on = None

def upgrade():
    op.create_table("charter_requests", sa.Column("id", sa.Integer(), primary_key=True), sa.Column("reference", sa.String(length=80), nullable=False), sa.Column("title", sa.String(length=255), nullable=False), sa.Column("status", sa.String(length=24), nullable=False), sa.Column("sectors_json", sa.Text(), nullable=False), sa.Column("created_by", sa.String(length=255)), sa.Column("decision_by", sa.String(length=255)), sa.Column("decision_note", sa.Text()), sa.Column("created_at", sa.DateTime(), nullable=False), sa.Column("decided_at", sa.DateTime()), sa.Column("updated_at", sa.DateTime(), nullable=False), sa.UniqueConstraint("reference"))
    op.create_index("ix_charter_requests_reference", "charter_requests", ["reference"])
    op.add_column("email_settings", sa.Column("charter_request_recipients", sa.String(length=1000), nullable=True))

def downgrade():
    op.drop_column("email_settings", "charter_request_recipients")
    op.drop_index("ix_charter_requests_reference", table_name="charter_requests")
    op.drop_table("charter_requests")
