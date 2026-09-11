"""add email sender selection

Revision ID: f1a2b3c4d5e6
Revises: f0a1b2c3d4e5
"""
from alembic import op
import sqlalchemy as sa

revision = "f1a2b3c4d5e6"
down_revision = "f0a1b2c3d4e5"
branch_labels = None
depends_on = None

def upgrade():
    op.add_column("email_settings", sa.Column("from_email", sa.String(length=255), nullable=True))

def downgrade():
    op.drop_column("email_settings", "from_email")
