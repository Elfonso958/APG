"""store Envision job title for crew-briefing access rules

Revision ID: f6a7b8c9d0e1
Revises: f5b6c7d8e9f0
"""

from alembic import op
import sqlalchemy as sa


revision = "f6a7b8c9d0e1"
down_revision = "f5b6c7d8e9f0"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("app_users", sa.Column("envision_job_title", sa.String(length=160), nullable=True))


def downgrade():
    op.drop_column("app_users", "envision_job_title")
