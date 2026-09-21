"""add crew briefing code and privacy setting

Revision ID: f7b8c9d0e1f2
Revises: f6a7b8c9d0e1
"""

from alembic import op
import sqlalchemy as sa


revision = "f7b8c9d0e1f2"
down_revision = "f6a7b8c9d0e1"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("app_users") as batch:
        batch.add_column(sa.Column("envision_crew_code", sa.String(length=32), nullable=True))
        batch.add_column(sa.Column("crew_briefing_private", sa.Boolean(), nullable=False, server_default=sa.false()))
        batch.create_index("ix_app_users_envision_crew_code", ["envision_crew_code"], unique=True)


def downgrade():
    with op.batch_alter_table("app_users") as batch:
        batch.drop_index("ix_app_users_envision_crew_code")
        batch.drop_column("crew_briefing_private")
        batch.drop_column("envision_crew_code")
