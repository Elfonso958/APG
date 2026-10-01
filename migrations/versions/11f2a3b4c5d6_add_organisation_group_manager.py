"""add organisation group manager

Revision ID: 11f2a3b4c5d6
Revises: 10e1f2a3b4c5
"""
from alembic import op
import sqlalchemy as sa

revision = "11f2a3b4c5d6"
down_revision = "10e1f2a3b4c5"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("organisation_chart_groups", sa.Column("report_to_node_id", sa.Integer(), nullable=True))
    op.create_index("ix_organisation_chart_groups_report_to_node_id", "organisation_chart_groups", ["report_to_node_id"])


def downgrade():
    op.drop_index("ix_organisation_chart_groups_report_to_node_id", table_name="organisation_chart_groups")
    op.drop_column("organisation_chart_groups", "report_to_node_id")
