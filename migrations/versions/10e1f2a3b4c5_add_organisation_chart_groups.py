"""add organisation chart groups

Revision ID: 10e1f2a3b4c5
Revises: 09d0e1f2a3b4
"""
from alembic import op
import sqlalchemy as sa

revision = "10e1f2a3b4c5"
down_revision = "09d0e1f2a3b4"
branch_labels = None
depends_on = None

def upgrade():
    op.create_table("organisation_chart_groups", sa.Column("id", sa.Integer(), primary_key=True), sa.Column("chart_id", sa.Integer(), sa.ForeignKey("organisation_charts.id"), nullable=False), sa.Column("title", sa.String(length=120), nullable=False), sa.Column("x", sa.Float(), nullable=False), sa.Column("y", sa.Float(), nullable=False), sa.Column("width", sa.Float(), nullable=False), sa.Column("height", sa.Float(), nullable=False), sa.Column("created_at", sa.DateTime(), nullable=False), sa.Column("updated_at", sa.DateTime(), nullable=False))
    op.create_index("ix_organisation_chart_groups_chart_id", "organisation_chart_groups", ["chart_id"])
    op.add_column("organisation_chart_nodes", sa.Column("group_id", sa.Integer()))
    op.create_index("ix_organisation_chart_nodes_group_id", "organisation_chart_nodes", ["group_id"])

def downgrade():
    op.drop_index("ix_organisation_chart_nodes_group_id", table_name="organisation_chart_nodes")
    op.drop_column("organisation_chart_nodes", "group_id")
    op.drop_index("ix_organisation_chart_groups_chart_id", table_name="organisation_chart_groups")
    op.drop_table("organisation_chart_groups")
