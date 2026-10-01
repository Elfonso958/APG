"""add organisation chart workspaces

Revision ID: 08c9d0e1f2a3
Revises: 07b8c9d0e1f2
"""
from alembic import op
import sqlalchemy as sa


revision = "08c9d0e1f2a3"
down_revision = "07b8c9d0e1f2"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "organisation_charts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("chart_type", sa.String(length=24), nullable=False, server_default="Proposed"),
        sa.Column("source_filename", sa.String(length=255)),
        sa.Column("created_by", sa.String(length=255)),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_table(
        "organisation_chart_nodes",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("chart_id", sa.Integer(), sa.ForeignKey("organisation_charts.id"), nullable=False),
        sa.Column("report_to_node_id", sa.Integer()),
        sa.Column("full_name", sa.String(length=255), nullable=False),
        sa.Column("job_title", sa.String(length=255)),
        sa.Column("department", sa.String(length=80)),
        sa.Column("employment_type", sa.String(length=120)),
        sa.Column("hours_per_week", sa.Float()),
        sa.Column("x", sa.Float(), nullable=False, server_default="80"),
        sa.Column("y", sa.Float(), nullable=False, server_default="80"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_organisation_chart_nodes_chart_id", "organisation_chart_nodes", ["chart_id"])
    op.create_index("ix_organisation_chart_nodes_report_to_node_id", "organisation_chart_nodes", ["report_to_node_id"])


def downgrade():
    op.drop_index("ix_organisation_chart_nodes_report_to_node_id", table_name="organisation_chart_nodes")
    op.drop_index("ix_organisation_chart_nodes_chart_id", table_name="organisation_chart_nodes")
    op.drop_table("organisation_chart_nodes")
    op.drop_table("organisation_charts")
