"""add organisation chart secondary roles

Revision ID: 09d0e1f2a3b4
Revises: 08c9d0e1f2a3
"""
from alembic import op
import sqlalchemy as sa

revision = "09d0e1f2a3b4"
down_revision = "08c9d0e1f2a3"
branch_labels = None
depends_on = None

def upgrade():
    op.add_column("organisation_chart_nodes", sa.Column("secondary_roles_json", sa.Text(), nullable=False, server_default="[]"))
    op.add_column("organisation_chart_nodes", sa.Column("secondary_report_to_node_id", sa.Integer()))
    op.create_index("ix_organisation_chart_nodes_secondary_report_to_node_id", "organisation_chart_nodes", ["secondary_report_to_node_id"])

def downgrade():
    op.drop_index("ix_organisation_chart_nodes_secondary_report_to_node_id", table_name="organisation_chart_nodes")
    op.drop_column("organisation_chart_nodes", "secondary_report_to_node_id")
    op.drop_column("organisation_chart_nodes", "secondary_roles_json")
