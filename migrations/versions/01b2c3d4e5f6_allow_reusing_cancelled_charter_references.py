"""allow reusing cancelled charter request references

Revision ID: 01b2c3d4e5f6
Revises: 00a1b2c3d4e5
"""
from alembic import op
import sqlalchemy as sa


revision = "01b2c3d4e5f6"
down_revision = "00a1b2c3d4e5"
branch_labels = None
depends_on = None


def upgrade():
    # The original unique constraint was unnamed. Rebuild the small, standalone
    # table so this works reliably on SQLite as well as production databases.
    op.create_table(
        "charter_requests_rebuilt",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("reference", sa.String(length=80), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("sectors_json", sa.Text(), nullable=False),
        sa.Column("created_by", sa.String(length=255), nullable=True),
        sa.Column("decision_by", sa.String(length=255), nullable=True),
        sa.Column("decision_note", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("decided_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.execute(
        "INSERT INTO charter_requests_rebuilt "
        "(id, reference, title, status, sectors_json, created_by, decision_by, "
        "decision_note, created_at, decided_at, updated_at) "
        "SELECT id, reference, title, status, sectors_json, created_by, decision_by, "
        "decision_note, created_at, decided_at, updated_at FROM charter_requests"
    )
    op.drop_table("charter_requests")
    op.rename_table("charter_requests_rebuilt", "charter_requests")
    op.create_index("ix_charter_requests_reference", "charter_requests", ["reference"])


def downgrade():
    # A later duplicate reference cannot safely be collapsed into a unique key.
    raise RuntimeError("Cannot restore the unique charter request reference constraint safely.")
