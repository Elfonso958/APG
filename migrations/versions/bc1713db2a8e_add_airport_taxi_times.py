"""add airport taxi times

Revision ID: bc1713db2a8e
Revises: 11f2a3b4c5d6
Create Date: 2026-10-08 15:13:16.507047

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'bc1713db2a8e'
down_revision = '11f2a3b4c5d6'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('app_config', schema=None) as batch_op:
        # A server default safely populates the existing singleton config row.
        batch_op.add_column(sa.Column(
            'airport_taxi_times_json', sa.Text(), nullable=False,
            server_default=sa.text("'{}'"),
        ))


def downgrade():
    with op.batch_alter_table('app_config', schema=None) as batch_op:
        batch_op.drop_column('airport_taxi_times_json')
