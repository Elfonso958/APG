"""store airport handling providers as records

Revision ID: fd4e5f6a7b8c
Revises: fc3d4e5f6a7b
"""
import json
from datetime import datetime
from alembic import op
import sqlalchemy as sa

revision = "fd4e5f6a7b8c"
down_revision = "fc3d4e5f6a7b"
branch_labels = None
depends_on = None

def upgrade():
    op.create_table("airport_handling_providers",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("airport", sa.String(length=8), nullable=False),
        sa.Column("label", sa.String(length=180), nullable=False),
        sa.Column("handler", sa.String(length=180)), sa.Column("contact", sa.String(length=180)),
        sa.Column("phone", sa.String(length=80)), sa.Column("additional_phone", sa.String(length=80)),
        sa.Column("email_addresses", sa.Text()), sa.Column("frequency", sa.String(length=80)),
        sa.Column("gpu", sa.String(length=255)), sa.Column("fuel", sa.Text()), sa.Column("notes", sa.Text()),
        sa.Column("created_at", sa.DateTime(), nullable=False), sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_airport_handling_providers_airport", "airport_handling_providers", ["airport"])
    bind = op.get_bind()
    row = bind.execute(sa.text("SELECT airport_handling_json FROM app_config WHERE id = 1")).first()
    try: entries = json.loads(row[0] or "[]") if row else []
    except (TypeError, ValueError): entries = []
    now = datetime.utcnow()
    for entry in entries if isinstance(entries, list) else []:
        if not isinstance(entry, dict) or not entry.get("airport") or not entry.get("label"):
            continue
        bind.execute(sa.text("""INSERT INTO airport_handling_providers (airport,label,handler,contact,phone,additional_phone,email_addresses,frequency,gpu,fuel,notes,created_at,updated_at) VALUES (:airport,:label,:handler,:contact,:phone,:additional_phone,:email_addresses,:frequency,:gpu,:fuel,:notes,:created_at,:updated_at)"""), {
            "airport": str(entry.get("airport")).upper(), "label": entry.get("label"), "handler": entry.get("handler"), "contact": entry.get("contact"), "phone": entry.get("phone"), "additional_phone": entry.get("additional_phone"), "email_addresses": "\n".join(entry.get("emails") or []), "frequency": entry.get("frequency"), "gpu": entry.get("gpu"), "fuel": entry.get("fuel"), "notes": entry.get("notes"), "created_at": now, "updated_at": now,
        })

def downgrade():
    op.drop_index("ix_airport_handling_providers_airport", table_name="airport_handling_providers")
    op.drop_table("airport_handling_providers")
