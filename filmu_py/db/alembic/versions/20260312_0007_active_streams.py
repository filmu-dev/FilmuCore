"""add active streams

Revision ID: 20260312_0007
Revises: 20260312_0006
Create Date: 2026-03-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260312_0007"
down_revision = "20260312_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "active_streams",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True, nullable=False),
        sa.Column("item_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("media_entry_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("role", sa.String(length=16), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.ForeignKeyConstraint(["item_id"], ["media_items.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["media_entry_id"], ["media_entries.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("item_id", "role", name="uq_active_streams_item_role"),
    )
    op.create_index("ix_active_streams_item_id", "active_streams", ["item_id"])
    op.create_index("ix_active_streams_media_entry_id", "active_streams", ["media_entry_id"])
    op.create_index("ix_active_streams_role", "active_streams", ["role"])
    op.create_index("ix_active_streams_item_created", "active_streams", ["item_id", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_active_streams_item_created", table_name="active_streams")
    op.drop_index("ix_active_streams_role", table_name="active_streams")
    op.drop_index("ix_active_streams_media_entry_id", table_name="active_streams")
    op.drop_index("ix_active_streams_item_id", table_name="active_streams")
    op.drop_table("active_streams")
