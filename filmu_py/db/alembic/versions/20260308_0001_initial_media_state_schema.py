"""initial media and lifecycle state schema

Revision ID: 20260308_0001
Revises:
Create Date: 2026-03-08
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "20260308_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "media_items",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True, nullable=False),
        sa.Column("external_ref", sa.String(length=128), nullable=False),
        sa.Column("title", sa.String(length=512), nullable=False),
        sa.Column("state", sa.String(length=64), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("external_ref", name="uq_media_items_external_ref"),
    )
    op.create_index("ix_media_items_external_ref", "media_items", ["external_ref"])
    op.create_index("ix_media_items_state", "media_items", ["state"])

    op.create_table(
        "item_state_events",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True, nullable=False),
        sa.Column("item_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("event", sa.String(length=64), nullable=False),
        sa.Column("previous_state", sa.String(length=64), nullable=False),
        sa.Column("next_state", sa.String(length=64), nullable=False),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["item_id"], ["media_items.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_item_state_events_item_id", "item_state_events", ["item_id"])
    op.create_index("ix_item_state_events_event", "item_state_events", ["event"])
    op.create_index("ix_item_state_events_next_state", "item_state_events", ["next_state"])
    op.create_index(
        "ix_item_state_events_item_created",
        "item_state_events",
        ["item_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_item_state_events_item_created", table_name="item_state_events")
    op.drop_index("ix_item_state_events_next_state", table_name="item_state_events")
    op.drop_index("ix_item_state_events_event", table_name="item_state_events")
    op.drop_index("ix_item_state_events_item_id", table_name="item_state_events")
    op.drop_table("item_state_events")

    op.drop_index("ix_media_items_state", table_name="media_items")
    op.drop_index("ix_media_items_external_ref", table_name="media_items")
    op.drop_table("media_items")
