"""Add transactional outbox events table.

Revision ID: 20260315_0014
Revises: 20260315_0013
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260315_0014"
down_revision = "20260315_0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "outbox_events",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("item_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.ForeignKeyConstraint(["item_id"], ["media_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_outbox_events_event_type", "outbox_events", ["event_type"], unique=False)
    op.create_index("ix_outbox_events_item_id", "outbox_events", ["item_id"], unique=False)
    op.create_index(
        "ix_outbox_events_published_at", "outbox_events", ["published_at"], unique=False
    )
    op.create_index("ix_outbox_events_failed_at", "outbox_events", ["failed_at"], unique=False)
    op.create_index(
        "ix_outbox_events_item_created",
        "outbox_events",
        ["item_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_outbox_events_pending_created",
        "outbox_events",
        ["published_at", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_events_pending_created", table_name="outbox_events")
    op.drop_index("ix_outbox_events_item_created", table_name="outbox_events")
    op.drop_index("ix_outbox_events_failed_at", table_name="outbox_events")
    op.drop_index("ix_outbox_events_published_at", table_name="outbox_events")
    op.drop_index("ix_outbox_events_item_id", table_name="outbox_events")
    op.drop_index("ix_outbox_events_event_type", table_name="outbox_events")
    op.drop_table("outbox_events")
