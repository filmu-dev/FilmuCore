"""Add durable consumer playback activity events.

Revision ID: 20260419_0028
Revises: 20260418_0027
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260419_0028"
down_revision = "20260418_0027"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "consumer_playback_activity_events",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("actor_id", sa.String(length=256), nullable=False),
        sa.Column("actor_type", sa.String(length=64), nullable=False),
        sa.Column("item_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("activity_kind", sa.String(length=32), nullable=False),
        sa.Column("target", sa.String(length=16), nullable=True),
        sa.Column("device_key", sa.String(length=128), nullable=False),
        sa.Column("device_label", sa.String(length=256), nullable=False),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.ForeignKeyConstraint(["item_id"], ["media_items.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_consumer_playback_activity_events_tenant_id"),
        "consumer_playback_activity_events",
        ["tenant_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_playback_activity_events_actor_id"),
        "consumer_playback_activity_events",
        ["actor_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_playback_activity_events_actor_type"),
        "consumer_playback_activity_events",
        ["actor_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_playback_activity_events_item_id"),
        "consumer_playback_activity_events",
        ["item_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_playback_activity_events_activity_kind"),
        "consumer_playback_activity_events",
        ["activity_kind"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_playback_activity_events_target"),
        "consumer_playback_activity_events",
        ["target"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_playback_activity_events_device_key"),
        "consumer_playback_activity_events",
        ["device_key"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_playback_activity_events_occurred_at"),
        "consumer_playback_activity_events",
        ["occurred_at"],
        unique=False,
    )
    op.create_index(
        "ix_consumer_playback_activity_actor_tenant_occurred",
        "consumer_playback_activity_events",
        ["actor_id", "tenant_id", "occurred_at"],
        unique=False,
    )
    op.create_index(
        "ix_consumer_playback_activity_device_tenant_occurred",
        "consumer_playback_activity_events",
        ["device_key", "tenant_id", "occurred_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_consumer_playback_activity_device_tenant_occurred",
        table_name="consumer_playback_activity_events",
    )
    op.drop_index(
        "ix_consumer_playback_activity_actor_tenant_occurred",
        table_name="consumer_playback_activity_events",
    )
    op.drop_index(
        op.f("ix_consumer_playback_activity_events_occurred_at"),
        table_name="consumer_playback_activity_events",
    )
    op.drop_index(
        op.f("ix_consumer_playback_activity_events_device_key"),
        table_name="consumer_playback_activity_events",
    )
    op.drop_index(
        op.f("ix_consumer_playback_activity_events_target"),
        table_name="consumer_playback_activity_events",
    )
    op.drop_index(
        op.f("ix_consumer_playback_activity_events_activity_kind"),
        table_name="consumer_playback_activity_events",
    )
    op.drop_index(
        op.f("ix_consumer_playback_activity_events_item_id"),
        table_name="consumer_playback_activity_events",
    )
    op.drop_index(
        op.f("ix_consumer_playback_activity_events_actor_type"),
        table_name="consumer_playback_activity_events",
    )
    op.drop_index(
        op.f("ix_consumer_playback_activity_events_actor_id"),
        table_name="consumer_playback_activity_events",
    )
    op.drop_index(
        op.f("ix_consumer_playback_activity_events_tenant_id"),
        table_name="consumer_playback_activity_events",
    )
    op.drop_table("consumer_playback_activity_events")
