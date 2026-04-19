"""Add governance operator state.

Revision ID: 20260411_0025
Revises: 20260411_0024
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260411_0025"
down_revision = "20260411_0024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "access_policy_revisions",
        sa.Column(
            "approval_status",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text("'bootstrap'"),
        ),
    )
    op.add_column(
        "access_policy_revisions",
        sa.Column("proposed_by", sa.String(length=256), nullable=True),
    )
    op.add_column(
        "access_policy_revisions",
        sa.Column("approved_by", sa.String(length=256), nullable=True),
    )
    op.add_column(
        "access_policy_revisions",
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "access_policy_revisions",
        sa.Column("approval_notes", sa.Text(), nullable=True),
    )
    op.create_index(
        op.f("ix_access_policy_revisions_approval_status"),
        "access_policy_revisions",
        ["approval_status"],
        unique=False,
    )

    op.create_table(
        "plugin_governance_overrides",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("plugin_name", sa.String(length=256), nullable=False),
        sa.Column(
            "state",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text("'approved'"),
        ),
        sa.Column("reason", sa.String(length=512), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("updated_by", sa.String(length=256), nullable=True),
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
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_plugin_governance_overrides_plugin_name"),
        "plugin_governance_overrides",
        ["plugin_name"],
        unique=True,
    )
    op.create_index(
        op.f("ix_plugin_governance_overrides_state"),
        "plugin_governance_overrides",
        ["state"],
        unique=False,
    )

    op.create_table(
        "control_plane_subscribers",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("stream_name", sa.String(length=256), nullable=False),
        sa.Column("group_name", sa.String(length=256), nullable=False),
        sa.Column("consumer_name", sa.String(length=256), nullable=False),
        sa.Column("node_id", sa.String(length=256), nullable=False),
        sa.Column("tenant_id", sa.String(length=64), nullable=True),
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text("'active'"),
        ),
        sa.Column("last_read_offset", sa.String(length=128), nullable=True),
        sa.Column("last_delivered_event_id", sa.String(length=128), nullable=True),
        sa.Column("last_acked_event_id", sa.String(length=128), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "claimed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "last_heartbeat_at",
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
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "stream_name",
            "group_name",
            "consumer_name",
            name="uq_control_plane_subscriber_identity",
        ),
    )
    op.create_index(
        op.f("ix_control_plane_subscribers_stream_name"),
        "control_plane_subscribers",
        ["stream_name"],
        unique=False,
    )
    op.create_index(
        op.f("ix_control_plane_subscribers_group_name"),
        "control_plane_subscribers",
        ["group_name"],
        unique=False,
    )
    op.create_index(
        op.f("ix_control_plane_subscribers_consumer_name"),
        "control_plane_subscribers",
        ["consumer_name"],
        unique=False,
    )
    op.create_index(
        op.f("ix_control_plane_subscribers_tenant_id"),
        "control_plane_subscribers",
        ["tenant_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_control_plane_subscribers_status"),
        "control_plane_subscribers",
        ["status"],
        unique=False,
    )
    op.create_index(
        "ix_control_plane_subscribers_status_heartbeat",
        "control_plane_subscribers",
        ["status", "last_heartbeat_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_control_plane_subscribers_status_heartbeat",
        table_name="control_plane_subscribers",
    )
    op.drop_index(
        op.f("ix_control_plane_subscribers_status"),
        table_name="control_plane_subscribers",
    )
    op.drop_index(
        op.f("ix_control_plane_subscribers_tenant_id"),
        table_name="control_plane_subscribers",
    )
    op.drop_index(
        op.f("ix_control_plane_subscribers_consumer_name"),
        table_name="control_plane_subscribers",
    )
    op.drop_index(
        op.f("ix_control_plane_subscribers_group_name"),
        table_name="control_plane_subscribers",
    )
    op.drop_index(
        op.f("ix_control_plane_subscribers_stream_name"),
        table_name="control_plane_subscribers",
    )
    op.drop_table("control_plane_subscribers")

    op.drop_index(
        op.f("ix_plugin_governance_overrides_state"),
        table_name="plugin_governance_overrides",
    )
    op.drop_index(
        op.f("ix_plugin_governance_overrides_plugin_name"),
        table_name="plugin_governance_overrides",
    )
    op.drop_table("plugin_governance_overrides")

    op.drop_index(
        op.f("ix_access_policy_revisions_approval_status"),
        table_name="access_policy_revisions",
    )
    op.drop_column("access_policy_revisions", "approval_notes")
    op.drop_column("access_policy_revisions", "approved_at")
    op.drop_column("access_policy_revisions", "approved_by")
    op.drop_column("access_policy_revisions", "proposed_by")
    op.drop_column("access_policy_revisions", "approval_status")
