"""add playback attachment lifecycle fields

Revision ID: 20260312_0003
Revises: 20260312_0002
Create Date: 2026-03-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260312_0003"
down_revision = "20260312_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "playback_attachments",
        sa.Column("preference_rank", sa.Integer(), nullable=False, server_default="100"),
    )
    op.add_column(
        "playback_attachments",
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "playback_attachments",
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "playback_attachments",
        sa.Column("last_refresh_error", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_playback_attachments_item_rank",
        "playback_attachments",
        ["item_id", "preference_rank"],
    )


def downgrade() -> None:
    op.drop_index("ix_playback_attachments_item_rank", table_name="playback_attachments")
    op.drop_column("playback_attachments", "last_refresh_error")
    op.drop_column("playback_attachments", "last_refreshed_at")
    op.drop_column("playback_attachments", "expires_at")
    op.drop_column("playback_attachments", "preference_rank")
