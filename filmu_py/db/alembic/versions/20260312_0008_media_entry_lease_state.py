"""add media entry lease state

Revision ID: 20260312_0008
Revises: 20260312_0007
Create Date: 2026-03-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260312_0008"
down_revision = "20260312_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "media_entries",
        sa.Column("refresh_state", sa.String(length=32), nullable=False, server_default="ready"),
    )
    op.add_column(
        "media_entries",
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "media_entries",
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "media_entries",
        sa.Column("last_refresh_error", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_media_entries_item_refresh_state",
        "media_entries",
        ["item_id", "refresh_state"],
    )


def downgrade() -> None:
    op.drop_index("ix_media_entries_item_refresh_state", table_name="media_entries")
    op.drop_column("media_entries", "last_refresh_error")
    op.drop_column("media_entries", "last_refreshed_at")
    op.drop_column("media_entries", "expires_at")
    op.drop_column("media_entries", "refresh_state")
