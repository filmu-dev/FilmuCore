"""add playback attachment refresh state

Revision ID: 20260312_0004
Revises: 20260312_0003
Create Date: 2026-03-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260312_0004"
down_revision = "20260312_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "playback_attachments",
        sa.Column("refresh_state", sa.String(length=32), nullable=False, server_default="ready"),
    )


def downgrade() -> None:
    op.drop_column("playback_attachments", "refresh_state")
