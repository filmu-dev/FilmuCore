"""add playback attachment provider file identity

Revision ID: 20260312_0005
Revises: 20260312_0004
Create Date: 2026-03-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260312_0005"
down_revision = "20260312_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "playback_attachments",
        sa.Column("provider_file_id", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "playback_attachments",
        sa.Column("provider_file_path", sa.String(length=2048), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("playback_attachments", "provider_file_path")
    op.drop_column("playback_attachments", "provider_file_id")
