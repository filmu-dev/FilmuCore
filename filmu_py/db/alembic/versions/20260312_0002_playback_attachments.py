"""add playback attachment schema

Revision ID: 20260312_0002
Revises: 20260308_0001
Create Date: 2026-03-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260312_0002"
down_revision = "20260308_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "playback_attachments",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True, nullable=False),
        sa.Column("item_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("locator", sa.String(length=2048), nullable=False),
        sa.Column("source_key", sa.String(length=128), nullable=True),
        sa.Column("provider", sa.String(length=64), nullable=True),
        sa.Column("provider_download_id", sa.String(length=128), nullable=True),
        sa.Column("original_filename", sa.String(length=1024), nullable=True),
        sa.Column("file_size", sa.Integer(), nullable=True),
        sa.Column("local_path", sa.String(length=2048), nullable=True),
        sa.Column("restricted_url", sa.String(length=2048), nullable=True),
        sa.Column("unrestricted_url", sa.String(length=2048), nullable=True),
        sa.Column("is_preferred", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["item_id"], ["media_items.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_playback_attachments_item_id", "playback_attachments", ["item_id"])
    op.create_index("ix_playback_attachments_kind", "playback_attachments", ["kind"])
    op.create_index("ix_playback_attachments_provider", "playback_attachments", ["provider"])
    op.create_index(
        "ix_playback_attachments_item_preferred",
        "playback_attachments",
        ["item_id", "is_preferred"],
    )


def downgrade() -> None:
    op.drop_index("ix_playback_attachments_item_preferred", table_name="playback_attachments")
    op.drop_index("ix_playback_attachments_provider", table_name="playback_attachments")
    op.drop_index("ix_playback_attachments_kind", table_name="playback_attachments")
    op.drop_index("ix_playback_attachments_item_id", table_name="playback_attachments")
    op.drop_table("playback_attachments")
