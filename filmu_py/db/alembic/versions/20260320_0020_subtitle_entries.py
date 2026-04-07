"""Add subtitle entry persistence.

Revision ID: 20260320_0020
Revises: 20260320_0019
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision = "20260320_0020"
down_revision = "20260320_0019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "subtitle_entries",
        sa.Column("id", UUID(as_uuid=False), primary_key=True, nullable=False),
        sa.Column(
            "item_id",
            UUID(as_uuid=False),
            sa.ForeignKey("media_items.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("language", sa.String(length=10), nullable=False),
        sa.Column("format", sa.String(length=20), nullable=False),
        sa.Column("source", sa.String(length=50), nullable=False, server_default=sa.text("'unknown'")),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("file_path", sa.Text(), nullable=True),
        sa.Column("is_default", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("is_forced", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("provider_subtitle_id", sa.String(length=200), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(
        "ix_subtitle_entries_item_id",
        "subtitle_entries",
        ["item_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_subtitle_entries_item_id", table_name="subtitle_entries")
    op.drop_table("subtitle_entries")
