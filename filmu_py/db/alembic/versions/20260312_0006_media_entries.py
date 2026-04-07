"""add media entries

Revision ID: 20260312_0006
Revises: 20260312_0005
Create Date: 2026-03-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260312_0006"
down_revision = "20260312_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "media_entries",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True, nullable=False),
        sa.Column("item_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("source_attachment_id", postgresql.UUID(as_uuid=False), nullable=True),
        sa.Column(
            "entry_type", sa.String(length=32), nullable=False, server_default=sa.text("'media'")
        ),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("original_filename", sa.String(length=1024), nullable=True),
        sa.Column("local_path", sa.String(length=2048), nullable=True),
        sa.Column("download_url", sa.String(length=2048), nullable=True),
        sa.Column("unrestricted_url", sa.String(length=2048), nullable=True),
        sa.Column("provider", sa.String(length=64), nullable=True),
        sa.Column("provider_download_id", sa.String(length=128), nullable=True),
        sa.Column("provider_file_id", sa.String(length=128), nullable=True),
        sa.Column("provider_file_path", sa.String(length=2048), nullable=True),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
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
        sa.ForeignKeyConstraint(["item_id"], ["media_items.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["source_attachment_id"],
            ["playback_attachments.id"],
            ondelete="SET NULL",
        ),
    )
    op.create_index("ix_media_entries_item_id", "media_entries", ["item_id"])
    op.create_index(
        "ix_media_entries_source_attachment_id", "media_entries", ["source_attachment_id"]
    )
    op.create_index("ix_media_entries_kind", "media_entries", ["kind"])
    op.create_index("ix_media_entries_provider", "media_entries", ["provider"])
    op.create_index("ix_media_entries_item_created", "media_entries", ["item_id", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_media_entries_item_created", table_name="media_entries")
    op.drop_index("ix_media_entries_provider", table_name="media_entries")
    op.drop_index("ix_media_entries_kind", table_name="media_entries")
    op.drop_index("ix_media_entries_source_attachment_id", table_name="media_entries")
    op.drop_index("ix_media_entries_item_id", table_name="media_entries")
    op.drop_table("media_entries")
