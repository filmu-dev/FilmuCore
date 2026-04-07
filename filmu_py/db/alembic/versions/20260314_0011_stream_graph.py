"""Add stream candidate graph persistence tables.

Revision ID: 20260314_0011
Revises: 20260314_0010
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260314_0011"
down_revision = "20260314_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "streams",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("media_item_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("infohash", sa.String(length=128), nullable=False),
        sa.Column("raw_title", sa.String(length=2048), nullable=False),
        sa.Column(
            "parsed_title",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("rank", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("lev_ratio", sa.Float(), nullable=True),
        sa.Column("resolution", sa.String(length=32), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["media_item_id"], ["media_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_streams_media_item_id", "streams", ["media_item_id"], unique=False)
    op.create_index("ix_streams_infohash", "streams", ["infohash"], unique=False)
    op.create_index("ix_streams_resolution", "streams", ["resolution"], unique=False)
    op.create_index(
        "ix_streams_media_item_created", "streams", ["media_item_id", "created_at"], unique=False
    )

    op.create_table(
        "stream_blacklist_relations",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("media_item_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("stream_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["media_item_id"], ["media_items.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["stream_id"], ["streams.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("media_item_id", "stream_id", name="uq_stream_blacklist_media_stream"),
    )
    op.create_index(
        "ix_stream_blacklist_relations_media_item_id",
        "stream_blacklist_relations",
        ["media_item_id"],
        unique=False,
    )
    op.create_index(
        "ix_stream_blacklist_relations_stream_id",
        "stream_blacklist_relations",
        ["stream_id"],
        unique=False,
    )
    op.create_index(
        "ix_stream_blacklist_media_item_created",
        "stream_blacklist_relations",
        ["media_item_id", "created_at"],
        unique=False,
    )

    op.create_table(
        "stream_relations",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("parent_stream_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("child_stream_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["parent_stream_id"], ["streams.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["child_stream_id"], ["streams.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "parent_stream_id", "child_stream_id", name="uq_stream_relations_parent_child"
        ),
    )
    op.create_index(
        "ix_stream_relations_parent_stream_id",
        "stream_relations",
        ["parent_stream_id"],
        unique=False,
    )
    op.create_index(
        "ix_stream_relations_child_stream_id", "stream_relations", ["child_stream_id"], unique=False
    )
    op.create_index(
        "ix_stream_relations_parent_created",
        "stream_relations",
        ["parent_stream_id", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_stream_relations_parent_created", table_name="stream_relations")
    op.drop_index("ix_stream_relations_child_stream_id", table_name="stream_relations")
    op.drop_index("ix_stream_relations_parent_stream_id", table_name="stream_relations")
    op.drop_table("stream_relations")

    op.drop_index("ix_stream_blacklist_media_item_created", table_name="stream_blacklist_relations")
    op.drop_index(
        "ix_stream_blacklist_relations_stream_id", table_name="stream_blacklist_relations"
    )
    op.drop_index(
        "ix_stream_blacklist_relations_media_item_id", table_name="stream_blacklist_relations"
    )
    op.drop_table("stream_blacklist_relations")

    op.drop_index("ix_streams_media_item_created", table_name="streams")
    op.drop_index("ix_streams_resolution", table_name="streams")
    op.drop_index("ix_streams_infohash", table_name="streams")
    op.drop_index("ix_streams_media_item_id", table_name="streams")
    op.drop_table("streams")
