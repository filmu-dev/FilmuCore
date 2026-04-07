"""Add first-class item request intent records.

Revision ID: 20260314_0009
Revises: 20260312_0008
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260314_0009"
down_revision = "20260312_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "item_requests",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("external_ref", sa.String(length=128), nullable=False),
        sa.Column("media_item_id", postgresql.UUID(as_uuid=False), nullable=True),
        sa.Column("media_type", sa.String(length=32), nullable=False),
        sa.Column("requested_title", sa.String(length=512), nullable=False),
        sa.Column("request_source", sa.String(length=64), nullable=False),
        sa.Column("request_count", sa.Integer(), nullable=False, server_default="1"),
        sa.Column(
            "first_requested_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "last_requested_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["media_item_id"], ["media_items.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("external_ref", name="uq_item_requests_external_ref"),
    )
    op.create_index(
        "ix_item_requests_external_ref", "item_requests", ["external_ref"], unique=False
    )
    op.create_index(
        "ix_item_requests_media_item_id", "item_requests", ["media_item_id"], unique=False
    )


def downgrade() -> None:
    op.drop_index("ix_item_requests_media_item_id", table_name="item_requests")
    op.drop_index("ix_item_requests_external_ref", table_name="item_requests")
    op.drop_table("item_requests")
