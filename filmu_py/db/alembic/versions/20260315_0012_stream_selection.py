"""Add durable stream selection flag.

Revision ID: 20260315_0012
Revises: 20260314_0011
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260315_0012"
down_revision = "20260314_0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "streams",
        sa.Column("selected", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.create_index(
        "ix_streams_media_item_selected",
        "streams",
        ["media_item_id", "selected"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_streams_media_item_selected", table_name="streams")
    op.drop_column("streams", "selected")
