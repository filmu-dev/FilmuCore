"""Add media-item retry cooldown timestamp.

Revision ID: 20260322_0021
Revises: 20260320_0020
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260322_0021"
down_revision = "20260320_0020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "media_items",
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("media_items", "next_retry_at")
