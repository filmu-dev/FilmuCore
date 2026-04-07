"""Add media item recovery attempt counter.

Revision ID: 20260315_0013
Revises: 20260315_0012
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260315_0013"
down_revision = "20260315_0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "media_items",
        sa.Column("recovery_attempt_count", sa.Integer(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    op.drop_column("media_items", "recovery_attempt_count")
