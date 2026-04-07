"""Add partial range tracking to item requests.

Revision ID: 20260320_0019
Revises: 20260318_0018
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260320_0019"
down_revision = "20260318_0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "item_requests",
        sa.Column(
            "requested_seasons",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.add_column(
        "item_requests",
        sa.Column(
            "requested_episodes",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.add_column(
        "item_requests",
        sa.Column(
            "is_partial",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )


def downgrade() -> None:
    op.drop_column("item_requests", "is_partial")
    op.drop_column("item_requests", "requested_episodes")
    op.drop_column("item_requests", "requested_seasons")
