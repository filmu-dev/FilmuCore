"""Add payload fields to consumer playback activity events.

Revision ID: 20260419_0029
Revises: 20260419_0028
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260419_0029"
down_revision = "20260419_0028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "consumer_playback_activity_events",
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.alter_column(
        "consumer_playback_activity_events",
        "payload",
        server_default=None,
    )


def downgrade() -> None:
    op.drop_column("consumer_playback_activity_events", "payload")
