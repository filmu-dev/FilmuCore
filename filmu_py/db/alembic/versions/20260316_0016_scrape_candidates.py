"""Add raw scrape candidate persistence table.

Revision ID: 20260316_0016
Revises: 20260316_0015
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260316_0016"
down_revision = "20260316_0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "scrape_candidates",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("item_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("info_hash", sa.String(length=128), nullable=False),
        sa.Column("raw_title", sa.String(length=2048), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["item_id"], ["media_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("item_id", "info_hash", name="uq_scrape_candidates_item_info_hash"),
    )
    op.create_index("ix_scrape_candidates_item_id", "scrape_candidates", ["item_id"], unique=False)
    op.create_index(
        "ix_scrape_candidates_info_hash", "scrape_candidates", ["info_hash"], unique=False
    )
    op.create_index(
        "ix_scrape_candidates_provider", "scrape_candidates", ["provider"], unique=False
    )
    op.create_index(
        "ix_scrape_candidates_item_created",
        "scrape_candidates",
        ["item_id", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_scrape_candidates_item_created", table_name="scrape_candidates")
    op.drop_index("ix_scrape_candidates_provider", table_name="scrape_candidates")
    op.drop_index("ix_scrape_candidates_info_hash", table_name="scrape_candidates")
    op.drop_index("ix_scrape_candidates_item_id", table_name="scrape_candidates")
    op.drop_table("scrape_candidates")
