"""Promote scrape candidate size tracking to bigint.

Revision ID: 20260318_0017
Revises: 20260316_0016
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260318_0017"
down_revision = "20260316_0016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "scrape_candidates",
        "size_bytes",
        existing_type=sa.Integer(),
        type_=sa.BigInteger(),
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "scrape_candidates",
        "size_bytes",
        existing_type=sa.BigInteger(),
        type_=sa.Integer(),
        existing_nullable=True,
    )
