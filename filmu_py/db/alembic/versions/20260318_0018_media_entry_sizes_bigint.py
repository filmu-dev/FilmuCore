"""Promote media entry size tracking fields to bigint.

Revision ID: 20260318_0018
Revises: 20260318_0017
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260318_0018"
down_revision = "20260318_0017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "media_entries",
        "size_bytes",
        existing_type=sa.Integer(),
        type_=sa.BigInteger(),
        existing_nullable=True,
    )
    op.alter_column(
        "playback_attachments",
        "file_size",
        existing_type=sa.Integer(),
        type_=sa.BigInteger(),
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "playback_attachments",
        "file_size",
        existing_type=sa.BigInteger(),
        type_=sa.Integer(),
        existing_nullable=True,
    )
    op.alter_column(
        "media_entries",
        "size_bytes",
        existing_type=sa.BigInteger(),
        type_=sa.Integer(),
        existing_nullable=True,
    )
