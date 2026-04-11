"""Add persisted access-policy revision inventory.

Revision ID: 20260411_0024
Revises: 20260410_0023
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260411_0024"
down_revision = "20260410_0023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "access_policy_revisions",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("version", sa.String(length=64), nullable=False),
        sa.Column(
            "source",
            sa.String(length=64),
            nullable=False,
            server_default=sa.text("'settings_bootstrap'"),
        ),
        sa.Column(
            "policy_data",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
        sa.Column(
            "activated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_access_policy_revisions_version"),
        "access_policy_revisions",
        ["version"],
        unique=True,
    )
    op.create_index(
        op.f("ix_access_policy_revisions_is_active"),
        "access_policy_revisions",
        ["is_active"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_access_policy_revisions_is_active"), table_name="access_policy_revisions")
    op.drop_index(op.f("ix_access_policy_revisions_version"), table_name="access_policy_revisions")
    op.drop_table("access_policy_revisions")
