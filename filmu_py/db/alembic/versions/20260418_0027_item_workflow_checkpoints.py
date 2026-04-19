"""Add durable item workflow checkpoints.

Revision ID: 20260418_0027
Revises: 20260412_0026
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260418_0027"
down_revision = "20260412_0026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "item_workflow_checkpoints",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("item_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column(
            "workflow_name",
            sa.String(length=64),
            nullable=False,
            server_default=sa.text("'item_pipeline'"),
        ),
        sa.Column("stage_name", sa.String(length=64), nullable=False),
        sa.Column(
            "resume_stage",
            sa.String(length=64),
            nullable=False,
            server_default=sa.text("'none'"),
        ),
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column("item_request_id", sa.String(length=64), nullable=True),
        sa.Column("selected_stream_id", sa.String(length=64), nullable=True),
        sa.Column("provider", sa.String(length=64), nullable=True),
        sa.Column("provider_download_id", sa.String(length=256), nullable=True),
        sa.Column(
            "checkpoint_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "compensation_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
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
        sa.ForeignKeyConstraint(["item_id"], ["media_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "item_id",
            "workflow_name",
            name="uq_item_workflow_checkpoint_identity",
        ),
    )
    op.create_index(
        op.f("ix_item_workflow_checkpoints_item_id"),
        "item_workflow_checkpoints",
        ["item_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_item_workflow_checkpoints_stage_name"),
        "item_workflow_checkpoints",
        ["stage_name"],
        unique=False,
    )
    op.create_index(
        op.f("ix_item_workflow_checkpoints_resume_stage"),
        "item_workflow_checkpoints",
        ["resume_stage"],
        unique=False,
    )
    op.create_index(
        op.f("ix_item_workflow_checkpoints_status"),
        "item_workflow_checkpoints",
        ["status"],
        unique=False,
    )
    op.create_index(
        op.f("ix_item_workflow_checkpoints_item_request_id"),
        "item_workflow_checkpoints",
        ["item_request_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_item_workflow_checkpoints_selected_stream_id"),
        "item_workflow_checkpoints",
        ["selected_stream_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_item_workflow_checkpoints_provider"),
        "item_workflow_checkpoints",
        ["provider"],
        unique=False,
    )
    op.create_index(
        "ix_item_workflow_checkpoints_resume_status",
        "item_workflow_checkpoints",
        ["resume_stage", "status"],
        unique=False,
    )
    op.create_index(
        "ix_item_workflow_checkpoints_updated_at",
        "item_workflow_checkpoints",
        ["updated_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_item_workflow_checkpoints_updated_at",
        table_name="item_workflow_checkpoints",
    )
    op.drop_index(
        "ix_item_workflow_checkpoints_resume_status",
        table_name="item_workflow_checkpoints",
    )
    op.drop_index(
        op.f("ix_item_workflow_checkpoints_provider"),
        table_name="item_workflow_checkpoints",
    )
    op.drop_index(
        op.f("ix_item_workflow_checkpoints_selected_stream_id"),
        table_name="item_workflow_checkpoints",
    )
    op.drop_index(
        op.f("ix_item_workflow_checkpoints_item_request_id"),
        table_name="item_workflow_checkpoints",
    )
    op.drop_index(
        op.f("ix_item_workflow_checkpoints_status"),
        table_name="item_workflow_checkpoints",
    )
    op.drop_index(
        op.f("ix_item_workflow_checkpoints_resume_stage"),
        table_name="item_workflow_checkpoints",
    )
    op.drop_index(
        op.f("ix_item_workflow_checkpoints_stage_name"),
        table_name="item_workflow_checkpoints",
    )
    op.drop_index(
        op.f("ix_item_workflow_checkpoints_item_id"),
        table_name="item_workflow_checkpoints",
    )
    op.drop_table("item_workflow_checkpoints")
