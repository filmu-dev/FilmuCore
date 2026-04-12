"""Add durable authorization-decision audit ledger.

Revision ID: 20260412_0026
Revises: 20260411_0025
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260412_0026"
down_revision = "20260411_0025"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "authorization_decision_audit",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("path", sa.String(length=512), nullable=False),
        sa.Column("method", sa.String(length=16), nullable=False),
        sa.Column("resource_scope", sa.String(length=64), nullable=False),
        sa.Column("actor_id", sa.String(length=256), nullable=False),
        sa.Column("actor_type", sa.String(length=64), nullable=False),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("target_tenant_id", sa.String(length=64), nullable=False),
        sa.Column(
            "required_permissions",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "matched_permissions",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "missing_permissions",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "constrained_permissions",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "constraint_failures",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "allowed",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("reason", sa.String(length=64), nullable=False),
        sa.Column("tenant_scope", sa.String(length=32), nullable=False),
        sa.Column("authentication_mode", sa.String(length=32), nullable=False),
        sa.Column("access_policy_version", sa.String(length=64), nullable=False),
        sa.Column("access_policy_source", sa.String(length=64), nullable=False),
        sa.Column("oidc_issuer", sa.String(length=256), nullable=True),
        sa.Column("oidc_subject", sa.String(length=256), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_authorization_decision_audit_occurred_at"),
        "authorization_decision_audit",
        ["occurred_at"],
        unique=False,
    )
    op.create_index(
        op.f("ix_authorization_decision_audit_resource_scope"),
        "authorization_decision_audit",
        ["resource_scope"],
        unique=False,
    )
    op.create_index(
        op.f("ix_authorization_decision_audit_actor_id"),
        "authorization_decision_audit",
        ["actor_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_authorization_decision_audit_actor_type"),
        "authorization_decision_audit",
        ["actor_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_authorization_decision_audit_tenant_id"),
        "authorization_decision_audit",
        ["tenant_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_authorization_decision_audit_target_tenant_id"),
        "authorization_decision_audit",
        ["target_tenant_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_authorization_decision_audit_allowed"),
        "authorization_decision_audit",
        ["allowed"],
        unique=False,
    )
    op.create_index(
        op.f("ix_authorization_decision_audit_reason"),
        "authorization_decision_audit",
        ["reason"],
        unique=False,
    )
    op.create_index(
        op.f("ix_authorization_decision_audit_tenant_scope"),
        "authorization_decision_audit",
        ["tenant_scope"],
        unique=False,
    )
    op.create_index(
        op.f("ix_authorization_decision_audit_authentication_mode"),
        "authorization_decision_audit",
        ["authentication_mode"],
        unique=False,
    )
    op.create_index(
        op.f("ix_authorization_decision_audit_access_policy_version"),
        "authorization_decision_audit",
        ["access_policy_version"],
        unique=False,
    )
    op.create_index(
        "ix_authorization_decision_audit_tenant_occurred",
        "authorization_decision_audit",
        ["tenant_id", "occurred_at"],
        unique=False,
    )
    op.create_index(
        "ix_authorization_decision_audit_actor_occurred",
        "authorization_decision_audit",
        ["actor_id", "occurred_at"],
        unique=False,
    )
    op.create_index(
        "ix_authorization_decision_audit_allowed_occurred",
        "authorization_decision_audit",
        ["allowed", "occurred_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_authorization_decision_audit_allowed_occurred",
        table_name="authorization_decision_audit",
    )
    op.drop_index(
        "ix_authorization_decision_audit_actor_occurred",
        table_name="authorization_decision_audit",
    )
    op.drop_index(
        "ix_authorization_decision_audit_tenant_occurred",
        table_name="authorization_decision_audit",
    )
    op.drop_index(
        op.f("ix_authorization_decision_audit_access_policy_version"),
        table_name="authorization_decision_audit",
    )
    op.drop_index(
        op.f("ix_authorization_decision_audit_authentication_mode"),
        table_name="authorization_decision_audit",
    )
    op.drop_index(
        op.f("ix_authorization_decision_audit_tenant_scope"),
        table_name="authorization_decision_audit",
    )
    op.drop_index(
        op.f("ix_authorization_decision_audit_reason"),
        table_name="authorization_decision_audit",
    )
    op.drop_index(
        op.f("ix_authorization_decision_audit_allowed"),
        table_name="authorization_decision_audit",
    )
    op.drop_index(
        op.f("ix_authorization_decision_audit_target_tenant_id"),
        table_name="authorization_decision_audit",
    )
    op.drop_index(
        op.f("ix_authorization_decision_audit_tenant_id"),
        table_name="authorization_decision_audit",
    )
    op.drop_index(
        op.f("ix_authorization_decision_audit_actor_type"),
        table_name="authorization_decision_audit",
    )
    op.drop_index(
        op.f("ix_authorization_decision_audit_actor_id"),
        table_name="authorization_decision_audit",
    )
    op.drop_index(
        op.f("ix_authorization_decision_audit_resource_scope"),
        table_name="authorization_decision_audit",
    )
    op.drop_index(
        op.f("ix_authorization_decision_audit_occurred_at"),
        table_name="authorization_decision_audit",
    )
    op.drop_table("authorization_decision_audit")
