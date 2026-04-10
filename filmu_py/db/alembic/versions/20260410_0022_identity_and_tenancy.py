"""Add identity, service-account, and tenant ownership tables.

Revision ID: 20260410_0022
Revises: 20260322_0021
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260410_0022"
down_revision = "20260322_0021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "tenants",
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("slug", sa.String(length=128), nullable=False),
        sa.Column("display_name", sa.String(length=256), nullable=False),
        sa.Column(
            "kind",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text("'tenant'"),
        ),
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text("'active'"),
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
    op.create_index(op.f("ix_tenants_slug"), "tenants", ["slug"], unique=True)

    op.execute(
        sa.text(
            """
            INSERT INTO tenants (id, slug, display_name, kind, status)
            VALUES ('global', 'global', 'Global', 'system', 'active')
            ON CONFLICT (id) DO NOTHING
            """
        )
    )

    op.create_table(
        "principals",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("principal_key", sa.String(length=256), nullable=False),
        sa.Column("principal_type", sa.String(length=32), nullable=False),
        sa.Column("authentication_mode", sa.String(length=32), nullable=False),
        sa.Column("display_name", sa.String(length=256), nullable=True),
        sa.Column("roles", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("scopes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text("'active'"),
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
        sa.Column("last_authenticated_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_principals_tenant_id"), "principals", ["tenant_id"], unique=False)
    op.create_index(
        op.f("ix_principals_principal_key"),
        "principals",
        ["principal_key"],
        unique=True,
    )
    op.create_index(
        op.f("ix_principals_principal_type"),
        "principals",
        ["principal_type"],
        unique=False,
    )

    op.create_table(
        "service_accounts",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("principal_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("api_key_id", sa.String(length=128), nullable=False),
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text("'active'"),
        ),
        sa.Column("description", sa.String(length=512), nullable=True),
        sa.Column("last_authenticated_at", sa.DateTime(timezone=True), nullable=True),
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
        sa.ForeignKeyConstraint(["principal_id"], ["principals.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("principal_id"),
    )
    op.create_index(
        op.f("ix_service_accounts_principal_id"),
        "service_accounts",
        ["principal_id"],
        unique=True,
    )
    op.create_index(
        op.f("ix_service_accounts_api_key_id"),
        "service_accounts",
        ["api_key_id"],
        unique=True,
    )

    op.add_column(
        "media_items",
        sa.Column(
            "tenant_id",
            sa.String(length=64),
            nullable=False,
            server_default=sa.text("'global'"),
        ),
    )
    op.create_index(op.f("ix_media_items_tenant_id"), "media_items", ["tenant_id"], unique=False)
    op.create_foreign_key(
        "fk_media_items_tenant_id_tenants",
        "media_items",
        "tenants",
        ["tenant_id"],
        ["id"],
        ondelete="RESTRICT",
    )

    op.add_column(
        "item_requests",
        sa.Column(
            "tenant_id",
            sa.String(length=64),
            nullable=False,
            server_default=sa.text("'global'"),
        ),
    )
    op.create_index(
        op.f("ix_item_requests_tenant_id"),
        "item_requests",
        ["tenant_id"],
        unique=False,
    )
    op.create_foreign_key(
        "fk_item_requests_tenant_id_tenants",
        "item_requests",
        "tenants",
        ["tenant_id"],
        ["id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    op.drop_constraint("fk_item_requests_tenant_id_tenants", "item_requests", type_="foreignkey")
    op.drop_index(op.f("ix_item_requests_tenant_id"), table_name="item_requests")
    op.drop_column("item_requests", "tenant_id")

    op.drop_constraint("fk_media_items_tenant_id_tenants", "media_items", type_="foreignkey")
    op.drop_index(op.f("ix_media_items_tenant_id"), table_name="media_items")
    op.drop_column("media_items", "tenant_id")

    op.drop_index(op.f("ix_service_accounts_api_key_id"), table_name="service_accounts")
    op.drop_index(op.f("ix_service_accounts_principal_id"), table_name="service_accounts")
    op.drop_table("service_accounts")

    op.drop_index(op.f("ix_principals_principal_type"), table_name="principals")
    op.drop_index(op.f("ix_principals_principal_key"), table_name="principals")
    op.drop_index(op.f("ix_principals_tenant_id"), table_name="principals")
    op.drop_table("principals")

    op.drop_index(op.f("ix_tenants_slug"), table_name="tenants")
    op.drop_table("tenants")
