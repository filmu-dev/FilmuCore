"""Scope item request uniqueness by tenant.

Revision ID: 20260410_0023
Revises: 20260410_0022
"""

from __future__ import annotations

from alembic import op

revision = "20260410_0023"
down_revision = "20260410_0022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("uq_item_requests_external_ref", "item_requests", type_="unique")
    op.create_unique_constraint(
        "uq_item_requests_tenant_external_ref",
        "item_requests",
        ["tenant_id", "external_ref"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_item_requests_tenant_external_ref", "item_requests", type_="unique")
    op.create_unique_constraint(
        "uq_item_requests_external_ref",
        "item_requests",
        ["external_ref"],
    )
