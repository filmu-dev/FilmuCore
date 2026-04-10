"""Scope item request uniqueness by tenant.

Revision ID: 20260410_0023
Revises: 20260410_0022
"""

from __future__ import annotations

import sqlalchemy as sa
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
    bind = op.get_bind()
    duplicate = bind.execute(
        sa.text(
            """
            SELECT external_ref, COUNT(*) AS duplicate_count
            FROM item_requests
            WHERE external_ref IS NOT NULL
            GROUP BY external_ref
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC, external_ref
            LIMIT 1
            """
        )
    ).mappings().first()
    if duplicate is not None:
        raise RuntimeError(
            "cannot restore global item_requests external_ref uniqueness while "
            f"duplicate external_ref '{duplicate['external_ref']}' exists across tenants "
            f"({duplicate['duplicate_count']} rows)"
        )

    op.drop_constraint("uq_item_requests_tenant_external_ref", "item_requests", type_="unique")
    op.create_unique_constraint(
        "uq_item_requests_external_ref",
        "item_requests",
        ["external_ref"],
    )
