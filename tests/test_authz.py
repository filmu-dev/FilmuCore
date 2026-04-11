from __future__ import annotations

from filmu_py.authz import describe_tenant_scope, evaluate_permissions


def test_evaluate_permissions_denies_cross_tenant_without_delegation() -> None:
    decision = evaluate_permissions(
        granted_permissions=("library:read",),
        required_permissions=("library:read",),
        actor_tenant_id="tenant-main",
        target_tenant_id="tenant-other",
        authorized_tenant_ids=("tenant-main",),
    )

    assert decision.allowed is False
    assert decision.reason == "tenant_forbidden"
    assert decision.tenant_scope == "self"


def test_evaluate_permissions_allows_cross_tenant_with_delegation() -> None:
    decision = evaluate_permissions(
        granted_permissions=("library:read",),
        required_permissions=("library:read",),
        actor_tenant_id="tenant-main",
        target_tenant_id="tenant-analytics",
        authorized_tenant_ids=("tenant-main", "tenant-analytics"),
    )

    assert decision.allowed is True
    assert decision.reason == "allowed"
    assert decision.tenant_scope == "delegated"
    assert decision.target_tenant_id == "tenant-analytics"


def test_describe_tenant_scope_marks_global_admin_scope() -> None:
    assert (
        describe_tenant_scope(
            actor_tenant_id="tenant-main",
            authorized_tenant_ids=("tenant-main",),
            granted_permissions=("*",),
        )
        == "all"
    )
