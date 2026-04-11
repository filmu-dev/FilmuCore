"""Authorization helper regression tests."""

from __future__ import annotations

from filmu_py.authz import effective_permissions


def test_effective_permissions_respects_explicit_empty_role_grants() -> None:
    permissions = effective_permissions(
        roles=["platform:admin"],
        scopes=[],
        role_permission_grants={},
    )

    assert permissions == ()
