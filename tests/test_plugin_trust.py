from __future__ import annotations

import json
from pathlib import Path

import pytest

from filmu_py.plugins.trust import (
    evaluate_plugin_publisher_policy,
    load_plugin_trust_store,
    verify_plugin_signature,
)


def test_load_plugin_trust_store_rejects_non_array_revocations(tmp_path: Path) -> None:
    trust_store_path = tmp_path / "trust-store.json"
    trust_store_path.write_text(
        json.dumps(
            {
                "keys": {
                    "key-1": {
                        "secret": "top-secret",
                    }
                },
                "revoked_key_ids": "key-1",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="revoked_key_ids must be an array"):
        load_plugin_trust_store(trust_store_path)


def test_verify_plugin_signature_does_not_auto_trust_builtin_distribution() -> None:
    verification = verify_plugin_signature(
        source_sha256="abc123",
        signature="hmac-sha256:" + ("a" * 64),
        signing_key_id="key-1",
        distribution="builtin",
        trust_store=None,
    )

    assert verification.verified is False
    assert verification.reason == "trust_store_unavailable"
    assert verification.trust_policy_decision == "unverified"


def test_load_plugin_trust_store_rejects_string_publisher_policy_arrays(tmp_path: Path) -> None:
    trust_store_path = tmp_path / "trust-store.json"
    trust_store_path.write_text(
        json.dumps(
            {
                "publishers": {
                    "example": {
                        "allowed_release_channels": "stable",
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="plugin trust store publisher field 'allowed_release_channels' must be an array",
    ):
        load_plugin_trust_store(trust_store_path)


def test_load_plugin_trust_store_rejects_non_boolean_signature_policy(tmp_path: Path) -> None:
    trust_store_path = tmp_path / "trust-store.json"
    trust_store_path.write_text(
        json.dumps(
            {
                "publishers": {
                    "example": {
                        "require_signature_verification": "yes",
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="plugin trust store publisher field 'require_signature_verification' must be a boolean",
    ):
        load_plugin_trust_store(trust_store_path)


def test_load_plugin_trust_store_reads_extended_publisher_policy_fields(tmp_path: Path) -> None:
    trust_store_path = tmp_path / "trust-store.json"
    trust_store_path.write_text(
        json.dumps(
            {
                "publishers": {
                    "example": {
                        "status": "suspended",
                        "allowed_distributions": ["filesystem"],
                        "allowed_permission_scopes": ["graphql:extend"],
                        "minimum_trust_level": "trusted",
                        "quarantine_on_violation": True,
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    trust_store = load_plugin_trust_store(trust_store_path)

    assert trust_store is not None
    policy = trust_store.publishers["example"]
    assert policy.status == "suspended"
    assert policy.allowed_distributions == frozenset({"filesystem"})
    assert policy.allowed_permission_scopes == frozenset({"graphql:extend"})
    assert policy.minimum_trust_level == "trusted"
    assert policy.quarantine_on_violation is True


def test_evaluate_plugin_publisher_policy_rejects_revoked_publishers(tmp_path: Path) -> None:
    trust_store_path = tmp_path / "operator-trust-store.json"
    store_payload = {
        "keys": {},
        "publishers": {
            "example": {
                "status": "revoked",
                "quarantine_on_violation": True,
            }
        },
    }
    trust_store_path.write_text(json.dumps(store_payload), encoding="utf-8")
    parsed = load_plugin_trust_store(trust_store_path)
    assert parsed is not None

    evaluation = evaluate_plugin_publisher_policy(
        publisher="example",
        release_channel="stable",
        sandbox_profile="restricted",
        tenancy_mode="shared",
        distribution="filesystem",
        trust_level="trusted",
        permission_scopes=frozenset({"graphql:extend"}),
        signature_verified=True,
        trust_store=parsed,
    )

    assert evaluation.allowed is False
    assert evaluation.reason == "publisher_status_revoked"
    assert evaluation.publisher_status == "revoked"
    assert evaluation.quarantine_recommended is True
