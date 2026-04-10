from __future__ import annotations

import json
from pathlib import Path

import pytest

from filmu_py.plugins.trust import load_plugin_trust_store, verify_plugin_signature


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
