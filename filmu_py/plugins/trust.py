"""Plugin trust-store loading and signature verification helpers."""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class TrustedSigningKey:
    """One trusted signing key loaded from the operator-managed trust store."""

    key_id: str
    secret: str
    algorithm: str = "hmac-sha256"
    status: str = "active"


@dataclass(frozen=True, slots=True)
class PluginTrustStore:
    """Parsed operator-managed trust store for plugin signature policy."""

    keys: dict[str, TrustedSigningKey]
    revoked_key_ids: frozenset[str]
    revoked_signatures: frozenset[str]
    source: str


@dataclass(frozen=True, slots=True)
class PluginSignatureVerification:
    """Result of evaluating one manifest signature against trust policy."""

    verified: bool
    reason: str
    trust_policy_decision: str
    trust_store_source: str | None


def load_plugin_trust_store(path: Path | None) -> PluginTrustStore | None:
    """Load one plugin trust store from JSON when configured."""

    if path is None:
        return None
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("plugin trust store must be a JSON object")

    raw_keys = raw.get("keys", {})
    if not isinstance(raw_keys, dict):
        raise ValueError("plugin trust store keys must be an object")

    keys: dict[str, TrustedSigningKey] = {}
    for key_id, payload in raw_keys.items():
        if not isinstance(payload, dict):
            raise ValueError(f"plugin trust store key '{key_id}' must be an object")
        secret = str(payload.get("secret", "")).strip()
        if not secret:
            raise ValueError(f"plugin trust store key '{key_id}' is missing secret")
        algorithm = str(payload.get("algorithm", "hmac-sha256")).strip().lower()
        if algorithm != "hmac-sha256":
            raise ValueError(f"plugin trust store key '{key_id}' uses unsupported algorithm")
        status = str(payload.get("status", "active")).strip().lower() or "active"
        keys[key_id] = TrustedSigningKey(
            key_id=key_id,
            secret=secret,
            algorithm=algorithm,
            status=status,
        )

    revoked_key_ids = frozenset(str(value).strip() for value in raw.get("revoked_key_ids", []))
    revoked_signatures = frozenset(
        _normalize_signature(str(value))
        for value in raw.get("revoked_signatures", [])
        if str(value).strip()
    )
    return PluginTrustStore(
        keys=keys,
        revoked_key_ids=revoked_key_ids,
        revoked_signatures=revoked_signatures,
        source=str(path),
    )


def verify_plugin_signature(
    *,
    source_sha256: str | None,
    signature: str | None,
    signing_key_id: str | None,
    distribution: str,
    trust_store: PluginTrustStore | None,
) -> PluginSignatureVerification:
    """Verify one plugin signature against the configured trust store."""

    if distribution == "builtin":
        return PluginSignatureVerification(
            verified=True,
            reason="builtin_trusted",
            trust_policy_decision="builtin",
            trust_store_source=(trust_store.source if trust_store is not None else None),
        )
    if signature is None:
        return PluginSignatureVerification(
            verified=False,
            reason="unsigned",
            trust_policy_decision="unsigned",
            trust_store_source=(trust_store.source if trust_store is not None else None),
        )
    if trust_store is None:
        return PluginSignatureVerification(
            verified=False,
            reason="trust_store_unavailable",
            trust_policy_decision="unverified",
            trust_store_source=None,
        )
    if signing_key_id is None or source_sha256 is None:
        return PluginSignatureVerification(
            verified=False,
            reason="signature_metadata_incomplete",
            trust_policy_decision="rejected",
            trust_store_source=trust_store.source,
        )
    if signing_key_id in trust_store.revoked_key_ids:
        return PluginSignatureVerification(
            verified=False,
            reason="signing_key_revoked",
            trust_policy_decision="rejected",
            trust_store_source=trust_store.source,
        )

    normalized_signature = _normalize_signature(signature)
    if normalized_signature in trust_store.revoked_signatures:
        return PluginSignatureVerification(
            verified=False,
            reason="signature_revoked",
            trust_policy_decision="rejected",
            trust_store_source=trust_store.source,
        )

    trusted_key = trust_store.keys.get(signing_key_id)
    if trusted_key is None:
        return PluginSignatureVerification(
            verified=False,
            reason="unknown_signing_key",
            trust_policy_decision="untrusted",
            trust_store_source=trust_store.source,
        )
    if trusted_key.status != "active":
        return PluginSignatureVerification(
            verified=False,
            reason=f"signing_key_{trusted_key.status}",
            trust_policy_decision="rejected",
            trust_store_source=trust_store.source,
        )

    expected = hmac.new(
        trusted_key.secret.encode("utf-8"),
        source_sha256.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected, normalized_signature):
        return PluginSignatureVerification(
            verified=False,
            reason="signature_invalid",
            trust_policy_decision="rejected",
            trust_store_source=trust_store.source,
        )

    return PluginSignatureVerification(
        verified=True,
        reason="signature_verified",
        trust_policy_decision="trusted",
        trust_store_source=trust_store.source,
    )


def _normalize_signature(value: str) -> str:
    """Normalize an operator-supplied signature string to raw lowercase hex."""

    normalized = value.strip().lower()
    if ":" in normalized:
        _algorithm, _, candidate = normalized.partition(":")
        return candidate.strip()
    return normalized
