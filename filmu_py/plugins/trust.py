"""Plugin trust-store loading and signature verification helpers."""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from pathlib import Path

_ALLOWED_PUBLISHER_STATUSES = {"active", "suspended", "revoked"}
_TRUST_LEVEL_ORDER = {"community": 0, "trusted": 1, "builtin": 2}


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
    publishers: dict[str, TrustedPublisherPolicy]
    revoked_key_ids: frozenset[str]
    revoked_signatures: frozenset[str]
    source: str


@dataclass(frozen=True, slots=True)
class TrustedPublisherPolicy:
    """One operator-approved publisher policy loaded from the trust store."""

    publisher: str
    allowed_release_channels: frozenset[str]
    allowed_distributions: frozenset[str]
    allowed_sandbox_profiles: frozenset[str]
    allowed_tenancy_modes: frozenset[str]
    allowed_permission_scopes: frozenset[str]
    status: str = "active"
    minimum_trust_level: str | None = None
    require_signature_verification: bool = True
    quarantine_on_violation: bool = False


@dataclass(frozen=True, slots=True)
class PluginSignatureVerification:
    """Result of evaluating one manifest signature against trust policy."""

    verified: bool
    reason: str
    trust_policy_decision: str
    trust_store_source: str | None


@dataclass(frozen=True, slots=True)
class PluginPublisherPolicyEvaluation:
    """Result of evaluating one manifest against operator publisher policy."""

    allowed: bool
    decision: str
    reason: str | None
    trust_store_source: str | None
    publisher_status: str | None = None
    quarantine_recommended: bool = False


def _load_policy_string_array(
    payload: dict[str, object],
    *,
    field_name: str,
) -> frozenset[str]:
    """Return one normalized string set for a publisher policy field."""

    raw_values = payload.get(field_name, [])
    if not isinstance(raw_values, list):
        raise ValueError(f"plugin trust store publisher field '{field_name}' must be an array")
    return frozenset(
        str(value).strip().lower()
        for value in raw_values
        if str(value).strip()
    )


def _load_policy_bool(
    payload: dict[str, object],
    *,
    field_name: str,
    default: bool,
) -> bool:
    """Return one validated boolean publisher policy field."""

    raw_value = payload.get(field_name, default)
    if not isinstance(raw_value, bool):
        raise ValueError(f"plugin trust store publisher field '{field_name}' must be a boolean")
    return raw_value


def _load_policy_optional_string(
    payload: dict[str, object],
    *,
    field_name: str,
    default: str | None = None,
) -> str | None:
    """Return one validated optional string publisher policy field."""

    raw_value = payload.get(field_name, default)
    if raw_value is None:
        return None
    if not isinstance(raw_value, str):
        raise ValueError(f"plugin trust store publisher field '{field_name}' must be a string")
    normalized = raw_value.strip().lower()
    return normalized or default


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

    raw_publishers = raw.get("publishers", {})
    if not isinstance(raw_publishers, dict):
        raise ValueError("plugin trust store publishers must be an object")
    publishers: dict[str, TrustedPublisherPolicy] = {}
    for publisher, payload in raw_publishers.items():
        if not isinstance(payload, dict):
            raise ValueError(f"plugin trust store publisher '{publisher}' must be an object")
        normalized_publisher = str(publisher).strip()
        if not normalized_publisher:
            raise ValueError("plugin trust store publisher names must not be empty")
        publishers[normalized_publisher] = TrustedPublisherPolicy(
            publisher=normalized_publisher,
            status=(
                _load_policy_optional_string(
                    payload,
                    field_name="status",
                    default="active",
                )
                or "active"
            ),
            allowed_release_channels=_load_policy_string_array(
                payload,
                field_name="allowed_release_channels",
            ),
            allowed_distributions=_load_policy_string_array(
                payload,
                field_name="allowed_distributions",
            ),
            allowed_sandbox_profiles=_load_policy_string_array(
                payload,
                field_name="allowed_sandbox_profiles",
            ),
            allowed_tenancy_modes=_load_policy_string_array(
                payload,
                field_name="allowed_tenancy_modes",
            ),
            allowed_permission_scopes=_load_policy_string_array(
                payload,
                field_name="allowed_permission_scopes",
            ),
            minimum_trust_level=_load_policy_optional_string(
                payload,
                field_name="minimum_trust_level",
            ),
            require_signature_verification=_load_policy_bool(
                payload,
                field_name="require_signature_verification",
                default=True,
            ),
            quarantine_on_violation=_load_policy_bool(
                payload,
                field_name="quarantine_on_violation",
                default=False,
            ),
        )
        if publishers[normalized_publisher].status not in _ALLOWED_PUBLISHER_STATUSES:
            raise ValueError(
                "plugin trust store publisher field 'status' must be one of: "
                "active, suspended, revoked"
            )
        minimum_trust_level = publishers[normalized_publisher].minimum_trust_level
        if minimum_trust_level is not None and minimum_trust_level not in _TRUST_LEVEL_ORDER:
            raise ValueError(
                "plugin trust store publisher field 'minimum_trust_level' must be one of: "
                "community, trusted, builtin"
            )

    raw_revoked_key_ids = raw.get("revoked_key_ids", [])
    if not isinstance(raw_revoked_key_ids, list):
        raise ValueError("plugin trust store revoked_key_ids must be an array")
    revoked_key_ids = frozenset(str(value).strip() for value in raw_revoked_key_ids if str(value).strip())

    raw_revoked_signatures = raw.get("revoked_signatures", [])
    if not isinstance(raw_revoked_signatures, list):
        raise ValueError("plugin trust store revoked_signatures must be an array")
    revoked_signatures = frozenset(
        _normalize_signature(str(value))
        for value in raw_revoked_signatures
        if str(value).strip()
    )
    return PluginTrustStore(
        keys=keys,
        publishers=publishers,
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


def evaluate_plugin_publisher_policy(
    *,
    publisher: str | None,
    release_channel: str,
    sandbox_profile: str,
    tenancy_mode: str,
    distribution: str,
    trust_level: str,
    permission_scopes: frozenset[str],
    signature_verified: bool,
    trust_store: PluginTrustStore | None,
) -> PluginPublisherPolicyEvaluation:
    """Evaluate one plugin manifest against operator publisher policy."""

    trust_store_source = trust_store.source if trust_store is not None else None
    if distribution == "builtin":
        return PluginPublisherPolicyEvaluation(
            allowed=True,
            decision="builtin",
            reason=None,
            trust_store_source=trust_store_source,
            publisher_status=None,
        )
    if trust_store is None:
        return PluginPublisherPolicyEvaluation(
            allowed=True,
            decision="unconfigured",
            reason="trust_store_unavailable",
            trust_store_source=None,
            publisher_status=None,
        )
    if not trust_store.publishers:
        return PluginPublisherPolicyEvaluation(
            allowed=True,
            decision="unconfigured",
            reason="publisher_policy_unconfigured",
            trust_store_source=trust_store_source,
            publisher_status=None,
        )
    if publisher is None:
        return PluginPublisherPolicyEvaluation(
            allowed=False,
            decision="rejected",
            reason="publisher_missing",
            trust_store_source=trust_store_source,
            publisher_status=None,
        )

    policy = trust_store.publishers.get(publisher)
    if policy is None:
        return PluginPublisherPolicyEvaluation(
            allowed=False,
            decision="untrusted",
            reason="publisher_unapproved",
            trust_store_source=trust_store_source,
            publisher_status=None,
        )
    if policy.status != "active":
        return PluginPublisherPolicyEvaluation(
            allowed=False,
            decision="rejected",
            reason=f"publisher_status_{policy.status}",
            trust_store_source=trust_store_source,
            publisher_status=policy.status,
            quarantine_recommended=(policy.status == "revoked"),
        )
    if policy.require_signature_verification and not signature_verified:
        return PluginPublisherPolicyEvaluation(
            allowed=False,
            decision="rejected",
            reason="publisher_requires_verified_signature",
            trust_store_source=trust_store_source,
            publisher_status=policy.status,
            quarantine_recommended=policy.quarantine_on_violation,
        )
    if policy.allowed_release_channels and release_channel not in policy.allowed_release_channels:
        return PluginPublisherPolicyEvaluation(
            allowed=False,
            decision="rejected",
            reason="release_channel_disallowed",
            trust_store_source=trust_store_source,
            publisher_status=policy.status,
            quarantine_recommended=policy.quarantine_on_violation,
        )
    if policy.allowed_distributions and distribution not in policy.allowed_distributions:
        return PluginPublisherPolicyEvaluation(
            allowed=False,
            decision="rejected",
            reason="distribution_disallowed",
            trust_store_source=trust_store_source,
            publisher_status=policy.status,
            quarantine_recommended=policy.quarantine_on_violation,
        )
    if policy.allowed_sandbox_profiles and sandbox_profile not in policy.allowed_sandbox_profiles:
        return PluginPublisherPolicyEvaluation(
            allowed=False,
            decision="rejected",
            reason="sandbox_profile_disallowed",
            trust_store_source=trust_store_source,
            publisher_status=policy.status,
            quarantine_recommended=policy.quarantine_on_violation,
        )
    if policy.allowed_tenancy_modes and tenancy_mode not in policy.allowed_tenancy_modes:
        return PluginPublisherPolicyEvaluation(
            allowed=False,
            decision="rejected",
            reason="tenancy_mode_disallowed",
            trust_store_source=trust_store_source,
            publisher_status=policy.status,
            quarantine_recommended=policy.quarantine_on_violation,
        )
    if (
        policy.minimum_trust_level is not None
        and _TRUST_LEVEL_ORDER.get(trust_level, -1)
        < _TRUST_LEVEL_ORDER[policy.minimum_trust_level]
    ):
        return PluginPublisherPolicyEvaluation(
            allowed=False,
            decision="rejected",
            reason="trust_level_below_policy",
            trust_store_source=trust_store_source,
            publisher_status=policy.status,
            quarantine_recommended=policy.quarantine_on_violation,
        )
    if policy.allowed_permission_scopes and not permission_scopes.issubset(
        policy.allowed_permission_scopes
    ):
        return PluginPublisherPolicyEvaluation(
            allowed=False,
            decision="rejected",
            reason="permission_scope_disallowed",
            trust_store_source=trust_store_source,
            publisher_status=policy.status,
            quarantine_recommended=policy.quarantine_on_violation,
        )
    return PluginPublisherPolicyEvaluation(
        allowed=True,
        decision="allowed",
        reason=None,
        trust_store_source=trust_store_source,
        publisher_status=policy.status,
    )


def _normalize_signature(value: str) -> str:
    """Normalize an operator-supplied signature string to raw lowercase hex."""

    normalized = value.strip().lower()
    if ":" in normalized:
        _algorithm, _, candidate = normalized.partition(":")
        return candidate.strip()
    return normalized
