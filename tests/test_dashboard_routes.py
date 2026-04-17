"""Dashboard-essential compatibility route tests."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from authlib.jose import jwt
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import AnyUrl, SecretStr
from redis.exceptions import ResponseError

from filmu_py.api.router import create_api_router
from filmu_py.api.routes import default as default_routes
from filmu_py.api.routes import runtime_governance as runtime_governance_routes
from filmu_py.api.routes import stream as stream_routes
from filmu_py.api.routes.internal_vfs import router as internal_vfs_router
from filmu_py.config import Settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.chunk_engine import ChunkCache
from filmu_py.core.event_bus import EventBus
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.graphql.plugin_registry import GraphQLPluginRegistry
from filmu_py.plugins import TestPluginContext
from filmu_py.plugins.builtins import register_builtin_plugins
from filmu_py.plugins.interfaces import StreamControlInput, StreamControlResult
from filmu_py.plugins.manifest import PluginManifest
from filmu_py.plugins.registry import PluginCapabilityKind, PluginRegistry
from filmu_py.resources import AppResources
from filmu_py.services.access_policy import snapshot_from_settings
from filmu_py.services.debrid import DownloaderAccountService
from filmu_py.services.media import StatsProjection, StatsYearReleaseRecord
from filmuvfs.catalog.v1 import catalog_pb2


class DummyRedis:
    """Minimal Redis stub used by route-level tests without network dependencies."""

    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.integers: dict[str, int] = {}
        self.sorted_sets: dict[str, dict[str, float]] = {}
        self.lists: dict[str, list[bytes]] = {}
        self.streams: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self.stream_groups: dict[str, dict[str, set[str]]] = {}

    def ping(self, **kwargs: Any) -> bool:
        _ = kwargs
        return True

    async def get(self, key: str) -> bytes | None:
        return self.values.get(key)

    async def set(self, key: str, value: bytes, ex: int | None = None) -> None:
        _ = ex
        self.values[key] = value

    async def delete(self, key: str) -> None:
        self.values.pop(key, None)

    async def incr(self, key: str) -> int:
        self.integers[key] = self.integers.get(key, 0) + 1
        return self.integers[key]

    async def expire(self, key: str, seconds: int) -> bool:
        _ = (key, seconds)
        return True

    async def xadd(
        self,
        name: str,
        fields: dict[str, str],
        *,
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> str:
        _ = (id, approximate)
        bucket = self.streams.setdefault(name, [])
        event_id = f"{len(bucket) + 1}-0"
        bucket.append((event_id, fields))
        if maxlen is not None and len(bucket) > maxlen:
            del bucket[: len(bucket) - maxlen]
        return event_id

    async def xread(
        self,
        streams: dict[str, str],
        *,
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        _ = block
        rows: list[tuple[str, list[tuple[str, dict[str, str]]]]] = []
        for stream_name, offset in streams.items():
            selected = [
                item for item in self.streams.get(stream_name, []) if _stream_id_gt(item[0], offset)
            ]
            if count is not None:
                selected = selected[:count]
            rows.append((stream_name, selected))
        return rows

    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "$",
        *,
        mkstream: bool = False,
    ) -> bool:
        _ = (id, mkstream)
        groups = self.stream_groups.setdefault(name, {})
        if groupname in groups:
            raise ResponseError("BUSYGROUP Consumer Group name already exists")
        groups[groupname] = set()
        return True

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        *,
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        _ = (consumername, block)
        rows: list[tuple[str, list[tuple[str, dict[str, str]]]]] = []
        for stream_name, offset in streams.items():
            selected = [
                item for item in self.streams.get(stream_name, []) if _stream_id_gt(item[0], "0-0")
            ]
            if offset != ">":
                selected = [item for item in selected if _stream_id_gt(item[0], offset)]
            if count is not None:
                selected = selected[:count]
            self.stream_groups.setdefault(stream_name, {}).setdefault(groupname, set()).update(
                item[0] for item in selected
            )
            rows.append((stream_name, selected))
        return rows

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        group = self.stream_groups.setdefault(name, {}).setdefault(groupname, set())
        acked = 0
        for event_id in ids:
            if event_id in group:
                group.remove(event_id)
                acked += 1
        return acked

    async def aclose(self, close_connection_pool: bool | None = None) -> None:
        _ = close_connection_pool
        return None

    async def zcard(self, key: str) -> int:
        return len(self.sorted_sets.get(key, {}))

    async def zcount(self, key: str, minimum: str | int, maximum: str | int) -> int:
        return sum(
            1
            for score in self.sorted_sets.get(key, {}).values()
            if _score_in_range(score, minimum=minimum, maximum=maximum)
        )

    async def zrangebyscore(
        self,
        key: str,
        minimum: str | int,
        maximum: str | int,
        *,
        start: int = 0,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[Any]:
        items = [
            (member, score)
            for member, score in self.sorted_sets.get(key, {}).items()
            if _score_in_range(score, minimum=minimum, maximum=maximum)
        ]
        items.sort(key=lambda item: item[1])
        if start:
            items = items[start:]
        if num is not None:
            items = items[:num]
        if withscores:
            return items
        return [member for member, _score in items]

    async def llen(self, key: str) -> int:
        return len(self.lists.get(key, []))

    async def lpush(self, key: str, *values: Any) -> int:
        bucket = self.lists.setdefault(key, [])
        for value in values:
            bucket.insert(0, value if isinstance(value, bytes) else str(value).encode("utf-8"))
        return len(bucket)

    async def ltrim(self, key: str, start: int, stop: int) -> bool:
        bucket = self.lists.get(key, [])
        self.lists[key] = bucket[start : stop + 1]
        return True

    async def lrange(self, key: str, start: int, stop: int) -> list[bytes]:
        bucket = self.lists.get(key, [])
        end = None if stop == -1 else stop + 1
        return bucket[start:end]

    def scan_iter(self, *, match: str | None = None) -> Any:
        prefix = match[:-1] if isinstance(match, str) and match.endswith("*") else match
        keys = list(self.values)

        async def _iterator() -> Any:
            for key in keys:
                if prefix is None or key.startswith(prefix):
                    yield key

        return _iterator()


def _score_in_range(score: float, *, minimum: str | int, maximum: str | int) -> bool:
    return _score_matches(score, minimum, lower=True) and _score_matches(score, maximum, lower=False)


def _score_matches(score: float, bound: str | int, *, lower: bool) -> bool:
    if bound == "-inf":
        return True
    if bound == "+inf":
        return True
    if isinstance(bound, str) and bound.startswith("("):
        target = float(bound[1:])
        return score > target if lower else score < target
    target = float(bound)
    return score >= target if lower else score <= target


def _stream_id_gt(left: str, right: str) -> bool:
    left_ms, left_seq = (int(part) for part in left.split("-", 1))
    right_ms, right_seq = (int(part) for part in right.split("-", 1))
    return (left_ms, left_seq) > (right_ms, right_seq)


class DummyDatabaseRuntime:
    """No-op DB runtime placeholder for application resources in tests."""

    async def dispose(self) -> None:
        return None


@dataclass
class DummyMediaService:
    """Deterministic media-service test double for dashboard routes."""

    snapshot: StatsProjection

    async def get_stats(self, *, tenant_id: str | None = None) -> StatsProjection:
        _ = tenant_id
        return self.snapshot

    async def get_calendar_snapshot(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        tenant_id: str | None = None,
    ) -> dict[str, Any]:
        _ = (start_date, end_date, tenant_id)
        return {}


class DummyAccessPolicyService:
    """Minimal access-policy inventory stub for route tests."""

    def __init__(self, settings: Settings) -> None:
        self.snapshot = snapshot_from_settings(settings.access_policy)
        now = datetime(2026, 4, 11, 12, 0, tzinfo=UTC)
        self.revisions: list[Any] = [self._build_revision_record(self.snapshot, now, True)]

    def _build_revision_record(self, snapshot: Any, at: datetime, is_active: bool) -> Any:
        record = type("AccessPolicyRevisionRecord", (), {})()
        record.version = snapshot.version
        record.source = snapshot.source
        record.approval_status = "approved" if is_active else "draft"
        record.proposed_by = "tenant-main:operator-1"
        record.approved_by = "tenant-main:operator-1" if is_active else None
        record.approved_at = at if is_active else None
        record.approval_notes = "test"
        record.is_active = is_active
        record.activated_at = at
        record.created_at = at
        record.updated_at = at
        record.role_grants = snapshot.role_grants
        record.principal_roles = snapshot.principal_roles
        record.principal_scopes = snapshot.principal_scopes
        record.principal_tenant_grants = snapshot.principal_tenant_grants
        record.permission_constraints = snapshot.permission_constraints
        record.audit_decisions = snapshot.audit_decisions
        record.alerting_enabled = snapshot.alerting_enabled
        record.repeated_denial_warning_threshold = snapshot.repeated_denial_warning_threshold
        record.repeated_denial_critical_threshold = snapshot.repeated_denial_critical_threshold
        record.to_snapshot = lambda snapshot=snapshot: snapshot
        return record

    async def list_revisions(self, *, limit: int = 20) -> list[Any]:
        return self.revisions[:limit]

    async def write_revision(
        self,
        *,
        version: str,
        source: str,
        role_grants: dict[str, list[str]],
        principal_roles: dict[str, list[str]],
        principal_scopes: dict[str, list[str]],
        principal_tenant_grants: dict[str, list[str]],
        permission_constraints: dict[str, dict[str, list[str]]],
        audit_decisions: bool,
        alerting_enabled: bool,
        repeated_denial_warning_threshold: int,
        repeated_denial_critical_threshold: int,
        proposed_by: str | None = None,
        approval_notes: str | None = None,
        auto_approve: bool = False,
        activate: bool = False,
    ) -> Any:
        now = datetime(2026, 4, 11, 12, 30, tzinfo=UTC)
        if activate and not auto_approve:
            raise ValueError("access policy revision must be approved before activation")
        if activate:
            for revision in self.revisions:
                revision.is_active = False
        snapshot = type(self.snapshot)(
            version=version,
            source=source,
            role_grants=role_grants,
            principal_roles=principal_roles,
            principal_scopes=principal_scopes,
            principal_tenant_grants=principal_tenant_grants,
            permission_constraints=permission_constraints,
            audit_decisions=audit_decisions,
            alerting_enabled=alerting_enabled,
            repeated_denial_warning_threshold=repeated_denial_warning_threshold,
            repeated_denial_critical_threshold=repeated_denial_critical_threshold,
        )
        record = self._build_revision_record(snapshot, now, activate)
        record.approval_status = "approved" if auto_approve else "draft"
        record.proposed_by = proposed_by
        record.approved_by = proposed_by if auto_approve else None
        record.approved_at = now if auto_approve else None
        record.approval_notes = approval_notes
        self.snapshot = snapshot if activate else self.snapshot
        self.revisions.insert(0, record)
        return record

    async def activate_revision(self, version: str) -> Any:
        for revision in self.revisions:
            if revision.version == version and revision.approval_status not in {"approved", "bootstrap"}:
                raise ValueError(
                    f"access policy revision '{version}' must be approved before activation"
                )
            revision.is_active = revision.version == version
            if revision.version == version:
                self.snapshot = revision.to_snapshot()
                return revision
        raise LookupError(f"unknown access policy revision '{version}'")

    async def approve_revision(
        self,
        version: str,
        *,
        approved_by: str | None,
        approval_notes: str | None = None,
        activate: bool = False,
    ) -> Any:
        for revision in self.revisions:
            if revision.version != version:
                continue
            revision.approval_status = "approved"
            revision.approved_by = approved_by
            revision.approved_at = datetime(2026, 4, 11, 12, 45, tzinfo=UTC)
            revision.approval_notes = approval_notes
            if activate:
                for candidate in self.revisions:
                    candidate.is_active = candidate.version == version
                self.snapshot = revision.to_snapshot()
            return revision
        raise LookupError(f"unknown access policy revision '{version}'")

    async def reject_revision(
        self,
        version: str,
        *,
        rejected_by: str | None,
        approval_notes: str | None = None,
    ) -> Any:
        for revision in self.revisions:
            if revision.version != version:
                continue
            revision.approval_status = "rejected"
            revision.approved_by = rejected_by
            revision.approval_notes = approval_notes
            revision.is_active = False
            return revision
        raise LookupError(f"unknown access policy revision '{version}'")


class DummyPluginGovernanceService:
    """Minimal persisted plugin-governance override stub for route tests."""

    def __init__(self) -> None:
        self.overrides: dict[str, Any] = {}

    async def list_overrides(self) -> dict[str, Any]:
        return dict(self.overrides)

    async def write_override(
        self,
        *,
        plugin_name: str,
        state: str,
        reason: str | None = None,
        notes: str | None = None,
        updated_by: str | None = None,
    ) -> Any:
        now = datetime(2026, 4, 11, 12, 50, tzinfo=UTC)
        record = type("PluginGovernanceOverrideRecord", (), {})()
        record.plugin_name = plugin_name
        record.state = state
        record.reason = reason
        record.notes = notes
        record.updated_by = updated_by
        record.created_at = now
        record.updated_at = now
        self.overrides[plugin_name] = record
        return record


class DummyControlPlaneService:
    """Minimal control-plane subscriber ledger stub for route tests."""

    def __init__(self) -> None:
        self.records: list[Any] = []

    async def list_subscribers(self, *, active_within_seconds: int = 120) -> list[Any]:
        _ = active_within_seconds
        return list(self.records)

    async def summarize_subscribers(self, *, active_within_seconds: int = 120) -> Any:
        _ = active_within_seconds
        active = sum(1 for record in self.records if getattr(record, "status", "") == "active")
        stale = sum(1 for record in self.records if getattr(record, "status", "") == "stale")
        error = sum(1 for record in self.records if getattr(record, "status", "") == "error")
        ack_pending = sum(
            1
            for record in self.records
            if getattr(record, "last_delivered_event_id", None)
            and getattr(record, "last_delivered_event_id", None)
            != getattr(record, "last_acked_event_id", None)
        )
        summary = type("ControlPlaneSummary", (), {})()
        summary.total_subscribers = len(self.records)
        summary.active_subscribers = active
        summary.stale_subscribers = stale
        summary.error_subscribers = error
        summary.fenced_subscribers = 0
        summary.ack_pending_subscribers = ack_pending
        summary.stream_count = len({record.stream_name for record in self.records})
        summary.group_count = len(
            {(record.stream_name, record.group_name) for record in self.records}
        )
        summary.node_count = len({record.node_id for record in self.records})
        summary.tenant_count = len(
            {record.tenant_id for record in self.records if getattr(record, "tenant_id", None)}
        )
        summary.oldest_heartbeat_age_seconds = 30.0 if self.records else None
        summary.status_counts = {"active": active, "stale": stale, "error": error}
        summary.required_actions = (
            ["recover_stale_control_plane_subscribers"] if stale else []
        ) + (["drain_control_plane_ack_backlog"] if ack_pending else [])
        summary.remaining_gaps = (
            ["at least one control-plane subscriber heartbeat is stale"] if stale else []
        ) + (
            ["one or more subscribers have unacknowledged delivered events"]
            if ack_pending
            else []
        )
        return summary

    async def remediate_subscribers(self, *, active_within_seconds: int = 120) -> Any:
        _ = active_within_seconds
        stale_marked = 0
        fence_resolved = 0
        error_recovered = 0
        for record in self.records:
            if getattr(record, "status", "") == "active":
                stale_marked += 1
                record.status = "stale"
            elif "consumer_fenced" in str(getattr(record, "last_error", None) or ""):
                fence_resolved += 1
                record.status = "stale"
            elif getattr(record, "status", "") == "error":
                error_recovered += 1
                record.status = "stale"

        result = type("ControlPlaneRemediationResult", (), {})()
        result.active_within_seconds = active_within_seconds
        result.stale_marked_subscribers = stale_marked
        result.fence_resolved_subscribers = fence_resolved
        result.error_recovered_subscribers = error_recovered
        result.total_updated_subscribers = stale_marked + fence_resolved + error_recovered
        result.summary = await self.summarize_subscribers(active_within_seconds=active_within_seconds)
        return result

    async def recover_ack_backlog(self, *, active_within_seconds: int = 120) -> Any:
        _ = active_within_seconds
        rewound = 0
        stale_marked = 0
        pending_without_ack = 0
        updated = 0
        for record in self.records:
            delivered = getattr(record, "last_delivered_event_id", None)
            acked = getattr(record, "last_acked_event_id", None)
            if not delivered or delivered == acked:
                continue
            if getattr(record, "status", "") == "active":
                record.status = "stale"
                stale_marked += 1
            if acked is None:
                pending_without_ack += 1
                updated += 1
                continue
            record.last_delivered_event_id = acked
            rewound += 1
            updated += 1

        result = type("ControlPlaneAckRecoveryResult", (), {})()
        result.active_within_seconds = active_within_seconds
        result.rewound_subscribers = rewound
        result.stale_marked_subscribers = stale_marked
        result.pending_without_ack_subscribers = pending_without_ack
        result.total_updated_subscribers = updated
        result.summary = await self.summarize_subscribers(active_within_seconds=active_within_seconds)
        return result


class DummyReplayBackplane:
    """Minimal replay backplane stub for pending-recovery route tests."""

    def __init__(self) -> None:
        self.claims: list[dict[str, Any]] = []

    async def claim_pending(
        self,
        *,
        group_name: str,
        consumer_name: str,
        node_id: str | None = None,
        tenant_id: str | None = None,
        min_idle_ms: int = 60_000,
        count: int = 100,
        start_id: str = "0-0",
        heartbeat_expiry_seconds: int = 120,
    ) -> Any:
        self.claims.append(
            {
                "group_name": group_name,
                "consumer_name": consumer_name,
                "node_id": node_id,
                "tenant_id": tenant_id,
                "min_idle_ms": min_idle_ms,
                "count": count,
                "start_id": start_id,
                "heartbeat_expiry_seconds": heartbeat_expiry_seconds,
            }
        )
        result = type("ReplayPendingClaimResult", (), {})()
        result.group_name = group_name
        result.consumer_name = consumer_name
        result.min_idle_ms = min_idle_ms
        result.claimed_events = [
            type("ReplayEvent", (), {"event_id": "21-0"})(),
            type("ReplayEvent", (), {"event_id": "22-0"})(),
        ]
        result.next_start_id = "23-0"
        result.pending_before = type(
            "ReplayPendingSummary",
            (),
            {
                "pending_count": 2,
                "oldest_event_id": "21-0",
                "latest_event_id": "22-0",
                "consumer_counts": {"consumer-1": 2},
            },
        )()
        result.pending_after = type(
            "ReplayPendingSummary",
            (),
            {
                "pending_count": 0,
                "oldest_event_id": None,
                "latest_event_id": None,
                "consumer_counts": {"recovery-ops": 0},
            },
        )()
        return result


class DummyAuthorizationAuditService:
    """In-memory authorization-decision ledger used by route tests."""

    def __init__(self) -> None:
        self.records: list[Any] = []

    async def record_decision(self, **payload: Any) -> None:
        record = type("AuthorizationDecisionAuditRecord", (), {})()
        record.occurred_at = payload.get(
            "occurred_at", datetime(2026, 4, 12, 10, 0, tzinfo=UTC)
        )
        for key, value in payload.items():
            setattr(record, key, value)
        self.records.insert(0, record)

    async def search(
        self,
        *,
        limit: int = 20,
        actor_id: str | None = None,
        tenant_id: str | None = None,
        target_tenant_id: str | None = None,
        permission: str | None = None,
        allowed: bool | None = None,
        reason: str | None = None,
        path_prefix: str | None = None,
    ) -> Any:
        records = list(self.records)
        if actor_id is not None:
            records = [record for record in records if record.actor_id == actor_id]
        if tenant_id is not None:
            records = [record for record in records if record.tenant_id == tenant_id]
        if target_tenant_id is not None:
            records = [
                record for record in records if record.target_tenant_id == target_tenant_id
            ]
        if permission is not None:
            records = [
                record for record in records if permission in tuple(record.required_permissions)
            ]
        if allowed is not None:
            records = [record for record in records if record.allowed is allowed]
        if reason is not None:
            records = [record for record in records if record.reason == reason]
        if path_prefix is not None:
            records = [record for record in records if record.path.startswith(path_prefix)]
        return type(
            "AuthorizationDecisionAuditSearchResult",
            (),
            {"total_matches": len(records), "records": tuple(records[:limit])},
        )()


class FailingAuthorizationAuditService:
    """Audit service double that simulates temporary ledger persistence failure."""

    async def record_decision(self, **payload: Any) -> None:
        _ = payload
        raise RuntimeError("audit store unavailable")


def _build_settings(
    *,
    arq_enabled: bool = False,
    temporal_enabled: bool = False,
    **overrides: Any,
) -> Settings:
    """Create deterministic settings payload for dashboard compatibility tests."""

    payload: dict[str, Any] = {
        "FILMU_PY_API_KEY": SecretStr("a" * 32),
        "FILMU_PY_POSTGRES_DSN": "postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        "FILMU_PY_REDIS_URL": AnyUrl("redis://localhost:6379/0"),
        "FILMU_PY_RUN_MIGRATIONS_ON_STARTUP": False,
        "FILMU_PY_LOG_LEVEL": "INFO",
        "FILMU_PY_OTEL_ENABLED": False,
        "FILMU_PY_OTEL_EXPORTER_OTLP_ENDPOINT": None,
        "FILMU_PY_OIDC": {
            "enabled": False,
            "rollout_stage": "disabled",
            "rollout_evidence_refs": [],
            "subject_mapping_ready": False,
            "issuer": None,
            "audience": None,
            "jwks_url": None,
            "jwks_json": None,
            "allowed_algorithms": ["RS256", "ES256"],
            "allow_api_key_fallback": True,
        },
        "FILMU_PY_LOG_SHIPPER": {
            "enabled": False,
            "type": "external_ndjson_tail",
            "target": None,
            "healthcheck_url": None,
            "field_mapping_version": "filmu-ecs-v1",
        },
        "FILMU_PY_OBSERVABILITY": {
            "environment_shipping_enabled": False,
            "search_backend": "none",
            "alerting_enabled": False,
            "rust_trace_correlation_enabled": False,
            "proof_refs": [],
        },
        "FILMU_PY_PLUGIN_RUNTIME": {
            "enforcement_mode": "report_only",
            "health_rollup_enabled": True,
            "require_strict_signatures": False,
            "require_source_digest": False,
            "proof_refs": [],
        },
        "FILMU_PY_ARQ_ENABLED": arq_enabled,
        "FILMU_PY_TEMPORAL_ENABLED": temporal_enabled,
    }
    payload.update(overrides)
    return Settings(**payload)


def _build_snapshot() -> StatsProjection:
    """Return a stable dashboard stats payload for route assertions."""

    return StatsProjection(
        total_items=12,
        completed_items=5,
        failed_items=1,
        incomplete_items=7,
        movies=3,
        shows=2,
        episodes=5,
        seasons=2,
        states={
            "Requested": 2,
            "Indexed": 2,
            "Scraped": 1,
            "Downloaded": 1,
            "Completed": 5,
            "Failed": 1,
            "Unreleased": 0,
        },
        activity={"2026-03-08": 3, "2026-03-09": 9},
        media_year_releases=[StatsYearReleaseRecord(year=2024, count=4)],
    )


def _build_client(
    *,
    arq_enabled: bool = False,
    temporal_enabled: bool = False,
    settings_overrides: dict[str, Any] | None = None,
    plugin_registry: PluginRegistry | None = None,
    plugin_load_report: Any | None = None,
    security_identity_service: Any | None = None,
    authorization_audit_service: Any | None = None,
    replay_backplane: Any | None = None,
) -> TestClient:
    """Build a FastAPI test app with compatibility routers and mocked resources."""

    settings = _build_settings(
        arq_enabled=arq_enabled,
        temporal_enabled=temporal_enabled,
        **(settings_overrides or {}),
    )
    redis = DummyRedis()
    registry = GraphQLPluginRegistry()

    app = FastAPI()
    app.state.resources = AppResources(
        settings=settings,
        redis=redis,  # type: ignore[arg-type]
        cache=CacheManager(redis=redis, namespace="test"),  # type: ignore[arg-type]
        chunk_cache=ChunkCache(max_bytes=8 * 1024 * 1024),
        rate_limiter=DistributedRateLimiter(redis=redis),  # type: ignore[arg-type]
        event_bus=EventBus(),
        db=DummyDatabaseRuntime(),  # type: ignore[arg-type]
        media_service=DummyMediaService(snapshot=_build_snapshot()),  # type: ignore[arg-type]
        graphql_plugin_registry=registry,
        plugin_registry=plugin_registry,
        security_identity_service=security_identity_service,
        access_policy_service=DummyAccessPolicyService(settings),
        access_policy_snapshot=snapshot_from_settings(settings.access_policy),
        authorization_audit_service=(
            DummyAuthorizationAuditService()
            if authorization_audit_service is None
            else authorization_audit_service
        ),
        control_plane_service=DummyControlPlaneService(),
        plugin_governance_service=DummyPluginGovernanceService(),
        replay_backplane=DummyReplayBackplane() if replay_backplane is None else replay_backplane,
    )
    app.state.plugin_load_report = plugin_load_report
    app.include_router(create_api_router())
    app.include_router(internal_vfs_router)

    return TestClient(app)


def _headers() -> dict[str, str]:
    """Return valid auth headers for compatibility API requests."""

    return {
        "x-api-key": "a" * 32,
        "x-actor-id": "operator-1",
        "x-tenant-id": "tenant-main",
        "x-actor-roles": "platform:admin,playback:operator",
        "x-actor-scopes": "backend:admin,playback:read",
    }


def test_stats_route_returns_dashboard_snapshot() -> None:
    """Stats route should expose the dashboard fields currently rendered by the frontend."""

    client = _build_client()
    response = client.get("/api/v1/stats", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["total_items"] == 12
    assert body["total_movies"] == 3
    assert body["total_symlinks"] == 0
    assert "Completed" in body["states"]
    assert "Unreleased" in body["states"]
    assert body["states"]["Completed"] == 5
    assert body["activity"]["2026-03-09"] == 9
    assert body["media_year_releases"] == [{"year": 2024, "count": 4}]


def test_vfs_catalog_watch_event_route_returns_protobuf_snapshot_and_delta() -> None:
    client = _build_client()

    class FakeVfsCatalogServer:
        async def build_poll_event(
            self,
            *,
            last_applied_generation_id: str | None = None,
        ) -> catalog_pb2.WatchCatalogEvent:
            event = catalog_pb2.WatchCatalogEvent(event_id=f"event:{last_applied_generation_id or 'snapshot'}")
            if last_applied_generation_id is None:
                event.snapshot.generation_id = "1"
            else:
                event.delta.generation_id = "2"
                event.delta.base_generation_id = last_applied_generation_id
            return event

    resources = cast(Any, client.app.state.resources)
    resources.vfs_catalog_server = FakeVfsCatalogServer()

    first = client.get(
        "/internal/vfs/watch-event.pb",
        headers={"x-filmu-vfs-key": "a" * 32},
    )
    assert first.status_code == 200
    assert first.headers["content-type"].startswith("application/x-protobuf")
    first_event = catalog_pb2.WatchCatalogEvent()
    first_event.ParseFromString(first.content)
    assert first_event.snapshot.generation_id == "1"

    second = client.get(
        "/internal/vfs/watch-event.pb",
        params={"last_applied_generation_id": "1"},
        headers={"x-filmu-vfs-key": "a" * 32},
    )
    assert second.status_code == 200
    second_event = catalog_pb2.WatchCatalogEvent()
    second_event.ParseFromString(second.content)
    assert second_event.delta.generation_id == "2"
    assert second_event.delta.base_generation_id == "1"


def test_vfs_catalog_refresh_entry_route_returns_protobuf_refresh_response() -> None:
    client = _build_client()

    class FakeVfsCatalogServer:
        async def refresh_catalog_entry_message(
            self,
            *,
            provider_file_id: str,
            handle_key: str,
            entry_id: str,
        ) -> catalog_pb2.RefreshCatalogEntryResponse:
            assert provider_file_id == "provider-file-1"
            assert handle_key == "handle-1"
            assert entry_id == "file:entry-1"
            return catalog_pb2.RefreshCatalogEntryResponse(
                success=True,
                new_url="https://cdn.example.test/fresh.mkv",
            )

    resources = cast(Any, client.app.state.resources)
    resources.vfs_catalog_server = FakeVfsCatalogServer()

    payload = catalog_pb2.RefreshCatalogEntryRequest(
        provider_file_id="provider-file-1",
        handle_key="handle-1",
        entry_id="file:entry-1",
    ).SerializeToString()
    response = client.post(
        "/internal/vfs/refresh-entry.pb",
        headers={
            "x-filmu-vfs-key": "a" * 32,
            "content-type": "application/x-protobuf",
        },
        content=payload,
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/x-protobuf")
    proto_response = catalog_pb2.RefreshCatalogEntryResponse()
    proto_response.ParseFromString(response.content)
    assert proto_response.success is True
    assert proto_response.new_url == "https://cdn.example.test/fresh.mkv"


def test_vfs_catalog_rollup_route_returns_rest_projection() -> None:
    client = _build_client()

    class FakeVfsCatalogSupplier:
        async def build_snapshot(self) -> Any:
            file_entry = type("VfsCatalogFileEntry", (), {})()
            file_entry.query_strategy = "snapshot"
            file_entry.provider_family = "realdebrid"
            file_entry.lease_state = "ready"
            file_entry.locator_source = "restricted_url"
            file_entry.restricted_fallback = True
            file_entry.provider_file_path = "Season 01/Episode 01.mkv"
            file_entry.active_roles = ("direct", "hls")

            entry = type("VfsCatalogEntry", (), {})()
            entry.file = file_entry

            blocked_item = type("VfsCatalogBlockedItem", (), {"reason": "missing_media_entry"})()
            stats = type("VfsCatalogStats", (), {})()
            stats.directory_count = 4
            stats.file_count = 1
            stats.blocked_item_count = 1

            snapshot = type("VfsCatalogSnapshot", (), {})()
            snapshot.generation_id = "7"
            snapshot.published_at = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
            snapshot.entries = (entry,)
            snapshot.blocked_items = (blocked_item,)
            snapshot.stats = stats
            return snapshot

        async def snapshot_for_generation(self, generation_id: int) -> Any:
            _ = generation_id
            return await self.build_snapshot()

    resources = cast(Any, client.app.state.resources)
    resources.vfs_catalog_supplier = FakeVfsCatalogSupplier()

    response = client.get("/api/v1/operations/vfs-catalog/rollup", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["generation_id"] == "7"
    assert body["directory_count"] == 4
    assert body["blocked_reason_counts"] == {"missing_media_entry": 1}
    assert body["provider_family_counts"] == {"realdebrid": 1}
    assert body["restricted_fallback_file_count"] == 1
    assert body["provider_path_preserved_file_count"] == 1
    assert body["multi_role_file_count"] == 1

def test_vfs_catalog_entry_detail_route_returns_rest_projection() -> None:
    client = _build_client()

    class FakeVfsCatalogSupplier:
        async def build_snapshot(self) -> Any:
            directory_root = type("DirectoryPayload", (), {"path": "/"})()
            directory_show = type("DirectoryPayload", (), {"path": "/shows"})()
            directory_season = type("DirectoryPayload", (), {"path": "/shows/Example Show/Season 01"})()

            empty_correlation = type(
                "Correlation",
                (),
                {
                    "item_id": None,
                    "media_entry_id": None,
                    "source_attachment_id": None,
                    "provider": None,
                    "provider_download_id": None,
                    "provider_file_id": None,
                    "provider_file_path": None,
                    "session_id": None,
                    "handle_key": None,
                    "tenant_id": "tenant-main",
                },
            )()
            file_correlation = type(
                "Correlation",
                (),
                {
                    "item_id": "item-episode-1",
                    "media_entry_id": "media-entry-1",
                    "source_attachment_id": "attachment-1",
                    "provider": "realdebrid",
                    "provider_download_id": "download-1",
                    "provider_file_id": "provider-file-1",
                    "provider_file_path": "Season 01/Episode 01.mkv",
                    "session_id": "session-1",
                    "handle_key": "handle-1",
                    "tenant_id": "tenant-main",
                },
            )()
            file_payload = type(
                "FilePayload",
                (),
                {
                    "item_id": "item-episode-1",
                    "item_title": "Example Show",
                    "item_external_ref": "tvdb:1",
                    "media_entry_id": "media-entry-1",
                    "source_attachment_id": "attachment-1",
                    "media_type": "episode",
                    "transport": "remote-direct",
                    "locator": "https://cdn.example.test/episode-1.mkv",
                    "local_path": None,
                    "restricted_url": "https://api.example.test/episode-1.mkv",
                    "unrestricted_url": "https://cdn.example.test/episode-1.mkv",
                    "original_filename": "Episode 01.mkv",
                    "size_bytes": 2048,
                    "lease_state": "ready",
                    "expires_at": None,
                    "last_refreshed_at": None,
                    "last_refresh_error": None,
                    "provider": "realdebrid",
                    "provider_download_id": "download-1",
                    "provider_file_id": "provider-file-1",
                    "provider_file_path": "Season 01/Episode 01.mkv",
                    "active_roles": ("direct",),
                    "source_key": "media-entry:media-entry-1",
                    "query_strategy": "by-provider-file-id",
                    "provider_family": "debrid",
                    "locator_source": "unrestricted-url",
                    "match_basis": "provider-file-id",
                    "restricted_fallback": False,
                },
            )()

            entries = (
                type(
                    "Entry",
                    (),
                    {
                        "entry_id": "dir:/",
                        "parent_entry_id": None,
                        "path": "/",
                        "name": "/",
                        "kind": "directory",
                        "correlation": empty_correlation,
                        "directory": directory_root,
                        "file": None,
                    },
                )(),
                type(
                    "Entry",
                    (),
                    {
                        "entry_id": "dir:/shows",
                        "parent_entry_id": "dir:/",
                        "path": "/shows",
                        "name": "shows",
                        "kind": "directory",
                        "correlation": empty_correlation,
                        "directory": directory_show,
                        "file": None,
                    },
                )(),
                type(
                    "Entry",
                    (),
                    {
                        "entry_id": "dir:/shows/Example Show/Season 01",
                        "parent_entry_id": "dir:/shows",
                        "path": "/shows/Example Show/Season 01",
                        "name": "Season 01",
                        "kind": "directory",
                        "correlation": empty_correlation,
                        "directory": directory_season,
                        "file": None,
                    },
                )(),
                type(
                    "Entry",
                    (),
                    {
                        "entry_id": "file:/shows/Example Show/Season 01/Episode 01.mkv",
                        "parent_entry_id": "dir:/shows/Example Show/Season 01",
                        "path": "/shows/Example Show/Season 01/Episode 01.mkv",
                        "name": "Episode 01.mkv",
                        "kind": "file",
                        "correlation": file_correlation,
                        "directory": None,
                        "file": file_payload,
                    },
                )(),
            )

            blocked_item = type(
                "BlockedItem",
                (),
                {
                    "item_id": "item-blocked-1",
                    "external_ref": "tmdb:99",
                    "title": "Blocked Example",
                    "reason": "missing_media_entry",
                },
            )()
            stats = type("Stats", (), {"directory_count": 3, "file_count": 1, "blocked_item_count": 1})()
            snapshot = type("Snapshot", (), {})()
            snapshot.generation_id = "11"
            snapshot.published_at = datetime(2026, 4, 15, 13, 0, tzinfo=UTC)
            snapshot.entries = entries
            snapshot.blocked_items = (blocked_item,)
            snapshot.stats = stats
            return snapshot

        async def snapshot_for_generation(self, generation_id: int) -> Any:
            _ = generation_id
            return await self.build_snapshot()

    resources = cast(Any, client.app.state.resources)
    resources.vfs_catalog_supplier = FakeVfsCatalogSupplier()

    response = client.get(
        "/api/v1/operations/vfs-catalog/entry",
        params={"path": "/shows/Example Show/Season 01"},
        headers=_headers(),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["found"] is True
    assert body["generation_id"] == "11"
    assert body["entry"]["kind"] == "directory"
    assert body["directories"] == []
    assert len(body["files"]) == 1
    assert body["files"][0]["correlation"]["provider_file_id"] == "provider-file-1"
    assert body["stats"]["blocked_item_count"] == 1
    assert body["blocked_items"][0]["reason"] == "missing_media_entry"
    assert body["remaining_gaps"] == ["one or more media items are still blocked from the VFS catalog"]
def test_services_route_reflects_runtime_flags() -> None:
    """Services route should expose the real provider enablement map."""

    client = _build_client(arq_enabled=True, temporal_enabled=False)
    response = client.get("/api/v1/services", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"real_debrid", "all_debrid", "debrid_link", "mdblist"}
    for provider_name in ("real_debrid", "all_debrid", "debrid_link", "mdblist"):
        assert set(body[provider_name]) == {"enabled"}
        assert isinstance(body[provider_name]["enabled"], bool)


def test_downloader_user_info_route_returns_normalized_payload(monkeypatch: Any) -> None:
    """Downloader user info should return a normalized payload even with no provider configured."""

    async def fake_get_active_provider_info(self: DownloaderAccountService) -> dict[str, Any]:
        _ = self
        return {"provider": None, "error": "no provider configured"}

    monkeypatch.setattr(
        DownloaderAccountService,
        "get_active_provider_info",
        fake_get_active_provider_info,
    )

    client = _build_client()
    response = client.get("/api/v1/downloader_user_info", headers=_headers())

    assert response.status_code == 200
    assert response.json() == {"provider": None, "error": "no provider configured"}


def test_plugins_route_returns_loaded_capability_plugins() -> None:
    plugin_registry = PluginRegistry()
    plugin_registry.register_capability(
        plugin_name="torrentio",
        kind=PluginCapabilityKind.SCRAPER,
        implementation=object(),
    )
    plugin_registry.register_capability(
        plugin_name="discord-notifier",
        kind=PluginCapabilityKind.NOTIFICATION,
        implementation=object(),
    )

    client = _build_client(plugin_registry=plugin_registry)
    response = client.get("/api/v1/plugins", headers=_headers())

    assert response.status_code == 200
    assert response.json() == [
        {
            "name": "discord-notifier",
            "capabilities": ["notification"],
            "status": "loaded",
            "ready": True,
            "configured": None,
            "version": None,
            "api_version": None,
            "min_host_version": None,
            "max_host_version": None,
            "publisher": None,
            "release_channel": None,
            "trust_level": None,
            "permission_scopes": [],
            "source_sha256": None,
            "signing_key_id": None,
            "signature_present": False,
            "signature_verified": False,
            "signature_verification_reason": None,
            "trust_policy_decision": None,
            "trust_store_source": None,
            "sandbox_profile": None,
            "tenancy_mode": None,
            "quarantined": False,
            "quarantine_reason": None,
            "publisher_policy_decision": None,
            "publisher_policy_status": None,
            "quarantine_recommended": False,
            "source": None,
            "warnings": [],
            "error": None,
        },
        {
            "name": "torrentio",
            "capabilities": ["scraper"],
            "status": "loaded",
            "ready": True,
            "configured": None,
            "version": None,
            "api_version": None,
            "min_host_version": None,
            "max_host_version": None,
            "publisher": None,
            "release_channel": None,
            "trust_level": None,
            "permission_scopes": [],
            "source_sha256": None,
            "signing_key_id": None,
            "signature_present": False,
            "signature_verified": False,
            "signature_verification_reason": None,
            "trust_policy_decision": None,
            "trust_store_source": None,
            "sandbox_profile": None,
            "tenancy_mode": None,
            "quarantined": False,
            "quarantine_reason": None,
            "publisher_policy_decision": None,
            "publisher_policy_status": None,
            "quarantine_recommended": False,
            "source": None,
            "warnings": [],
            "error": None,
        },
    ]


def test_plugin_events_route_returns_declared_events_and_hook_subscriptions() -> None:
    plugin_registry = PluginRegistry()
    plugin_registry.register_manifest(
        PluginManifest.model_validate(
            {
                "name": "torrentio",
                "version": "1.0.0",
                "api_version": "1",
                "entry_module": "plugin.py",
                "publishable_events": ["torrentio.scan.completed"],
            }
        )
    )
    plugin_registry.register_manifest(
        PluginManifest.model_validate(
            {
                "name": "hook-plugin",
                "version": "1.0.0",
                "api_version": "1",
                "entry_module": "plugin.py",
                "event_hook": "ExampleHook",
            }
        )
    )

    class ExampleHook:
        subscribed_events = frozenset({"item.completed", "item.state.changed"})

    plugin_registry.register_capability(
        plugin_name="hook-plugin",
        kind=PluginCapabilityKind.EVENT_HOOK,
        implementation=ExampleHook(),
    )

    client = _build_client(plugin_registry=plugin_registry)
    response = client.get("/api/v1/plugins/events", headers=_headers())

    assert response.status_code == 200
    assert response.json() == [
        {
            "name": "hook-plugin",
            "publisher": None,
            "publishable_events": [],
            "hook_subscriptions": ["item.completed", "item.state.changed"],
        },
        {
            "name": "torrentio",
            "publisher": None,
            "publishable_events": ["torrentio.scan.completed"],
            "hook_subscriptions": [],
        },
    ]


def test_plugin_stream_control_route_executes_registered_plugin() -> None:
    plugin_registry = PluginRegistry()

    class ExampleStreamControl:
        plugin_name = "stream-control-plugin"

        async def initialize(self, ctx: object) -> None:
            _ = ctx

        async def control(self, request: StreamControlInput) -> StreamControlResult:
            return StreamControlResult(
                action=request.action,
                item_identifier=request.item_identifier,
                accepted=True,
                outcome="handled",
                controller_attached=True,
                metadata={"source": "test"},
            )

    plugin_registry.register_capability(
        plugin_name="stream-control-plugin",
        kind=PluginCapabilityKind.STREAM_CONTROL,
        implementation=ExampleStreamControl(),
    )

    client = _build_client(plugin_registry=plugin_registry)
    response = client.post(
        "/api/v1/plugins/stream-control",
        headers=_headers(),
        json={
            "plugin_name": "stream-control-plugin",
            "action": "trigger_direct_playback_refresh",
            "item_identifier": "item-123",
            "prefer_queued": True,
            "metadata": {"reason": "operator-test"},
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "plugin_name": "stream-control-plugin",
        "action": "trigger_direct_playback_refresh",
        "item_identifier": "item-123",
        "accepted": True,
        "outcome": "handled",
        "detail": None,
        "controller_attached": True,
        "retry_after_seconds": None,
        "metadata": {"source": "test"},
    }


def test_plugin_governance_route_returns_operator_policy_summary() -> None:
    plugin_registry = PluginRegistry()
    plugin_registry.register_manifest(
        PluginManifest.model_validate(
            {
                "name": "external-scraper",
                "version": "1.0.0",
                "api_version": "1",
                "distribution": "filesystem",
                "publisher": "community",
                "release_channel": "stable",
                "trust_level": "community",
                "sandbox_profile": "restricted",
                "tenancy_mode": "tenant",
                "entry_module": "plugin.py",
                "scraper": "ExternalScraper",
            }
        )
    )
    plugin_registry.register_capability(
        plugin_name="external-scraper",
        kind=PluginCapabilityKind.SCRAPER,
        implementation=object(),
    )

    client = _build_client(plugin_registry=plugin_registry)
    response = client.get("/api/v1/plugins/governance", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["summary"] == {
        "total_plugins": 1,
        "loaded_plugins": 1,
        "load_failed_plugins": 0,
        "ready_plugins": 1,
        "unready_plugins": 0,
        "healthy_plugins": 1,
        "degraded_plugins": 0,
        "non_builtin_plugins": 1,
        "isolated_non_builtin_plugins": 0,
        "quarantined_plugins": 0,
        "quarantine_recommended_plugins": 0,
        "unsigned_external_plugins": 1,
        "unverified_signature_plugins": 0,
        "publisher_policy_rejections": 0,
        "trust_policy_rejections": 0,
        "sandbox_profile_counts": {"restricted": 1},
        "tenancy_mode_counts": {"tenant": 1},
        "runtime_policy_mode": "report_only",
        "runtime_isolation_ready": False,
        "recommended_actions": ["require_external_plugin_signature"],
        "remaining_gaps": [
            "non-builtin plugin runtime isolation exit gates are not fully satisfied",
            "operator quarantine/revocation still depends on runtime policy enforcement",
            "external plugin artifact provenance or signature verification is still incomplete",
        ],
    }
    assert body["plugins"][0]["name"] == "external-scraper"


def test_plugin_governance_route_marks_wave4_runtime_policy_ready() -> None:
    client = _build_client(
        settings_overrides={
            "FILMU_PY_PLUGIN_RUNTIME": {
                "enforcement_mode": "isolated_runtime_required",
                "require_strict_signatures": True,
                "require_source_digest": True,
                "proof_refs": ["ops/wave4/plugin-runtime-isolation.md"],
            }
        }
    )

    response = client.get("/api/v1/plugins/governance", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["summary"]["runtime_policy_mode"] == "isolated_runtime_required"
    assert body["summary"]["runtime_isolation_ready"] is True
    assert body["summary"]["healthy_plugins"] == 0
    assert body["summary"]["degraded_plugins"] == 0
    assert body["summary"]["remaining_gaps"] == []


def test_plugin_governance_route_ignores_unconfigured_builtin_plugins_for_runtime_exit() -> None:
    registry = PluginRegistry()
    harness = TestPluginContext(settings={"plugins": {}})
    register_builtin_plugins(registry, context_provider=harness.provider())
    client = _build_client(
        plugin_registry=registry,
        settings_overrides={
            "FILMU_PY_PLUGIN_RUNTIME": {
                "enforcement_mode": "isolated_runtime_required",
                "require_strict_signatures": True,
                "require_source_digest": True,
                "proof_refs": ["ops/wave4/plugin-runtime-isolation.md"],
            }
        },
    )

    response = client.get("/api/v1/plugins/governance", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["summary"]["runtime_isolation_ready"] is True
    assert body["summary"]["non_builtin_plugins"] == 0
    assert body["summary"]["unready_plugins"] == 1
    assert body["summary"]["degraded_plugins"] == 1
    stremthru = next(plugin for plugin in body["plugins"] if plugin["name"] == "stremthru")
    assert stremthru["ready"] is False
    assert stremthru["configured"] is False


def test_plugins_route_surfaces_manifest_compatibility_and_stremthru_readiness() -> None:
    plugin_registry = PluginRegistry()
    plugin_registry.register_manifest(
        PluginManifest.model_validate(
            {
                "name": "stremthru",
                "version": "1.2.3",
                "api_version": "1",
                "distribution": "builtin",
                "publisher": "filmu",
                "release_channel": "builtin",
                "trust_level": "builtin",
                "sandbox_profile": "host",
                "tenancy_mode": "control_plane",
                "entry_module": "plugin.py",
                "downloader": "StremThruDownloader",
                "min_host_version": "0.1.0",
            }
        )
    )
    plugin_registry.register_capability(
        plugin_name="stremthru",
        kind=PluginCapabilityKind.DOWNLOADER,
        implementation=object(),
    )

    client = _build_client(plugin_registry=plugin_registry)
    response = client.get("/api/v1/plugins", headers=_headers())

    assert response.status_code == 200
    assert response.json() == [
        {
            "name": "stremthru",
            "capabilities": ["downloader"],
            "status": "loaded",
            "ready": False,
            "configured": False,
            "version": "1.2.3",
            "api_version": "1",
            "min_host_version": "0.1.0",
            "max_host_version": None,
            "publisher": "filmu",
            "release_channel": "builtin",
            "trust_level": "builtin",
            "permission_scopes": ["download:transfer"],
            "source_sha256": None,
            "signing_key_id": None,
            "signature_present": False,
            "signature_verified": False,
            "signature_verification_reason": None,
            "trust_policy_decision": None,
            "trust_store_source": None,
            "sandbox_profile": "host",
            "tenancy_mode": "control_plane",
            "quarantined": False,
            "quarantine_reason": None,
            "publisher_policy_decision": None,
            "publisher_policy_status": None,
            "quarantine_recommended": False,
            "source": "builtin",
            "warnings": [],
            "error": None,
        }
    ]


def test_plugins_route_surfaces_load_failures_from_startup_report() -> None:
    class _Failure:
        plugin_name = "future-plugin"
        plugin_dir = "plugins/future-plugin"
        source = "entry_point"
        reason = "api_version_incompatible"

    class _Report:
        def __init__(self) -> None:
            self.loaded: list[object] = []
            self.failed = [_Failure()]

    client = _build_client(plugin_registry=PluginRegistry(), plugin_load_report=_Report())
    response = client.get("/api/v1/plugins", headers=_headers())

    assert response.status_code == 200
    assert response.json() == [
        {
            "name": "future-plugin",
            "capabilities": [],
            "status": "load_failed",
            "ready": False,
            "configured": None,
            "version": None,
            "api_version": None,
            "min_host_version": None,
            "max_host_version": None,
            "publisher": None,
            "release_channel": None,
            "trust_level": None,
            "permission_scopes": [],
            "source_sha256": None,
            "signing_key_id": None,
            "signature_present": False,
            "signature_verified": False,
            "signature_verification_reason": None,
            "trust_policy_decision": None,
            "trust_store_source": None,
            "sandbox_profile": None,
            "tenancy_mode": None,
            "quarantined": False,
            "quarantine_reason": None,
            "publisher_policy_decision": None,
            "publisher_policy_status": None,
            "quarantine_recommended": False,
            "source": "entry_point",
            "warnings": [],
            "error": "api_version_incompatible",
        }
    ]


def test_plugin_governance_override_route_persists_operator_state() -> None:
    client = _build_client()

    response = client.post(
        "/api/v1/plugins/governance/stremthru",
        headers=_headers(),
        json={"state": "quarantined", "reason": "compatibility drift", "notes": "hold rollout"},
    )

    assert response.status_code == 200
    assert response.json()["plugin_name"] == "stremthru"
    assert response.json()["state"] == "quarantined"

    list_response = client.get("/api/v1/plugins/governance/overrides", headers=_headers())
    assert list_response.status_code == 200
    assert list_response.json()[0]["plugin_name"] == "stremthru"


def test_control_plane_subscribers_route_returns_persisted_rows() -> None:
    client = _build_client()
    service = cast(Any, client.app.state.resources.control_plane_service)
    record = type("ControlPlaneSubscriberRecord", (), {})()
    now = datetime(2026, 4, 11, 13, 0, tzinfo=UTC)
    record.stream_name = "filmu:events"
    record.group_name = "filmu-api"
    record.consumer_name = "consumer-1"
    record.node_id = "node-a"
    record.tenant_id = "tenant-main"
    record.status = "active"
    record.last_read_offset = ">"
    record.last_delivered_event_id = "2-0"
    record.last_acked_event_id = "1-0"
    record.last_error = None
    record.claimed_at = now
    record.last_heartbeat_at = now
    record.created_at = now
    record.updated_at = now
    service.records.append(record)

    response = client.get("/api/v1/operations/control-plane/subscribers", headers=_headers())

    assert response.status_code == 200
    assert response.json()[0]["consumer_name"] == "consumer-1"
    assert response.json()[0]["last_delivered_event_id"] == "2-0"


def test_has_unresolved_fence_ignores_stale_historical_fence_errors() -> None:
    now = datetime(2026, 4, 11, 13, 0, tzinfo=UTC)

    resolved = type("ControlPlaneSubscriberRecord", (), {})()
    resolved.status = "active"
    resolved.last_error = "consumer_fenced owner=node-a contender=node-b"
    resolved.updated_at = now
    resolved.last_heartbeat_at = now + timedelta(seconds=30)

    unresolved = type("ControlPlaneSubscriberRecord", (), {})()
    unresolved.status = "active"
    unresolved.last_error = "consumer_fenced owner=node-a contender=node-b"
    unresolved.updated_at = now
    unresolved.last_heartbeat_at = now - timedelta(seconds=30)

    explicitly_fenced = type("ControlPlaneSubscriberRecord", (), {})()
    explicitly_fenced.status = "fenced"
    explicitly_fenced.last_error = None
    explicitly_fenced.updated_at = now
    explicitly_fenced.last_heartbeat_at = now + timedelta(seconds=30)

    assert default_routes._has_unresolved_fence(resolved) is False
    assert default_routes._has_unresolved_fence(unresolved) is True
    assert default_routes._has_unresolved_fence(explicitly_fenced) is True


def test_auth_context_route_returns_current_identity_and_persisted_mapping() -> None:
    class _IdentityService:
        async def record_auth_context(self, auth_context: Any) -> Any:
            return type(
                "IdentityResolution",
                (),
                {
                    "principal_key": auth_context.actor_id,
                    "principal_type": auth_context.actor_type,
                    "service_account_api_key_id": auth_context.api_key_id,
                },
            )()

    client = _build_client(security_identity_service=_IdentityService())
    response = client.get("/api/v1/auth/context", headers=_headers())

    assert response.status_code == 200
    assert response.json() == {
        "authentication_mode": "api_key",
        "api_key_id": "primary",
        "actor_id": "operator-1",
        "actor_type": "service",
        "tenant_id": "tenant-main",
        "authorized_tenant_ids": ["tenant-main"],
        "authorization_tenant_scope": "all",
        "roles": ["platform:admin", "playback:operator"],
        "scopes": ["backend:admin", "playback:read"],
        "effective_permissions": ["*", "playback:operate", "playback:read"],
        "oidc_issuer": None,
        "oidc_subject": None,
        "oidc_token_validated": False,
        "access_policy_version": "default-v1",
        "access_policy_source": "settings",
        "quota_policy_version": None,
        "principal_key": "operator-1",
        "principal_type": "service",
        "service_account_api_key_id": "primary",
    }


def test_auth_context_route_accepts_valid_oidc_bearer_token() -> None:
    token = jwt.encode(
        {"alg": "HS256", "kid": "test"},
        {
            "iss": "https://issuer.example.test",
            "sub": "user-123",
            "aud": "filmu-api",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
            "tenant_id": "tenant-oidc",
            "roles": ["playback:operator"],
            "scope": "library:read playback:operate",
        },
        {"kty": "oct", "k": "c2VjcmV0", "kid": "test"},
    )
    client = _build_client(
        settings_overrides={
            "FILMU_PY_OIDC": {
                "enabled": True,
                "issuer": "https://issuer.example.test",
                "audience": "filmu-api",
                "jwks_json": {"keys": [{"kty": "oct", "k": "c2VjcmV0", "kid": "test"}]},
                "allowed_algorithms": ["HS256"],
            }
        }
    )

    token_value = token.decode("utf-8") if isinstance(token, bytes) else token
    response = client.get(
        "/api/v1/auth/context",
        headers={"authorization": f"Bearer {token_value}"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["authentication_mode"] == "oidc"
    assert body["actor_id"] == "user-123"
    assert body["tenant_id"] == "tenant-oidc"
    assert body["oidc_issuer"] == "https://issuer.example.test"
    assert body["oidc_subject"] == "user-123"
    assert body["oidc_token_validated"] is True
    assert body["effective_permissions"] == ["library:read", "playback:operate", "playback:read"]


def test_oidc_can_disable_api_key_fallback() -> None:
    client = _build_client(
        settings_overrides={
            "FILMU_PY_OIDC": {
                "enabled": True,
                "issuer": "https://issuer.example.test",
                "audience": "filmu-api",
                "jwks_json": {"keys": [{"kty": "oct", "k": "c2VjcmV0", "kid": "test"}]},
                "allowed_algorithms": ["HS256"],
                "allow_api_key_fallback": False,
            }
        }
    )

    response = client.get("/api/v1/auth/context", headers=_headers())

    assert response.status_code == 401
    assert response.json()["detail"] == "OIDC bearer token required"


def test_auth_policy_route_returns_authorization_posture() -> None:
    client = _build_client()

    response = client.get(
        "/api/v1/auth/policy",
        headers={
            **_headers(),
            "x-actor-roles": "",
            "x-actor-scopes": "library:read,playback:operate",
            "x-auth-issuer": "https://issuer.example.test",
            "x-auth-subject": "user-123",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert (
        body["permissions_model"]
        == "role_scope_effective_permissions_with_route_and_resource_scope_constraints_and_tenant_scope"
    )
    assert body["policy_source"] == "settings"
    assert body["access_policy_version"] == "default-v1"
    assert body["quota_policy_version"] is None
    assert body["authorization_tenant_scope"] == "self"
    assert body["oidc_claims_present"] is True
    assert body["oidc_token_validated"] is False
    assert body["oidc_rollout_stage"] == "disabled"
    assert body["oidc_rollout_evidence_refs"] == []
    assert body["oidc_subject_mapping_ready"] is False
    assert body["oidc_rollout_status"] == "blocked"
    assert body["oidc_configuration_complete"] is False
    assert body["oidc_allow_api_key_fallback"] is True
    assert body["audit_mode"] == "persisted_decision_ledger"
    assert body["policy_alerting_enabled"] is True
    assert body["repeated_denial_warning_threshold"] == 3
    assert body["repeated_denial_critical_threshold"] == 5
    assert body["warnings"] == [
        "authentication is still API-key anchored",
        "oidc claims were supplied by headers and were not token-validated",
    ]
    assert body["role_grants"]["platform:admin"] == ["*"]
    assert "security:apikey.rotate" in body["permission_constraints"]
    decisions = {decision["name"]: decision for decision in body["decisions"]}
    assert decisions["library_read"]["allowed"] is True
    assert decisions["item_write"]["allowed"] is False
    assert decisions["item_write"]["missing_permissions"] == ["library:write"]
    assert decisions["playback_operate"]["allowed"] is True
    assert decisions["settings_write"]["allowed"] is False
    assert decisions["settings_write"]["missing_permissions"] == ["settings:write"]
    assert decisions["plugin_governance_write"]["allowed"] is False
    assert decisions["plugin_governance_write"]["missing_permissions"] == ["settings:write"]
    assert decisions["api_key_rotate"]["allowed"] is False
    assert decisions["api_key_rotate"]["reason"] == "missing_permissions"
    assert body["remaining_gaps"] == [
        "OIDC/SSO validation is disabled for this environment",
    ]


def test_generate_apikey_route_enforces_route_context_constraint() -> None:
    client = _build_client()

    response = client.post(
        "/api/v1/generateapikey",
        headers={
            **_headers(),
            "x-actor-type": "user",
            "x-actor-scopes": "security:apikey.rotate",
        },
    )

    assert response.status_code == 403
    assert "permission_constrained" in response.json()["detail"]

    audit_response = client.get(
        "/api/v1/auth/policy/audit",
        headers=_headers(),
        params={"reason": "permission_constrained"},
    )
    assert audit_response.status_code == 200
    body = audit_response.json()
    assert body["total_matches"] == 1
    assert body["records"][0]["path"] == "/api/v1/generateapikey"
    assert body["records"][0]["constrained_permissions"] == ["security:apikey.rotate"]
    assert body["records"][0]["constraint_failures"] == ["security:apikey.rotate:actor_type"]


def test_auth_policy_route_uses_real_policy_approval_probe_path() -> None:
    client = _build_client(
        settings_overrides={
            "FILMU_PY_ACCESS_POLICY": {
                "role_grants": {"platform:admin": ["*"]},
                "permission_constraints": {
                    "security:policy.approve": {
                        "route_prefixes": ["/api/v1/auth/policy/revisions/probe-version/approve"],
                        "tenant_scopes": ["self", "delegated", "all"],
                    }
                },
            }
        }
    )

    response = client.get(
        "/api/v1/auth/policy",
        headers={
            **_headers(),
            "x-actor-roles": "",
            "x-actor-scopes": "security:policy.approve",
        },
    )

    assert response.status_code == 200
    decisions = {decision["name"]: decision for decision in response.json()["decisions"]}
    assert decisions["policy_approve"]["allowed"] is True
    assert decisions["policy_approve"]["reason"] == "allowed"


def test_auth_policy_route_reports_wave2_ready_oidc_rollout() -> None:
    token = jwt.encode(
        {"alg": "HS256", "kid": "test"},
        {
            "iss": "https://issuer.example.test",
            "sub": "user-456",
            "aud": "filmu-api",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
            "tenant_id": "tenant-main",
            "roles": [],
            "scope": "library:write playback:operate settings:write security:policy.approve",
        },
        {"kty": "oct", "k": "c2VjcmV0", "kid": "test"},
    )
    client = _build_client(
        settings_overrides={
            "FILMU_PY_OIDC": {
                "enabled": True,
                "rollout_stage": "enforced",
                "rollout_evidence_refs": ["ops://oidc/staging-smoke-2026-04-12"],
                "subject_mapping_ready": True,
                "issuer": "https://issuer.example.test",
                "audience": "filmu-api",
                "jwks_json": {"keys": [{"kty": "oct", "k": "c2VjcmV0", "kid": "test"}]},
                "allowed_algorithms": ["HS256"],
                "allow_api_key_fallback": False,
            }
        }
    )

    token_value = token.decode("utf-8") if isinstance(token, bytes) else token
    response = client.get(
        "/api/v1/auth/policy",
        headers={"authorization": f"Bearer {token_value}"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["oidc_rollout_status"] == "ready"
    assert body["oidc_rollout_stage"] == "enforced"
    assert body["oidc_subject_mapping_ready"] is True
    assert body["oidc_rollout_evidence_refs"] == ["ops://oidc/staging-smoke-2026-04-12"]
    assert body["remaining_gaps"] == []
    decisions = {decision["name"]: decision for decision in body["decisions"]}
    assert decisions["item_write"]["allowed"] is True
    assert decisions["playback_operate"]["allowed"] is True
    assert decisions["settings_write"]["allowed"] is True
    assert decisions["plugin_governance_write"]["allowed"] is True
    assert decisions["policy_approve"]["allowed"] is True


def test_authorization_audit_failures_do_not_break_allowed_requests() -> None:
    client = _build_client(
        authorization_audit_service=FailingAuthorizationAuditService()
    )

    response = client.get("/api/v1/stats", headers=_headers())

    assert response.status_code == 200


def test_authorization_audit_failures_do_not_mask_denied_requests() -> None:
    client = _build_client(
        authorization_audit_service=FailingAuthorizationAuditService()
    )

    response = client.post(
        "/api/v1/generateapikey",
        headers={
            **_headers(),
            "x-actor-type": "user",
            "x-actor-scopes": "security:apikey.rotate",
        },
    )

    assert response.status_code == 403
    assert "permission_constrained" in response.json()["detail"]


def test_auth_policy_audit_surfaces_privileged_api_key_usage_alert() -> None:
    client = _build_client(
        settings_overrides={
            "FILMU_PY_ACCESS_POLICY": {
                "alerting_enabled": True,
                "repeated_denial_warning_threshold": 2,
                "repeated_denial_critical_threshold": 4,
            }
        }
    )

    for _ in range(2):
        response = client.get("/api/v1/auth/policy/revisions", headers=_headers())
        assert response.status_code == 200

    audit_response = client.get("/api/v1/auth/policy/audit", headers=_headers())

    assert audit_response.status_code == 200
    alerts = {alert["code"]: alert for alert in audit_response.json()["alerts"]}
    assert alerts["privileged_api_key_usage"]["severity"] == "warning"
    assert alerts["privileged_api_key_usage"]["count"] == 3


def test_auth_policy_revision_routes_return_inventory_and_support_approval_flow() -> None:
    client = _build_client()

    list_response = client.get("/api/v1/auth/policy/revisions", headers=_headers())

    assert list_response.status_code == 200
    assert list_response.json()["active_version"] == "default-v1"
    assert "permission_constraints" in list_response.json()["revisions"][0]
    assert list_response.json()["revisions"][0]["alerting_enabled"] is True

    write_response = client.post(
        "/api/v1/auth/policy/revisions",
        headers=_headers(),
        json={
            "version": "operator-v2",
            "source": "operator_api",
            "activate": True,
            "role_grants": {"tenant:analyst": ["library:read"]},
            "principal_roles": {"user-1": ["tenant:analyst"]},
            "principal_scopes": {"user-1": ["library:read"]},
            "principal_tenant_grants": {"user-1": ["tenant-analytics"]},
            "permission_constraints": {
                "library:read": {"route_prefixes": ["/api/v1/items", "/api/v1/stats"]}
            },
            "audit_decisions": True,
            "alerting_enabled": True,
            "repeated_denial_warning_threshold": 2,
            "repeated_denial_critical_threshold": 4,
        },
    )

    assert write_response.status_code == 200
    body = write_response.json()
    assert body["version"] == "operator-v2"
    assert body["is_active"] is True
    assert body["approval_status"] == "approved"
    assert body["role_grants"] == {"tenant:analyst": ["library:read"]}
    assert body["permission_constraints"]["library:read"]["route_prefixes"] == [
        "/api/v1/items",
        "/api/v1/stats",
    ]
    assert body["alerting_enabled"] is True
    assert body["repeated_denial_warning_threshold"] == 2
    assert body["repeated_denial_critical_threshold"] == 4

    activate_response = client.post(
        "/api/v1/auth/policy/revisions/operator-v2/activate",
        headers=_headers(),
    )

    assert activate_response.status_code == 200
    assert activate_response.json()["version"] == "operator-v2"
    assert activate_response.json()["is_active"] is True

    draft_response = client.post(
        "/api/v1/auth/policy/revisions",
        headers={
            **_headers(),
            "x-actor-roles": "",
            "x-actor-scopes": "settings:write",
        },
        json={
            "version": "operator-v3",
            "source": "operator_api",
            "activate": False,
            "role_grants": {"tenant:viewer": ["library:read"]},
        },
    )
    assert draft_response.status_code == 200
    assert draft_response.json()["approval_status"] == "draft"

    approve_response = client.post(
        "/api/v1/auth/policy/revisions/operator-v3/approve",
        headers=_headers(),
        json={"approval_notes": "approved for rollout", "activate": True},
    )
    assert approve_response.status_code == 200
    assert approve_response.json()["approval_status"] == "approved"
    assert approve_response.json()["is_active"] is True

    audit_response = client.get("/api/v1/auth/policy/audit", headers=_headers())
    assert audit_response.status_code == 200
    assert "entries" in audit_response.json()
    assert "records" in audit_response.json()


def test_operations_governance_route_returns_enterprise_slice_posture(
    monkeypatch: Any,
) -> None:
    playback_gate_governance = stream_routes._empty_playback_gate_governance_snapshot()
    playback_gate_governance.update(
        {
            "playback_gate_snapshot_available": 1,
            "playback_gate_gate_mode": "strict",
            "playback_gate_rollout_readiness": "blocked",
            "playback_gate_rollout_reasons": ["playback_gate_failed_or_incomplete"],
            "playback_gate_rollout_next_action": "resolve_failed_playback_gate_proofs",
            "playback_gate_policy_validation_status": "unverified",
        }
    )
    monkeypatch.setattr(
        default_routes,
        "_playback_gate_governance_snapshot",
        lambda: dict(playback_gate_governance),
    )
    monkeypatch.setattr(
        default_routes,
        "_vfs_runtime_governance_snapshot",
        lambda *args, **kwargs: runtime_governance_routes._empty_vfs_runtime_governance_snapshot(),
    )

    client = _build_client(arq_enabled=True)

    response = client.get(
        "/api/v1/operations/governance",
        headers={
            **_headers(),
            "x-auth-issuer": "https://issuer.example.test",
            "x-auth-subject": "operator-1",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {
        "generated_at",
        "playback_gate",
        "operational_evidence",
        "identity_authz",
        "tenant_boundary",
        "vfs_data_plane",
        "distributed_control_plane",
        "runtime_lifecycle",
        "sre_program",
        "operator_log_pipeline",
        "plugin_runtime_isolation",
        "heavy_stage_workload_isolation",
        "release_metadata_performance",
    }
    assert body["playback_gate"]["status"] == "blocked"
    assert "proof:playback:gate:enterprise package entrypoint exists" in body[
        "playback_gate"
    ]["evidence"]
    assert "playback_gate_rollout_readiness=blocked" in body["playback_gate"]["evidence"]
    assert body["operational_evidence"]["status"] == "partial"
    assert (
        "playback_gate_runner_status=unknown"
        in body["operational_evidence"]["evidence"]
    )
    assert (
        "playback_gate_windows_provider_movie_ready=0"
        in body["operational_evidence"]["evidence"]
    )
    assert (
        "playback_gate_windows_provider_tv_ready=0"
        in body["operational_evidence"]["evidence"]
    )
    assert (
        "rank_streams_no_winner_total=0" in body["operational_evidence"]["evidence"]
    )
    assert (
        "debrid_rate_limited_total=0" in body["operational_evidence"]["evidence"]
    )
    assert "record_playback_gate_runner_readiness" in body["operational_evidence"][
        "required_actions"
    ]
    assert "record_github_main_policy_validation" in body["operational_evidence"][
        "required_actions"
    ]
    assert "rerun_native_windows_provider_proof_movie" in body["operational_evidence"][
        "required_actions"
    ]
    assert "rerun_native_windows_provider_proof_tv" in body["operational_evidence"][
        "required_actions"
    ]
    assert "run_windows_vfs_soak_all_profiles" in body["operational_evidence"][
        "required_actions"
    ]
    assert body["identity_authz"]["status"] == "blocked"
    assert "authentication_mode=api_key" in body["identity_authz"]["evidence"]
    assert "oidc_claims_present=True" in body["identity_authz"]["evidence"]
    assert "permission_constraint_count=5" in body["identity_authz"]["evidence"]
    assert "authorization_decision_audit_persistence=True" in body["identity_authz"]["evidence"]
    assert "policy_alerting_enabled=True" in body["identity_authz"]["evidence"]
    assert "resource_scope_constraint_coverage=True" in body["identity_authz"]["evidence"]
    assert "oidc_rollout_status=blocked" in body["identity_authz"]["evidence"]
    assert body["tenant_boundary"]["status"] == "partial"
    assert "request_tenant_id=tenant-main" in body["tenant_boundary"]["evidence"]
    assert body["vfs_data_plane"]["status"] == "partial"
    assert "chunk_cache_enabled=True" in body["vfs_data_plane"]["evidence"]
    assert body["distributed_control_plane"]["status"] == "not_ready"
    assert "EventBus backend=process_local" in body["distributed_control_plane"]["evidence"]
    assert (
        "GET /api/v1/workers/queue/history exposes dead-letter age/reason rollups and bounded replay filters"
        in body["distributed_control_plane"]["evidence"]
    )
    assert body["runtime_lifecycle"]["status"] == "blocked"
    assert "runtime_phase=bootstrap" in body["runtime_lifecycle"]["evidence"]
    assert body["sre_program"]["status"] == "partial"
    assert body["operator_log_pipeline"]["status"] == "partial"
    assert "structured_logging_enabled=True" in body["operator_log_pipeline"]["evidence"]
    assert body["plugin_runtime_isolation"]["status"] == "partial"
    assert body["heavy_stage_workload_isolation"]["status"] == "partial"
    assert "stream_refresh_queue_ready=1" in body["heavy_stage_workload_isolation"]["evidence"]
    assert (
        "heavy_stage_executor_mode=process_pool_preferred"
        in body["heavy_stage_workload_isolation"]["evidence"]
    )
    assert "heavy_stage_max_workers=2" in body["heavy_stage_workload_isolation"]["evidence"]
    assert "queued_refresh_proof_ref_count=0" in body["heavy_stage_workload_isolation"]["evidence"]
    assert "heavy_stage_proof_ref_count=0" in body["heavy_stage_workload_isolation"]["evidence"]
    assert body["heavy_stage_workload_isolation"]["status"] == "partial"
    assert body["release_metadata_performance"]["status"] == "partial"
    assert (
        "GET /api/v1/workers/metadata-reindex and /api/v1/workers/metadata-reindex/history expose bounded operator rollups"
        in body["release_metadata_performance"]["evidence"]
    )
    assert (
        "repairable failed items now receive identifier repair plus immediate index re-entry inside the scheduled metadata program"
        in body["release_metadata_performance"]["evidence"]
    )
    assert (
        "metadata reindex/reconciliation trends are not yet exposed on a dedicated operator summary surface"
        not in body["release_metadata_performance"]["remaining_gaps"]
    )


def test_operations_governance_route_blocks_operational_evidence_on_live_worker_blockers(
    monkeypatch: Any,
) -> None:
    playback_gate_governance = stream_routes._empty_playback_gate_governance_snapshot()
    playback_gate_governance.update(
        {
            "playback_gate_snapshot_available": 1,
            "playback_gate_gate_mode": "strict",
            "playback_gate_runner_status": "ready",
            "playback_gate_runner_ready": 1,
            "playback_gate_runner_required_failures": 0,
            "playback_gate_provider_gate_required": 1,
            "playback_gate_provider_gate_ran": 1,
            "playback_gate_stability_ready": 1,
            "playback_gate_provider_parity_ready": 1,
            "playback_gate_windows_provider_ready": 1,
            "playback_gate_windows_provider_movie_ready": 1,
            "playback_gate_windows_provider_tv_ready": 1,
            "playback_gate_windows_provider_coverage": [
                "emby:movie",
                "emby:tv",
                "plex:movie",
                "plex:tv",
            ],
            "playback_gate_windows_soak_ready": 1,
            "playback_gate_windows_soak_repeat_count": 1,
            "playback_gate_windows_soak_profile_coverage_complete": 1,
            "playback_gate_windows_soak_profile_coverage": [
                "concurrent",
                "continuous",
                "full",
                "seek",
            ],
            "playback_gate_policy_validation_status": "ready",
            "playback_gate_policy_ready": 1,
            "playback_gate_rollout_readiness": "ready",
            "playback_gate_rollout_reasons": ["playback_gate_green"],
            "playback_gate_rollout_next_action": "keep_required_checks_enforced",
        }
    )
    monkeypatch.setattr(
        default_routes,
        "_playback_gate_governance_snapshot",
        lambda: dict(playback_gate_governance),
    )
    monkeypatch.setattr(
        default_routes,
        "_worker_blocker_snapshot",
        lambda: {
            "rank_streams_no_winner_total": 2,
            "rank_streams_no_winner_reason_counts": {"no_candidates_passing_fetch": 2},
            "rank_streams_no_winner_last_reason": "no_candidates_passing_fetch",
            "debrid_rate_limited_total": 1,
            "debrid_rate_limited_provider_counts": {"realdebrid": 1},
            "debrid_rate_limited_last_provider": "realdebrid",
            "debrid_rate_limited_last_retry_after_seconds": 30.0,
        },
    )

    client = _build_client(
        settings_overrides={
            "FILMU_PY_OIDC": {
                "enabled": True,
                "rollout_stage": "enforced",
                "rollout_evidence_refs": ["ops://oidc/prod-smoke-2026-04-12"],
                "subject_mapping_ready": True,
                "issuer": "https://issuer.example.test",
                "audience": "filmu-api",
                "jwks_json": {"keys": [{"kty": "oct", "k": "c2VjcmV0", "kid": "test"}]},
                "allowed_algorithms": ["HS256"],
                "allow_api_key_fallback": False,
            },
            "FILMU_PY_PLUGIN_RUNTIME": {
                "enforcement_mode": "isolated_runtime_required",
                "require_strict_signatures": True,
                "require_source_digest": True,
                "proof_refs": ["ops/wave4/plugin-runtime-isolation.md"],
            },
            "FILMU_PY_OBSERVABILITY": {
                "environment_shipping_enabled": True,
                "search_backend": "opensearch",
                "alerting_enabled": True,
                "rust_trace_correlation_enabled": True,
                "required_correlation_fields": [
                    "@timestamp",
                    "trace.id",
                    "labels.tenant_id",
                ],
                "proof_refs": ["ops/wave4/log-pipeline-rollout.md"],
            },
        }
    )
    token = jwt.encode(
        {"alg": "HS256", "kid": "test"},
        {
            "iss": "https://issuer.example.test",
            "sub": "user-789",
            "aud": "filmu-api",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
            "tenant_id": "tenant-main",
            "roles": [],
            "scope": "library:write playback:operate settings:write security:policy.approve",
        },
        {"kty": "oct", "k": "c2VjcmV0", "kid": "test"},
    )
    token_value = token.decode("utf-8") if isinstance(token, bytes) else token

    response = client.get(
        "/api/v1/operations/governance",
        headers={"authorization": f"Bearer {token_value}"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["operational_evidence"]["status"] == "blocked"
    assert "rank_streams_no_winner_total=2" in body["operational_evidence"]["evidence"]
    assert (
        "rank_streams_no_winner_reason_counts=no_candidates_passing_fetch:2"
        in body["operational_evidence"]["evidence"]
    )
    assert "debrid_rate_limited_total=1" in body["operational_evidence"]["evidence"]
    assert (
        "debrid_rate_limited_provider_counts=realdebrid:1"
        in body["operational_evidence"]["evidence"]
    )
    assert body["operational_evidence"]["required_actions"] == [
        "reduce_rank_streams_fetch_rejections",
        "mitigate_debrid_rate_limit_pressure",
    ]
    assert (
        "rank_streams.no_winner is currently active with bounded reason counters"
        in body["operational_evidence"]["remaining_gaps"]
    )
    assert (
        "debrid_item.rate_limited is currently active against live providers"
        in body["operational_evidence"]["remaining_gaps"]
    )


def test_operations_governance_route_marks_identity_ready_when_wave2_exit_gates_are_satisfied() -> None:
    token = jwt.encode(
        {"alg": "HS256", "kid": "test"},
        {
            "iss": "https://issuer.example.test",
            "sub": "user-789",
            "aud": "filmu-api",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
            "tenant_id": "tenant-main",
            "roles": [],
            "scope": "library:write playback:operate settings:write security:policy.approve",
        },
        {"kty": "oct", "k": "c2VjcmV0", "kid": "test"},
    )
    client = _build_client(
        settings_overrides={
            "FILMU_PY_OIDC": {
                "enabled": True,
                "rollout_stage": "enforced",
                "rollout_evidence_refs": ["ops://oidc/prod-smoke-2026-04-12"],
                "subject_mapping_ready": True,
                "issuer": "https://issuer.example.test",
                "audience": "filmu-api",
                "jwks_json": {"keys": [{"kty": "oct", "k": "c2VjcmV0", "kid": "test"}]},
                "allowed_algorithms": ["HS256"],
                "allow_api_key_fallback": False,
            }
        },
    )

    token_value = token.decode("utf-8") if isinstance(token, bytes) else token
    response = client.get(
        "/api/v1/operations/governance",
        headers={"authorization": f"Bearer {token_value}"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["identity_authz"]["status"] == "ready"
    assert body["identity_authz"]["remaining_gaps"] == []


def test_operations_governance_route_blocks_queued_refresh_without_runtime_queue_attachment() -> None:
    client = _build_client(
        arq_enabled=True,
        settings_overrides={
            "FILMU_PY_STREAM": {"refresh_dispatch_mode": "queued"},
            "FILMU_PY_ORCHESTRATION": {
                "queued_refresh_proof_refs": ["ops/wave3/queued-refresh-soak.md"],
                "heavy_stage_isolation": {
                    "executor_mode": "thread_pool_only",
                    "max_workers": 3,
                    "max_tasks_per_child": 0,
                    "proof_refs": ["ops/wave3/heavy-stage-failure-injection.md"],
                },
            },
        },
    )

    response = client.get("/api/v1/operations/governance", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    heavy_stage = body["heavy_stage_workload_isolation"]
    assert heavy_stage["status"] == "blocked"
    assert "stream_refresh_dispatch_mode=queued" in heavy_stage["evidence"]
    assert "stream_refresh_queue_ready=0" in heavy_stage["evidence"]
    assert "heavy_stage_executor_mode=thread_pool_only" in heavy_stage["evidence"]
    assert "heavy_stage_max_workers=3" in heavy_stage["evidence"]
    assert "heavy_stage_max_tasks_per_child=0" in heavy_stage["evidence"]
    assert "heavy_stage_spawn_context_required=1" in heavy_stage["evidence"]
    assert "heavy_stage_max_worker_ceiling=2" in heavy_stage["evidence"]
    assert "heavy_stage_policy_violations=worker_ceiling_exceeded" in heavy_stage["evidence"]
    assert "heavy_stage_process_isolation_required=0" in heavy_stage["evidence"]
    assert "queued_refresh_proof_ref_count=1" in heavy_stage["evidence"]
    assert "heavy_stage_proof_ref_count=1" in heavy_stage["evidence"]
    assert "heavy_stage_exit_ready=0" in heavy_stage["evidence"]
    assert "require_process_backed_heavy_stage_isolation" in heavy_stage["required_actions"]
    assert any(
        gap == "heavy stages are not yet forced into process-backed isolation"
        for gap in heavy_stage["remaining_gaps"]
    )


def test_operations_governance_route_marks_wave3_ready_when_exit_gates_are_satisfied() -> None:
    client = _build_client(
        arq_enabled=True,
        settings_overrides={
            "FILMU_PY_STREAM": {"refresh_dispatch_mode": "queued"},
            "FILMU_PY_ORCHESTRATION": {
                "queued_refresh_proof_refs": ["ops/wave3/queued-refresh-soak.md"],
                "heavy_stage_isolation": {
                    "executor_mode": "process_pool_required",
                    "max_workers": 2,
                    "max_tasks_per_child": 25,
                    "require_spawn_context": True,
                    "max_worker_ceiling": 2,
                    "proof_refs": ["ops/wave3/heavy-stage-failure-injection.md"],
                },
            },
        },
    )
    resources = cast(Any, client.app.state.resources)
    resources.arq_redis = resources.redis
    resources.queued_direct_playback_refresh_controller = object()
    resources.queued_hls_failed_lease_refresh_controller = object()
    resources.queued_hls_restricted_fallback_refresh_controller = object()

    response = client.get("/api/v1/operations/governance", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    heavy_stage = body["heavy_stage_workload_isolation"]
    assert heavy_stage["status"] == "ready"
    assert heavy_stage["required_actions"] == []
    assert heavy_stage["remaining_gaps"] == []
    assert "stream_refresh_dispatch_mode=queued" in heavy_stage["evidence"]
    assert "stream_refresh_queue_ready=1" in heavy_stage["evidence"]
    assert "heavy_stage_executor_mode=process_pool_required" in heavy_stage["evidence"]
    assert "heavy_stage_max_workers=2" in heavy_stage["evidence"]
    assert "heavy_stage_max_tasks_per_child=25" in heavy_stage["evidence"]
    assert "heavy_stage_spawn_context_required=1" in heavy_stage["evidence"]
    assert "heavy_stage_max_worker_ceiling=2" in heavy_stage["evidence"]
    assert "heavy_stage_policy_violations=none" in heavy_stage["evidence"]
    assert "heavy_stage_process_isolation_required=1" in heavy_stage["evidence"]
    assert "queued_refresh_proof_ref_count=1" in heavy_stage["evidence"]
    assert "heavy_stage_proof_ref_count=1" in heavy_stage["evidence"]
    assert "heavy_stage_exit_ready=1" in heavy_stage["evidence"]


def test_operations_governance_route_marks_wave4_ready_when_exit_gates_are_satisfied() -> None:
    client = _build_client(
        settings_overrides={
            "FILMU_PY_OTEL_ENABLED": True,
            "FILMU_PY_OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel.example.test/v1/traces",
            "FILMU_PY_LOG_SHIPPER": {
                "enabled": True,
                "type": "vector",
                "target": "http://logs.example.test",
                "healthcheck_url": "http://logs.example.test/health",
            },
            "FILMU_PY_OBSERVABILITY": {
                "environment_shipping_enabled": True,
                "search_backend": "opensearch",
                "alerting_enabled": True,
                "rust_trace_correlation_enabled": True,
                "proof_refs": ["ops/wave4/log-pipeline-rollout.md"],
            },
            "FILMU_PY_PLUGIN_RUNTIME": {
                "enforcement_mode": "isolated_runtime_required",
                "require_strict_signatures": True,
                "require_source_digest": True,
                "proof_refs": ["ops/wave4/plugin-runtime-isolation.md"],
            },
        }
    )

    response = client.get("/api/v1/operations/governance", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["operator_log_pipeline"]["status"] == "ready"
    assert body["operator_log_pipeline"]["required_actions"] == []
    assert body["operator_log_pipeline"]["remaining_gaps"] == []
    assert "log_search_backend=opensearch" in body["operator_log_pipeline"]["evidence"]
    assert (
        "rust_trace_correlation_enabled=True" in body["operator_log_pipeline"]["evidence"]
    )
    assert body["plugin_runtime_isolation"]["status"] == "ready"
    assert body["plugin_runtime_isolation"]["required_actions"] == []
    assert body["plugin_runtime_isolation"]["remaining_gaps"] == []
    assert (
        "plugin_runtime_enforcement_mode=isolated_runtime_required"
        in body["plugin_runtime_isolation"]["evidence"]
    )
    assert "plugin_runtime_exit_ready=1" in body["plugin_runtime_isolation"]["evidence"]
    assert "resource_scope_constraint_coverage=True" in body["identity_authz"]["evidence"]


def test_operations_governance_route_surfaces_live_vfs_rollout_posture(
    tmp_path: Path, monkeypatch: Any
) -> None:
    runtime_status_path = tmp_path / "filmuvfs-runtime-status.json"
    runtime_status_path.write_text(
        json.dumps(
            {
                "runtime": {
                    "open_handles": 4,
                    "peak_open_handles": 9,
                    "active_reads": 2,
                    "peak_active_reads": 5,
                    "chunk_cache_weighted_bytes": 8192,
                },
                "chunk_cache": {
                    "backend": "hybrid",
                    "hits": 9,
                    "misses": 3,
                    "memory_bytes": 4096,
                    "memory_max_bytes": 16384,
                    "memory_hits": 6,
                    "memory_misses": 4,
                    "disk_bytes": 12288,
                    "disk_max_bytes": 65536,
                    "disk_hits": 3,
                    "disk_misses": 1,
                    "disk_writes": 2,
                    "disk_write_errors": 1,
                    "disk_evictions": 4,
                },
                "handle_startup": {
                    "total": 5,
                    "ok": 3,
                    "error": 1,
                    "estale": 1,
                    "average_duration_ms": 105,
                    "max_duration_ms": 412,
                },
                "mounted_reads": {
                    "total": 8,
                    "ok": 6,
                    "error": 1,
                    "estale": 1,
                    "average_duration_ms": 13,
                    "max_duration_ms": 48,
                },
                "upstream_failures": {
                    "unexpected_status_too_many_requests": 2,
                    "unexpected_status_server_error": 1,
                },
                "upstream_retryable_events": {
                    "status_too_many_requests": 8,
                    "status_server_error": 9,
                },
                "backend_fallback": {
                    "attempts": 10,
                    "success": 7,
                    "failure": 3,
                },
                "prefetch": {
                    "available_permits": 1,
                    "active_permits": 3,
                    "active_background_tasks": 2,
                    "background_backpressure": 2,
                    "fairness_denied": 1,
                    "global_backpressure_denied": 1,
                    "background_error": 1,
                },
                "chunk_coalescing": {
                    "waits_miss": 1,
                },
                "inline_refresh": {
                    "error": 2,
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("FILMU_PY_VFS_RUNTIME_STATUS_PATH", str(runtime_status_path))
    client = _build_client(arq_enabled=True)

    response = client.get("/api/v1/operations/governance", headers=_headers())

    assert response.status_code == 200
    vfs_data_plane = response.json()["vfs_data_plane"]
    assert vfs_data_plane["status"] == "blocked"
    assert "vfs_runtime_snapshot_available=1" in vfs_data_plane["evidence"]
    assert "vfs_runtime_rollout_readiness=blocked" in vfs_data_plane["evidence"]
    assert "vfs_runtime_rollout_reasons=backend_fallback_failures,mounted_read_errors,prefetch_background_errors,disk_cache_write_errors" in vfs_data_plane["evidence"]
    assert "vfs_runtime_cache_hit_ratio=0.750" in vfs_data_plane["evidence"]
    assert "vfs_runtime_provider_pressure_incidents=22" in vfs_data_plane["evidence"]
    assert "vfs_runtime_cache_pressure_class=critical" in vfs_data_plane["evidence"]
    assert "vfs_runtime_chunk_coalescing_pressure_class=warning" in vfs_data_plane["evidence"]
    assert "vfs_runtime_upstream_wait_class=critical" in vfs_data_plane["evidence"]
    assert "vfs_runtime_refresh_pressure_class=critical" in vfs_data_plane["evidence"]
    assert "vfs_runtime_cache_pressure_reasons=disk_write_errors,disk_evictions_observed" in vfs_data_plane["evidence"]
    assert "resolve_blocking_runtime_failures" in vfs_data_plane["required_actions"]
    assert any(
        gap.startswith("live runtime rollout reasons are present:")
        for gap in vfs_data_plane["remaining_gaps"]
    )


def test_operations_governance_route_promotes_wave1_when_gate_and_canary_are_ready(
    tmp_path: Path, monkeypatch: Any
) -> None:
    runtime_status_path = tmp_path / "filmuvfs-runtime-status.json"
    runtime_status_path.write_text(
        json.dumps(
            {
                "runtime": {
                    "open_handles": 2,
                    "peak_open_handles": 4,
                    "active_reads": 1,
                    "peak_active_reads": 2,
                    "chunk_cache_weighted_bytes": 4096,
                },
                "chunk_cache": {
                    "backend": "hybrid",
                    "hits": 8,
                    "misses": 2,
                    "memory_bytes": 2048,
                    "memory_max_bytes": 8192,
                    "memory_hits": 6,
                    "memory_misses": 2,
                    "disk_bytes": 8192,
                    "disk_max_bytes": 65536,
                    "disk_hits": 2,
                    "disk_misses": 0,
                    "disk_writes": 1,
                    "disk_write_errors": 0,
                    "disk_evictions": 0,
                },
                "handle_startup": {
                    "total": 3,
                    "ok": 3,
                    "error": 0,
                    "estale": 0,
                    "cancelled": 0,
                    "average_duration_ms": 41,
                    "max_duration_ms": 88,
                },
                "mounted_reads": {
                    "total": 6,
                    "ok": 6,
                    "error": 0,
                    "estale": 0,
                    "cancelled": 0,
                    "average_duration_ms": 8,
                    "max_duration_ms": 18,
                },
                "upstream_failures": {
                    "unexpected_status_too_many_requests": 0,
                    "unexpected_status_server_error": 0,
                },
                "upstream_retryable_events": {
                    "status_too_many_requests": 0,
                    "status_server_error": 0,
                },
                "backend_fallback": {
                    "attempts": 0,
                    "success": 0,
                    "failure": 0,
                },
                "prefetch": {
                    "available_permits": 4,
                    "active_permits": 0,
                    "active_background_tasks": 0,
                    "background_backpressure": 0,
                    "fairness_denied": 0,
                    "global_backpressure_denied": 0,
                    "background_error": 0,
                },
                "chunk_coalescing": {
                    "waits_miss": 0,
                },
                "inline_refresh": {
                    "error": 0,
                },
            }
        ),
        encoding="utf-8",
    )
    artifacts_root = tmp_path / "playback-proof-artifacts"
    windows_artifacts_root = artifacts_root / "windows-native-stack"
    windows_artifacts_root.mkdir(parents=True)
    captured_at = datetime.now(UTC).replace(microsecond=0)
    captured_at_text = captured_at.isoformat().replace("+00:00", "Z")
    expires_at_text = (captured_at + timedelta(hours=4)).isoformat().replace("+00:00", "Z")
    (artifacts_root / "stability-summary-20260412-020101.json").write_text(
        json.dumps(
            {
                "timestamp": captured_at_text,
                "environment_class": "windows-native:enterprise",
                "repeat_count": 2,
                "dry_run": False,
                "all_green": True,
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "ci-execution-summary.json").write_text(
        json.dumps(
            {
                "required_check_name": "Playback Gate / Playback Gate",
                "gate_mode": "full",
                "provider_gate_required": True,
                "provider_gate_ran": True,
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "media-server-gate-20260412-020102.json").write_text(
        json.dumps({"timestamp": captured_at_text, "all_green": True}),
        encoding="utf-8",
    )
    (artifacts_root / "windows-media-server-gate-20260412-020103.json").write_text(
        json.dumps(
            {
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 72,
                "status": "passed",
                "coverage": ["emby:movie", "plex:movie"],
                "coverage_complete": True,
                "failure_reasons": [],
                "required_actions": [],
                "media_type": "movie",
                "results": [
                    {"provider": "emby", "status": "passed"},
                    {"provider": "plex", "status": "passed"},
                ],
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "windows-media-server-gate-20260412-020104.json").write_text(
        json.dumps(
            {
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 72,
                "status": "passed",
                "coverage": ["emby:tv", "plex:tv"],
                "coverage_complete": True,
                "failure_reasons": [],
                "required_actions": [],
                "media_type": "tv",
                "results": [
                    {"provider": "emby", "status": "passed"},
                    {"provider": "plex", "status": "passed"},
                ],
            }
        ),
        encoding="utf-8",
    )
    (windows_artifacts_root / "soak-program-summary-20260412-020105.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "artifact_kind": "windows_vfs_soak_program",
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 72,
                "status": "passed",
                "ready": True,
                "all_green": True,
                "repeat_count": 1,
                "profiles": ["continuous", "seek", "concurrent", "full"],
                "profile_coverage": ["continuous", "seek", "concurrent", "full"],
                "profile_coverage_complete": True,
                "failure_reasons": [],
                "required_actions": [],
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "playback-gate-runner-readiness.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "artifact_kind": "playback_gate_runner_readiness",
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 24,
                "status": "ready",
                "required_failure_count": 0,
                "required_actions": [],
                "failure_reasons": [],
                "checks": [
                    {"name": "docker", "required": True, "ok": True},
                    {"name": "pwsh", "required": True, "ok": True},
                ],
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "github-main-policy-current.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "artifact_kind": "github_main_policy_validation",
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 24,
                "required_actions": [],
                "failure_reasons": [],
                "validation": {
                    "status": "ready",
                    "stale": False,
                    "required_actions": [],
                    "failure_reasons": [],
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("FILMU_PY_VFS_RUNTIME_STATUS_PATH", str(runtime_status_path))
    monkeypatch.setenv("FILMU_PY_PLAYBACK_PROOF_ARTIFACTS_ROOT", str(artifacts_root))
    client = _build_client(arq_enabled=True)

    response = client.get("/api/v1/operations/governance", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["playback_gate"]["status"] == "ready"
    assert "playback_gate_rollout_readiness=ready" in body["playback_gate"]["evidence"]
    assert "keep_required_checks_enforced" in body["playback_gate"]["required_actions"]
    assert body["vfs_data_plane"]["status"] == "ready"
    assert "vfs_runtime_rollout_canary_decision=promote_to_next_environment_class" in body[
        "vfs_data_plane"
    ]["evidence"]
    assert "vfs_runtime_rollout_merge_gate=ready" in body["vfs_data_plane"]["evidence"]
    assert "vfs_runtime_cache_pressure_class=healthy" in body["vfs_data_plane"]["evidence"]
    assert "vfs_runtime_upstream_wait_class=healthy" in body["vfs_data_plane"]["evidence"]


def test_playback_gate_evidence_route_surfaces_missing_operational_proofs(
    tmp_path: Path, monkeypatch: Any
) -> None:
    artifacts_root = tmp_path / "playback-proof-artifacts"
    artifacts_root.mkdir(parents=True)
    monkeypatch.setenv("FILMU_PY_PLAYBACK_PROOF_ARTIFACTS_ROOT", str(artifacts_root))
    monkeypatch.setattr(
        default_routes,
        "playback_gate_governance_snapshot",
        stream_routes._empty_playback_gate_governance_snapshot,
    )
    client = _build_client()

    response = client.get("/api/v1/operations/playback-gate/evidence", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["runner_ready"] is False
    assert body["policy_ready"] is False
    assert "record_playback_gate_runner_readiness" in body["required_actions"]
    assert "record_github_main_policy_validation" in body["required_actions"]


def test_playback_gate_evidence_route_surfaces_stale_runner_and_policy_contracts(
    monkeypatch: Any,
) -> None:
    playback_gate_governance = stream_routes._empty_playback_gate_governance_snapshot()
    playback_gate_governance.update(
        {
            "playback_gate_runner_status": "ready",
            "playback_gate_runner_ready": 0,
            "playback_gate_runner_required_failures": 0,
            "playback_gate_runner_recorded_at": "2026-04-12T02:01:05Z",
            "playback_gate_runner_expires_at": "2026-04-13T02:01:05Z",
            "playback_gate_runner_stale": 1,
            "playback_gate_runner_failure_reasons": [],
            "playback_gate_runner_required_actions": [
                "capture_runner_prerequisites_on_github_hosted_runner"
            ],
            "playback_gate_policy_validation_status": "ready",
            "playback_gate_policy_ready": 0,
            "playback_gate_policy_validation_recorded_at": "2026-04-12T02:01:06Z",
            "playback_gate_policy_validation_expires_at": "2026-04-13T02:01:06Z",
            "playback_gate_policy_validation_stale": 1,
            "playback_gate_policy_failure_reasons": [],
            "playback_gate_policy_required_actions": [
                "validate_github_main_policy_from_admin_authenticated_host"
            ],
            "playback_gate_rollout_readiness": "blocked",
            "playback_gate_rollout_reasons": [
                "runner_readiness_stale",
                "github_main_policy_stale",
            ],
            "playback_gate_rollout_next_action": "resolve_failed_playback_gate_proofs",
        }
    )
    monkeypatch.setattr(
        default_routes,
        "playback_gate_governance_snapshot",
        lambda: dict(playback_gate_governance),
    )
    client = _build_client()

    response = client.get("/api/v1/operations/playback-gate/evidence", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["runner_stale"] is True
    assert body["policy_stale"] is True
    assert body["runner_required_actions"] == [
        "capture_runner_prerequisites_on_github_hosted_runner"
    ]
    assert body["policy_required_actions"] == [
        "validate_github_main_policy_from_admin_authenticated_host"
    ]
    assert "refresh_playback_gate_runner_readiness" in body["required_actions"]
    assert "refresh_github_main_policy_validation" in body["required_actions"]


def test_playback_gate_evidence_route_surfaces_provider_gate_taxonomy(
    monkeypatch: Any,
) -> None:
    playback_gate_governance = stream_routes._empty_playback_gate_governance_snapshot()
    playback_gate_governance.update(
        {
            "playback_gate_runner_status": "ready",
            "playback_gate_runner_ready": 1,
            "playback_gate_policy_validation_status": "ready",
            "playback_gate_policy_ready": 1,
            "playback_gate_provider_gate_required": 1,
            "playback_gate_provider_gate_ran": 1,
            "playback_gate_provider_parity_ready": 0,
            "playback_gate_provider_gate_recorded_at": "2026-04-12T02:01:04Z",
            "playback_gate_provider_gate_expires_at": "2026-04-13T02:01:04Z",
            "playback_gate_provider_gate_stale": 0,
            "playback_gate_provider_gate_failure_reasons": [
                "provider_gate_docker_plex_mount_path_drift",
                "provider_gate_wsl_host_binary_stale",
            ],
            "playback_gate_provider_gate_required_actions": [
                "realign_docker_plex_mount_path",
                "rebuild_wsl_host_mount_binary",
            ],
            "playback_gate_windows_provider_ready": 1,
            "playback_gate_windows_soak_ready": 1,
            "playback_gate_rollout_readiness": "blocked",
            "playback_gate_rollout_reasons": [
                "provider_gate_docker_plex_mount_path_drift",
                "provider_gate_wsl_host_binary_stale",
            ],
            "playback_gate_rollout_next_action": "resolve_failed_playback_gate_proofs",
        }
    )
    monkeypatch.setattr(
        default_routes,
        "playback_gate_governance_snapshot",
        lambda: dict(playback_gate_governance),
    )
    client = _build_client()

    response = client.get("/api/v1/operations/playback-gate/evidence", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["provider_gate_required"] is True
    assert body["provider_gate_ran"] is True
    assert body["provider_parity_ready"] is False
    assert body["provider_gate_stale"] is False
    assert body["provider_gate_failure_reasons"] == [
        "provider_gate_docker_plex_mount_path_drift",
        "provider_gate_wsl_host_binary_stale",
    ]
    assert body["provider_gate_required_actions"] == [
        "realign_docker_plex_mount_path",
        "rebuild_wsl_host_mount_binary",
    ]
    assert "rerun_media_server_provider_gate" in body["required_actions"]
    assert "realign_docker_plex_mount_path" in body["required_actions"]
    assert "rebuild_wsl_host_mount_binary" in body["required_actions"]


def test_playback_gate_evidence_route_surfaces_stale_windows_soak_and_media_proofs(
    monkeypatch: Any,
) -> None:
    playback_gate_governance = stream_routes._empty_playback_gate_governance_snapshot()
    playback_gate_governance.update(
        {
            "playback_gate_runner_status": "ready",
            "playback_gate_runner_ready": 1,
            "playback_gate_policy_validation_status": "ready",
            "playback_gate_policy_ready": 1,
            "playback_gate_windows_provider_ready": 0,
            "playback_gate_windows_provider_recorded_at": "2026-04-12T02:01:03Z",
            "playback_gate_windows_provider_expires_at": "2026-04-13T02:01:03Z",
            "playback_gate_windows_provider_stale": 1,
            "playback_gate_windows_provider_failure_reasons": [
                "windows_provider_movie_proof_stale",
                "windows_provider_tv_proof_stale",
            ],
            "playback_gate_windows_provider_required_actions": [
                "refresh_native_windows_provider_proof_matrix",
                "rerun_native_windows_provider_proof_movie",
                "rerun_native_windows_provider_proof_tv",
            ],
            "playback_gate_windows_soak_ready": 0,
            "playback_gate_windows_soak_recorded_at": "2026-04-12T02:01:05Z",
            "playback_gate_windows_soak_expires_at": "2026-04-13T02:01:05Z",
            "playback_gate_windows_soak_stale": 1,
            "playback_gate_windows_soak_failure_reasons": [
                "windows_vfs_soak_program_stale"
            ],
            "playback_gate_windows_soak_required_actions": [
                "refresh_windows_vfs_soak_program"
            ],
            "playback_gate_rollout_readiness": "blocked",
            "playback_gate_rollout_reasons": [
                "windows_provider_gate_stale",
                "windows_vfs_soak_stale",
            ],
            "playback_gate_rollout_next_action": "resolve_failed_playback_gate_proofs",
        }
    )
    monkeypatch.setattr(
        default_routes,
        "playback_gate_governance_snapshot",
        lambda: dict(playback_gate_governance),
    )
    client = _build_client()

    response = client.get("/api/v1/operations/playback-gate/evidence", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["windows_provider_stale"] is True
    assert body["windows_soak_stale"] is True
    assert body["windows_provider_required_actions"] == [
        "refresh_native_windows_provider_proof_matrix",
        "rerun_native_windows_provider_proof_movie",
        "rerun_native_windows_provider_proof_tv",
    ]
    assert body["windows_soak_required_actions"] == ["refresh_windows_vfs_soak_program"]
    assert "refresh_native_windows_provider_proof_matrix" in body["required_actions"]
    assert "refresh_windows_vfs_soak_program" in body["required_actions"]


def test_vfs_rollout_control_route_persists_operator_pause_and_affects_merge_gate(
    tmp_path: Path, monkeypatch: Any
) -> None:
    runtime_status_path = tmp_path / "filmuvfs-runtime-status.json"
    runtime_status_path.write_text(
        json.dumps(
            {
                "runtime": {"open_handles": 1, "peak_open_handles": 1, "active_reads": 0, "peak_active_reads": 0},
                "chunk_cache": {
                    "backend": "hybrid",
                    "hits": 2,
                    "misses": 0,
                    "memory_bytes": 1024,
                    "memory_max_bytes": 8192,
                    "disk_bytes": 1024,
                    "disk_max_bytes": 65536,
                    "disk_write_errors": 0,
                    "disk_evictions": 0,
                },
                "mounted_reads": {"total": 2, "ok": 2, "error": 0, "estale": 0, "cancelled": 0},
                "backend_fallback": {"attempts": 0, "success": 0, "failure": 0},
                "prefetch": {
                    "available_permits": 4,
                    "active_permits": 0,
                    "background_backpressure": 0,
                    "fairness_denied": 0,
                    "global_backpressure_denied": 0,
                    "background_error": 0,
                },
                "chunk_coalescing": {"waits_miss": 0},
                "inline_refresh": {"error": 0},
            }
        ),
        encoding="utf-8",
    )
    artifacts_root = tmp_path / "playback-proof-artifacts"
    windows_artifacts_root = artifacts_root / "windows-native-stack"
    windows_artifacts_root.mkdir(parents=True)
    state_path = windows_artifacts_root / "filmuvfs-windows-state.json"
    state_path.write_text(
        json.dumps(
            {
                "last_mount_health": "green",
                "preserved_state": {"last_good_generation": 41},
                "notes": "previous note",
            }
        ),
        encoding="utf-8",
    )
    captured_at = datetime.now(UTC).replace(microsecond=0)
    captured_at_text = captured_at.isoformat().replace("+00:00", "Z")
    expires_at_text = (captured_at + timedelta(hours=4)).isoformat().replace("+00:00", "Z")
    (artifacts_root / "stability-summary-20260412-020101.json").write_text(
        json.dumps(
            {
                "timestamp": captured_at_text,
                "environment_class": "windows-native:managed",
                "repeat_count": 2,
                "dry_run": False,
                "all_green": True,
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "ci-execution-summary.json").write_text(
        json.dumps({"gate_mode": "full", "provider_gate_required": True, "provider_gate_ran": True}),
        encoding="utf-8",
    )
    (artifacts_root / "media-server-gate-20260412-020102.json").write_text(
        json.dumps({"timestamp": captured_at_text, "all_green": True}),
        encoding="utf-8",
    )
    (artifacts_root / "windows-media-server-gate-20260412-020103.json").write_text(
        json.dumps(
            {
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 72,
                "status": "passed",
                "coverage": ["emby:movie", "plex:movie"],
                "coverage_complete": True,
                "failure_reasons": [],
                "required_actions": [],
                "media_type": "movie",
                "results": [
                    {"provider": "emby", "status": "passed"},
                    {"provider": "plex", "status": "passed"},
                ],
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "windows-media-server-gate-20260412-020104.json").write_text(
        json.dumps(
            {
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 72,
                "status": "passed",
                "coverage": ["emby:tv", "plex:tv"],
                "coverage_complete": True,
                "failure_reasons": [],
                "required_actions": [],
                "media_type": "tv",
                "results": [
                    {"provider": "emby", "status": "passed"},
                    {"provider": "plex", "status": "passed"},
                ],
            }
        ),
        encoding="utf-8",
    )
    (windows_artifacts_root / "soak-program-summary-20260412-020105.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "artifact_kind": "windows_vfs_soak_program",
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 72,
                "status": "passed",
                "ready": True,
                "all_green": True,
                "repeat_count": 1,
                "profiles": ["continuous", "seek", "concurrent", "full"],
                "profile_coverage": ["continuous", "seek", "concurrent", "full"],
                "profile_coverage_complete": True,
                "failure_reasons": [],
                "required_actions": [],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("FILMU_PY_VFS_RUNTIME_STATUS_PATH", str(runtime_status_path))
    monkeypatch.setenv("FILMU_PY_PLAYBACK_PROOF_ARTIFACTS_ROOT", str(artifacts_root))
    audit_calls: list[dict[str, Any]] = []

    def fake_audit_action(request: Any, **kwargs: Any) -> None:
        _ = request
        audit_calls.append(dict(kwargs))

    monkeypatch.setattr(default_routes, "audit_action", fake_audit_action)

    client = _build_client()

    write_response = client.post(
        "/api/v1/operations/vfs-rollout/control",
        headers=_headers(),
        json={
            "environment_class": "windows-native:managed",
            "runtime_status_path": str(runtime_status_path),
            "promotion_paused": True,
            "promotion_pause_reason": "repeat soak after manual review",
            "promotion_pause_expires_at": expires_at_text,
            "notes": "holding canary for manual review",
        },
    )

    assert write_response.status_code == 200
    write_body = write_response.json()
    assert write_body["promotion_paused"] is True
    assert write_body["promotion_pause_reason"] == "repeat soak after manual review"
    assert write_body["promotion_pause_expires_at"] == expires_at_text
    assert write_body["promotion_pause_active"] is True
    assert write_body["rollback_requested"] is False
    assert write_body["updated_by"] == "tenant-main:operator-1"
    assert write_body["merge_gate"] == "hold"
    assert write_body["canary_decision"] == "hold_canary_and_repeat_soak"
    assert write_body["history"][0]["summary"].startswith("promotion pause enabled")
    assert write_body["history"][0]["promotion_pause_active"] is True
    assert audit_calls == [
        {
            "action": "operations.vfs_rollout.write_control",
            "target": "operations.vfs_rollout",
            "details": {
                "promotion_paused": True,
                "promotion_pause_reason": "repeat soak after manual review",
                "rollback_requested": None,
                "rollback_reason": None,
                "environment_class": "windows-native:managed",
            },
        }
    ]
    persisted_state = json.loads(state_path.read_text(encoding="utf-8"))
    assert persisted_state["last_mount_health"] == "green"
    assert persisted_state["preserved_state"] == {"last_good_generation": 41}
    assert persisted_state["notes"] == "holding canary for manual review"

    governance_response = client.get("/api/v1/operations/governance", headers=_headers())

    assert governance_response.status_code == 200
    body = governance_response.json()
    assert "vfs_runtime_rollout_canary_decision=hold_canary_and_repeat_soak" in body[
        "vfs_data_plane"
    ]["evidence"]


def test_vfs_rollout_control_route_rejects_rollback_without_reason(
    tmp_path: Path, monkeypatch: Any
) -> None:
    artifacts_root = tmp_path / "playback-proof-artifacts"
    (artifacts_root / "windows-native-stack").mkdir(parents=True)
    monkeypatch.setenv("FILMU_PY_PLAYBACK_PROOF_ARTIFACTS_ROOT", str(artifacts_root))

    client = _build_client()

    response = client.post(
        "/api/v1/operations/vfs-rollout/control",
        headers=_headers(),
        json={"rollback_requested": True},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "rollback_reason_required"


def test_control_plane_summary_route_returns_ack_backlog_visibility() -> None:
    client = _build_client()
    resources = cast(Any, client.app.state.resources)
    record = type("ControlPlaneSubscriberRecord", (), {})()
    record.stream_name = "filmu:events"
    record.group_name = "filmu-api"
    record.consumer_name = "consumer-1"
    record.node_id = "node-a"
    record.tenant_id = "tenant-main"
    record.status = "stale"
    record.last_read_offset = ">"
    record.last_delivered_event_id = "11-0"
    record.last_acked_event_id = "10-0"
    record.last_error = None
    record.claimed_at = datetime(2026, 4, 12, 2, 0, tzinfo=UTC)
    record.last_heartbeat_at = datetime(2026, 4, 12, 2, 0, tzinfo=UTC)
    record.created_at = datetime(2026, 4, 12, 2, 0, tzinfo=UTC)
    record.updated_at = datetime(2026, 4, 12, 2, 0, tzinfo=UTC)
    resources.control_plane_service.records = [record]

    response = client.get("/api/v1/operations/control-plane/summary", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["total_subscribers"] == 1
    assert body["stale_subscribers"] == 1
    assert body["ack_pending_subscribers"] == 1
    assert "recover_stale_control_plane_subscribers" in body["required_actions"]
    assert "drain_control_plane_ack_backlog" in body["required_actions"]


def test_control_plane_remediation_route_sweeps_rows_into_recoverable_stale_state() -> None:
    client = _build_client()
    resources = cast(Any, client.app.state.resources)
    active_record = type("ControlPlaneSubscriberRecord", (), {})()
    active_record.stream_name = "filmu:events"
    active_record.group_name = "filmu-api"
    active_record.consumer_name = "consumer-1"
    active_record.node_id = "node-a"
    active_record.tenant_id = "tenant-main"
    active_record.status = "active"
    active_record.last_read_offset = ">"
    active_record.last_delivered_event_id = "11-0"
    active_record.last_acked_event_id = "10-0"
    active_record.last_error = None
    active_record.claimed_at = datetime(2026, 4, 12, 2, 0, tzinfo=UTC)
    active_record.last_heartbeat_at = datetime(2026, 4, 12, 2, 0, tzinfo=UTC)
    active_record.created_at = datetime(2026, 4, 12, 2, 0, tzinfo=UTC)
    active_record.updated_at = datetime(2026, 4, 12, 2, 0, tzinfo=UTC)
    fenced_record = type("ControlPlaneSubscriberRecord", (), {})()
    fenced_record.stream_name = "filmu:events"
    fenced_record.group_name = "filmu-api"
    fenced_record.consumer_name = "consumer-2"
    fenced_record.node_id = "node-b"
    fenced_record.tenant_id = "tenant-main"
    fenced_record.status = "error"
    fenced_record.last_read_offset = ">"
    fenced_record.last_delivered_event_id = "12-0"
    fenced_record.last_acked_event_id = "11-0"
    fenced_record.last_error = "consumer_fenced owner=node-a contender=node-b"
    fenced_record.claimed_at = datetime(2026, 4, 12, 2, 0, tzinfo=UTC)
    fenced_record.last_heartbeat_at = datetime(2026, 4, 12, 2, 0, tzinfo=UTC)
    fenced_record.created_at = datetime(2026, 4, 12, 2, 0, tzinfo=UTC)
    fenced_record.updated_at = datetime(2026, 4, 12, 2, 0, tzinfo=UTC)
    resources.control_plane_service.records = [active_record, fenced_record]

    response = client.post("/api/v1/operations/control-plane/remediation", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["stale_marked_subscribers"] == 1
    assert body["fence_resolved_subscribers"] == 1
    assert body["error_recovered_subscribers"] == 0
    assert body["total_updated_subscribers"] == 2
    assert body["summary"]["stale_subscribers"] == 2


def test_control_plane_ack_recovery_route_rewinds_stale_backlog() -> None:
    client = _build_client()
    service = cast(DummyControlPlaneService, client.app.state.resources.control_plane_service)

    record = type("ControlPlaneSubscriberRecord", (), {})()
    record.stream_name = "filmu:events"
    record.group_name = "filmu-api"
    record.consumer_name = "consumer-1"
    record.node_id = "node-a"
    record.tenant_id = "tenant-main"
    record.status = "stale"
    record.last_read_offset = ">"
    record.last_delivered_event_id = "11-0"
    record.last_acked_event_id = "10-0"
    record.last_error = None
    record.claimed_at = datetime(2026, 4, 12, 2, 0, tzinfo=UTC)
    record.last_heartbeat_at = datetime(2026, 4, 12, 2, 0, tzinfo=UTC)
    record.created_at = datetime(2026, 4, 12, 2, 0, tzinfo=UTC)
    record.updated_at = datetime(2026, 4, 12, 2, 0, tzinfo=UTC)
    service.records = [record]

    response = client.post("/api/v1/operations/control-plane/ack-recovery", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["rewound_subscribers"] == 1
    assert body["stale_marked_subscribers"] == 0
    assert body["pending_without_ack_subscribers"] == 0
    assert body["total_updated_subscribers"] == 1
    assert body["summary"]["ack_pending_subscribers"] == 0
    assert service.records[0].last_delivered_event_id == "10-0"


def test_control_plane_pending_recovery_route_claims_replay_backlog() -> None:
    client = _build_client()
    resources = cast(Any, client.app.state.resources)
    service = cast(DummyControlPlaneService, resources.control_plane_service)
    replay_backplane = cast(DummyReplayBackplane, resources.replay_backplane)

    record = type("ControlPlaneSubscriberRecord", (), {})()
    record.stream_name = "filmu:events"
    record.group_name = "filmu-api"
    record.consumer_name = "consumer-1"
    record.node_id = "node-a"
    record.tenant_id = "tenant-main"
    record.status = "stale"
    record.last_read_offset = ">"
    record.last_delivered_event_id = "22-0"
    record.last_acked_event_id = "20-0"
    record.last_error = None
    record.claimed_at = datetime(2026, 4, 12, 2, 0, tzinfo=UTC)
    record.last_heartbeat_at = datetime(2026, 4, 12, 2, 0, tzinfo=UTC)
    record.created_at = datetime(2026, 4, 12, 2, 0, tzinfo=UTC)
    record.updated_at = datetime(2026, 4, 12, 2, 0, tzinfo=UTC)
    service.records = [record]

    response = client.post(
        "/api/v1/operations/control-plane/pending-recovery",
        params={"group_name": "filmu-api", "consumer_name": "recovery-ops", "claim_limit": 25},
        headers=_headers(),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["group_name"] == "filmu-api"
    assert body["consumer_name"] == "recovery-ops"
    assert body["claimed_count"] == 2
    assert body["claimed_event_ids"] == ["21-0", "22-0"]
    assert body["next_start_id"] == "23-0"
    assert body["pending_count_before"] == 2
    assert body["pending_count_after"] == 0
    assert body["pending_consumer_counts"] == {"recovery-ops": 0}
    assert body["required_actions"] == []
    assert replay_backplane.claims[0]["count"] == 25
    assert replay_backplane.claims[0]["tenant_id"] == "tenant-main"


def test_control_plane_automation_route_surfaces_background_recovery_status() -> None:
    client = _build_client()
    resources = cast(Any, client.app.state.resources)
    summary = type("ControlPlaneSummary", (), {})()
    summary.total_subscribers = 0
    summary.active_subscribers = 0
    summary.stale_subscribers = 0
    summary.error_subscribers = 0
    summary.fenced_subscribers = 0
    summary.ack_pending_subscribers = 0
    summary.stream_count = 0
    summary.group_count = 0
    summary.node_count = 0
    summary.tenant_count = 0
    summary.oldest_heartbeat_age_seconds = None
    summary.status_counts = {}
    summary.required_actions = ()
    summary.remaining_gaps = ()

    class DummyAutomation:
        def snapshot(self) -> Any:
            snapshot = type("ControlPlaneAutomationSnapshot", (), {})()
            snapshot.enabled = True
            snapshot.runner_status = "running"
            snapshot.interval_seconds = 300
            snapshot.active_within_seconds = 120
            snapshot.pending_min_idle_ms = 60_000
            snapshot.claim_limit = 25
            snapshot.max_claim_passes = 3
            snapshot.consumer_group = "filmu-api"
            snapshot.consumer_name = "recovery-automation"
            snapshot.service_attached = True
            snapshot.backplane_attached = True
            snapshot.last_run_at = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
            snapshot.last_success_at = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
            snapshot.last_failure_at = None
            snapshot.consecutive_failures = 0
            snapshot.last_error = None
            snapshot.remediation_updated_subscribers = 2
            snapshot.rewound_subscribers = 1
            snapshot.claimed_pending_events = 3
            snapshot.claim_passes = 2
            snapshot.pending_count_after = 0
            snapshot.summary = summary
            return snapshot

    resources.control_plane_automation = DummyAutomation()

    response = client.get("/api/v1/operations/control-plane/automation", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["enabled"] is True
    assert body["runner_status"] == "running"
    assert body["claimed_pending_events"] == 3
    assert body["pending_count_after"] == 0
    assert body["summary"]["total_subscribers"] == 0


def test_control_plane_automation_route_deduplicates_remaining_gaps() -> None:
    client = _build_client(
        settings_overrides={
            "FILMU_PY_CONTROL_PLANE_AUTOMATION": {
                "enabled": False,
            }
        }
    )
    resources = cast(Any, client.app.state.resources)
    resources.control_plane_service = None
    resources.control_plane_automation = None
    resources.replay_backplane = None

    response = client.get("/api/v1/operations/control-plane/automation", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert set(body["required_actions"]) == {
        "attach_control_plane_service",
        "attach_redis_replay_backplane",
        "enable_control_plane_automation",
    }
    assert body["remaining_gaps"] == [
        "durable replay/control-plane ownership is not configured",
        "background replay/control-plane automation is disabled",
        "durable replay pending-entry recovery is not attached",
    ]


def test_plugin_integration_readiness_route_validates_builtin_enterprise_plugins() -> None:
    plugin_settings = {
        "scraping": {
            "comet": {
                "enabled": True,
                "url": "https://comet.example",
                "contract_proof_refs": ["ops/plugins/comet-contract.md"],
                "soak_proof_refs": ["ops/plugins/comet-soak.md"],
            }
        },
        "content": {
            "overseerr": {
                "enabled": True,
                "url": "https://seerr.example",
                "api_key": "seerr-key",
                "contract_proof_refs": ["ops/plugins/seerr-contract.md"],
                "soak_proof_refs": ["ops/plugins/seerr-soak.md"],
            },
            "listrr": {
                "enabled": True,
                "url": "https://listrr.example",
                "movie_lists": ["movies-a"],
                "contract_proof_refs": ["ops/plugins/listrr-contract.md"],
                "soak_proof_refs": ["ops/plugins/listrr-soak.md"],
            },
        },
        "updaters": {
            "plex": {
                "enabled": True,
                "url": "https://plex.example",
                "token": "plex-token",
                "contract_proof_refs": ["ops/plugins/plex-contract.md"],
                "soak_proof_refs": ["ops/plugins/plex-soak.md"],
            }
        },
    }
    registry = PluginRegistry()
    harness = TestPluginContext(settings=plugin_settings)
    register_builtin_plugins(registry, context_provider=harness.provider())
    client = _build_client(
        plugin_registry=registry,
        settings_overrides={
            "FILMU_PY_SCRAPING": plugin_settings["scraping"],
            "FILMU_PY_UPDATERS": plugin_settings["updaters"],
        },
    )
    resources = cast(Any, client.app.state.resources)
    resources.plugin_settings_payload = plugin_settings

    response = client.get("/api/v1/operations/plugins/integration-readiness", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    by_name = {entry["name"]: entry for entry in body["plugins"]}
    assert by_name["comet"]["ready"] is True
    assert by_name["seerr"]["config_source"] == "content.overseerr"
    assert by_name["listrr"]["missing_settings"] == []
    assert by_name["plex"]["ready"] is True


def test_observability_convergence_route_surfaces_cross_process_exit_gates() -> None:
    client = _build_client(
        settings_overrides={
            "FILMU_PY_OTEL_ENABLED": True,
            "FILMU_PY_OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector.internal:4318",
            "FILMU_PY_LOG_SHIPPER": {
                "enabled": True,
                "type": "vector",
                "target": "opensearch://logs-filmu",
                "healthcheck_url": "https://ops.example.test/vector/health",
                "field_mapping_version": "filmu-ecs-v1",
            },
            "FILMU_PY_OBSERVABILITY": {
                "environment_shipping_enabled": True,
                "search_backend": "opensearch",
                "alerting_enabled": True,
                "rust_trace_correlation_enabled": True,
                "required_correlation_fields": [
                    "request.id",
                    "trace.id",
                    "tenant.id",
                    "vfs.session_id",
                    "vfs.daemon_id",
                    "catalog.entry_id",
                    "provider.file_id",
                    "vfs.handle_key",
                ],
                "proof_refs": ["ops/wave4/log-pipeline-rollout.md"],
            },
        }
    )

    response = client.get("/api/v1/operations/observability/convergence", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    assert body["structured_logging_enabled"] is True
    assert body["otel_enabled"] is True
    assert body["otel_endpoint_configured"] is True
    assert body["log_shipper_enabled"] is True
    assert body["log_shipper_type"] == "vector"
    assert body["log_shipper_target_configured"] is True
    assert body["log_shipper_healthcheck_configured"] is True
    assert body["search_backend"] == "opensearch"
    assert body["environment_shipping_enabled"] is True
    assert body["alerting_enabled"] is True
    assert body["rust_trace_correlation_enabled"] is True
    assert body["correlation_contract_complete"] is True
    assert body["required_correlation_fields"] == [
        "request.id",
        "trace.id",
        "tenant.id",
        "vfs.session_id",
        "vfs.daemon_id",
        "catalog.entry_id",
        "provider.file_id",
        "vfs.handle_key",
    ]
    assert body["proof_refs"] == ["ops/wave4/log-pipeline-rollout.md"]
    assert body["required_actions"] == []
    assert body["remaining_gaps"] == []


def test_downloader_orchestration_route_surfaces_ordered_failover_and_plugin_gap() -> None:
    plugin_registry = PluginRegistry()
    harness = TestPluginContext(settings={"plugins": {}})
    register_builtin_plugins(plugin_registry, context_provider=harness.provider())
    client = _build_client(
        plugin_registry=plugin_registry,
        settings_overrides={
            "FILMU_PY_DOWNLOADERS": {
                "real_debrid": {"enabled": True, "api_key": "rd-token"},
                "all_debrid": {"enabled": True, "api_key": "ad-token"},
                "debrid_link": {"enabled": False, "api_key": ""},
                "stremthru": {
                    "enabled": True,
                    "url": "https://stremthru.example.test",
                    "token": "st-token",
                },
            }
        },
    )

    response = client.get("/api/v1/operations/downloader-orchestration", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["selection_mode"] == "ordered_failover_policy_fanout"
    assert body["selected_provider"] == "realdebrid"
    assert body["multi_provider_enabled"] is True
    assert body["plugin_downloaders_registered"] == 1
    assert body["worker_plugin_dispatch_ready"] is True
    assert body["fanout_ready"] is True
    assert body["multi_container_ready"] is True
    assert body["required_actions"] == []
    providers = {(row["name"], row["source"]): row for row in body["providers"]}
    assert providers[("realdebrid", "builtin")]["enabled"] is True
    assert providers[("realdebrid", "builtin")]["selected"] is True
    assert providers[("alldebrid", "builtin")]["enabled"] is True
    assert providers[("stremthru", "plugin")]["configured"] is True
    assert providers[("stremthru", "plugin")]["priority"] == 4
    assert body["remaining_gaps"] == []


def test_tenant_quota_route_returns_current_policy_visibility() -> None:
    client = _build_client(
        settings_overrides={
            "FILMU_PY_TENANT_QUOTAS": {
                "enabled": True,
                "version": "quota-v2",
                "tenants": {"tenant-main": {"api_requests_per_minute": 25}},
            }
        }
    )

    response = client.get("/api/v1/tenants/quota", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["tenant_id"] == "tenant-main"
    assert body["enabled"] is True
    assert body["policy_version"] == "quota-v2"
    assert body["api_requests_per_minute"] == 25
    assert body["enforcement_points"][0] == "api_request_intake"


def test_worker_queue_route_returns_control_plane_snapshot() -> None:
    client = _build_client(arq_enabled=True)
    redis = cast(DummyRedis, client.app.state.resources.redis)
    now_milliseconds = time.time() * 1000.0
    redis.sorted_sets["filmu-py"] = {
        "job-ready": now_milliseconds - 1_000.0,
        "job-deferred": now_milliseconds + 3_000.0,
    }
    redis.values["arq:in-progress:job-ready"] = b"active"
    redis.values["arq:retry:job-ready"] = b"retry"
    redis.values["arq:result:job-done"] = b"done"
    redis.lists["arq:dead-letter:filmu-py"] = [
        json.dumps(
            {
                "task": "scrape_item",
                "reason": "timeout",
                "reason_code": "timeout",
                "queued_at": "2026-04-12T12:00:00+00:00",
            }
        ).encode("utf-8")
    ]

    response = client.get("/api/v1/workers/queue", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["queue_name"] == "filmu-py"
    assert body["arq_enabled"] is True
    assert body["observed_at"].endswith("Z")
    assert body["total_jobs"] == 2
    assert body["ready_jobs"] == 1
    assert body["deferred_jobs"] == 1
    assert body["in_progress_jobs"] == 1
    assert body["retry_jobs"] == 1
    assert body["result_jobs"] == 1
    assert body["dead_letter_jobs"] == 1
    assert body["alert_level"] == "critical"
    assert body["alerts"][0]["code"] == "dead_letter_backlog"
    assert body["dead_letter_oldest_age_seconds"] is not None
    assert body["dead_letter_reason_counts"] == {"timeout": 1}
    assert 0.5 <= body["oldest_ready_age_seconds"] <= 3.0
    assert 0.0 <= body["next_scheduled_in_seconds"] <= 5.0


def test_worker_queue_history_route_returns_bounded_snapshots() -> None:
    client = _build_client(arq_enabled=True)
    redis = cast(DummyRedis, client.app.state.resources.redis)
    now_milliseconds = time.time() * 1000.0
    redis.sorted_sets["filmu-py"] = {"job-ready": now_milliseconds - 2_000.0}

    first = client.get("/api/v1/workers/queue", headers=_headers())
    second = client.get("/api/v1/workers/queue/history", headers=_headers())

    assert first.status_code == 200
    assert second.status_code == 200
    body = second.json()
    history = body["history"]
    assert len(history) == 1
    assert history[0]["total_jobs"] == 1
    assert history[0]["ready_jobs"] == 1
    assert history[0]["alert_level"] == "ok"
    assert body["applied_filters"] == {
        "alert_level": None,
        "min_dead_letter_jobs": 0,
        "reason_code": None,
    }
    assert body["summary"] == {
        "points": 1,
        "latest_alert_level": "ok",
        "critical_points": 0,
        "warning_points": 0,
        "dead_letter_points": 0,
        "max_ready_jobs": 1,
        "max_dead_letter_jobs": 0,
        "max_oldest_ready_age_seconds": history[0]["oldest_ready_age_seconds"],
        "latest_dead_letter_oldest_age_seconds": None,
        "max_dead_letter_oldest_age_seconds": None,
        "latest_dead_letter_reason": None,
        "latest_dead_letter_reason_counts": {},
        "total_dead_letter_reason_counts": {},
        "dead_letter_reason_points": {},
    }


def test_worker_queue_history_route_supports_dead_letter_filters_and_reason_rollups() -> None:
    client = _build_client(arq_enabled=True)
    redis = cast(DummyRedis, client.app.state.resources.redis)
    redis.lists["arq:queue-status-history:filmu-py"] = [
        json.dumps(
            {
                "observed_at": "2026-04-13T01:05:00Z",
                "total_jobs": 4,
                "ready_jobs": 1,
                "deferred_jobs": 0,
                "in_progress_jobs": 1,
                "retry_jobs": 0,
                "dead_letter_jobs": 2,
                "oldest_ready_age_seconds": 12.0,
                "next_scheduled_in_seconds": None,
                "alert_level": "critical",
                "dead_letter_oldest_age_seconds": 240.0,
                "dead_letter_reason_counts": {"timeout": 2, "rate_limited": 1},
            }
        ).encode("utf-8"),
        json.dumps(
            {
                "observed_at": "2026-04-13T01:00:00Z",
                "total_jobs": 2,
                "ready_jobs": 0,
                "deferred_jobs": 1,
                "in_progress_jobs": 0,
                "retry_jobs": 1,
                "dead_letter_jobs": 1,
                "oldest_ready_age_seconds": None,
                "next_scheduled_in_seconds": 30.0,
                "alert_level": "warning",
                "dead_letter_oldest_age_seconds": 180.0,
                "dead_letter_reason_counts": {"timeout": 1},
            }
        ).encode("utf-8"),
    ]

    response = client.get(
        "/api/v1/workers/queue/history",
        params={"alert_level": "critical", "reason_code": "timeout", "min_dead_letter_jobs": 2},
        headers=_headers(),
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body["history"]) == 1
    assert body["applied_filters"] == {
        "alert_level": "critical",
        "min_dead_letter_jobs": 2,
        "reason_code": "timeout",
    }
    assert body["summary"] == {
        "points": 1,
        "latest_alert_level": "critical",
        "critical_points": 1,
        "warning_points": 0,
        "dead_letter_points": 1,
        "max_ready_jobs": 1,
        "max_dead_letter_jobs": 2,
        "max_oldest_ready_age_seconds": 12.0,
        "latest_dead_letter_oldest_age_seconds": 240.0,
        "max_dead_letter_oldest_age_seconds": 240.0,
        "latest_dead_letter_reason": "timeout",
        "latest_dead_letter_reason_counts": {"timeout": 2, "rate_limited": 1},
        "total_dead_letter_reason_counts": {"rate_limited": 1, "timeout": 2},
        "dead_letter_reason_points": {"rate_limited": 1, "timeout": 1},
    }


def test_worker_metadata_reindex_route_returns_latest_run_summary() -> None:
    client = _build_client(arq_enabled=True)
    redis = cast(DummyRedis, client.app.state.resources.redis)
    redis.lists["arq:metadata-reindex-history:filmu-py"] = [
        json.dumps(
            {
                "observed_at": "2026-04-13T00:30:00Z",
                "processed": 3,
                "queued": 1,
                "reconciled": 1,
                "skipped_active": 1,
                "failed": 0,
                "outcome": "ok",
                "run_failed": False,
                "last_error": None,
            }
        ).encode("utf-8")
    ]

    response = client.get("/api/v1/workers/metadata-reindex", headers=_headers())

    assert response.status_code == 200
    assert response.json() == {
        "queue_name": "filmu-py",
        "schedule_offset_minutes": 30,
        "has_history": True,
        "observed_at": "2026-04-13T00:30:00Z",
        "processed": 3,
        "queued": 1,
        "reconciled": 1,
        "skipped_active": 1,
        "failed": 0,
        "repair_attempted": 0,
        "repair_enriched": 0,
        "repair_skipped_no_tmdb_id": 0,
        "repair_failed": 0,
        "repair_requeued": 0,
        "repair_skipped_active": 0,
        "outcome": "ok",
        "run_failed": False,
        "last_error": None,
    }


def test_worker_metadata_reindex_history_route_returns_bounded_summary() -> None:
    client = _build_client(arq_enabled=True)
    redis = cast(DummyRedis, client.app.state.resources.redis)
    redis.lists["arq:metadata-reindex-history:filmu-py"] = [
        json.dumps(
            {
                "observed_at": "2026-04-13T00:35:00Z",
                "processed": 0,
                "queued": 0,
                "reconciled": 0,
                "skipped_active": 0,
                "failed": 0,
                "repair_attempted": 1,
                "repair_enriched": 1,
                "repair_skipped_no_tmdb_id": 0,
                "repair_failed": 0,
                "repair_requeued": 1,
                "repair_skipped_active": 0,
                "outcome": "critical",
                "run_failed": True,
                "last_error": "metadata source unavailable",
            }
        ).encode("utf-8"),
        json.dumps(
            {
                "observed_at": "2026-04-13T00:30:00Z",
                "processed": 3,
                "queued": 1,
                "reconciled": 1,
                "skipped_active": 1,
                "failed": 1,
                "repair_attempted": 0,
                "repair_enriched": 0,
                "repair_skipped_no_tmdb_id": 1,
                "repair_failed": 1,
                "repair_requeued": 0,
                "repair_skipped_active": 0,
                "outcome": "warning",
                "run_failed": False,
                "last_error": None,
            }
        ).encode("utf-8"),
    ]

    response = client.get("/api/v1/workers/metadata-reindex/history", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["queue_name"] == "filmu-py"
    assert body["schedule_offset_minutes"] == 30
    assert len(body["history"]) == 2
    assert body["summary"] == {
        "points": 2,
        "latest_outcome": "critical",
        "critical_points": 1,
        "warning_points": 1,
        "total_processed": 3,
        "total_queued": 1,
        "total_reconciled": 1,
        "total_skipped_active": 1,
        "total_failed": 1,
        "total_repair_attempted": 1,
        "total_repair_enriched": 1,
        "total_repair_skipped_no_tmdb_id": 1,
        "total_repair_failed": 1,
        "total_repair_requeued": 1,
        "total_repair_skipped_active": 0,
        "max_processed": 3,
        "max_failed": 1,
        "latest_run_failed": True,
        "latest_error": "metadata source unavailable",
    }


def test_runtime_lifecycle_route_returns_bounded_transition_history() -> None:
    client = _build_client()

    response = client.get("/api/v1/operations/runtime", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["phase"] == "bootstrap"
    assert body["health"] == "healthy"
    assert body["detail"] == "runtime_bootstrap_pending"
    assert len(body["transitions"]) == 1
    assert body["transitions"][0]["phase"] == "bootstrap"


def test_stats_route_rejects_cross_tenant_requests_without_delegated_scope() -> None:
    client = _build_client()

    response = client.get(
        "/api/v1/stats?tenant_id=tenant-other",
        headers={
            **_headers(),
            "x-actor-roles": "",
            "x-actor-scopes": "library:read",
        },
    )

    assert response.status_code == 403


def test_calendar_route_allows_cross_tenant_requests_with_authorized_tenants() -> None:
    client = _build_client()

    response = client.get(
        "/api/v1/calendar?tenant_id=tenant-analytics",
        headers={
            **_headers(),
            "x-actor-roles": "",
            "x-actor-scopes": "library:read",
            "x-actor-authorized-tenants": "tenant-main,tenant-analytics",
        },
    )

    assert response.status_code == 200


def test_dashboard_routes_require_api_key() -> None:
    """Dashboard-essential routes remain protected by the shared API-key dependency."""

    client = _build_client()
    for path in ["/api/v1/stats", "/api/v1/services", "/api/v1/downloader_user_info"]:
        response = client.get(path)
        assert response.status_code == 401
