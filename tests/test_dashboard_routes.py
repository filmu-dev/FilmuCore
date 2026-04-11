"""Dashboard-essential compatibility route tests."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, cast

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import AnyUrl, SecretStr

from filmu_py.api.router import create_api_router
from filmu_py.config import Settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.graphql.plugin_registry import GraphQLPluginRegistry
from filmu_py.plugins.manifest import PluginManifest
from filmu_py.plugins.registry import PluginCapabilityKind, PluginRegistry
from filmu_py.resources import AppResources
from filmu_py.services.debrid import DownloaderAccountService
from filmu_py.services.media import StatsProjection, StatsYearReleaseRecord


class DummyRedis:
    """Minimal Redis stub used by route-level tests without network dependencies."""

    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.sorted_sets: dict[str, dict[str, float]] = {}
        self.lists: dict[str, list[bytes]] = {}

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


def _build_settings(*, arq_enabled: bool = False, temporal_enabled: bool = False) -> Settings:
    """Create deterministic settings payload for dashboard compatibility tests."""

    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
        FILMU_PY_ARQ_ENABLED=arq_enabled,
        FILMU_PY_TEMPORAL_ENABLED=temporal_enabled,
    )


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
    plugin_registry: PluginRegistry | None = None,
    plugin_load_report: Any | None = None,
    security_identity_service: Any | None = None,
) -> TestClient:
    """Build a FastAPI test app with compatibility routers and mocked resources."""

    settings = _build_settings(arq_enabled=arq_enabled, temporal_enabled=temporal_enabled)
    redis = DummyRedis()
    registry = GraphQLPluginRegistry()

    app = FastAPI()
    app.state.resources = AppResources(
        settings=settings,
        redis=redis,  # type: ignore[arg-type]
        cache=CacheManager(redis=redis, namespace="test"),  # type: ignore[arg-type]
        rate_limiter=DistributedRateLimiter(redis=redis),  # type: ignore[arg-type]
        event_bus=EventBus(),
        db=DummyDatabaseRuntime(),  # type: ignore[arg-type]
        media_service=DummyMediaService(snapshot=_build_snapshot()),  # type: ignore[arg-type]
        graphql_plugin_registry=registry,
        plugin_registry=plugin_registry,
        security_identity_service=security_identity_service,
    )
    app.state.plugin_load_report = plugin_load_report
    app.include_router(create_api_router())

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
        "quarantined_plugins": 0,
        "quarantine_recommended_plugins": 0,
        "unsigned_external_plugins": 1,
        "unverified_signature_plugins": 0,
        "publisher_policy_rejections": 0,
        "trust_policy_rejections": 0,
        "sandbox_profile_counts": {"restricted": 1},
        "tenancy_mode_counts": {"tenant": 1},
        "recommended_actions": ["require_external_plugin_signature"],
        "remaining_gaps": [
            "runtime sandbox isolation is still in-process",
            "operator quarantine/revocation persistence is still trust-store driven",
            "external plugin artifact provenance is not yet SBOM/signing-policy complete",
        ],
    }
    assert body["plugins"][0]["name"] == "external-scraper"


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
        "principal_key": "operator-1",
        "principal_type": "service",
        "service_account_api_key_id": "primary",
    }


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
    assert body["permissions_model"] == "role_scope_effective_permissions_with_tenant_scope"
    assert body["authorization_tenant_scope"] == "self"
    assert body["oidc_claims_present"] is True
    assert body["warnings"] == ["authentication is still API-key anchored"]
    decisions = {decision["name"]: decision for decision in body["decisions"]}
    assert decisions["library_read"]["allowed"] is True
    assert decisions["playback_operate"]["allowed"] is True
    assert decisions["settings_write"]["allowed"] is False
    assert decisions["settings_write"]["missing_permissions"] == ["settings:write"]
    assert decisions["api_key_rotate"]["allowed"] is False
    assert body["remaining_gaps"] == [
        "OIDC/SSO validation is not yet active",
        "ABAC policy is limited to permission and tenant-scope checks",
        "policy inventory is not yet persisted as first-class operator configuration",
    ]


def test_operations_governance_route_returns_enterprise_slice_posture() -> None:
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
        "identity_authz",
        "tenant_boundary",
        "distributed_control_plane",
        "sre_program",
        "operator_log_pipeline",
    }
    assert body["playback_gate"]["status"] == "partial"
    assert "proof:playback:gate:enterprise package entrypoint exists" in body[
        "playback_gate"
    ]["evidence"]
    assert body["identity_authz"]["status"] == "partial"
    assert "authentication_mode=api_key" in body["identity_authz"]["evidence"]
    assert "oidc_claims_present=True" in body["identity_authz"]["evidence"]
    assert body["tenant_boundary"]["status"] == "partial"
    assert "request_tenant_id=tenant-main" in body["tenant_boundary"]["evidence"]
    assert body["distributed_control_plane"]["status"] == "not_ready"
    assert "EventBus backend=process_local" in body["distributed_control_plane"]["evidence"]
    assert body["sre_program"]["status"] == "partial"
    assert body["operator_log_pipeline"]["status"] == "partial"
    assert "structured_logging_enabled=True" in body["operator_log_pipeline"]["evidence"]


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
    redis.lists["arq:dead-letter:filmu-py"] = [b"dlq"]

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
    assert body["summary"] == {
        "points": 1,
        "latest_alert_level": "ok",
        "critical_points": 0,
        "warning_points": 0,
        "max_ready_jobs": 1,
        "max_dead_letter_jobs": 0,
        "max_oldest_ready_age_seconds": history[0]["oldest_ready_age_seconds"],
    }


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
