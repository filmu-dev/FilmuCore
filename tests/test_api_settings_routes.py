"""Compatibility tests for `/api/v1/settings/*` routes."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import AnyUrl, SecretStr

from filmu_py.api import deps as api_deps
from filmu_py.api.router import create_api_router
from filmu_py.api.routes import default as default_routes
from filmu_py.api.routes import settings as settings_routes
from filmu_py.config import Settings
from filmu_py.config import get_settings as get_runtime_settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.graphql.plugin_registry import GraphQLPluginRegistry
from filmu_py.resources import AppResources
from filmu_py.workers import tasks


class DummyRedis:
    """Minimal Redis stub used by route-level tests without network dependencies."""

    def ping(self, **kwargs: Any) -> bool:
        _ = kwargs
        return True

    async def aclose(self, close_connection_pool: bool | None = None) -> None:
        _ = close_connection_pool
        return None


class DummyDatabaseRuntime:
    """No-op DB runtime placeholder for application resources in tests."""

    def __init__(self) -> None:
        self.settings_blob: dict[str, Any] | None = None

    async def dispose(self) -> None:
        return None


class DummyMediaService:
    """No-op media service placeholder for typed app resources in tests."""

    pass


def _build_settings() -> Settings:
    """Create deterministic settings payload for API compatibility tests."""

    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_API_KEY_ID="primary-test",
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
    )


def _compatibility_payload() -> dict[str, Any]:
    """Return one valid full compatibility payload for route tests."""

    return _build_settings().to_compatibility_dict()


def _build_client() -> tuple[TestClient, AppResources]:
    """Build a FastAPI test app with only compatibility routers and mocked resources."""

    settings = _build_settings()
    redis = DummyRedis()

    app = FastAPI()
    resources = AppResources(
        settings=settings,
        redis=redis,  # type: ignore[arg-type]
        cache=CacheManager(redis=redis, namespace="test"),  # type: ignore[arg-type]
        rate_limiter=DistributedRateLimiter(redis=redis),  # type: ignore[arg-type]
        event_bus=EventBus(),
        db=DummyDatabaseRuntime(),  # type: ignore[arg-type]
        media_service=DummyMediaService(),  # type: ignore[arg-type]
        graphql_plugin_registry=GraphQLPluginRegistry(),
    )
    app.state.resources = resources
    app.include_router(create_api_router())

    return TestClient(app), resources


def _headers(**overrides: str) -> dict[str, str]:
    """Return valid auth headers for compatibility API requests."""

    headers = {
        "x-api-key": "a" * 32,
        "x-actor-id": "operator-1",
        "x-tenant-id": "tenant-main",
        "x-actor-roles": "platform:admin,settings:write",
        "x-actor-scopes": "backend:admin,settings:write",
    }
    headers.update(overrides)
    return headers


def _install_settings_persistence_stubs(monkeypatch: Any) -> None:
    """Patch settings persistence helpers to write into the dummy DB runtime."""

    async def fake_save_settings(db: DummyDatabaseRuntime, data: dict[str, Any]) -> None:
        db.settings_blob = deepcopy(data)

    monkeypatch.setattr(settings_routes, "persist_settings_blob", fake_save_settings)
    monkeypatch.setattr(default_routes, "save_settings", fake_save_settings)


def test_settings_schema_keys_ignores_unknown_keys() -> None:
    """Unknown schema keys should be ignored for phased compatibility rollout safety."""

    client, _ = _build_client()
    response = client.get(
        "/api/v1/settings/schema/keys",
        params={"keys": "filesystem,unknown_key", "title": "Settings"},
        headers=_headers(),
    )

    assert response.status_code == 200
    body = response.json()
    assert "mount_path" in body["properties"]
    assert "filesystem" not in body["properties"]
    assert "unknown_key" not in body["properties"]


def test_settings_get_returns_runtime_compatibility_dict() -> None:
    """Top-level settings GET should project the in-memory runtime settings instance."""

    client, resources = _build_client()
    response = client.get("/api/v1/settings", headers=_headers())

    assert response.status_code == 200
    assert response.json() == resources.settings.to_compatibility_dict()


def test_settings_get_paths_returns_null_for_missing_path() -> None:
    """Missing settings paths should return `null` instead of failing the whole request."""

    client, _ = _build_client()
    response = client.get(
        "/api/v1/settings/get/filesystem,missing.path",
        headers=_headers(),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["mount_path"] == "/mnt/rivenfs"
    assert "filesystem" not in body
    assert body["missing.path"] is None


def test_settings_schema_keys_unwraps_nested_downloaders_section() -> None:
    """Nested compatibility sections should expose their inner fields at the schema top level."""

    client, _ = _build_client()
    response = client.get(
        "/api/v1/settings/schema/keys",
        params={"keys": "downloaders", "title": "Settings"},
        headers=_headers(),
    )

    assert response.status_code == 200
    body = response.json()
    assert "real_debrid" in body["properties"]
    assert body["properties"]["real_debrid"]["type"] == "object"
    assert "api_key" in body["properties"]["real_debrid"]["properties"]
    assert "enabled" in body["properties"]["real_debrid"]["properties"]


def test_settings_get_paths_unwraps_scraping_section_values() -> None:
    """Top-level nested section reads should return section contents without the wrapper key."""

    client, _ = _build_client()
    response = client.get("/api/v1/settings/get/scraping", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert "after_2" in body
    assert "torrentio" in body
    assert body["torrentio"]["enabled"] is False
    assert "scraping" not in body


def test_settings_schema_keys_for_scalar_version_is_unchanged() -> None:
    """Scalar compatibility keys should keep their direct schema descriptor shape."""

    client, _ = _build_client()
    response = client.get(
        "/api/v1/settings/schema/keys",
        params={"keys": "version", "title": "Settings"},
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json()["properties"] == {"version": {"type": "string"}}


def test_settings_set_requires_value_for_each_path() -> None:
    """Path-based updates must provide one value per requested path."""

    client, _ = _build_client()
    response = client.post(
        "/api/v1/settings/set/filesystem,logging",
        json={"filesystem": {"library_profiles": {}}},
        headers=_headers(),
    )

    assert response.status_code == 400
    assert "Missing values for paths" in response.json()["detail"]


def test_settings_put_valid_payload_persists_and_updates_runtime_settings(monkeypatch: Any) -> None:
    """A valid full payload should be validated, persisted, and activated in memory."""

    client, resources = _build_client()
    _install_settings_persistence_stubs(monkeypatch)
    payload = _compatibility_payload()
    payload["downloaders"]["real_debrid"] = {"enabled": False, "api_key": ""}
    payload["downloaders"]["all_debrid"] = {"enabled": True, "api_key": "ad-token"}

    response = client.put("/api/v1/settings", json=payload, headers=_headers())

    assert response.status_code == 200
    assert response.json() == payload
    assert resources.db.settings_blob == payload
    assert resources.settings.downloaders.real_debrid.enabled is False
    assert resources.settings.downloaders.all_debrid.enabled is True
    assert resources.settings.downloaders.all_debrid.api_key == "ad-token"


def test_settings_put_invalid_ranking_payload_returns_422_without_runtime_or_db_mutation(
    monkeypatch: Any,
) -> None:
    """Invalid compatibility payloads should be rejected before persistence or runtime swap."""

    client, resources = _build_client()
    _install_settings_persistence_stubs(monkeypatch)
    original = resources.settings.to_compatibility_dict()
    payload = _compatibility_payload()
    del payload["ranking"]["custom_ranks"]["hdr"]["dolby_vision"]

    response = client.put("/api/v1/settings", json=payload, headers=_headers())

    assert response.status_code == 422
    assert isinstance(response.json()["detail"], list)
    assert resources.db.settings_blob is None
    assert resources.settings.to_compatibility_dict() == original


def test_settings_get_after_put_returns_updated_values(monkeypatch: Any) -> None:
    """Subsequent GETs should serve the updated in-memory settings after a successful PUT."""

    client, _ = _build_client()
    _install_settings_persistence_stubs(monkeypatch)
    payload = _compatibility_payload()
    payload["logging"]["retention_hours"] = 72

    put_response = client.put("/api/v1/settings", json=payload, headers=_headers())
    get_response = client.get("/api/v1/settings", headers=_headers())

    assert put_response.status_code == 200
    assert get_response.status_code == 200
    assert get_response.json()["logging"]["retention_hours"] == 72


def test_settings_set_paths_persists_merged_runtime_payload(monkeypatch: Any) -> None:
    """Path-based compatibility saves should persist the merged full payload for the current frontend."""

    client, resources = _build_client()
    _install_settings_persistence_stubs(monkeypatch)
    payload = {
        "filesystem": {
            "mount_path": "/mnt/rivenfs",
            "library_profiles": {
                "kids": {
                    "name": "Kids",
                    "library_path": "/kids",
                    "enabled": True,
                    "filter_rules": {"content_types": ["movie"]},
                }
            },
            "cache_dir": "/dev/shm/riven-cache",
            "cache_max_size_mb": 12600,
            "cache_ttl_seconds": 3600,
            "cache_eviction": "LRU",
            "cache_metrics": True,
            "movie_dir_template": "{title} ({year}) {{tmdb-{tmdb_id}}}",
            "movie_file_template": "{title} ({year})",
            "show_dir_template": "{title} ({year}) {{tvdb-{tvdb_id}}}",
            "season_dir_template": "Season {season:02d}",
            "episode_file_template": "{show[title]} - s{season:02d}e{episode:02d}",
        }
    }

    response = client.post("/api/v1/settings/set/filesystem", json=payload, headers=_headers())

    assert response.status_code == 200
    assert resources.settings.filesystem.library_profiles["kids"].library_path == "/kids"
    assert resources.db.settings_blob is not None
    assert resources.db.settings_blob["filesystem"]["library_profiles"]["kids"]["name"] == "Kids"
    assert resources.plugin_settings_payload == resources.db.settings_blob


def test_settings_set_filesystem_accepts_flat_unwrapped_properties(monkeypatch: Any) -> None:
    """Flat section payloads should be re-wrapped into the compatibility section before persistence."""

    client, resources = _build_client()
    _install_settings_persistence_stubs(monkeypatch)
    payload = {
        "mount_path": "/mnt/custom",
        "cache_dir": "/tmp/riven-cache",
        "cache_max_size_mb": 2048,
    }

    response = client.post("/api/v1/settings/set/filesystem", json=payload, headers=_headers())

    assert response.status_code == 200
    assert resources.settings.filesystem.mount_path == "/mnt/custom"
    assert resources.settings.filesystem.cache_dir == "/tmp/riven-cache"
    assert resources.settings.filesystem.cache_max_size_mb == 2048
    assert resources.db.settings_blob is not None
    assert resources.db.settings_blob["filesystem"]["mount_path"] == "/mnt/custom"


def test_settings_set_multiple_sections_accepts_flat_unwrapped_properties(monkeypatch: Any) -> None:
    """Flat payloads spanning multiple nested sections should be partitioned and merged correctly."""

    client, resources = _build_client()
    _install_settings_persistence_stubs(monkeypatch)
    payload = {
        "after_2": 3.5,
        "bucket_limit": 9,
        "name": "custom-ranking",
        "enabled": True,
        "schedule_offset_minutes": 45,
    }

    response = client.post(
        "/api/v1/settings/set/scraping,ranking,indexer",
        json=payload,
        headers=_headers(),
    )

    assert response.status_code == 200
    assert resources.settings.scraping.after_2 == 3.5
    assert resources.settings.scraping.bucket_limit == 9
    assert resources.settings.ranking.name == "custom-ranking"
    assert resources.settings.ranking.enabled is True
    assert resources.settings.indexer.schedule_offset_minutes == 45
    assert resources.db.settings_blob is not None
    assert resources.db.settings_blob["scraping"]["after_2"] == 3.5
    assert resources.db.settings_blob["ranking"]["name"] == "custom-ranking"
    assert resources.db.settings_blob["indexer"]["schedule_offset_minutes"] == 45


def test_settings_get_then_set_then_get_round_trip_for_unwrapped_section(monkeypatch: Any) -> None:
    """GET-unwrapped section values should be accepted back by SET without re-wrapping on the client."""

    client, _ = _build_client()
    _install_settings_persistence_stubs(monkeypatch)

    get_before = client.get("/api/v1/settings/get/filesystem", headers=_headers())
    assert get_before.status_code == 200

    set_response = client.post(
        "/api/v1/settings/set/filesystem",
        json=get_before.json(),
        headers=_headers(),
    )
    assert set_response.status_code == 200

    get_after = client.get("/api/v1/settings/get/filesystem", headers=_headers())
    assert get_after.status_code == 200
    assert get_after.json() == get_before.json()


def test_generate_apikey_rotates_runtime_key_and_persists_it(monkeypatch: Any) -> None:
    """API key regeneration should activate and persist the new backend key immediately."""

    client, resources = _build_client()
    _install_settings_persistence_stubs(monkeypatch)
    response = client.post("/api/v1/generateapikey", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert isinstance(body["key"], str)
    assert len(body["key"]) >= 32
    assert body["key"] != "a" * 32
    assert isinstance(body["api_key_id"], str)
    assert body["api_key_id"] != "primary-test"
    assert "Update BACKEND_API_KEY" in body["warning"]
    assert resources.settings.api_key.get_secret_value() == body["key"]
    assert resources.settings.api_key_id == body["api_key_id"]
    assert resources.db.settings_blob is not None
    assert resources.db.settings_blob["api_key"] == body["key"]
    assert resources.db.settings_blob["api_key_id"] == body["api_key_id"]


def test_settings_put_requires_settings_write_permission(monkeypatch: Any) -> None:
    client, _resources = _build_client()
    _install_settings_persistence_stubs(monkeypatch)
    payload = _compatibility_payload()

    response = client.put(
        "/api/v1/settings",
        json=payload,
        headers=_headers(
            **{"x-actor-roles": "", "x-actor-scopes": "playback:read"},
        ),
    )

    assert response.status_code == 403


def test_generate_apikey_requires_rotation_permission(monkeypatch: Any) -> None:
    client, _resources = _build_client()
    _install_settings_persistence_stubs(monkeypatch)

    response = client.post(
        "/api/v1/generateapikey",
        headers=_headers(
            **{
                "x-actor-roles": "",
                "x-actor-scopes": "settings:write",
            }
        ),
    )

    assert response.status_code == 403


def test_settings_put_emits_audit_event_with_actor_and_tenant(monkeypatch: Any) -> None:
    client, _resources = _build_client()
    _install_settings_persistence_stubs(monkeypatch)
    payload = _compatibility_payload()
    captured: list[dict[str, Any]] = []

    def fake_audit_action(request: Any, **kwargs: Any) -> None:
        auth = api_deps.get_auth_context(request)
        captured.append(
            {
                "action": kwargs["action"],
                "target": kwargs["target"],
                "actor_id": auth.actor_id,
                "tenant_id": auth.tenant_id,
                "roles": auth.roles,
            }
        )

    monkeypatch.setattr(settings_routes, "audit_action", fake_audit_action)
    response = client.put(
        "/api/v1/settings",
        json=payload,
        headers=_headers(
            **{
                "x-actor-id": "platform-admin",
                "x-tenant-id": "tenant-enterprise",
                "x-actor-roles": "platform:admin,settings:write",
            }
        ),
    )

    assert response.status_code == 200
    assert captured == [
        {
            "action": "settings.put_current",
            "target": "runtime.settings",
            "actor_id": "platform-admin",
            "tenant_id": "tenant-enterprise",
            "roles": ("platform:admin", "settings:write"),
        }
    ]


def test_generate_apikey_emits_audit_event_with_actor_and_tenant(monkeypatch: Any) -> None:
    client, _resources = _build_client()
    _install_settings_persistence_stubs(monkeypatch)
    captured: list[dict[str, Any]] = []

    def fake_audit_action(request: Any, **kwargs: Any) -> None:
        auth = api_deps.get_auth_context(request)
        captured.append(
            {
                "action": kwargs["action"],
                "target": kwargs["target"],
                "actor_id": auth.actor_id,
                "tenant_id": auth.tenant_id,
                "scopes": auth.scopes,
            }
        )

    monkeypatch.setattr(default_routes, "audit_action", fake_audit_action)
    response = client.post(
        "/api/v1/generateapikey",
        headers=_headers(
            **{
                "x-actor-id": "security-admin",
                "x-tenant-id": "tenant-enterprise",
                "x-actor-scopes": "backend:admin,security:apikey.rotate",
            }
        ),
    )

    assert response.status_code == 200
    assert captured == [
        {
            "action": "security.generate_apikey",
            "target": "runtime.api_key",
            "actor_id": "security-admin",
            "tenant_id": "tenant-enterprise",
            "scopes": ("backend:admin", "security:apikey.rotate"),
        }
    ]


def test_settings_put_audit_uses_configured_api_key_identifier(monkeypatch: Any) -> None:
    client, _resources = _build_client()
    _install_settings_persistence_stubs(monkeypatch)
    payload = _compatibility_payload()
    captured: list[dict[str, Any]] = []

    def fake_audit_action(request: Any, **kwargs: Any) -> None:
        auth = api_deps.get_auth_context(request)
        captured.append(
            {
                "action": kwargs["action"],
                "api_key_id": auth.api_key_id,
                "actor_id": auth.actor_id,
            }
        )

    monkeypatch.setattr(settings_routes, "audit_action", fake_audit_action)
    response = client.put(
        "/api/v1/settings",
        json=payload,
        headers={"x-api-key": "a" * 32, "x-actor-roles": "platform:admin"},
    )

    assert response.status_code == 200
    assert captured == [
        {
            "action": "settings.put_current",
            "api_key_id": "primary-test",
            "actor_id": "api-key:primary-test",
        }
    ]


def test_worker_reads_updated_downloader_settings_after_put(monkeypatch: Any) -> None:
    """Worker-side settings resolution should observe the latest runtime settings after a PUT."""

    client, _ = _build_client()
    _install_settings_persistence_stubs(monkeypatch)
    payload = _compatibility_payload()
    payload["downloaders"]["real_debrid"] = {"enabled": False, "api_key": ""}
    payload["downloaders"]["all_debrid"] = {"enabled": True, "api_key": "ad-token"}

    response = client.put("/api/v1/settings", json=payload, headers=_headers())

    assert response.status_code == 200
    provider, api_key = tasks._resolve_enabled_downloader(
        get_runtime_settings(),
        item_id="item-worker",
        item_request_id="request-worker",
    )
    assert (provider, api_key) == ("alldebrid", "ad-token")


def test_settings_routes_require_api_key() -> None:
    """Settings routes remain protected by the shared API-key dependency."""

    client, _ = _build_client()
    response = client.get("/api/v1/settings")
    assert response.status_code == 401


def test_generate_apikey_route_requires_api_key() -> None:
    """API key regeneration route remains protected by the shared API-key dependency."""

    client, _ = _build_client()
    response = client.post("/api/v1/generateapikey")
    assert response.status_code == 401
