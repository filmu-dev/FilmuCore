"""Startup hydration tests for persisted compatibility settings."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, Protocol, cast

import grpc
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient
from pydantic import AnyUrl, SecretStr

from filmu_py import app as app_module
from filmu_py.config import Settings, get_settings
from filmu_py.plugins.registry import PluginRegistry
from filmu_py.services.vfs_catalog import (
    VfsCatalogCorrelationKeys,
    VfsCatalogDelta,
    VfsCatalogDirectoryEntry,
    VfsCatalogEntry,
    VfsCatalogFileEntry,
    VfsCatalogSnapshot,
    VfsCatalogStats,
)
from filmuvfs.catalog.v1 import catalog_pb2, catalog_pb2_grpc


class _WatchCatalogStub(Protocol):
    def WatchCatalog(
        self,
        request_iterator: AsyncIterator[catalog_pb2.WatchCatalogRequest],
    ) -> _WatchCatalogCall: ...


class _WatchCatalogCall(Protocol):
    def __aiter__(self) -> AsyncIterator[catalog_pb2.WatchCatalogEvent]: ...

    async def __anext__(self) -> catalog_pb2.WatchCatalogEvent: ...

    def cancel(self) -> bool: ...


class DummyRedis:
    """Minimal async Redis stand-in used for app lifespan tests."""

    async def ping(self, **kwargs: Any) -> bool:
        _ = kwargs
        return True

    async def aclose(self, close_connection_pool: bool | None = None) -> None:
        _ = close_connection_pool
        return None

    async def enqueue_job(self, *_args: Any, **_kwargs: Any) -> bool:
        return True


class DummyDatabaseRuntime:
    """Minimal DB runtime used to exercise startup hydration without a real database."""

    def __init__(self, dsn: str, *, echo: bool = False) -> None:
        self.dsn = dsn
        self.echo = echo
        self.disposed = False

    async def dispose(self) -> None:
        self.disposed = True


class DummySecurityIdentityService:
    """Async identity-plane stub for startup tests."""

    def __init__(self) -> None:
        self.bootstrapped = False

    async def bootstrap(self, _settings: Settings) -> None:
        self.bootstrapped = True

    async def record_auth_context(self, _auth_context: object) -> None:
        return None


class DummyAccessPolicyService:
    """Async access-policy stub for startup tests."""

    def __init__(self) -> None:
        self.bootstrapped = False

    async def bootstrap(self, settings: Settings) -> object:
        self.bootstrapped = True
        return {"version": settings.access_policy.version, "source": "settings"}


class DummyCatalogSupplier:
    """Small async supplier stand-in used to exercise the gRPC bridge during lifespan tests."""

    def __init__(self, snapshot: VfsCatalogSnapshot) -> None:
        self._snapshot = snapshot

    async def build_snapshot(self) -> VfsCatalogSnapshot:
        return self._snapshot

    async def build_delta(self, previous: VfsCatalogSnapshot) -> VfsCatalogDelta:
        return VfsCatalogDelta(
            generation_id=previous.generation_id,
            base_generation_id=previous.generation_id,
            published_at=previous.published_at,
            upserts=(),
            removals=(),
            stats=previous.stats,
        )

    async def build_delta_since(self, generation_id: int) -> VfsCatalogDelta | None:
        if self._snapshot.generation_id.isdigit() and int(self._snapshot.generation_id) == generation_id:
            return await self.build_delta(self._snapshot)
        return None

    async def snapshot_for_generation(self, generation_id: int) -> VfsCatalogSnapshot | None:
        if self._snapshot.generation_id.isdigit() and int(self._snapshot.generation_id) == generation_id:
            return self._snapshot
        return None


def _build_catalog_snapshot() -> VfsCatalogSnapshot:
    published_at = datetime(2026, 3, 17, 18, 0, tzinfo=UTC)
    correlation = VfsCatalogCorrelationKeys(
        item_id="item-grpc-movie",
        media_entry_id="media-entry-grpc-movie",
        provider="realdebrid",
        provider_download_id="download-grpc-movie",
        provider_file_id="provider-file-grpc-movie",
        provider_file_path="Movies/Test Movie.mkv",
    )
    entries = (
        VfsCatalogEntry(
            entry_id="dir:/",
            parent_entry_id=None,
            path="/",
            name="/",
            kind="directory",
            directory=VfsCatalogDirectoryEntry(path="/"),
        ),
        VfsCatalogEntry(
            entry_id="dir:/movies",
            parent_entry_id="dir:/",
            path="/movies",
            name="movies",
            kind="directory",
            directory=VfsCatalogDirectoryEntry(path="/movies"),
        ),
        VfsCatalogEntry(
            entry_id="dir:/movies/Test Movie (2024)",
            parent_entry_id="dir:/movies",
            path="/movies/Test Movie (2024)",
            name="Test Movie (2024)",
            kind="directory",
            directory=VfsCatalogDirectoryEntry(path="/movies/Test Movie (2024)"),
        ),
        VfsCatalogEntry(
            entry_id="file:media-entry-grpc-movie",
            parent_entry_id="dir:/movies/Test Movie (2024)",
            path="/movies/Test Movie (2024)/Test Movie.mkv",
            name="Test Movie.mkv",
            kind="file",
            correlation=correlation,
            file=VfsCatalogFileEntry(
                item_id="item-grpc-movie",
                item_title="Test Movie",
                item_external_ref="tmdb:12345",
                media_entry_id="media-entry-grpc-movie",
                source_attachment_id="attachment-grpc-movie",
                media_type="movie",
                transport="remote-direct",
                locator="https://cdn.example.com/grpc-movie",
                unrestricted_url="https://cdn.example.com/grpc-movie",
                restricted_url="https://api.example.com/grpc-movie",
                original_filename="Test Movie.mkv",
                size_bytes=987654321,
                lease_state="ready",
                expires_at=datetime(2026, 3, 17, 19, 0, tzinfo=UTC),
                last_refreshed_at=published_at,
                provider="realdebrid",
                provider_download_id="download-grpc-movie",
                provider_file_id="provider-file-grpc-movie",
                provider_file_path="Movies/Test Movie.mkv",
                active_roles=("direct",),
                source_key="media-entry:media-entry-grpc-movie",
                query_strategy="by-provider-file-id",
                provider_family="debrid",
                locator_source="unrestricted-url",
                match_basis="provider-file-id",
            ),
        ),
    )
    return VfsCatalogSnapshot(
        generation_id="generation-grpc-startup",
        published_at=published_at,
        entries=entries,
        stats=VfsCatalogStats(directory_count=3, file_count=1, blocked_item_count=0),
    )


async def _read_initial_catalog_event(target: str) -> catalog_pb2.WatchCatalogEvent:
    async def request_stream() -> AsyncIterator[catalog_pb2.WatchCatalogRequest]:
        yield catalog_pb2.WatchCatalogRequest(
            subscribe=catalog_pb2.CatalogSubscribe(
                daemon_id="pytest-daemon",
                want_full_snapshot=True,
                correlation=catalog_pb2.CatalogCorrelationKeys(),
            )
        )
        await asyncio.sleep(0.05)

    async with grpc.aio.insecure_channel(target) as channel:
        stub_factory = cast(Any, catalog_pb2_grpc.FilmuVfsCatalogServiceStub)
        stub = cast(_WatchCatalogStub, stub_factory(channel))
        call = stub.WatchCatalog(request_stream())
        async for event in call:
            call.cancel()
            return event
    raise AssertionError("expected the FilmuVFS catalog stream to yield an initial event")


def _build_env_settings(
    *,
    tmdb_api_key: str = "",
    grpc_bind_address: str = "127.0.0.1:50051",
) -> Settings:
    """Return a deterministic environment-backed settings object for startup tests."""

    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        TMDB_API_KEY=tmdb_api_key,
        FILMU_PY_GRPC_BIND_ADDRESS=grpc_bind_address,
        FILMU_PY_ARQ_ENABLED=False,
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
    )


def _patch_app_startup(
    monkeypatch: Any,
    persisted_payload: dict[str, Any] | None,
    *,
    supplier_builder: Any = None,
) -> None:
    """Patch app-startup collaborators so lifespan can be tested in isolation."""

    monkeypatch.setattr(app_module, "configure_logging", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(app_module, "setup_observability", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        app_module,
        "load_plugins",
        lambda *_args, **_kwargs: SimpleNamespace(loaded=[], failed=[]),
    )
    monkeypatch.setattr(app_module, "register_builtin_plugins", lambda *_args, **_kwargs: ())
    monkeypatch.setattr(app_module, "create_graphql_router", lambda *_args, **_kwargs: APIRouter())
    monkeypatch.setattr(app_module, "_redis_from_settings", lambda _settings: DummyRedis())
    async def fake_create_pool(*_args: Any, **_kwargs: Any) -> DummyRedis:
        return DummyRedis()

    monkeypatch.setattr(app_module, "create_pool", fake_create_pool)
    monkeypatch.setattr(app_module, "run_migrations", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(app_module, "DatabaseRuntime", DummyDatabaseRuntime)
    monkeypatch.setattr(
        app_module,
        "build_security_identity_service",
        lambda _resources: DummySecurityIdentityService(),
    )
    monkeypatch.setattr(
        app_module,
        "build_access_policy_service",
        lambda _resources: DummyAccessPolicyService(),
    )
    monkeypatch.setattr(app_module, "build_playback_refresh_controller", lambda _resources: None)
    monkeypatch.setattr(
        app_module, "build_hls_failed_lease_refresh_controller", lambda _resources: None
    )
    monkeypatch.setattr(
        app_module,
        "build_hls_restricted_fallback_refresh_controller",
        lambda _resources: None,
    )
    monkeypatch.setattr(
        app_module,
        "build_vfs_catalog_supplier",
        supplier_builder or (lambda _resources: None),
    )

    async def fake_load_settings(_: DummyDatabaseRuntime) -> dict[str, Any] | None:
        return deepcopy(persisted_payload) if persisted_payload is not None else None

    monkeypatch.setattr(app_module, "load_settings", fake_load_settings)


def test_startup_without_persisted_settings_uses_env_defaults_without_failing(
    monkeypatch: Any,
) -> None:
    """When no settings row exists, startup should keep the environment-backed runtime settings."""

    env_settings = _build_env_settings()
    _patch_app_startup(monkeypatch, persisted_payload=None)

    with TestClient(app_module.create_app(env_settings)) as client:
        app = cast(FastAPI, client.app)
        runtime = app.state.resources.settings
        assert runtime.to_compatibility_dict() == env_settings.to_compatibility_dict()
        assert get_settings().to_compatibility_dict() == env_settings.to_compatibility_dict()


def test_startup_with_persisted_row_hydrates_runtime_settings(monkeypatch: Any) -> None:
    """When a persisted settings row exists, startup should hydrate and activate it."""

    env_settings = _build_env_settings()
    persisted = env_settings.to_compatibility_dict()
    persisted["api_key"] = "b" * 32
    persisted["downloaders"]["real_debrid"] = {"enabled": False, "api_key": ""}
    persisted["downloaders"]["all_debrid"] = {"enabled": True, "api_key": "ad-token"}
    _patch_app_startup(monkeypatch, persisted_payload=persisted)

    with TestClient(app_module.create_app(env_settings)) as client:
        app = cast(FastAPI, client.app)
        runtime = app.state.resources.settings
        assert runtime.to_compatibility_dict() == persisted
        assert runtime.downloaders.all_debrid.enabled is True
        assert runtime.downloaders.all_debrid.api_key == "ad-token"
        assert get_settings().to_compatibility_dict() == persisted


def test_startup_with_blank_persisted_tmdb_key_preserves_env_only_tmdb_key(
    monkeypatch: Any,
) -> None:
    """Persisted settings should not erase the env-only TMDB key when the compatibility blob leaves it blank."""

    env_settings = _build_env_settings(tmdb_api_key="tmdb-token")
    persisted = env_settings.to_compatibility_dict()
    persisted["tmdb_api_key"] = ""
    monkeypatch.setenv("TMDB_API_KEY", "tmdb-token")
    _patch_app_startup(monkeypatch, persisted_payload=persisted)

    with TestClient(app_module.create_app(env_settings)) as client:
        app = cast(FastAPI, client.app)
        runtime = app.state.resources.settings
        assert runtime.tmdb_api_key == "tmdb-token"
        assert app.state.resources.media_service._resolve_tmdb_client() is not None


def test_startup_wires_media_service_for_tmdb_request_enrichment(monkeypatch: Any) -> None:
    """App startup should supply runtime settings and a limiter to `MediaService`."""

    env_settings = _build_env_settings(tmdb_api_key="tmdb-token")
    _patch_app_startup(monkeypatch, persisted_payload=None)

    with TestClient(app_module.create_app(env_settings)) as client:
        app = cast(FastAPI, client.app)
        media_service = app.state.resources.media_service
        resolved_settings = media_service._resolve_settings()
        assert resolved_settings is not None
        assert resolved_settings.tmdb_api_key == "tmdb-token"
        assert media_service._rate_limiter is app.state.resources.rate_limiter
        assert media_service._resolve_tmdb_client() is not None


def test_startup_attaches_plugin_registry_and_capability_context_provider(monkeypatch: Any) -> None:
    env_settings = _build_env_settings()
    _patch_app_startup(monkeypatch, persisted_payload=None)

    load_calls: list[dict[str, Any]] = []

    def fake_load_plugins(*args: Any, **kwargs: Any) -> SimpleNamespace:
        load_calls.append(dict(kwargs))
        return SimpleNamespace(loaded=[], failed=[])

    def fake_register_builtin_plugins(
        registry: PluginRegistry, *, context_provider: Any
    ) -> tuple[str, ...]:
        assert isinstance(registry, PluginRegistry)
        built_context = context_provider.build("torrentio")
        assert built_context.plugin_name == "torrentio"
        return ("torrentio",)

    monkeypatch.setattr(app_module, "load_plugins", fake_load_plugins)
    monkeypatch.setattr(app_module, "register_builtin_plugins", fake_register_builtin_plugins)

    with TestClient(app_module.create_app(env_settings)) as client:
        app = cast(FastAPI, client.app)
        resources = app.state.resources
        assert resources.plugin_registry is not None
        assert resources.graphql_plugin_registry is resources.plugin_registry.graphql
        assert app.state.builtin_plugin_registrations == ("torrentio",)

    assert len(load_calls) == 2
    assert load_calls[0]["register_graphql"] is True
    assert load_calls[0]["register_capabilities"] is False
    assert load_calls[0]["trust_store_path"] is None
    assert load_calls[0]["strict_signatures"] is False
    assert load_calls[0]["runtime_policy"].enforcement_mode == "report_only"
    assert load_calls[0].get("context_provider") is None
    assert load_calls[1]["register_graphql"] is False
    assert load_calls[1]["register_capabilities"] is True
    assert load_calls[1]["trust_store_path"] is None
    assert load_calls[1]["strict_signatures"] is False
    assert load_calls[1]["runtime_policy"].enforcement_mode == "report_only"
    assert load_calls[1]["context_provider"] is not None


def test_startup_starts_vfs_catalog_grpc_server_when_catalog_supplier_is_available(
    monkeypatch: Any,
) -> None:
    env_settings = _build_env_settings(grpc_bind_address="127.0.0.1:0")
    expected_snapshot = _build_catalog_snapshot()
    _patch_app_startup(
        monkeypatch,
        persisted_payload=None,
        supplier_builder=lambda _resources: DummyCatalogSupplier(expected_snapshot),
    )

    with TestClient(app_module.create_app(env_settings)) as client:
        app = cast(FastAPI, client.app)
        resources = app.state.resources
        server = resources.vfs_catalog_server
        assert server is not None
        assert server.bound_address is not None

        event = asyncio.run(_read_initial_catalog_event(server.target))

        assert event.HasField("snapshot")
        assert event.snapshot.generation_id == expected_snapshot.generation_id
        assert event.snapshot.stats.directory_count == 3
        assert event.snapshot.stats.file_count == 1
        assert sorted(entry.path for entry in event.snapshot.entries) == sorted(
            entry.path for entry in expected_snapshot.entries
        )
