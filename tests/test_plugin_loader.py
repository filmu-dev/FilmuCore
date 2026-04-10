"""Filesystem plugin discovery and GraphQL contribution tests."""

from __future__ import annotations

import hashlib
import json
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from types import ModuleType
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import AnyUrl, SecretStr

from filmu_py.config import Settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.graphql import GraphQLPluginRegistry, create_graphql_router
from filmu_py.plugins import load_graphql_plugins
from filmu_py.plugins import loader as plugin_loader
from filmu_py.resources import AppResources


class DummyRedis:
    """Minimal async Redis stub for non-networked plugin GraphQL tests."""

    def ping(self, **kwargs: Any) -> bool:
        return True

    async def aclose(self, close_connection_pool: bool | None = None) -> None:  # pragma: no cover
        _ = close_connection_pool
        return None


class DummyDatabaseRuntime:
    """No-op DB runtime placeholder for resource wiring in tests."""

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[None, None]:
        yield None

    async def dispose(self) -> None:  # pragma: no cover
        return None


class FakeMediaService:
    """Placeholder media service for GraphQL tests that do not hit item storage."""

    def __init__(self) -> None:
        self._noop = None


def _build_settings() -> Settings:
    """Return deterministic settings for plugin loader tests."""

    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
        FILMU_PY_SERVICE_NAME="filmu-python-test",
    )


def _build_test_client(registry: GraphQLPluginRegistry) -> TestClient:
    """Create a lightweight FastAPI app exposing only GraphQL router for tests."""

    settings = _build_settings()
    redis = DummyRedis()

    app = FastAPI()
    app.state.resources = AppResources(
        settings=settings,
        redis=redis,
        cache=CacheManager(redis=redis, namespace="test"),
        rate_limiter=DistributedRateLimiter(redis=redis),
        event_bus=EventBus(),
        db=DummyDatabaseRuntime(),
        media_service=FakeMediaService(),
        graphql_plugin_registry=registry,
    )
    app.include_router(create_graphql_router(registry), prefix="/graphql")
    return TestClient(app)


def _write_plugin(plugin_dir: Path, *, manifest: dict[str, Any], module_source: str) -> None:
    """Write a drop-in plugin manifest and entry module into a test directory."""

    plugin_dir.mkdir(parents=True, exist_ok=True)
    (plugin_dir / "plugin.json").write_text(json.dumps(manifest), encoding="utf-8")
    (plugin_dir / "plugin.py").write_text(module_source, encoding="utf-8")


class FakeEntryPoint:
    """Minimal entry-point test double for packaged plugin discovery."""

    def __init__(self, name: str, factory: Any) -> None:
        self.name = name
        self._factory = factory

    def load(self) -> Any:
        return self._factory


def _build_packaged_plugin_entry_point(
    *,
    name: str,
    manifest: dict[str, Any],
    module_source: str,
) -> FakeEntryPoint:
    """Return one packaged plugin entry-point double that yields a manifest/module pair."""

    module = ModuleType(f"test_packaged_plugin_{name.replace('-', '_')}")
    sys.modules[module.__name__] = module
    exec(module_source, module.__dict__)

    def factory() -> tuple[dict[str, Any], ModuleType]:
        return manifest, module

    return FakeEntryPoint(name=name, factory=factory)


def test_filesystem_plugin_loader_registers_graphql_query(tmp_path: Path) -> None:
    """A valid filesystem plugin should register GraphQL resolvers into the schema."""

    plugins_dir = tmp_path / "plugins"
    _write_plugin(
        plugins_dir / "echo-plugin",
        manifest={
            "name": "echo-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "entry_module": "plugin.py",
            "graphql": {"query_resolvers": ["EchoQuery"]},
        },
        module_source="""import strawberry

@strawberry.type
class EchoQuery:
    @strawberry.field
    def plugin_echo(self) -> str:
        return "hello-from-plugin"
""",
    )

    registry = GraphQLPluginRegistry()
    report = load_graphql_plugins(plugins_dir, registry)

    assert len(report.loaded) == 1
    assert report.failed == []
    assert report.loaded[0].plugin_name == "echo-plugin"
    assert report.loaded[0].registered_query_resolvers == 1

    client = _build_test_client(registry)
    response = client.post("/graphql", json={"query": "query { pluginEcho }"})

    assert response.status_code == 200
    payload = response.json()
    assert "errors" not in payload
    assert payload["data"]["pluginEcho"] == "hello-from-plugin"


def test_filesystem_plugin_loader_isolates_broken_plugins(tmp_path: Path) -> None:
    """A broken plugin should be reported without blocking valid plugin registration."""

    plugins_dir = tmp_path / "plugins"
    _write_plugin(
        plugins_dir / "working-plugin",
        manifest={
            "name": "working-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "entry_module": "plugin.py",
            "graphql": {"query_resolvers": ["WorkingQuery"]},
        },
        module_source="""import strawberry

@strawberry.type
class WorkingQuery:
    @strawberry.field
    def plugin_status(self) -> str:
        return "ready"
""",
    )
    broken_plugin_dir = plugins_dir / "broken-plugin"
    broken_plugin_dir.mkdir(parents=True, exist_ok=True)
    (broken_plugin_dir / "plugin.json").write_text(
        json.dumps(
            {
                "name": "broken-plugin",
                "version": "1.0.0",
                "api_version": "1",
                "entry_module": "missing.py",
                "graphql": {"query_resolvers": ["BrokenQuery"]},
            }
        ),
        encoding="utf-8",
    )

    registry = GraphQLPluginRegistry()
    report = load_graphql_plugins(plugins_dir, registry)

    assert len(report.loaded) == 1
    assert len(report.failed) == 1
    assert report.loaded[0].plugin_name == "working-plugin"
    assert "entry module does not exist" in report.failed[0].reason

    client = _build_test_client(registry)
    response = client.post("/graphql", json={"query": "query { pluginStatus }"})

    assert response.status_code == 200
    payload = response.json()
    assert "errors" not in payload
    assert payload["data"]["pluginStatus"] == "ready"


def test_filesystem_plugin_loader_isolates_plugin_import_exception(tmp_path: Path) -> None:
    """Unexpected plugin import exceptions should be reported without aborting discovery."""

    plugins_dir = tmp_path / "plugins"
    _write_plugin(
        plugins_dir / "working-plugin",
        manifest={
            "name": "working-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "entry_module": "plugin.py",
            "graphql": {"query_resolvers": ["WorkingQuery"]},
        },
        module_source="""import strawberry

@strawberry.type
class WorkingQuery:
    @strawberry.field
    def plugin_status(self) -> str:
        return "ready"
""",
    )
    _write_plugin(
        plugins_dir / "exploding-plugin",
        manifest={
            "name": "exploding-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "entry_module": "plugin.py",
            "graphql": {"query_resolvers": ["ExplodingQuery"]},
        },
        module_source="""raise RuntimeError("plugin exploded during import")
""",
    )

    registry = GraphQLPluginRegistry()
    report = load_graphql_plugins(plugins_dir, registry)

    assert len(report.loaded) == 1
    assert len(report.failed) == 1
    assert report.loaded[0].plugin_name == "working-plugin"
    assert "plugin exploded during import" in report.failed[0].reason

    client = _build_test_client(registry)
    response = client.post("/graphql", json={"query": "query { pluginStatus }"})

    assert response.status_code == 200
    payload = response.json()
    assert "errors" not in payload
    assert payload["data"]["pluginStatus"] == "ready"


def test_filesystem_plugin_loader_registers_settings_extension(tmp_path: Path) -> None:
    """A valid filesystem plugin can extend the nested GraphQL settings object."""

    plugins_dir = tmp_path / "plugins"
    _write_plugin(
        plugins_dir / "settings-plugin",
        manifest={
            "name": "settings-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "entry_module": "plugin.py",
            "graphql": {"settings_resolvers": ["SettingsExtension"]},
        },
        module_source="""import strawberry

@strawberry.type
class EchoSettings:
    enabled: bool
    slug: str

@strawberry.type
class SettingsExtension:
    @strawberry.field
    def echo(self) -> EchoSettings:
        return EchoSettings(enabled=True, slug="settings-plugin")
""",
    )

    registry = GraphQLPluginRegistry()
    report = load_graphql_plugins(plugins_dir, registry)

    assert len(report.loaded) == 1
    assert report.failed == []
    assert report.loaded[0].plugin_name == "settings-plugin"

    client = _build_test_client(registry)
    response = client.post(
        "/graphql",
        json={"query": "query { settings { filmu { version } echo { enabled slug } } }"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert "errors" not in payload
    settings = payload["data"]["settings"]
    assert settings["filmu"]["version"]
    assert settings["echo"] == {"enabled": True, "slug": "settings-plugin"}


def test_packaged_entry_point_plugin_loads_alongside_filesystem_plugin(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    """Entry-point plugins should load alongside filesystem plugins without changing schema wiring."""

    plugins_dir = tmp_path / "plugins"
    _write_plugin(
        plugins_dir / "filesystem-plugin",
        manifest={
            "name": "filesystem-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "entry_module": "plugin.py",
            "graphql": {"query_resolvers": ["FilesystemQuery"]},
        },
        module_source="""import strawberry

@strawberry.type
class FilesystemQuery:
    @strawberry.field
    def filesystem_echo(self) -> str:
        return \"filesystem\"
""",
    )
    packaged_entry_point = _build_packaged_plugin_entry_point(
        name="packaged-plugin",
        manifest={
            "name": "packaged-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "entry_module": "ignored.py",
            "graphql": {"query_resolvers": ["PackagedQuery"]},
        },
        module_source="""import strawberry

@strawberry.type
class PackagedQuery:
    @strawberry.field
    def packaged_echo(self) -> str:
        return \"packaged\"
""",
    )
    monkeypatch.setattr(
        plugin_loader,
        "entry_points",
        lambda group=None: [packaged_entry_point] if group == "filmu.plugins" else [],
    )

    registry = GraphQLPluginRegistry()
    report = load_graphql_plugins(plugins_dir, registry, host_version="0.1.0")

    assert {item.plugin_name for item in report.loaded} == {"filesystem-plugin", "packaged-plugin"}
    assert report.failed == []

    client = _build_test_client(registry)
    response = client.post("/graphql", json={"query": "query { filesystemEcho packagedEcho }"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"] == {"filesystemEcho": "filesystem", "packagedEcho": "packaged"}


def test_broken_packaged_plugin_does_not_block_filesystem_plugin(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    """Broken entry-point plugins must not block working filesystem plugins."""

    plugins_dir = tmp_path / "plugins"
    _write_plugin(
        plugins_dir / "filesystem-plugin",
        manifest={
            "name": "filesystem-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "entry_module": "plugin.py",
            "graphql": {"query_resolvers": ["FilesystemQuery"]},
        },
        module_source="""import strawberry

@strawberry.type
class FilesystemQuery:
    @strawberry.field
    def plugin_status(self) -> str:
        return \"ready\"
""",
    )

    def broken_factory() -> tuple[dict[str, Any], ModuleType]:
        raise RuntimeError("packaged plugin exploded")

    monkeypatch.setattr(
        plugin_loader,
        "entry_points",
        lambda group=None: (
            [FakeEntryPoint(name="broken-packaged-plugin", factory=broken_factory)]
            if group == "filmu.plugins"
            else []
        ),
    )

    registry = GraphQLPluginRegistry()
    report = load_graphql_plugins(plugins_dir, registry, host_version="0.1.0")

    assert len(report.loaded) == 1
    assert report.loaded[0].plugin_name == "filesystem-plugin"
    assert len(report.failed) == 1
    assert report.failed[0].reason == "packaged plugin exploded"

    client = _build_test_client(registry)
    response = client.post("/graphql", json={"query": "query { pluginStatus }"})
    assert response.status_code == 200
    assert response.json()["data"]["pluginStatus"] == "ready"


def test_packaged_plugin_with_higher_min_host_version_is_marked_incompatible(
    monkeypatch: Any,
) -> None:
    """Packaged plugins requiring a newer host version must fail with a stable reason."""

    packaged_entry_point = _build_packaged_plugin_entry_point(
        name="too-new-plugin",
        manifest={
            "name": "too-new-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "min_host_version": "9.9.9",
            "entry_module": "ignored.py",
            "graphql": {"query_resolvers": ["TooNewQuery"]},
        },
        module_source="""import strawberry

@strawberry.type
class TooNewQuery:
    @strawberry.field
    def never_loaded(self) -> str:
        return \"nope\"
""",
    )
    monkeypatch.setattr(
        plugin_loader,
        "entry_points",
        lambda group=None: [packaged_entry_point] if group == "filmu.plugins" else [],
    )

    registry = GraphQLPluginRegistry()
    report = load_graphql_plugins(Path("missing-plugins-dir"), registry, host_version="0.1.0")

    assert report.loaded == []
    assert len(report.failed) == 1
    assert report.failed[0].reason == "host_version_incompatible"
    assert report.failed[0].plugin_name == "too-new-plugin"
    assert report.failed[0].source == "entry_point"


def test_packaged_plugin_without_min_host_version_loads_normally(monkeypatch: Any) -> None:
    """Absent host-version requirement should stay backward compatible."""

    packaged_entry_point = _build_packaged_plugin_entry_point(
        name="default-compatible-plugin",
        manifest={
            "name": "default-compatible-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "entry_module": "ignored.py",
            "graphql": {"query_resolvers": ["CompatibleQuery"]},
        },
        module_source="""import strawberry

@strawberry.type
class CompatibleQuery:
    @strawberry.field
    def packaged_status(self) -> str:
        return \"ok\"
""",
    )
    monkeypatch.setattr(
        plugin_loader,
        "entry_points",
        lambda group=None: [packaged_entry_point] if group == "filmu.plugins" else [],
    )

    registry = GraphQLPluginRegistry()
    report = load_graphql_plugins(Path("missing-plugins-dir"), registry, host_version="0.1.0")

    assert len(report.loaded) == 1
    assert report.failed == []
    assert report.loaded[0].plugin_name == "default-compatible-plugin"


def test_packaged_plugin_with_equal_or_lower_min_host_version_loads_normally(
    monkeypatch: Any,
) -> None:
    """Plugins pinned to the same or older host version should load successfully."""

    packaged_entry_points = [
        _build_packaged_plugin_entry_point(
            name="equal-version-plugin",
            manifest={
                "name": "equal-version-plugin",
                "version": "1.0.0",
                "api_version": "1",
                "min_host_version": "0.1.0",
                "entry_module": "ignored.py",
                "graphql": {"query_resolvers": ["EqualQuery"]},
            },
            module_source="""import strawberry

@strawberry.type
class EqualQuery:
    @strawberry.field
    def equal_version(self) -> str:
        return \"equal\"
""",
        ),
        _build_packaged_plugin_entry_point(
            name="older-version-plugin",
            manifest={
                "name": "older-version-plugin",
                "version": "1.0.0",
                "api_version": "1",
                "min_host_version": "0.0.5",
                "entry_module": "ignored.py",
                "graphql": {"query_resolvers": ["OlderQuery"]},
            },
            module_source="""import strawberry

@strawberry.type
class OlderQuery:
    @strawberry.field
    def older_version(self) -> str:
        return \"older\"
""",
        ),
    ]
    monkeypatch.setattr(
        plugin_loader,
        "entry_points",
        lambda group=None: packaged_entry_points if group == "filmu.plugins" else [],
    )

    registry = GraphQLPluginRegistry()
    report = load_graphql_plugins(Path("missing-plugins-dir"), registry, host_version="0.1.0")

    assert {item.plugin_name for item in report.loaded} == {
        "equal-version-plugin",
        "older-version-plugin",
    }
    assert report.failed == []


def test_filesystem_plugin_with_newer_api_version_is_marked_incompatible(tmp_path: Path) -> None:
    plugins_dir = tmp_path / "plugins"
    _write_plugin(
        plugins_dir / "future-api-plugin",
        manifest={
            "name": "future-api-plugin",
            "version": "1.0.0",
            "api_version": "2",
            "entry_module": "plugin.py",
            "graphql": {"query_resolvers": ["FutureApiQuery"]},
        },
        module_source="""import strawberry

@strawberry.type
class FutureApiQuery:
    @strawberry.field
    def never_loaded(self) -> str:
        return "nope"
""",
    )

    registry = GraphQLPluginRegistry()
    report = load_graphql_plugins(plugins_dir, registry, host_version="0.1.0")

    assert report.loaded == []
    assert len(report.failed) == 1
    assert report.failed[0].plugin_name == "future-api-plugin"
    assert report.failed[0].reason == "api_version_incompatible"
    assert report.failed[0].source == "filesystem"


def test_filesystem_plugin_with_max_host_version_below_runtime_is_marked_incompatible(
    tmp_path: Path,
) -> None:
    plugins_dir = tmp_path / "plugins"
    _write_plugin(
        plugins_dir / "legacy-plugin",
        manifest={
            "name": "legacy-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "max_host_version": "0.0.9",
            "entry_module": "plugin.py",
            "graphql": {"query_resolvers": ["LegacyQuery"]},
        },
        module_source="""import strawberry

@strawberry.type
class LegacyQuery:
    @strawberry.field
    def never_loaded(self) -> str:
        return "nope"
""",
    )

    registry = GraphQLPluginRegistry()
    report = load_graphql_plugins(plugins_dir, registry, host_version="0.1.0")

    assert report.loaded == []
    assert len(report.failed) == 1
    assert report.failed[0].plugin_name == "legacy-plugin"
    assert report.failed[0].reason == "host_version_incompatible"
    assert report.failed[0].source == "filesystem"


def test_filesystem_plugin_loader_validates_declared_source_digest(tmp_path: Path) -> None:
    plugins_dir = tmp_path / "plugins"
    module_source = """import strawberry

@strawberry.type
class SignedQuery:
    @strawberry.field
    def signed_echo(self) -> str:
        return "signed"
"""
    _write_plugin(
        plugins_dir / "signed-plugin",
        manifest={
            "name": "signed-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "publisher": "filmu-labs",
            "signature": "sig-store-record",
            "signing_key_id": "filmu-labs-root",
            "entry_module": "plugin.py",
            "graphql": {"query_resolvers": ["SignedQuery"]},
        },
        module_source=module_source,
    )
    module_digest = hashlib.sha256(
        (plugins_dir / "signed-plugin" / "plugin.py").read_bytes()
    ).hexdigest()
    manifest_path = plugins_dir / "signed-plugin" / "plugin.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["source_sha256"] = module_digest
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    registry = GraphQLPluginRegistry()
    report = load_graphql_plugins(plugins_dir, registry)

    assert len(report.loaded) == 1
    assert report.failed == []
    assert report.loaded[0].source_sha256 == module_digest
    assert report.loaded[0].signing_key_id == "filmu-labs-root"
    assert report.loaded[0].signature_present is True
    assert report.loaded[0].sandbox_profile == "restricted"


def test_filesystem_plugin_loader_rejects_source_digest_mismatch(tmp_path: Path) -> None:
    plugins_dir = tmp_path / "plugins"
    _write_plugin(
        plugins_dir / "tampered-plugin",
        manifest={
            "name": "tampered-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "publisher": "filmu-labs",
            "source_sha256": "f" * 64,
            "entry_module": "plugin.py",
            "graphql": {"query_resolvers": ["TamperedQuery"]},
        },
        module_source="""import strawberry

@strawberry.type
class TamperedQuery:
    @strawberry.field
    def tampered_echo(self) -> str:
        return "tampered"
""",
    )

    registry = GraphQLPluginRegistry()
    report = load_graphql_plugins(plugins_dir, registry)

    assert report.loaded == []
    assert len(report.failed) == 1
    assert report.failed[0].plugin_name == "tampered-plugin"
    assert report.failed[0].reason == "source_digest_mismatch"


def test_filesystem_plugin_loader_rejects_quarantined_plugins(tmp_path: Path) -> None:
    plugins_dir = tmp_path / "plugins"
    _write_plugin(
        plugins_dir / "quarantined-plugin",
        manifest={
            "name": "quarantined-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "publisher": "filmu-labs",
            "quarantined": True,
            "quarantine_reason": "malware-suspected",
            "entry_module": "plugin.py",
            "graphql": {"query_resolvers": ["NeverLoadedQuery"]},
        },
        module_source="""import strawberry

@strawberry.type
class NeverLoadedQuery:
    @strawberry.field
    def should_not_load(self) -> str:
        return "nope"
""",
    )

    registry = GraphQLPluginRegistry()
    report = load_graphql_plugins(plugins_dir, registry)

    assert report.loaded == []
    assert len(report.failed) == 1
    assert report.failed[0].plugin_name == "quarantined-plugin"
    assert report.failed[0].reason == "plugin_quarantined"
