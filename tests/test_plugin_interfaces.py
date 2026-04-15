"""Plugin capability interface, registry, and test-harness coverage."""

from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from pathlib import Path
from typing import Any, cast

import pytest

from filmu_py.plugins import (
    ContentRequest,
    ContentServicePlugin,
    DownloaderPlugin,
    DownloadFileRecord,
    DownloadLinkResult,
    DownloadLinksInput,
    DownloadStatusInput,
    DownloadStatusResult,
    ExternalIdentifiers,
    IndexerInput,
    IndexerPlugin,
    IndexerResult,
    MagnetAddInput,
    MagnetAddResult,
    NotificationEvent,
    NotificationPlugin,
    PluginEventHookWorker,
    PluginRegistry,
    PluginSettingsRegistry,
    ScraperPlugin,
    ScraperResult,
    ScraperSearchInput,
    StreamControlAction,
    StreamControlInput,
    StreamControlPlugin,
    StreamControlResult,
    TestPluginContext,
    load_plugins,
)


async def _next_object(iterator: Any) -> Any:
    return await iterator.__anext__()


def _write_plugin(plugin_dir: Path, *, manifest: dict[str, Any], module_source: str) -> None:
    plugin_dir.mkdir(parents=True, exist_ok=True)
    (plugin_dir / "plugin.json").write_text(json.dumps(manifest), encoding="utf-8")
    (plugin_dir / "plugin.py").write_text(module_source, encoding="utf-8")


def test_load_plugins_registers_each_capability_type(tmp_path: Path) -> None:
    plugins_dir = tmp_path / "plugins"
    _write_plugin(
        plugins_dir / "scraper-plugin",
        manifest={
            "name": "scraper-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "capabilities": ["scraper"],
            "entry_module": "plugin.py",
            "scraper": "ExampleScraper",
        },
        module_source="""from filmu_py.plugins import ScraperPlugin, ScraperResult, ScraperSearchInput

class ExampleScraper:
    async def initialize(self, ctx):
        self.plugin_name = ctx.plugin_name

    async def search(self, metadata: ScraperSearchInput) -> list[ScraperResult]:
        return [ScraperResult(title=metadata.title or \"unknown\", provider=self.plugin_name)]
""",
    )
    _write_plugin(
        plugins_dir / "downloader-plugin",
        manifest={
            "name": "downloader-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "capabilities": ["downloader"],
            "entry_module": "plugin.py",
            "downloader": "ExampleDownloader",
        },
        module_source="""from datetime import datetime, UTC
from filmu_py.plugins import (
    DownloadFileRecord,
    DownloadLinkResult,
    DownloadLinksInput,
    DownloadStatusInput,
    DownloadStatusResult,
    MagnetAddInput,
    MagnetAddResult,
)

class ExampleDownloader:
    async def initialize(self, ctx):
        self.plugin_name = ctx.plugin_name

    async def add_magnet(self, request: MagnetAddInput) -> MagnetAddResult:
        return MagnetAddResult(download_id=\"download-1\", queued_at=datetime(2026, 3, 1, tzinfo=UTC))

    async def get_status(self, request: DownloadStatusInput) -> DownloadStatusResult:
        return DownloadStatusResult(
            download_id=request.download_id,
            status=\"ready\",
            files=(DownloadFileRecord(file_id=\"file-1\", path=\"movie.mkv\"),),
        )

    async def get_download_links(self, request: DownloadLinksInput) -> list[DownloadLinkResult]:
        return [DownloadLinkResult(url=\"https://example.com/file\", file_id=\"file-1\")]
""",
    )
    _write_plugin(
        plugins_dir / "indexer-plugin",
        manifest={
            "name": "indexer-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "capabilities": ["indexer"],
            "entry_module": "plugin.py",
            "indexer": "ExampleIndexer",
        },
        module_source="""from filmu_py.plugins import ExternalIdentifiers, IndexerInput, IndexerResult

class ExampleIndexer:
    async def initialize(self, ctx):
        self.plugin_name = ctx.plugin_name

    async def enrich(self, item: IndexerInput) -> IndexerResult:
        return IndexerResult(title=item.title + \" enriched\", external_ids=ExternalIdentifiers(tmdb_id=\"1\"))
""",
    )
    _write_plugin(
        plugins_dir / "content-plugin",
        manifest={
            "name": "content-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "capabilities": ["content_service"],
            "entry_module": "plugin.py",
            "content_service": "ExampleContentService",
        },
        module_source="""from filmu_py.plugins import ContentRequest

class ExampleContentService:
    async def initialize(self, ctx):
        self.plugin_name = ctx.plugin_name

    async def poll(self) -> list[ContentRequest]:
        return [ContentRequest(external_ref=\"tmdb:1\", media_type=\"movie\", title=\"Movie\", source=\"example\")]
""",
    )
    _write_plugin(
        plugins_dir / "stream-control-plugin",
        manifest={
            "name": "stream-control-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "capabilities": ["stream_control"],
            "entry_module": "plugin.py",
            "stream_control": "ExampleStreamControl",
        },
        module_source="""from filmu_py.plugins import StreamControlResult

class ExampleStreamControl:
    async def initialize(self, ctx):
        self.plugin_name = ctx.plugin_name

    async def control(self, request):
        return StreamControlResult(
            action=request.action,
            item_identifier=request.item_identifier,
            accepted=True,
            outcome="handled",
        )
""",
    )
    _write_plugin(
        plugins_dir / "notification-plugin",
        manifest={
            "name": "notification-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "capabilities": ["notification"],
            "entry_module": "plugin.py",
            "notification": "ExampleNotification",
        },
        module_source="""class ExampleNotification:
    async def initialize(self, ctx):
        self.plugin_name = ctx.plugin_name
        self.sent = []

    async def send(self, event):
        self.sent.append(event)
""",
    )

    registry = PluginRegistry()
    harness = TestPluginContext(settings={"mode": "test"})

    report = load_plugins(plugins_dir, registry, context_provider=harness.provider())

    assert len(report.loaded) == 6
    assert report.failed == []
    assert {item.plugin_name for item in report.loaded} == {
        "scraper-plugin",
        "downloader-plugin",
        "indexer-plugin",
        "content-plugin",
        "stream-control-plugin",
        "notification-plugin",
    }
    assert [cast(Any, plugin).plugin_name for plugin in registry.get_scrapers()] == [
        "scraper-plugin"
    ]
    assert [cast(Any, plugin).plugin_name for plugin in registry.get_downloaders()] == [
        "downloader-plugin"
    ]
    assert [cast(Any, plugin).plugin_name for plugin in registry.get_indexers()] == [
        "indexer-plugin"
    ]
    assert [cast(Any, plugin).plugin_name for plugin in registry.get_content_services()] == [
        "content-plugin"
    ]
    assert [cast(Any, plugin).plugin_name for plugin in registry.get_notifications()] == [
        "notification-plugin"
    ]
    assert [cast(Any, plugin).plugin_name for plugin in registry.get_stream_controls()] == [
        "stream-control-plugin"
    ]


def test_load_plugins_injects_context_into_capability_instances(tmp_path: Path) -> None:
    plugins_dir = tmp_path / "plugins"
    _write_plugin(
        plugins_dir / "context-aware-plugin",
        manifest={
            "name": "context-aware-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "capabilities": ["scraper"],
            "entry_module": "plugin.py",
            "scraper": "ContextAwareScraper",
        },
        module_source="""from filmu_py.plugins import ScraperResult, ScraperSearchInput

class ContextAwareScraper:
    async def initialize(self, ctx):
        self.ctx = ctx

    async def search(self, metadata: ScraperSearchInput) -> list[ScraperResult]:
        self.ctx.logger.info(\"plugin.search\", title=metadata.title)
        return [ScraperResult(title=metadata.title or \"unknown\", metadata={\"mode\": self.ctx.settings[\"mode\"]})]
""",
    )

    harness = TestPluginContext(
        settings={"plugins": {"context-aware-plugin": {"mode": "test-mode"}}}
    )
    registry = PluginRegistry()
    report = load_plugins(plugins_dir, registry, context_provider=harness.provider())

    assert len(report.loaded) == 1
    scraper = cast(Any, registry.get_scrapers()[0])
    result = asyncio.run(
        scraper.search(
            ScraperSearchInput(
                title="Plugin Movie", external_ids=ExternalIdentifiers(tmdb_id="123")
            )
        )
    )

    assert result[0].metadata == {"mode": "test-mode"}
    assert harness.logger.entries == [("info", "plugin.search", {"title": "Plugin Movie"})]
    assert scraper.ctx.plugin_name == "context-aware-plugin"


def test_load_plugins_skips_declared_capabilities_that_do_not_implement_the_protocol(
    tmp_path: Path,
) -> None:
    plugins_dir = tmp_path / "plugins"
    _write_plugin(
        plugins_dir / "broken-scraper-plugin",
        manifest={
            "name": "broken-scraper-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "capabilities": ["scraper"],
            "entry_module": "plugin.py",
            "scraper": "BrokenScraper",
        },
        module_source="""class BrokenScraper:
    async def initialize(self, ctx):
        self.plugin_name = ctx.plugin_name
""",
    )

    registry = PluginRegistry()
    report = load_plugins(plugins_dir, registry, context_provider=TestPluginContext().provider())

    assert len(report.loaded) == 1
    assert report.failed == []
    assert report.loaded[0].registered_capabilities == ()
    assert any(
        "does not implement the required protocol" in message
        for message in report.loaded[0].skipped
    )
    assert registry.get_scrapers() == []


def test_test_plugin_context_works_standalone() -> None:
    harness = TestPluginContext(
        settings={"plugins": {"standalone-plugin": {"mode": "standalone", "feature": True}}}
    )
    context = harness.build("standalone-plugin")

    assert context.plugin_name == "standalone-plugin"
    assert context.settings["mode"] == "standalone"
    try:
        cast(dict[str, object], context.settings)["mode"] = "mutated"
    except TypeError:
        pass
    else:  # pragma: no cover
        raise AssertionError("settings view must be read-only")

    async def exercise_harness() -> dict[str, Any]:
        await context.cache.set("plugin:key", b"value")
        cached = await context.cache.get("plugin:key")
        decision = await context.rate_limiter.acquire("plugin:test", 2.0, 1.0)

        subscription = cast(Any, harness.event_bus.subscribe("plugin.topic"))
        iterator = subscription.__aiter__()
        next_event: asyncio.Task[Any] = asyncio.create_task(_next_object(iterator))
        await asyncio.sleep(0)
        await harness.event_bus.publish("plugin.topic", {"status": "ok"})
        payload = await next_event
        with suppress(AttributeError, TypeError):
            await subscription.aclose()

        context.logger.info("plugin.test", cache_key="plugin:key")
        return {
            "cached": cached,
            "decision": decision,
            "payload": payload,
        }

    exercised = asyncio.run(exercise_harness())

    assert exercised["cached"] == b"value"
    assert exercised["decision"].allowed is True
    assert exercised["payload"] == {"status": "ok"}
    assert harness.event_bus.known_topics() == {"plugin.topic"}
    assert harness.logger.entries == [("info", "plugin.test", {"cache_key": "plugin:key"})]


def test_plugin_settings_registry_registers_gets_and_locks() -> None:
    registry = PluginSettingsRegistry()
    registry.register(
        "example-plugin",
        schema={"token": {"required": True}},
        values={"token": "abc", "enabled": True},
    )

    loaded = registry.get("example-plugin")
    loaded["token"] = "mutated"

    assert registry.get("example-plugin") == {"token": "abc", "enabled": True}
    registry.lock()
    with pytest.raises(RuntimeError):
        registry.register("other-plugin", schema={}, values={})


def test_load_plugins_injects_host_datasource_into_capabilities(tmp_path: Path) -> None:
    plugins_dir = tmp_path / "plugins"
    _write_plugin(
        plugins_dir / "datasource-plugin",
        manifest={
            "name": "datasource-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "capabilities": ["scraper"],
            "entry_module": "plugin.py",
            "scraper": "DatasourceScraper",
            "datasource": "host",
        },
        module_source="""from filmu_py.plugins import ScraperResult, ScraperSearchInput

class DatasourceScraper:
    async def initialize(self, ctx):
        self.ctx = ctx

    async def search(self, metadata: ScraperSearchInput) -> list[ScraperResult]:
        return [ScraperResult(title=metadata.title or 'unknown', metadata={'has_datasource': self.ctx.datasource is not None})]
""",
    )

    harness = TestPluginContext(settings={"plugins": {"datasource-plugin": {"enabled": True}}})
    registry = PluginRegistry()
    report = load_plugins(plugins_dir, registry, context_provider=harness.provider())

    assert report.failed == []
    scraper = cast(Any, registry.get_scrapers()[0])
    result = asyncio.run(scraper.search(ScraperSearchInput(title="Datasource Movie")))
    assert result[0].metadata == {"has_datasource": True}


def test_load_plugins_registers_event_hook_workers(tmp_path: Path) -> None:
    plugins_dir = tmp_path / "plugins"
    _write_plugin(
        plugins_dir / "hook-plugin",
        manifest={
            "name": "hook-plugin",
            "version": "1.0.0",
            "api_version": "1",
            "capabilities": ["event_hook"],
            "entry_module": "plugin.py",
            "event_hook": "ExampleHook",
            "publishable_events": ["hook-plugin.ready"],
        },
        module_source="""class ExampleHook:
    subscribed_events = frozenset({'item.completed'})

    async def initialize(self, ctx):
        self.ctx = ctx

    async def handle(self, event_type, payload):
        self.last = (event_type, payload)
""",
    )

    registry = PluginRegistry()
    report = load_plugins(plugins_dir, registry, context_provider=TestPluginContext().provider())

    assert report.failed == []
    hooks = registry.get_event_hooks()
    assert len(hooks) == 1
    assert isinstance(hooks[0], PluginEventHookWorker)
    assert hooks[0].subscribed_events == frozenset({"item.completed"})


def test_protocol_dataclasses_are_runtime_friendly() -> None:
    scraper_result = ScraperResult(title="Movie", metadata={"quality": "1080p"})
    magnet_result = MagnetAddResult(download_id="download-1")
    status_result = DownloadStatusResult(
        download_id="download-1",
        status="ready",
        files=(DownloadFileRecord(file_id="file-1", path="movie.mkv"),),
    )
    link_results = [DownloadLinkResult(url="https://example.com/file")]
    indexer_result = IndexerResult(title="Movie", metadata={"provider": "tmdb"})
    content_request = ContentRequest(
        external_ref="tmdb:1",
        media_type="movie",
        title="Movie",
        source="example",
    )
    event = NotificationEvent(event_type="item.completed", title="Done", message="done")

    class LocalScraper:
        async def initialize(self, ctx: object) -> None:
            self.ctx = ctx

        async def search(self, metadata: ScraperSearchInput) -> list[ScraperResult]:
            return [scraper_result]

    class LocalDownloader:
        async def initialize(self, ctx: object) -> None:
            self.ctx = ctx

        async def add_magnet(self, request: MagnetAddInput) -> MagnetAddResult:
            return magnet_result

        async def get_status(self, request: DownloadStatusInput) -> DownloadStatusResult:
            return status_result

        async def get_download_links(self, request: DownloadLinksInput) -> list[DownloadLinkResult]:
            return link_results

    class LocalIndexer:
        async def initialize(self, ctx: object) -> None:
            self.ctx = ctx

        async def enrich(self, item: IndexerInput) -> IndexerResult:
            return indexer_result

    class LocalContentService:
        async def initialize(self, ctx: object) -> None:
            self.ctx = ctx

        async def poll(self) -> list[ContentRequest]:
            return [content_request]

    class LocalNotification:
        async def initialize(self, ctx: object) -> None:
            self.ctx = ctx

        async def send(self, event: NotificationEvent) -> None:
            self.event = event

    class LocalStreamControl:
        async def initialize(self, ctx: object) -> None:
            self.ctx = ctx

        async def control(self, request: StreamControlInput) -> StreamControlResult:
            return StreamControlResult(
                action=request.action,
                item_identifier=request.item_identifier,
                accepted=True,
                outcome="handled",
            )

    assert isinstance(LocalScraper(), ScraperPlugin)
    assert isinstance(LocalDownloader(), DownloaderPlugin)
    assert isinstance(LocalIndexer(), IndexerPlugin)
    assert isinstance(LocalContentService(), ContentServicePlugin)
    notifier = LocalNotification()
    assert isinstance(notifier, NotificationPlugin)
    assert isinstance(LocalStreamControl(), StreamControlPlugin)
    asyncio.run(notifier.send(event))
    assert notifier.event == event
    stream_result = asyncio.run(
        LocalStreamControl().control(
            StreamControlInput(
                action=StreamControlAction.SERVING_STATUS_SNAPSHOT,
                item_identifier=None,
            )
        )
    )
    assert stream_result.outcome == "handled"
