# FilmuCore Plugin SDK (Draft)

This document describes the first real **Plugin SDK capability interface layer** in FilmuCore and the direction for making it better than the current `riven-ts` plugin SDK baseline.

The current Python backend now has:

- safe filesystem and packaged entry-point discovery
- strict manifest validation
- GraphQL resolver contribution
- typed non-GraphQL capability protocols in [`filmu_py/plugins/interfaces.py`](../filmu_py/plugins/interfaces.py)
- typed plugin context injection in [`filmu_py/plugins/context.py`](../filmu_py/plugins/context.py)
- plugin-scoped settings registration/locking in [`filmu_py/plugins/settings.py`](../filmu_py/plugins/settings.py)
- datasource-aware runtime context construction in [`filmu_py/plugins/context.py`](../filmu_py/plugins/context.py)
- capability registration in [`filmu_py/plugins/registry.py`](../filmu_py/plugins/registry.py)
- typed event-hook execution plus publishable-event governance in [`filmu_py/plugins/hooks.py`](../filmu_py/plugins/hooks.py) and [`filmu_py/core/event_bus.py`](../filmu_py/core/event_bus.py)
- a standalone test harness in [`filmu_py/plugins/testing.py`](../filmu_py/plugins/testing.py)

The deliberate design choice is to use **Python `Protocol` interfaces + typed dataclasses** instead of loose event payload schemas. That keeps the SDK duck-typed and pip-packagable later, while still giving plugin authors stronger contracts than `riven-ts` currently exposes.

---

## 1. The Plugin Manifest

Every plugin requires a [`PluginManifest`](../filmu_py/plugins/manifest.py). It tells FilmuCore how to load the plugin, what versions it supports, and which capabilities it exports.

```python
from filmu_py.plugins import GraphQLResolverExports, PluginManifest

manifest = PluginManifest(
    name="my-custom-notifier",
    version="1.0.0",
    api_version="1",
    min_host_version="0.1.0",
    capabilities=frozenset({"notification", "graphql"}),
    entry_module="main.py",
    notification="MyNotificationPlugin",
    graphql=GraphQLResolverExports(
        query_resolvers=("MyCustomQuery",),
        mutation_resolvers=("NotifyUserMutation",),
    ),
)
```

Current manifest export fields:

- `graphql`: GraphQL resolver exports
- `scraper`: scraper capability symbol
- `downloader`: downloader capability symbol
- `indexer`: indexer capability symbol
- `content_service`: content-service capability symbol
- `notification`: notification capability symbol
- `event_hook`: typed event-hook worker symbol
- `datasource`: host datasource name requested by the plugin
- `publishable_events`: namespaced events the plugin is allowed to emit

Manifest capability strings are still intentionally simple at this stage. The stronger contract now lives in the typed interfaces, not in free-form JSON.

---

## 2. Capability Interfaces

The first SDK slice introduces typed capability protocols in [`filmu_py/plugins/interfaces.py`](../filmu_py/plugins/interfaces.py):

- [`ScraperPlugin`](../filmu_py/plugins/interfaces.py)
- [`DownloaderPlugin`](../filmu_py/plugins/interfaces.py)
- [`IndexerPlugin`](../filmu_py/plugins/interfaces.py)
- [`ContentServicePlugin`](../filmu_py/plugins/interfaces.py)
- [`NotificationPlugin`](../filmu_py/plugins/interfaces.py)
- [`PluginEventHookWorker`](../filmu_py/plugins/interfaces.py)

Each capability uses explicit dataclasses for request and response shapes, for example:

```python
from filmu_py.plugins import (
    ExternalIdentifiers,
    ScraperPlugin,
    ScraperResult,
    ScraperSearchInput,
)


class ExampleScraper:
    async def initialize(self, ctx) -> None:
        self.ctx = ctx

    async def search(self, metadata: ScraperSearchInput) -> list[ScraperResult]:
        self.ctx.logger.info("plugin.search", title=metadata.title)
        return [
            ScraperResult(
                title=metadata.title or "unknown",
                provider=self.ctx.plugin_name,
                metadata={"tmdb_id": metadata.external_ids.tmdb_id},
            )
        ]


search_input = ScraperSearchInput(
    title="Movie",
    external_ids=ExternalIdentifiers(tmdb_id="123"),
)
```

Compared with `riven-ts`, this gives plugin authors:

- typed protocol checking instead of only event-shape conventions
- explicit DTO/dataclass contracts instead of loose records
- cleaner future packaging into a separate SDK wheel

## 3. Plugin Context Injection

Plugins are initialized with a scoped [`PluginContext`](../filmu_py/plugins/context.py) built by [`PluginContextProvider`](../filmu_py/plugins/context.py):

```python
from filmu_py.plugins import PluginContext


class ExampleNotificationPlugin:
    async def initialize(self, ctx: PluginContext) -> None:
        self.ctx = ctx

    async def send(self, event) -> None:
        self.ctx.logger.info(
            "plugin.notification.send",
            event_type=event.event_type,
            plugin=self.ctx.plugin_name,
        )
```

Current context surface:

- read-only `settings`
- `event_bus`
- `rate_limiter`
- `cache`
- `logger`
- optional `datasource`

This is intentionally narrower than the full host runtime. The SDK layer is meant to avoid leaking host-internal service objects into plugin code.

## 4. Capability Registration

[`PluginRegistry`](../filmu_py/plugins/registry.py) now tracks both:

- GraphQL resolver registration through its embedded GraphQL registry
- non-GraphQL capability implementations through typed capability accessors

Current accessors:

- `get_scrapers()`
- `get_downloaders()`
- `get_indexers()`
- `get_content_services()`
- `get_notifications()`
- `get_event_hooks()`

The loader entry point for the broader SDK surface is now [`load_plugins()`](../filmu_py/plugins/loader.py). The existing [`load_graphql_plugins()`](../filmu_py/plugins/loader.py) remains as a backward-compatible GraphQL-only wrapper.

The host runtime now also wires this SDK surface through [`filmu_py/app.py`](../filmu_py/app.py) and [`filmu_py/resources.py`](../filmu_py/resources.py):

- app creation loads filesystem and packaged plugins for GraphQL registration before schema construction
- lifespan startup builds a real [`PluginContextProvider`](../filmu_py/plugins/context.py) from runtime settings, event bus, limiter, cache, and logger
- startup then loads non-GraphQL capability plugins into the shared [`PluginRegistry`](../filmu_py/plugins/registry.py)
- built-in plugins register programmatically after resources exist through [`register_builtin_plugins()`](../filmu_py/plugins/builtins.py)

The current compatibility API also exposes loaded non-GraphQL capability plugins through [`GET /api/v1/plugins`](../filmu_py/api/routes/default.py) and declared publishable/subscribed plugin events through [`GET /api/v1/plugins/events`](../filmu_py/api/routes/default.py).

If a plugin declares a capability but:

- the symbol is missing
- the object does not satisfy the required protocol
- `initialize(ctx)` fails

the loader skips that capability gracefully and records a stable skipped reason without crashing host startup.

### 4.1 Built-in plugins

Built-in plugins that ship with FilmuCore now register through [`register_builtin_plugins()`](../filmu_py/plugins/builtins.py) rather than filesystem discovery.

This keeps app-shipped plugins always available while still allowing the same implementation to serve as an SDK example for third-party authors.

## 5. Extending the GraphQL API (Currently Supported)

FilmuCore uses Strawberry for its GraphQL interface. Plugins can still export fully-typed resolver classes exactly as before. The capability layer does not replace GraphQL contribution; it broadens the SDK around it.

**Example: `main.py`**
```python
import strawberry

@strawberry.type
class MyCustomQuery:
    @strawberry.field
    def custom_greeting(self, name: str) -> str:
        return f"Hello from the plugin, {name}!"

# Export the resolver so the manifest loader can find it.
# Note: The export name MUST match the string declared in the manifest.
MyCustomQuery = MyCustomQuery
```

---

## 6. The Async Event Bus

FilmuCore operates heavily on an event-driven architecture, orchestrated by the [`EventBus`](../filmu_py/core/event_bus.py). Plugins receive an event-bus handle inside [`PluginContext`](../filmu_py/plugins/context.py) and can subscribe to system events.

### Known Core Topics:
- `item.state.changed`: Fired when a media item moves between stages (e.g., `requested` -> `indexed` -> `downloaded`).
- `notifications`: Fired to broadcast user-facing alerts.
- `logging`: Raw system log streams.

### Subscribing to Events

Plugins can run long-lived background tasks that subscribe to target topics:

```python
import asyncio
from filmu_py.core.event_bus import EventBus

async def plugin_startup(event_bus: EventBus):
    asyncio.create_task(listen_for_state_changes(event_bus))

async def listen_for_state_changes(event_bus: EventBus):
    async for envelope in event_bus.subscribe("item.state.changed"):
        payload = envelope.payload
        print(f"Item {payload['item_id']} changed to state: {payload['to_state']}")
```

### Publishing Custom Events

Plugins can also publish their own events, but namespaced plugin events are now governed: the plugin must declare them in `publishable_events`, otherwise the host event bus drops-and-warns the publish attempt instead of fanning out an undeclared event.

```python
await event_bus.publish("my_plugin.scan.completed", {
    "items_found": 42,
    "status": "success"
})
```

Typed hook workers are also now part of the SDK/runtime baseline. They execute through [`PluginHookWorkerExecutor`](../filmu_py/plugins/hooks.py) with timeout isolation, success/error/timeout telemetry, and manifest/registry-backed subscription lists.

---

## 7. Local Development & Testing

The first SDK slice includes a standalone test harness in [`filmu_py/plugins/testing.py`](../filmu_py/plugins/testing.py).

Use [`TestPluginContext`](../filmu_py/plugins/testing.py) when writing plugin tests without booting the full application:

```python
import asyncio

from filmu_py.plugins import NotificationEvent, TestPluginContext


class ExampleNotificationPlugin:
    async def initialize(self, ctx) -> None:
        self.ctx = ctx

    async def send(self, event: NotificationEvent) -> None:
        self.ctx.logger.info("plugin.send", event_type=event.event_type)


harness = TestPluginContext(settings={"mode": "test"})
ctx = harness.build("example-plugin")
plugin = ExampleNotificationPlugin()
asyncio.run(plugin.initialize(ctx))
asyncio.run(
    plugin.send(
        NotificationEvent(
            event_type="item.completed",
            title="Done",
            message="Finished",
        )
    )
)

assert harness.logger.entries
```

The test harness provides fake in-memory implementations of:

- cache
- rate limiter
- event bus
- logger

This is the first step toward a separately published SDK package and a better third-party authoring experience than the current `riven-ts` baseline.

### 7.1 First real example — Torrentio

The first end-to-end capability implementation is now [`TorrentioScraper`](../filmu_py/plugins/builtin/torrentio.py).

It demonstrates:

- `ScraperPlugin` implementation over a real external API pattern
- configuration pulled from the compatibility settings tree
- plugin-owned request pacing through the injected rate limiter
- structured plugin logging through the injected logger
- normalized conversion from Stremio/Torrentio `streams[]` payloads into [`ScraperResult`](../filmu_py/plugins/interfaces.py)

This built-in path now also feeds the real worker pipeline through [`scrape_item`](../filmu_py/workers/tasks.py), which resolves registered [`ScraperPlugin`](../filmu_py/plugins/interfaces.py) implementations from the shared runtime registry and persists their normalized [`ScrapeResult`](../filmu_py/plugins/interfaces.py) outputs as scrape candidates.

## 8. What This Slice Does Not Do Yet

This capability-interface slice intentionally does **not** yet:

- ship real downloader/indexer/content-service/notification plugin implementations beyond the first scraper example
- expose a publishable-event governance model
- provide datasource or DB-session injection beyond the approved context surface

Those are the next platform slices after the interfaces, context, registry, and test harness are proven stable.

## 9. Current Discovery & Loading Notes

During local development, plugins can be placed inside the designated `plugins/` directory. The loader parses the manifest, validates host compatibility, imports the declared entry module with `importlib`, resolves exports, initializes capability implementations with a scoped context, and registers them into [`PluginRegistry`](../filmu_py/plugins/registry.py).

Packaged plugins can also be discovered if they are installed and registered under the Python entry-point group `filmu.plugins`.
