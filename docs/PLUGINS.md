# Plugin Compatibility Design

## Purpose

Define a plugin system that is compatible with current and future Filmu-style extensibility while keeping host/runtime isolation.

## Scope note

This document now describes both the currently implemented plugin-runtime baseline and the remaining intended direction of the plugin platform.

For the currently implemented filesystem-plugin baseline, see [`PLUGIN_QUICKSTART.md`](PLUGIN_QUICKSTART.md).

If there is any conflict between the two documents, [`PLUGIN_QUICKSTART.md`](PLUGIN_QUICKSTART.md) is the source of truth for what works today.

## Discovery sources

1. Python entry points (`filmu.plugins`) — now implemented for packaged distribution.
2. Drop-in directories under `$FILMU_PLUGINS_DIR` with valid `plugin.json` — implemented baseline.

## Contract boundary

`plugin.json` defines:

- plugin identity and version
- optional minimum host version gate
- host API compatibility range
- capability list
- settings schema path
- entry module path
- optional datasource name
- optional event-hook export
- optional publishable event declarations
- GraphQL resolver export names for query/mutation/subscription contribution
- GraphQL settings-root resolver export names for nested `settings { ... }` contribution

Example baseline manifest:

```json
{
  "name": "echo-plugin",
  "version": "1.0.0",
  "min_host_version": "0.1.0",
  "api_version": "1",
  "entry_module": "plugin.py",
  "graphql": {
    "query_resolvers": ["EchoQuery"]
  }
}
```

## Injection boundary

Plugins receive a scoped [`PluginContext`](../filmu_py/plugins/context.py) object with approved services:

- read-only plugin-scoped settings
- event bus
- rate limiter
- cache
- logger
- optional datasource wrapper over host-approved internals

The datasource boundary is deliberate: plugins do not receive raw host internals directly on the context object. If a plugin needs DB-session or HTTP-client access, it should receive that through a host-approved datasource implementation rather than by importing host internals directly.

## Startup behavior

- Discovery/import errors mark plugin as failed.
- Failed plugins are skipped without blocking host startup.
- Plugins with `min_host_version` greater than the running `FILMU_PY_VERSION` are rejected with a stable `host_version_incompatible` failure reason.
- Successful plugins now register both manifest-declared GraphQL resolvers and runtime capability implementations such as scrapers, downloaders, content services, notifications, and typed event hooks.
- Built-in capability plugins are also registered programmatically through [`../filmu_py/plugins/builtins.py`](../filmu_py/plugins/builtins.py) once runtime resources exist.

## Current capability model

The first non-GraphQL capability slice now exists.

Implemented pieces:

- typed capability protocols in [`../filmu_py/plugins/interfaces.py`](../filmu_py/plugins/interfaces.py)
- scoped runtime context injection in [`../filmu_py/plugins/context.py`](../filmu_py/plugins/context.py)
- plugin-scoped settings registry and locking in [`../filmu_py/plugins/settings.py`](../filmu_py/plugins/settings.py)
- host datasource injection through [`../filmu_py/plugins/context.py`](../filmu_py/plugins/context.py)
- capability registration/accessors in [`../filmu_py/plugins/registry.py`](../filmu_py/plugins/registry.py)
- typed event-hook execution in [`../filmu_py/plugins/hooks.py`](../filmu_py/plugins/hooks.py)
- namespaced publishable-event governance in [`../filmu_py/core/event_bus.py`](../filmu_py/core/event_bus.py)
- standalone test harness in [`../filmu_py/plugins/testing.py`](../filmu_py/plugins/testing.py)
- built-in programmatic plugin registration in [`../filmu_py/plugins/builtins.py`](../filmu_py/plugins/builtins.py)
- first built-in scraper example in [`../filmu_py/plugins/builtin/torrentio.py`](../filmu_py/plugins/builtin/torrentio.py)
- built-in MDBList, StremThru, and webhook notification implementations in [`../filmu_py/plugins/builtin/mdblist.py`](../filmu_py/plugins/builtin/mdblist.py), [`../filmu_py/plugins/builtin/stremthru.py`](../filmu_py/plugins/builtin/stremthru.py), and [`../filmu_py/plugins/builtin/notifications.py`](../filmu_py/plugins/builtin/notifications.py)
- runtime visibility for declared publishable events and subscriptions through [`/api/v1/plugins/events`](../filmu_py/api/routes/default.py)

Current runtime ownership:

- GraphQL plugin loading still happens early for schema construction.
- Non-GraphQL capability loading now happens during startup once real runtime resources exist.
- The shared registry is attached to [`../filmu_py/resources.py`](../filmu_py/resources.py) and surfaced through [`../filmu_py/api/routes/default.py`](../filmu_py/api/routes/default.py) for runtime visibility.
- The shared event bus now enforces publishable-event governance and dispatches typed event hooks through [`../filmu_py/core/event_bus.py`](../filmu_py/core/event_bus.py) and [`../filmu_py/plugins/hooks.py`](../filmu_py/plugins/hooks.py).
- Worker/runtime plugin contexts now hydrate from the same persisted plugin settings payload semantics used by the app runtime.

## Remaining growth areas

Beyond the currently implemented runtime baseline, the plugin platform should still grow toward:

- stronger compatibility/version policy and manifest/schema validation
- richer datasource surfaces only where they are justified by real plugin needs
- distributable external-author ergonomics around packaged plugins
- deeper operator/runtime policy around the now-real MDBList/StremThru/notification integrations
- queue-backed or otherwise more durable hook execution only if/when the in-process executor stops being sufficient

The publishable-event governance piece is already implemented and matters because the TS backend treats event publication as a governed capability rather than an unrestricted side effect. Filmu should preserve and deepen that boundary as the plugin model broadens.

## Current packaged discovery path

Packaged plugins can now register under the Python entry-point group `filmu.plugins`.

The entry point should resolve to a callable that returns a `(manifest, module)` pair, where:

- `manifest` is either a raw manifest dict or a validated [`PluginManifest`](../filmu_py/plugins/manifest.py)
- `module` is the Python module/object containing the manifest-declared resolver exports

The loader applies the same manifest validation and GraphQL resolver-registration path to both filesystem and packaged plugins, so schema composition remains unaware of which discovery path found a plugin.

## Implementation modules

- Implemented:
  - `filmu_py/plugins/manifest.py`
  - `filmu_py/plugins/loader.py`
  - `filmu_py/plugins/context.py`
  - `filmu_py/plugins/settings.py`
  - `filmu_py/plugins/registry.py`
  - `filmu_py/plugins/hooks.py`
  - `filmu_py/plugins/builtins.py`
- Remaining:
  - stricter compatibility/version policy beyond the minimal manifest fields
  - richer external-author packaging/distribution guidance
  - deeper compatibility/version policy and external-author guidance around the now-real MDBList, StremThru, and notification integrations

## TS backend audit reference

See [`RIVEN_TS_AUDIT.md`](RIVEN_TS_AUDIT.md) for the March 2026 plugin-runtime audit findings, including the dependency-scan discovery model and the current TS parity bar for Filmu.
