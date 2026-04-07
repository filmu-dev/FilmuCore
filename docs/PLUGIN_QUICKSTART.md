# Plugin Quickstart (Current Baseline)

## Purpose

This document explains the **current implemented baseline** for adding plugins to `filmu-python`.

It is intentionally narrow:

- It covers how to add a **drop-in filesystem plugin**.
- It covers the **smallest currently supported authoring path** through both drop-in manifests and packaged entry points.
- It focuses on GraphQL contribution examples because that is still the easiest entry point for plugin authoring.
- It does **not** try to restate the full plugin runtime, compatibility policy, or long-term SDK contract.

Status alignment note:

- The broader plugin runtime is already real.
- A formal [`PluginContext`](../filmu_py/plugins/context.py) boundary, plugin-scoped settings, datasource injection, worker-side plugin resolution, typed event hook workers, and built-in integrations already exist.
- This quickstart should be read as the narrowest "how to start" document, not as a complete description of everything the runtime can do.

For the higher-level plugin design and roadmap, see [`PLUGINS.md`](PLUGINS.md) and [`PLUGIN_SDK.md`](PLUGIN_SDK.md).

---

## What is implemented today

The current plugin system can:

1. Discover plugin folders from the configured plugin directory.
2. Discover packaged plugins from Python entry points under `filmu.plugins`.
3. Read and validate a `plugin.json` manifest.
4. Import the configured Python entry module.
5. Build a scoped [`PluginContext`](../filmu_py/plugins/context.py) for runtime initialization.
6. Register manifest-declared Strawberry resolver classes into the GraphQL schema.
7. Skip broken or version-incompatible plugins without blocking application startup.

Implementation references:

- Plugin directory setting: [`filmu_py/config.py`](../filmu_py/config.py)
- Manifest validation: [`filmu_py/plugins/manifest.py`](../filmu_py/plugins/manifest.py)
- Plugin loading: [`filmu_py/plugins/loader.py`](../filmu_py/plugins/loader.py)
- Plugin context: [`filmu_py/plugins/context.py`](../filmu_py/plugins/context.py)
- App startup wiring: [`filmu_py/app.py`](../filmu_py/app.py)
- Example coverage: [`tests/test_plugin_loader.py`](../tests/test_plugin_loader.py)

---

## Where plugins are loaded from

Plugins are loaded from two sources:

1. `FILMU_PY_PLUGINS_DIR` for drop-in filesystem plugins
2. Python entry points registered under `filmu.plugins`

- Default value: `plugins`
- This is configured in [`filmu_py/config.py`](../filmu_py/config.py)

If you do not override it, the app will look for a local folder structure like this from the app working directory:

```text
plugins/
  echo-plugin/
    plugin.json
    plugin.py
```

If the plugins directory does not exist, startup continues normally and plugin discovery is skipped.

---

## Minimal plugin layout

Each plugin currently needs:

1. A dedicated folder under the plugins directory
2. A `plugin.json` manifest
3. A Python entry module referenced by the manifest

Example:

```text
plugins/
  echo-plugin/
    plugin.json
    plugin.py
```

---

## Minimal `plugin.json`

Example manifest:

```json
{
  "name": "echo-plugin",
  "version": "1.0.0",
  "api_version": "1",
  "entry_module": "plugin.py",
  "graphql": {
    "query_resolvers": ["EchoQuery"]
  }
}
```

### Required fields in practice

- `name`: stable plugin identifier, no whitespace
- `version`: plugin version string
- `min_host_version`: optional minimum `FILMU_PY_VERSION` required by the plugin
- `api_version`: current compatibility marker
- `entry_module`: relative path to a Python file inside the plugin folder
- `graphql`: declared GraphQL export names

### Current validation rules

The loader currently enforces:

- `entry_module` must be a **relative** path
- `entry_module` must stay inside the plugin directory
- `entry_module` must point to a `.py` file
- Declared resolver export names must be valid Python identifiers
- Resolver export names must be unique within each resolver kind

These rules are implemented in [`filmu_py/plugins/manifest.py`](../filmu_py/plugins/manifest.py).

---

## Minimal plugin module

Example [`plugin.py`](../tests/test_plugin_loader.py):

```python
import strawberry


@strawberry.type
class EchoQuery:
    @strawberry.field
    def plugin_echo(self) -> str:
        return "hello-from-plugin"
```

The manifest must reference the exported class name exactly:

```json
{
  "graphql": {
    "query_resolvers": ["EchoQuery"]
  }
}
```

At startup, the loader imports the module, resolves `EchoQuery`, and registers it as a query contribution.

---

## Supported GraphQL contribution keys

The current manifest supports these GraphQL export groups:

- `query_resolvers`
- `settings_resolvers`
- `mutation_resolvers`
- `subscription_resolvers`

Each value is a list of exported class names from the plugin entry module.

`settings_resolvers` extend the nested `settings { ... }` object rather than the top-level query root.

Example:

```json
{
  "graphql": {
    "settings_resolvers": ["SettingsExtension"]
  }
}
```

With a module such as:

```python
import strawberry


@strawberry.type
class EchoSettings:
    enabled: bool


@strawberry.type
class SettingsExtension:
    @strawberry.field
    def echo(self) -> EchoSettings:
        return EchoSettings(enabled=True)
```

This would allow a GraphQL query like:

```graphql
query {
  settings {
    filmu {
      version
    }
    echo {
      enabled
    }
  }
}
```

---

## How to add a plugin

### 1. Create the plugin folder

Create a subfolder inside your configured plugins directory.

Example:

```text
plugins/echo-plugin/
```

### 2. Add `plugin.json`

Add a manifest that declares the entry module and exported resolver names.

### 3. Add the Python module

Create the entry module file referenced by `entry_module`.

### 4. Define Strawberry resolver classes

Add one or more Strawberry resolver classes and export them by name.

### 5. Restart the backend

Plugin discovery currently happens during application creation in [`filmu_py/app.py`](../filmu_py/app.py), so restart the backend after adding or changing a plugin.

---

## What happens if a plugin is broken

Broken plugins are isolated and reported.

Current examples covered by tests:

- Missing `entry_module`
- Missing exported resolver symbol
- Import-time exception from the plugin module
- Entry-point factory failure
- `min_host_version` newer than the running host version

In those cases:

- the broken plugin is recorded as a failure
- startup continues
- valid plugins still load

Behavior is covered in [`tests/test_plugin_loader.py`](../tests/test_plugin_loader.py).

---

## Current limitations

This is a baseline, not the final plugin platform.

Implemented already, but documented in the broader SDK docs rather than this quickstart:

- a formal `PluginContext` injection boundary
- plugin-scoped settings and datasource injection
- worker-side plugin resolution
- typed event hook workers and publishable-event governance

Still not established as a stable public third-party SDK:

- route registration as a documented stable extension surface
- scheduler/job registration as a documented stable extension surface
- REST compatibility contributions as a documented stable extension surface
- SSE contributions as a documented stable extension surface
- long-term version/compatibility policy for external plugin authors
- packaging/distribution guidance as a finalized contract

For now, plugin authoring should still be treated as a **controlled internal feature surface** rather than a fully stabilized external SDK, even though the runtime already supports more than GraphQL-only extension.

## Packaged entry-point shape

Packaged plugins can now register an entry point in the `filmu.plugins` group.

The entry point should resolve to a callable returning `(manifest, module)`:

```python
def build_plugin():
    return {
        "name": "echo-plugin",
        "version": "1.0.0",
        "min_host_version": "0.1.0",
        "api_version": "1",
        "entry_module": "ignored.py",
        "graphql": {"query_resolvers": ["EchoQuery"]},
    }, plugin_module
```

This keeps the packaged discovery path on the same validated manifest contract as the filesystem path.

---

## Recommended current workflow

If you want to add a plugin today:

1. Start with one small GraphQL query contribution.
2. Keep the manifest minimal.
3. Avoid importing host internals beyond what is already necessary.
4. Use [`tests/test_plugin_loader.py`](../tests/test_plugin_loader.py) as the reference pattern.
5. Restart the backend and verify the new GraphQL field appears.

---

## Later documentation

A later document should cover the **proper long-term plugin authoring model**, including:

- stable plugin APIs
- capability boundaries
- dependency injection contract
- version compatibility policy
- testing strategy for external plugin authors
- packaging and distribution guidance

This document only describes the current implemented baseline.
