# Plugin Distribution Policy

This document defines the current plugin compatibility and distribution contract for FilmuCore.

## Supported distribution modes

- `builtin`: shipped inside the FilmuCore repository and registered programmatically
- `filesystem`: discovered from a plugin directory that contains a validated `plugin.json`
- `entry_point`: discovered from an installed Python distribution entry point under `filmu.plugins`

## Required manifest contract

Every plugin manifest must provide:

- `name`
- `version`
- `api_version`
- `entry_module`

Optional compatibility bounds:

- `min_host_version`
- `max_host_version`

The current host supports only plugin `api_version = "1"`.

## Compatibility rules

- A plugin is rejected with `api_version_incompatible` when its manifest declares an unsupported API version.
- A plugin is rejected with `host_version_incompatible` when the running FilmuCore version is below `min_host_version` or above `max_host_version`.
- Manifest validation failures remain isolated to the failing plugin and must not block other plugin loads.

## Operator visibility

`GET /api/v1/plugins` is the operator-facing runtime view for plugin readiness.

It now exposes additive compatibility metadata for each plugin:

- version and API version
- min/max host bounds
- source/distribution
- readiness/configured state
- loader warnings or startup failure reason

Built-in integrations may also expose readiness warnings derived from runtime settings, such as `stremthru` being present but not configured with a token.
