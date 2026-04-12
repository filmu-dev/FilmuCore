# Plugin Capability Model Matrix

## Purpose

Turn the active plugin/platform track from [`../STATUS.md`](../STATUS.md) and [`../EXECUTION_PLAN.md`](../EXECUTION_PLAN.md) into an executable planning artifact.

This document maps:

- the **current Python plugin baseline**
- the **fuller TS-origin plugin platform model**
- the **missing capability layers** needed for `filmu-python` to become a stronger extensibility platform than `riven-ts`

It is intentionally broader than GraphQL-only plugin work.

---

## Current Python plugin baseline

Current implemented plugin capabilities in `filmu-python` are centered around:

- filesystem plugin discovery
- packaged entry-point discovery
- manifest validation + safe module loading
- GraphQL query/settings resolver contribution
- typed runtime capability registration across scraper/downloader/indexer/content-service/notification/event-hook implementations
- plugin-scoped settings registry plus datasource-aware `PluginContextProvider` injection
- namespaced publishable-event governance plus timeout-isolated typed hook execution
- plugin load/hook telemetry plus runtime visibility via `/api/v1/plugins` and `/api/v1/plugins/events`
- built-in Prowlarr/RARBG/Torrentio scrapers plus real MDBList/webhook-notification/StremThru implementations
- worker-side plugin registry resolution from persisted plugin settings payload semantics
- failure isolation for invalid plugins and hanging/failing hooks

Key current implementation references:

- [`filmu_py/plugins/manifest.py`](../../filmu_py/plugins/manifest.py)
- [`filmu_py/plugins/loader.py`](../../filmu_py/plugins/loader.py)
- [`filmu_py/graphql/plugin_registry.py`](../../filmu_py/graphql/plugin_registry.py)
- [`filmu_py/graphql/schema.py`](../../filmu_py/graphql/schema.py)

What this baseline is good at:

- safe startup-time plugin loading
- controlled GraphQL schema extension
- clear manifest-declared contribution boundaries
- typed invocation of capability protocols
- worker-scope plugin loading with full `PluginContextProvider` (settings, event bus, rate limiter, cache, per-plugin logger)
- real provider-backed scrape pipeline through built-in scrapers

What it does **not** yet provide:

- richer external-author packaging/distribution guidance
- durable/queue-backed hook execution if operational pressure eventually requires it
- stronger environment/runtime evidence for the now-enforced non-builtin isolation policy
- pluginized parity for Seerr/Listrr intake, Comet scraping, Plex post-download hooks, and TMDB/TVDB index-provider plugins

StremThru DownloaderPlugin is now a real implementation (Slice E): `add_magnet()`, `get_status()`, and `get_download_links()` now talk to the StremThru v0 API with token-based auth, `httpx.AsyncClient`, and downloader-plugin DTO normalization. With that change, all three built-in plugin stubs called out in the earlier Slice D update are now real implementations.

April 2026 policy update:

- [`../../filmu_py/plugins/manifest.py`](../../filmu_py/plugins/manifest.py) now validates additive plugin policy metadata for `publisher`, `release_channel`, `trust_level`, `permission_scopes`, capability-derived minimum scopes, and built-in publishable-event namespacing.
- [`../../filmu_py/plugins/loader.py`](../../filmu_py/plugins/loader.py) now surfaces those policy fields on load success, validates manifest policy on registration, and warns when non-builtin plugins rely on implicit capability-derived scopes instead of explicit declarations.
- [`../../filmu_py/api/routes/default.py`](../../filmu_py/api/routes/default.py) now exposes those policy/health fields on `GET /api/v1/plugins` and `GET /api/v1/plugins/events`, which gives operators a first explicit plugin trust/compatibility surface rather than a capability-only list.
- [`../../filmu_py/plugins/manifest.py`](../../filmu_py/plugins/manifest.py) now also validates `source_sha256`, `signature`, `signing_key_id`, `sandbox_profile`, and quarantine fields, while [`../../filmu_py/plugins/loader.py`](../../filmu_py/plugins/loader.py) now rejects quarantined plugins and filesystem/source digest mismatches before registration.
- [`../../filmu_py/plugins/trust.py`](../../filmu_py/plugins/trust.py) now loads an operator-managed JSON trust store, verifies HMAC-SHA256 plugin signatures, supports revocation lists for key IDs and signatures, and lets startup enforce strict signature policy for non-builtin plugins through `FILMU_PY_PLUGIN_TRUST_STORE_PATH` and `FILMU_PY_PLUGIN_STRICT_SIGNATURES`.
- [`../../filmu_py/api/routes/default.py`](../../filmu_py/api/routes/default.py) now surfaces `signature_verified`, `signature_verification_reason`, `trust_policy_decision`, and `trust_store_source` on `GET /api/v1/plugins`, so operators can distinguish unsigned, untrusted, revoked, and verified plugins without reading logs.
- persisted operator overrides now sit above manifest defaults through [`../../filmu_py/services/plugin_governance.py`](../../filmu_py/services/plugin_governance.py), and operators can manage approved/quarantined/revoked state through `GET /api/v1/plugins/governance`, `GET /api/v1/plugins/governance/overrides`, and `POST /api/v1/plugins/governance/{plugin_name}`.

---

## Reference breadth from the original TS platform

The original TS backend’s plugin model includes much more than resolver discovery.

Confirmed TS capabilities include:

- plugin package registration and discovery
- settings schema registration into a shared plugin settings container
- datasource construction per plugin
- GraphQL resolvers
- hook execution on typed program events
- queue-backed plugin workers
- GraphQL context composition using plugin datasources/settings
- plugin validation lifecycle during bootstrap
- current workspace inventory that includes `plugin-comet`, `plugin-listrr`, `plugin-mdblist`, `plugin-notifications`, `plugin-plex`, `plugin-seerr`, `plugin-stremthru`, `plugin-tmdb`, `plugin-torrentio`, and `plugin-tvdb`
- verified hook wiring for TMDB indexing, Seerr request intake, Plex post-download actions, and notifications
- typed event taxonomy breadth beyond the older scrape/download story, including provider-list, cache-check, and stream-link request families

Representative TS references:

- [`packages/plugin-seerr/lib/index.ts`](../../../riven/../../packages/plugin-seerr/lib/index.ts) (plugin contract shape)
- [`lib/state-machines/plugin-registrar/index.ts`](../../../riven/lib/state-machines/plugin-registrar/index.ts)
- [`lib/state-machines/plugin-registrar/actors/register-plugin-hook-workers.actor.ts`](../../../riven/lib/state-machines/plugin-registrar/actors/register-plugin-hook-workers.actor.ts)
- [`lib/graphql/build-context.ts`](../../../riven/lib/graphql/build-context.ts)

---

## Plugin capability matrix

| Capability                                    | Current Python state             | TS reference breadth                                                                                             | Why it matters                                                                                               | Priority               |
| --------------------------------------------- | -------------------------------- | ---------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ---------------------- |
| **Filesystem plugin discovery**               | Implemented                      | TS uses a richer packaged plugin model                                                                           | Good current baseline for safe internal plugins                                                              | Done baseline          |
| **Packaged entry-point discovery**            | Implemented baseline             | TS discovers installed `@repo/plugin-*` dependencies from the app manifest rather than true entry-point metadata | Needed for real plugin distribution and future external authors, and is a clear place for Filmu to exceed TS | Done baseline / deepen |
| **Manifest validation**                       | Implemented baseline             | TS has config + schema-driven validation                                                                         | Good start, but still too narrow for full platform semantics                                                 | Done baseline / deepen |
| **Version compatibility policy**              | Implemented baseline (`api_version`, host min/max, release channel, trust metadata, permission scopes) | TS has stronger typed platform contracts                                                                 | Needed to evolve safely without plugin breakage                                                              | **P1**                 |
| **Shared plugin settings registry/container** | Implemented baseline             | TS has `PluginSettings` model and registration flow                                                              | Needed for consistent plugin configuration lifecycle                                                         | Done baseline          |
| **Datasource injection**                      | Implemented baseline             | TS constructs datasources per plugin                                                                             | Needed for clean provider/service access inside plugins                                                      | Done baseline / deepen |
| **Plugin runtime context injection**          | Implemented baseline             | TS builds GraphQL/plugin context dynamically                                                                     | Needed to keep plugins decoupled from host internals                                                         | Done baseline / deepen |
| **GraphQL resolver contribution**             | Implemented baseline             | TS supports GraphQL plugin contribution broadly                                                                  | Current strongest Python plugin capability                                                                   | Done baseline          |
| **Hook-worker execution on typed events**     | Implemented baseline (in-process) | TS has queue-backed plugin hook workers                                                                         | Required for parity beyond GraphQL-only plugins                                                              | Done baseline / deepen |
| **Capability registration beyond GraphQL**    | Implemented baseline             | TS plugin hooks + datasources + settings imply broader capability model                                          | Needed so the backend can grow into a real plugin platform                                                   | Done baseline / deepen |
| **Publishable-event governance**              | Implemented baseline             | TS tracks publishable event set explicitly                                                                       | Needed to prevent event/queue sprawl and undefined fan-out                                                   | Done baseline          |
| **Plugin telemetry and health visibility**    | Improved baseline                | TS logs/telemetry are more embedded in bootstrap/runtime                                                         | Needed for operability and safer growth                                                                      | **P1**                 |
| **Plugin isolation model**                    | Startup + hook isolation + policy baseline | TS also isolates validation/runtime registration more deeply                                                    | Needed to keep bad plugins from degrading the platform                                                       | **P1**                 |
| **Operator governance overrides**             | Implemented baseline (`approved` / `quarantined` / `revoked`) | TS has broader ecosystem policy and package lifecycle management                                                | Needed for day-2 operability and quarantine/revocation without code changes                                 | Done baseline / deepen |

---

## Recommended capability layers for Python

The Python plugin roadmap should be built in layers rather than as a single large rewrite.

### Layer 1 — Discovery and contract safety

Add:

- packaged entry-point discovery
- stronger manifest/schema validation
- explicit compatibility/version rules

Goal:

- make plugin loading safe and predictable before broadening runtime power

Current update:

- The loader now discovers both drop-in filesystem plugins and packaged plugins registered under the Python entry-point group `filmu.plugins`.
- Both paths now flow through the same validated manifest contract and the same `PluginLoadSuccess` / `PluginLoadFailure` reporting path, so GraphQL schema composition stays discovery-source agnostic.
- The manifest model now also carries `min_host_version`, and plugins that require a newer host version fail safely with `host_version_incompatible` instead of partially loading.
- The manifest/loader surface now also carries operator-facing `publisher`, `release_channel`, `trust_level`, bounded `permission_scopes`, provenance metadata, sandbox posture, and quarantine state, and built-ins are now pinned to explicit `builtin` policy metadata plus host-level sandbox posture instead of relying on implicit convention.

### Layer 2 — Configuration and datasources

Delivered baseline:

- shared plugin settings container/registry
- datasource construction rules
- explicit plugin context contract

Remaining focus:

- broaden datasource surfaces only where justified by real plugin needs
- keep plugins decoupled from host internals as the datasource surface grows

### Layer 3 — Runtime capability model

Delivered baseline:

- hook-worker/event execution
- typed capability registration
- publishable-event governance

Remaining focus:

- decide whether durable/queued hook execution is actually needed beyond the current in-process executor
- keep runtime isolation and telemetry ahead of further capability growth

### Layer 4 — Operability and policy

Add:

- plugin telemetry
- health/status visibility
- stronger runtime isolation and failure containment
- future external plugin author guidance

Goal:

- make the plugin platform sustainable, not just powerful

---

## Recommended Python plugin contract shape

The Python platform should eventually define capabilities explicitly, for example along lines like:

- `settings`
- `datasources`
- `graphql`
- `event_hooks`
- `stream_control`
- `future_admin_or_projection_hooks`

Each capability should be:

- declared explicitly
- versioned
- validated at startup
- observable at runtime

This is how Python can beat the TS model: not just matching breadth, but making capability boundaries more explicit and more operable.

---

## What not to do

- Do **not** let plugin growth remain centered only on GraphQL resolver lists.
- Do **not** add hook workers before datasource/context boundaries are defined.
- Do **not** rely on free-form plugin imports of host internals.
- Do **not** expose a “powerful” plugin model without publishability and failure-isolation rules.

---

## Minimum implementation sequence for Priority 4

1. Strengthen plugin compatibility/version policy and manifest/schema validation.
2. Harden external-author packaging/distribution guidance around the now-implemented discovery paths.
3. Keep plugin telemetry/health summaries and runtime isolation green after the now-landed Wave 4 exit gates.
4. Keep the trust-store/signature baseline green in operator workflows after the stronger non-builtin runtime policy landed.
5. Only add durable queue-backed hook execution if operational evidence shows the in-process executor is insufficient.

This sequence keeps the platform safe before it becomes broad.

---

## Success checkpoint

Priority 4 should be considered meaningfully advanced when:

- plugins can be discovered from both filesystem manifests and packaged entry points, exceeding the current TS dependency-scan discovery model
- plugins receive settings and datasources through an explicit contract
- plugins can extend backend behavior beyond GraphQL through controlled event hooks
- the host can observe which plugin capabilities are active and which events are publishable
- failure isolation remains strong as plugin power increases

Current checkpoint:

- Fully closed in-repo for discovery/runtime/governance, including trust-store verification, operator override controls, plugin health rollups, and enforceable non-builtin runtime isolation policy.
- Remaining work is recurring external-author/runtime evidence and broader plugin breadth, not missing Wave 4 isolation or health-rollup primitives.

## Recent TS audit correction (March-April 2026)

- The current TS backend already provides `PluginSettings`, datasource construction, optional plugin runtime context injection, validator retries, queue-backed hook workers, and publishable-event gating.
- **April 2026 local re-audit:** the refreshed local `apps/riven/package.json` now carries `plugin-comet`, `plugin-mdblist`, `plugin-notifications`, and `plugin-stremthru`, plus the retained `plugin-listrr`, `plugin-plex`, `plugin-seerr`, `plugin-tmdb`, `plugin-torrentio`, and `plugin-tvdb` dependencies.
- The same local workspace still physically retains `packages/plugin-realdebrid`, so the current local TS source should be treated as a mixed filesystem baseline rather than a neat one-direction migration. Shared package layers such as `util-plugin-sdk`, `util-plugin-testing`, `util-rank-torrent-name`, and several `packages/core/*` support packages remain part of the broader platform baseline.
- However, its discovery model is still dependency-scan based from the app manifest, not a generalized entry-point system.
- Filmu should use that distinction as one of the clearest ways to exceed `riven-ts` while preserving stricter capability boundaries, and should aim to replicate MDBList, StremThru, and Webhook Notification capabilities explicitly as built-in plugins or entry-point extensions.
- Filmu should therefore benchmark against the newer TS baseline as a **monorepo plugin/platform ecosystem**, not just a backend app that happens to load a few plugins.
- **Slice D/E update:** MDBList list-sync, Webhook Notifications, and StremThru are now full implementations. `MDBListContentService` polls the MDBList API and returns `ContentRequest` objects consumed by the `poll_content_services` ARQ cron. `WebhookNotificationPlugin` sends real Discord embeds and generic JSON webhooks and is dual-registered as both a `NotificationPlugin` and a `PluginEventHookWorker`, receiving `item.state.changed` events through the `PluginHookWorkerExecutor` fan-out added in Slice B. `StremThruDownloaderPlugin` now talks to the StremThru v0 API with token-authenticated downloader operations. The `poll_content_services` ARQ cron (every 30 min) now provides the first scheduled content-service intake path through the plugin platform.
- Current Filmu built-in plugin breadth is narrower and more explicit: `TorrentioScraper`, `ProwlarrScraper`, `RarbgScraper`, `MDBListContentService`, `StremThruDownloaderPlugin`, and `WebhookNotificationPlugin` are source-verified; this is a real baseline, but it is not yet the same breadth as the TS packaged ecosystem.
- TMDB and TVDB are currently host services in Filmu, not pluginized index-provider equivalents.
- There is still no current built-in Filmu equivalent to Seerr intake, Plex post-download hook plugins, Comet scraping, or Listrr intake.

## Serving-control note (March 2026)

- The backend now has a shared serving/status substrate, but plugins still do not have a controlled stream-control capability surface layered on top of it.
- Future plugin stream capabilities should attach to explicit serving-session/accounting contracts rather than importing serving internals directly.
