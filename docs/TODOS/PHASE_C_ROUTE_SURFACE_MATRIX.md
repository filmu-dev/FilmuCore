# Phase C Route Surface Matrix

## Purpose

Turn Phase C into an executable route-by-route plan for the current frontend.

This document complements [`LOCAL_FRONTEND_TESTING_READINESS.md`](../LOCAL_FRONTEND_TESTING_READINESS.md):

- [`LOCAL_FRONTEND_TESTING_READINESS.md`](../LOCAL_FRONTEND_TESTING_READINESS.md) remains the source of truth for **readiness thresholds and blockers**.
- This document is the source of truth for the **backend route work breakdown** needed to clear those blockers.

---

## Guiding rule

The backend is the **motor** for the frontend.

So Phase C should not be treated as a random list of endpoints. It should be implemented in the order that unlocks the most meaningful current-frontend capability with the least wasted work.

---

## Current delivered baseline

Phase C core breadth is now effectively delivered for the current frontend compatibility surface.

Delivered backend surfaces now include:

- settings compatibility routes under [`filmu_py/api/routes/settings.py`](../../filmu_py/api/routes/settings.py)
- historical logs via [`/api/v1/logs`](../../filmu_py/api/routes/default.py)
- SSE topics via [`/api/v1/stream/{event_type}`](../../filmu_py/api/routes/stream.py)
- dashboard routes via [`/api/v1/stats`](../../filmu_py/api/routes/default.py), [`/api/v1/services`](../../filmu_py/api/routes/default.py), and [`/api/v1/downloader_user_info`](../../filmu_py/api/routes/default.py)
- item routes via [`/api/v1/items`](../../filmu_py/api/routes/items.py) and related detail/action endpoints in [`filmu_py/api/routes/items.py`](../../filmu_py/api/routes/items.py)
- calendar compatibility via [`/api/v1/calendar`](../../filmu_py/api/routes/default.py)
- API key regeneration compatibility via [`/api/v1/generateapikey`](../../filmu_py/api/routes/default.py)
- scrape compatibility baselines via [`filmu_py/api/routes/scrape.py`](../../filmu_py/api/routes/scrape.py)
- legacy watch alias compatibility via [`filmu_py/api/routes/triven.py`](../../filmu_py/api/routes/triven.py)

The route matrix below should therefore be read as a current-state map: most Phase C breadth is delivered, while the active remaining frontier is playback hardening.

---

## Phase C route matrix

| Frontend surface          | Backend routes needed                                                                                                     | Current state                                                                                                                   | Domain prerequisites                                                                                                                                       | Priority               |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------- |
| Settings                  | `/api/v1/settings`, `/api/v1/settings/schema`, `/api/v1/settings/schema/keys`, `/api/v1/settings/get/{paths}`, `/api/v1/settings/set/{paths}` | Implemented with persisted single-row compatibility-blob storage, startup hydration, runtime reads, and compatibility get/set surfaces | Runtime settings model, compatibility translation layer, startup hydration, single-row persisted blob semantics                                             | Done / maintain        |
| Library Profiles          | same settings routes, especially filesystem/library profile paths                                                         | Baseline implemented, but depends on richer persistence semantics later                                                         | Filesystem settings shape, stable schema metadata                                                                                                          | Done / maintain        |
| Logs UI                   | `/api/v1/logs`, `/api/v1/stream/logging`                                                                                  | Implemented through history route + generic stream topic                                                                        | Log broker and SSE fan-out                                                                                                                                 | Done / maintain        |
| Notifications UI          | `/api/v1/stream/notifications`                                                                                            | Minimal baseline implemented through generic stream topic                                                                       | Notification payload generation on completed transitions                                                                                                   | Done / maintain        |
| Dashboard                 | `/api/v1/stats`, `/api/v1/services`, `/api/v1/downloader_user_info`                                                       | Implemented baseline                                                                                                            | Aggregated stats model, service registry/status model, downloader account/user-info abstraction                                                            | Done baseline / deepen |
| Library list              | `/api/v1/items`                                                                                                           | Implemented baseline + additive cooldown metadata (`next_retry_at`, `recovery_attempt_count`, `is_in_cooldown`)                | Richer item list model, filters/sort/pagination semantics                                                                                                  | Done baseline / deepen |
| Item add/request          | `/api/v1/items/add`                                                                                                        | Implemented compatibility baseline after first local stack run exposed a `405` gap, with partial season/episode request ranges | Request-intake semantics, identifier normalization, stable add response contract                                                                            | Done baseline / deepen |
| Library actions           | `/api/v1/items/reset`, `/api/v1/items/retry`, `/api/v1/items/remove`                                                      | Implemented baseline                                                                                                            | Action semantics, idempotent mutation rules, state transition guardrails                                                                                   | Done baseline / deepen |
| Item details              | `/api/v1/items/{id}`                                                                                                      | Implemented baseline + persisted playback attachment projection + resolved playback snapshot + first `media_entries` projection + additive cooldown metadata | Richer item detail model with metadata, streams, status, actionability, attachment context, current playback readiness, and VFS-facing media-entry context | Done baseline / deepen |
| Calendar                  | `/api/v1/calendar`                                                                                                        | Implemented baseline                                                                                                            | Calendar projection model, normalized date fields on items/episodes                                                                                        | Done baseline / deepen |
| API key regeneration flow | `/api/v1/generateapikey`                                                                                                  | Implemented with live runtime rotation, persisted storage, and explicit operator warning                                        | Rotation workflow, BFF alignment rules, explicit operational messaging                                                                                     | Done baseline / deepen |
| Stream file playback      | `/api/v1/stream/file/{item_id}`                                                                                           | Implemented with explicit byte-range handling, governed sessions, and lease-refresh integration                                                                                                                | Link resolver, direct range serving, item-to-file resolution                                                                                               | Done baseline / mature |
| HLS playback              | `/api/v1/stream/hls/{item_id}/index.m3u8`, `/api/v1/stream/hls/{item_id}/{file_path}`                                     | Implemented for local generation, upstream proxying, and transcode fallback, with explicit lifecycle/governance               | Link resolver, HLS pipeline, ffmpeg process control, segment lifecycle                                                                                     | Done baseline / mature |

---

## Recommended implementation order

## Completed core breadth

The Phase C core breadth that unlocked Threshold B is already in place:

1. `/api/v1/stats`
2. `/api/v1/services`
3. `/api/v1/downloader_user_info`
4. `/api/v1/items`
5. `/api/v1/items/{id}`
6. `/api/v1/items/reset`
7. `/api/v1/items/retry`
8. `/api/v1/items/remove`
9. `/api/v1/calendar`
10. `/api/v1/generateapikey`
11. `/api/v1/items/add`

## Current active frontier (playback surface)

1. `/api/v1/stream/file/{item_id}` direct-play hardening
2. `/api/v1/stream/hls/{item_id}/index.m3u8`
3. `/api/v1/stream/hls/{item_id}/{file_path}` child-file hardening

Why this order now:

- Threshold B is already reached for meaningful local frontend workflow testing
- the remaining practical unblocker is playback parity rather than broader route breadth
- these routes should be built on the same shared stream/VFS foundations being planned for FilmuVFS
- the recommended local integration harness is now [`docker-compose.local.yml`](../../docker-compose.local.yml), documented in [`../LOCAL_DOCKER_STACK.md`](../LOCAL_DOCKER_STACK.md)
- that local harness is now also backend-startup-safe for the async PostgreSQL DSN path and can bring up the current frontend and Python backend together for practical local testing
- the first local run also confirmed that `POST /api/v1/items/add` and external-id item-detail lookup are real compatibility surfaces that must stay aligned with the current frontend, not optional cleanup work
- the HLS frontier has now moved beyond the earlier remote-direct unsupported-transcode seam and back to production-grade ffmpeg/governance work plus end-to-end player validation

## Cross-cutting domain dependencies

Several remaining Phase C capability gaps are blocked less by routing code and more by domain-model depth.

The main shared prerequisites are:

### 1. Richer media item representation

Needed for:

- `/api/v1/items`
- `/api/v1/items/{id}`
- library actions
- dashboard summaries

Minimum needed fields likely include:

- item identity
- current lifecycle state
- metadata summary
- stream status
- timestamps relevant to sort/order/calendar
- enough structure for actionability and retry/reset/remove behavior

### 2. Service/downloader visibility model

Needed for:

- `/api/v1/services`
- `/api/v1/downloader_user_info`

This should not be hacked in as free-form dicts. It needs a stable compatibility model.

### 3. Calendar projection model

Needed for:

- `/api/v1/calendar`

This may require a dedicated projection/query path rather than overloading the item-list model.

### 4. Auth rotation workflow rules

Needed for:

- `/api/v1/generateapikey`

This must stay aligned with [`AUTH.md`](../AUTH.md) and must preserve BFF safety.

### 5. Shared stream/VFS control-plane foundations

Needed for:

- direct file playback
- HLS playback
- eventual FilmuVFS mount behavior

This should be built once, not separately for HTTP and VFS.

---

## What not to do

- Do **not** implement Phase C as isolated thin endpoints with no domain-model plan.
- Do **not** build playback-first while dashboard/library/calendar remain unusable locally.
- Do **not** let HTTP playback paths diverge architecturally from the future FilmuVFS byte-serving engine.
- Do **not** sacrifice stable route contracts just to quickly satisfy current frontend calls.

---

## Success checkpoint

Phase C core breadth is already meaningfully advanced. The current next checkpoint should be considered reached when:

- stream file playback works against `filmu-python` without contract workarounds
- HLS routes behave as real playback endpoints rather than `501` placeholders
- playback routes share intentional stream/VFS foundations instead of route-local hacks
- the next emphasis can then shift more heavily toward plugin breadth, orchestration depth, and FilmuVFS performance work

## Serving-core update (March 2026)

- `/api/v1/stream/file/{item_id}` now has explicit byte-range behavior through the shared serving core.
- `/api/v1/stream/hls/*` now uses that same substrate for local generation, upstream proxying, path safety, cleanup/concurrency, and serving-session/accounting hooks.
- `/api/v1/stream/status` now exposes the first internal session/governance view for the serving layer.
- Focused backend-side regression coverage now also mirrors the current frontend BFF direct-range proxy contract and the player HLS query-parameter pattern, reducing the chance of playback contract drift while full browser/BFF validation remains pending.
- HLS playlist responses now also use explicit `Cache-Control: no-store` while HLS child files/segments use `Cache-Control: public, max-age=3600`, which gives the current VOD playback path a clearer freshness policy across both backend and BFF expectations.
- Remote-HLS playlist fetches and segment proxy opens now also retry one transient timeout/transport failure and then enter a short cooldown with `Retry-After` for repeated failures against the same upstream playlist URL, which gives the current playback route surface a first bounded upstream backoff policy above one-shot failure mapping.
- Direct-play selection now also preserves explicit source authority in the shared selector while still preferring ready local files over remote direct links when authority is otherwise equal, which reduces drift between attachment-backed and persisted-entry-backed direct-play resolution.
- Direct-play selection now also exposes explicit named source classes in the shared resolver, which makes the current route surface easier to evolve without hiding all direct-play policy behind tuple ordering alone.
- Direct-play selection now also exposes explicit provider-link health-state classes in that shared resolver, which reduces the chance that stale/refreshing/failed provider-backed direct links get treated as undifferentiated generic direct sources during future route hardening.
- Direct-play resolution now also has an explicit service-layer policy decision seam for serve/fail plus refresh intent, which makes the current route surface easier to harden further without coupling route behavior directly to low-level attachment-selection branches.
- Direct-play resolution now also carries an explicit internal refresh-recommendation payload for stale/refreshing/degraded provider-backed direct cases, which gives later orchestration and control-plane work a cleaner handoff point than inferring refresh targets from route outcomes alone.
- Direct-play resolution now also has a service-layer refresh-dispatch seam that maps those recommendations onto existing media-entry or persisted-attachment refresh-request models, which reduces the gap between route policy and later control-plane execution work without changing the current route contract.
- Direct-play resolution now also has one-shot service-level execution helpers for those refresh dispatches, which reduces the gap between route policy and real control-plane execution while still keeping execution off the live HTTP request path.
- Those one-shot direct-play refresh executions now also honor a provider-specific `stream_link_refresh` limiter bucket and return explicit retry-after/backpressure metadata on denial, which reduces the gap between playback policy and provider-pressure handling without changing the live route contract.
- Direct-play resolution now also has a small background scheduling seam above that limiter-aware one-shot execution path, which keeps the live route eligible to remain non-blocking while giving control-plane callers explicit run-later requests and preserved `retry_after_seconds` scheduling guidance.
- The backend now also has a small in-process control-plane caller above that scheduling seam, which gives later route-adjacent or service-level orchestration a deduplicated background trigger without yet committing to ARQ or event-backplane wiring.
- That in-process control-plane caller now also sits on the app-resource/runtime boundary rather than the route layer, which gives the next route-adjacent step a stable app-scoped trigger point while preserving the current non-blocking direct-play contract.
- The playback layer now also exposes a small helper above that app-scoped controller attachment, which gives future route-adjacent work a named service-boundary trigger rather than forcing direct resource/controller access when the HTTP integration step eventually lands.
- The direct-play route now also uses that helper opportunistically for remote direct winners and selected failed direct leases, which gives `/api/v1/stream/file/{item_id}` its first real route-adjacent non-blocking trigger into the refresh control-plane without widening into a broader orchestration path.
- That route-adjacent trigger path now also suppresses duplicate background trigger creation when the same item is already pending and surfaces additive trigger-pressure governance counters through `/api/v1/stream/status`, which gives operators a first route-surface view of repeated direct-play backoff pressure beyond raw limiter metrics alone.
- The direct-play route now also preserves a stable inline `Content-Disposition` filename when the resolved direct-play attachment carries filename metadata, while still respecting upstream-provided `content-disposition` on proxied remote direct responses, which hardens the current frontend BFF file-serving contract without widening the route taxonomy.
- The direct-play route now also consumes a service-layer direct-file serving descriptor that is itself built from an internal `DirectFileLinkResolution` model in [`../filmu_py/services/playback.py`](../filmu_py/services/playback.py), and that internal resolution is now built from the explicit direct-play decision seam rather than raw attachment shape alone, which moves direct-file provenance, source classification, and filename derivation farther away from route-local attachment handling without widening the HTTP surface.
- That internal direct-file provenance now also projects debrid-first persisted lifecycle context such as owner kind/id, provider family, locator source, restricted-fallback state, match basis, and persisted refresh/error fields, which deepens the service-layer read model without widening the current direct-play route surface.
- That lifecycle model now also projects onto the internal resolved direct/HLS playback snapshot used by adjacent playback and future VFS-facing read paths, which deepens non-route playback read models without changing the current Phase C route contract.
- The HLS route family now also uses a parallel app-scoped helper/controller opportunistically when a selected HLS lease fails closed with `503`, which gives `/api/v1/stream/hls/{item_id}/index.m3u8` and `/api/v1/stream/hls/{item_id}/{file_path}` their first real route-adjacent non-blocking trigger into durable failed-lease refresh work without widening into a broader orchestration path.
- That narrow HLS failed-lease trigger path now also suppresses duplicate background trigger creation when the same item is already pending and surfaces additive trigger-pressure governance counters through `/api/v1/stream/status`, which gives operators a first HLS route-surface view of repeated failed-lease backoff pressure beyond raw limiter metrics alone.
- The HLS route family now also uses a second parallel app-scoped helper/controller opportunistically when the resolved HLS winner is a selected `media-entry:restricted-fallback` remote playlist, which gives `/api/v1/stream/hls/{item_id}/index.m3u8` and `/api/v1/stream/hls/{item_id}/{file_path}` a separate narrow route-adjacent non-blocking trigger for stale/refreshing selected HLS leases without widening into broader winner-class orchestration.
- That narrow HLS restricted-fallback trigger path now also suppresses duplicate background trigger creation when the same item is already pending and surfaces additive parallel trigger-pressure governance counters through `/api/v1/stream/status`, which gives operators a separate HLS route-surface view of stale/refreshing backoff pressure beyond the existing failed-lease family.
