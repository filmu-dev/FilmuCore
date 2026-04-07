# Local Frontend + Python Backend Testing Readiness

## Purpose

Define when the current `Triven_frontend` can be meaningfully tested against `filmu-python` locally, and make clear what works now versus what still blocks full local validation.

Status alignment note:

- [`STATUS.md`](STATUS.md) is the canonical current-status document for this repository.
- This document is frontend-scoped guidance and depends on the external `Triven_frontend` repository, which is not included in this workspace.
- Backend-side playback proof, stale-link recovery proof, and preferred-client playback validation are now documented as achieved in [`STATUS.md`](STATUS.md).
- Read any older "not yet fully validated" statements below as local-frontend-environment caveats unless they explicitly refer to the backend runtime itself.

This document assumes the backend is the **motor** for the frontend:

- the frontend owns UX for browsing, requesting, and playback flows
- the backend must provide stable contracts and execution behavior
- local testing becomes useful only when enough of those backend contracts exist

---

## Short answer

**You can now start meaningful local frontend + Python backend testing with the root Docker stack.**

For backend status:

- backend-side playback proof is now documented as achieved in [`STATUS.md`](STATUS.md)
- playback-gate CI promotion is still a separate follow-up item in [`STATUS.md`](STATUS.md)

For local current-frontend validation:

- full reproduction still depends on the external `Triven_frontend` repo, its environment, and aligned credentials
- this workspace alone does not contain the current frontend source tree needed to reproduce that path end-to-end

Important re-audit finding:

- Current inspection of [`Triven_frontend/src`](../../../Triven_frontend/src) did not show direct backend GraphQL consumption in the current protected-route/backend integration paths.
- The current frontend is effectively BFF/REST-driven for backend operations.
- Therefore, GraphQL parity is strategically important, but `/api/v1/*` route breadth is the immediate gating factor for meaningful local frontend testing.

You can test now:

- auth/session wiring on the frontend itself
- settings page schema/get/set flows already implemented in `filmu-python`
- dashboard/library/calendar compatibility flows
- logs/notification SSE proxy plumbing against the new Python compatibility routes
- scrape auto/manual compatibility baselines and scrape-session scaffolding
- GraphQL/plugin development in targeted local tests

Important local-run limitation:

- real scrape/debrid progression now exists through the current ARQ stages and built-in scraper/provider clients
- in practical terms, items will still stall locally unless scraper/downloader settings and real provider credentials are configured for the running stack
- while the stack startup path now auto-recovers a common stale `/mnt/filmuvfs` mount-root failure mode via [`../start_local_stack.ps1`](../start_local_stack.ps1), that only removes one local bring-up blocker; it does not change upstream scraper/provider availability constraints

First local compatibility findings that have now been resolved:

- the first containerized run exposed a missing `POST /api/v1/items/add` compatibility route
- the same run also exposed that item-detail lookup needed to match the external TMDB/TVDB identifier families the frontend actually sends
- the same compatibility sweep also had to absorb the frontend bug that sends brand-new TV requests with selected seasons to `POST /api/v1/scrape/auto`
- those request/detail/scrape compatibility gaps are now implemented, so the current local blocker has moved back toward playback and downstream orchestration realism rather than basic request-route absence

Within this workspace alone, you cannot independently reproduce the entire current-frontend playback path because the frontend implementation being referenced here lives outside this repo.

What is no longer blocked at the backend layer:

- backend playback proof
- stale-link route recovery proof
- preferred-client playback proof

What still depends on external frontend checkout and operator environment:

- end-to-end validation against the current `Triven_frontend` implementation
- synchronized frontend auth/BFF environment and playback client setup

The recommended local stack is documented in [`LOCAL_DOCKER_STACK.md`](LOCAL_DOCKER_STACK.md) and implemented in [`docker-compose.local.yml`](../docker-compose.local.yml).

That local stack now also provisions isolated real Plex at `http://localhost:32401/web` and real Emby at `http://localhost:8097` on top of the same mounted media tree, so local media-server parity testing is no longer blocked on environment bring-up.

Inline summary of that stack:

- PostgreSQL
- Redis
- the `filmu-python` backend container
- the current frontend container
- matching `FILMU_PY_API_KEY` and `BACKEND_API_KEY` values

---

## What the frontend currently depends on most

High-frequency frontend backend dependencies found in the current frontend include:

### Already available in `filmu-python`

- [`/api/v1/settings/get/{paths}`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/settings.py:179)
- [`/api/v1/settings/schema/keys`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/settings.py:130)
- [`/api/v1/settings/set/{paths}`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/settings.py:212)
- [`/api/v1/settings`](../filmu_py/api/routes/settings.py)
- [`/api/v1/stats`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/default.py:44)
- [`/api/v1/services`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/default.py:44)
- [`/api/v1/downloader_user_info`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/default.py:44)
- [`/api/v1/items`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/items.py:1)
- [`/api/v1/items/add`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/items.py:1)
- [`/api/v1/items/{id}`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/items.py:1)
- [`/api/v1/items/reset`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/items.py:1)
- [`/api/v1/items/retry`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/items.py:1)
- [`/api/v1/items/remove`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/items.py:1)
- [`/api/v1/calendar`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/default.py:44)
- [`/api/v1/generateapikey`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/default.py:44)
- [`/api/v1/scrape/auto`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/scrape.py:1)

Settings and key-rotation behavior now also have these backend guarantees:

- full settings payloads can be persisted and reloaded across backend restarts
- the runtime settings object is hydrated from persisted compatibility JSON during startup when a row exists
- API key rotation is now real on the backend side, so the frontend/BFF `BACKEND_API_KEY` must be updated and the frontend server restarted before the next protected call after rotation
- `/api/v1/scrape/auto` now also creates missing items from external ids plus optional `requested_seasons` / `requested_episodes` before enqueueing the real scrape worker path, which keeps the current broken TV-request routing working without frontend changes
- `/api/v1/items` and `/api/v1/items/{id}` now also expose additive retry-cooldown metadata (`next_retry_at`, `recovery_attempt_count`, `is_in_cooldown`) for future UI/readiness work without changing the legacy lifecycle state strings
- [`/api/v1/scrape`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/scrape.py:1)
- [`/api/v1/scrape/start_session`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/scrape.py:1)
- [`/api/v1/scrape/session/{session_id}`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/scrape.py:1)
- [`/api/v1/triven/item/{id}`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/triven.py:1)
- [`/api/v1/logs`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/default.py:44)
- [`/api/v1/stream/event_types`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/stream.py:40)
- [`/api/v1/stream/{event_type}`](Triven_backend - ts/apps/riven-python/filmu_py/api/routes/stream.py:50)

### Still required by the current frontend or still dependent on external validation context

The frontend still depends on playback and orchestration surfaces that either remain active hardening areas or still require validation in the external frontend environment, including:

- continued mounted/VFS and playback hardening beyond the already-proven backend baseline
- end-to-end current-frontend playback validation against the current external frontend checkout and environment

The first local run also confirmed a remaining non-playback realism gap even after the `items/add` route was added:

- requested items can now be created and then resolved by external TMDB/TVDB-backed detail lookups
- those items can now also progress through real scrape/debrid stages when runtime scraper/downloader settings and credentials are configured, but local realism still depends on those live integrations

These appear in the current frontend route/server code and OpenAPI client usage, for example in:

- [`dashboard/+page.server.ts`](<../../../Triven_frontend/src/routes/(protected)/dashboard/+page.server.ts>)
- [`library/+page.server.ts`](<../../../Triven_frontend/src/routes/(protected)/library/+page.server.ts>)
- [`library/library.remote.ts`](<../../../Triven_frontend/src/routes/(protected)/library/library.remote.ts>)
- [`calendar/+page.server.ts`](<../../../Triven_frontend/src/routes/(protected)/calendar/+page.server.ts>)
- [`api/settings/regenerate-apikey/+server.ts`](<../../../Triven_frontend/src/routes/(protected)/api/settings/regenerate-apikey/+server.ts>)
- [`lib/providers/riven.ts`](../../../Triven_frontend/src/lib/providers/riven.ts)

---

## What can be tested locally now

## 1. Settings flows

You can already test:

- settings schema loading
- settings tab hydration
- settings path reads
- settings path writes
- library profiles persistence through the current settings-based path

This is the strongest currently testable frontend/backend integration area.

## 2. Logs and notifications transport plumbing

You can already test:

- frontend SSE proxy connection behavior
- `/api/logs` proxy path to backend logs stream
- `/api/notifications` proxy path to backend notifications stream
- historical log fetch through `/api/v1/logs`

This validates transport and backend compatibility shape, even if the total notification feature set is still minimal.

## 3. GraphQL and plugin work in isolation

You can already test backend GraphQL/plugin behavior directly against `filmu-python`, especially:

- plugin discovery
- settings-root GraphQL extensions
- GraphQL query shape and schema composition

---

## What is blocked from full local frontend testing

## 1. Real playback parity

Current backend playback state:

- `/api/v1/stream/file/{item_id}` now has a hardened metadata-driven direct-play baseline with better priority for explicit active/selected playback metadata and a richer attachment abstraction for local files, remote links, remote HLS playlists, and future debrid-services-backed attachments
- `/api/v1/stream/hls/{item_id}/*` can now also use a resolved `remote-direct` winner as an ffmpeg transcode input when no explicit HLS playlist or local file winner exists, closing one earlier unsupported-transcode seam in the HLS decision path
- the playback service now also prefers persisted `media_entries` + `active_stream` selection when those domain rows exist, so HTTP playback no longer depends only on attachment ranking or metadata heuristics once the durable selection slice is populated
- persisted media-entry lease state now also influences playback resolution, so stale or expired selected entries can fall back to their persisted restricted URL while the system moves toward real lease-refresh orchestration
- degraded restricted-link fallbacks now also lose to ready local-file or non-degraded direct candidates, so `/api/v1/stream/file/{item_id}` is less likely to pin direct playback to a stale restricted path when a stronger direct source already exists in the same persisted selection set
- provider-backed unrestricted direct URLs now also outrank otherwise-generic direct URLs, so direct playback is more likely to stay on the richer provider-resolved source when multiple usable direct candidates coexist
- provider-backed direct ranking now also considers lease freshness and preserved provider-native identity while still keeping ready local files ahead of remote direct links, so the direct route is less likely to choose the shorter-lived provider-backed URL when a fresher provider-backed direct candidate already exists
- selected degraded direct media entries can now also recover to sibling direct media entries that share the same persisted provider file identity, so active-stream selection is less brittle when one entry for the same provider-backed file has gone stale before another sibling entry
- a still-usable selected direct `active_stream` remains authoritative over fresher sibling entries for the same provider-backed file, so the new sibling recovery logic does not silently override an intentional active selection unless that selected row is degraded, missing, or invalid
- when no usable direct `active_stream` winner exists, same-file provider-backed direct siblings now collapse by `provider_file_id`/`provider_file_path` before ranking, while different-file groups remain separate; this reduces duplicate same-file competition without silently conflating distinct provider-backed files
- among those remaining different-file provider-backed direct candidates, the service now prefers richer preserved provider identity first and lease freshness second, so fallback ordering is no longer dependent on incidental row order when several distinct provider-backed files remain viable
- provider-backed lease refresh orchestration now exists at the playback-service layer for persisted `media_entries`, so the backend has its first durable seam for refreshing selected playback leases without dropping back to attachment-only mutation
- selected failed leases now fail closed with explicit `503` behavior instead of silently degrading, which gives the backend a first intentional playback-risk policy above the durable lease model
- local HLS generation now also has explicit timeout/failure cleanup behavior in the shared serving layer, reducing one class of partial generated-playlist leakage even though full production HLS governance is still incomplete
- generated-local HLS cache reuse is now also source-aware, so a cached HLS directory is regenerated when the effective input source changes instead of being silently reused across different winners for the same item id
- generated local HLS playlists now also verify that all referenced child files still exist before cache reuse, and generated child-file serving now rejects unreferenced files under the output directory, which reduces stale or stray generated-HLS artifact exposure
- generated local HLS playlists now also fail explicit structural validation when they are empty, malformed, or segmentless, and the serving layer now tracks manifest-invalid / manifest-regenerated governance counters instead of silently reusing that state
- `/api/v1/stream/status` now also exposes those malformed-manifest counters directly, and malformed generated-local playlists now surface through the HLS routes as the same simplified `503` risk response used for other local-generation failures
- `/api/v1/stream/status` now also exposes additive normalized HLS failure-reason counters, so timeout, lease-failed, malformed-manifest, and similar route-level HLS risks are visible to operators even though the client contract still intentionally collapses them to `503`
- `/api/v1/stream/status` now also distinguishes missing generated child files and upstream remote playlist/segment failures in that HLS route failure taxonomy, so operator visibility is broader than the simplified client-facing route contract
- upstream remote HLS playlists are now also structurally validated before rewrite/proxy handoff, so obviously empty or segmentless upstream payloads fail clearly as upstream-playlist defects instead of being forwarded as if they were valid playback manifests
- upstream remote HLS playlist fetches and segment proxy opens now also fail with explicit `504` / `502` transport policy instead of leaking lower-level client exceptions, which makes the remote-HLS path more predictable for operators and the frontend BFF
- backend-side regression coverage now also mirrors the current frontend BFF direct-play header-forwarding contract and the player HLS query-parameter pattern, so the Python backend is less likely to drift away from the currently shipped frontend playback path even before full browser/BFF end-to-end validation is run
- HLS playlist responses now also declare `Cache-Control: no-store` while HLS child files/segments declare `Cache-Control: public, max-age=3600`, which makes the current VOD-oriented freshness policy explicit and aligned with the frontend proxy assumptions
- remote-HLS playlist fetches and segment proxy opens now also retry one transient timeout/transport failure before surfacing the error, and repeated transient failures for the same upstream playlist URL now enter a short cooldown with `Retry-After`, which makes the remote-HLS path less brittle under brief upstream instability without changing the basic client contract
- direct-play selection now also preserves extraction-time source authority inside the shared selector, so explicit active/selected direct sources remain authoritative over generic top-level fallback metadata while ready local files still beat remote direct links when source authority is otherwise equal
- direct-play selection now also has a named source-class layer in the shared resolver, so selected-local, selected-provider-direct, fallback-local, fallback-provider-direct, and degraded restricted fallback cases are explicit categories rather than only implicit tuple ordering
- direct-play selection now also has explicit provider-link health-state classes in that shared resolver, so ready, stale, refreshing, failed, and degraded provider-backed direct links are no longer collapsed into generic remote-direct behavior when the selector decides among direct-play candidates
- direct-play resolution now also has an explicit service-layer serve/fail decision seam with additive refresh-intent metadata, so the current `404`/`503` route contract is no longer only an emergent side effect of scattered helper branches
- direct-play resolution now also emits an explicit internal refresh-recommendation payload for stale/refreshing/degraded provider-backed direct cases, so future orchestration can attach to a named recommendation model instead of inferring refresh targets only from final route outcomes
- direct-play resolution now also translates those refresh recommendations into the existing media-entry or persisted-attachment refresh-request models inside the playback service, so later control-plane execution work now has a cleaner handoff seam than route-level outcome inference alone
- direct-play resolution now also has one-shot service-level execution helpers for those translated refresh dispatches, so provider-backed refresh execution can now be exercised deliberately outside the HTTP request path without yet turning `/api/v1/stream/file/*` into a blocking orchestration endpoint
- those one-shot direct-play refresh executions are now also provider-rate-limit aware, so the playback service can fail fast with explicit retry-after/backpressure metadata instead of blindly attempting every provider refresh under pressure
- the playback service now also has a small background scheduling seam above those one-shot direct-play refresh executions, so later control-plane callers can schedule refresh work without blocking `/api/v1/stream/file/*` and can carry limiter `retry_after_seconds` forward as explicit run-later guidance
- the backend now also has a small in-process control-plane caller above that scheduling seam, so service-layer code can trigger deduplicated background direct-play refresh work immediately without adding ARQ or event-backplane wiring yet
- that in-process caller is now also attached at the app-resource/runtime boundary, so later route-adjacent work can reuse one app-scoped controller instance without first introducing HTTP-surface coupling or broader orchestration infrastructure
- the playback layer now also exposes a small helper above that app-scoped controller attachment, so later integration points can trigger app-scoped background direct-play refresh work through a named service-boundary helper rather than reaching into app resources ad hoc
- `/api/v1/stream/file/{item_id}` now also starts that app-scoped direct-play refresh trigger opportunistically for remote direct winners and selected failed direct leases, while still keeping the HTTP response path non-blocking with respect to provider refresh execution
- that direct-play trigger path now also suppresses duplicate route-trigger task creation when refresh work is already pending for the same item and exposes additive provider-pressure/backoff governance through `/api/v1/stream/status`, which gives operators more direct visibility into limiter/cooldown pressure without widening the route contract
- `/api/v1/stream/file/{item_id}` now also preserves a stable inline `Content-Disposition` filename when the resolved direct-play attachment knows its filename, and it still preserves upstream-provided `content-disposition` headers for proxied remote direct responses when those already exist
- the playback service now also resolves a small internal direct-file link-resolution model from the explicit direct-play decision seam before building the route-facing serving descriptor, so filename/provenance logic is no longer coupled directly to route-local attachment handling or raw attachment-shape classification even though the HTTP surface stays unchanged
- that internal direct-file provenance now also carries a debrid-first lifecycle snapshot derived from already-persisted attachment/media-entry fields such as owner kind, provider family, locator source, fallback state, and persisted refresh/error metadata, while the route still consumes only the descriptor subset it already used
- that same lifecycle model now also projects onto the internal resolved direct/HLS playback snapshot used by adjacent playback and future VFS-facing read paths, while the current public playback and item-detail route shapes remain unchanged
- the backend now also has a small in-process controller and app-resource helper for selected failed HLS lease refresh work, so the HLS route family can attach to the durable media-entry lease refresh seam without first broadening into ARQ or a larger event/control-plane design
- `/api/v1/stream/hls/{item_id}/index.m3u8` and `/api/v1/stream/hls/{item_id}/{file_path}` now also start that app-scoped HLS failed-lease refresh trigger when the selected HLS lease fails closed with `503`, while still keeping the HLS response path non-blocking with respect to provider refresh execution
- that narrow HLS trigger path now also suppresses duplicate route-trigger task creation when failed-lease refresh work is already pending and exposes additive HLS failed-lease trigger-pressure governance through `/api/v1/stream/status`, which gives operators a first route-surface view of repeated HLS failed-lease backoff pressure beyond raw limiter metrics alone
- the backend now also has a second small in-process controller and app-resource helper for selected stale or refreshing HLS media-entry leases that currently degrade to a remote-HLS restricted fallback, so the HLS route family can attach to that winner state without generalizing the existing failed-lease controller
- `/api/v1/stream/hls/{item_id}/index.m3u8` and `/api/v1/stream/hls/{item_id}/{file_path}` now also start that second app-scoped HLS trigger only when the resolved HLS source carries `source_key == "media-entry:restricted-fallback"`, which keeps the route-level predicate narrow and avoids a second service-layer resolution pass
- that restricted-fallback HLS trigger path now also suppresses duplicate route-trigger task creation when the same fallback refresh is already pending and exposes additive parallel governance through `/api/v1/stream/status`, which gives operators a separate view of stale/refreshing HLS backoff pressure alongside the existing failed-lease counters
- the route layer now also collapses HLS generation and lease-risk failures into a simpler client-facing `503` policy while still keeping true missing generated child files as `404`
- `/api/v1/stream/hls/{item_id}/index.m3u8` and downstream HLS file requests now support a partial baseline for local file-backed generation and for upstream HLS playlist proxying when item metadata already exposes a playlist URL

Blocked by the remaining implementation gaps:

- end-to-end validation of the current frontend playback flow against the now-production-governed HLS surface
- broader route-level direct-play/HLS playback-risk handling and governance beyond the new simplified HLS `503` mapping baseline plus degraded-direct fallback awareness, including whether to widen HLS trigger policy beyond the now-implemented selected failed-lease and selected restricted-fallback slices
- deeper direct-file source and link abstraction beyond the new filename-preserving serving contract, especially broader provider/link lifecycle projection across real VFS open/read path consumers and clearer cross-service provenance boundaries
- the broader unfinished link-resolver abstraction beyond the new decision-backed internal direct-file link-resolution model plus lifecycle projection on resolved snapshots, especially once that internal provenance needs to drive true VFS-facing resolution rather than only HTTP-serving synthesis
- the broader unfinished FilmuVFS/stream implementation plan in [`VFS.md`](VFS.md).

## 2. Public watch/media-type redirect alias

This is now available via `/api/v1/triven/item/{id}`.

## 3. Deeper manual scrape orchestration

Current scrape/manual-scrape routes are still compatibility-oriented rather than full parity flows, but they are no longer purely synthetic:

- [`/api/v1/scrape/auto`](../filmu_py/api/routes/scrape.py) now reuses the real [`request_item()`](../filmu_py/services/media.py) + [`scrape_item()`](../filmu_py/workers/tasks.py) path for missing items, including partial TV requests with selected seasons/episodes
- [`/api/v1/scrape/start_session`](../filmu_py/api/routes/scrape.py) now also enqueues the real [`scrape_item()`](../filmu_py/workers/tasks.py) worker path
- [`/api/v1/scrape/session/{session_id}`](../filmu_py/api/routes/scrape.py) now polls persisted item lifecycle state
- completing a manual session now persists parsed stream candidates for the selected filenames

They still lack:

- downloader-backed torrent/container inspection
- durable session storage beyond the current in-memory compatibility record
- real attribute-application behavior
- real stream-result surfacing from [`GET /api/v1/scrape`](../filmu_py/api/routes/scrape.py)
- deeper manual-session parity with the full downloader/orchestration model

---

## Readiness thresholds

## Threshold A - Limited local integration testing (**available now**)

You can start now if your goal is:

- settings page validation
- logs/notifications stream validation
- backend contract debugging
- GraphQL/plugin development validation

## Threshold B - Meaningful current-frontend workflow testing (**available now**)

This threshold is now reached for the current compatibility slices covering:

1. dashboard
2. library list/detail/action flows
3. calendar
4. settings + API key regeneration
5. logs/notifications SSE
6. scrape auto/manual compatibility baselines

## Threshold C - Full current frontend playback validation

This now requires the above plus external frontend/environment alignment:

1. the external `Triven_frontend` checkout and its protected-route/BFF environment
2. aligned `BACKEND_URL` / `BACKEND_API_KEY` configuration
3. end-to-end validation of the current frontend playback flow against the Python backend

That is the point where the current frontend can be tested end-to-end against the Python backend for realistic playback flows.

---

## Recommended next implementation order for local frontend validation

1. Run end-to-end validation of the current external frontend against the now-proven backend playback surface

2. Continue deeper mounted playback/VFS hardening and observability based on that validation

3. Continue deeper manual scrape orchestration and downloader-backed session semantics where product behavior still depends on it

This sequence reflects the actual remaining gaps and gives the fastest path to useful local frontend testing while preserving the backend-as-motor architecture.

---

## Bottom line

You can start local frontend + python-backend testing **now** with a **meaningful current-workflow slice**.

If the goal is full local validation of the current frontend, the backend no longer appears blocked on first playback completion; the remaining gap is reproducing and validating that flow against the external frontend environment while continuing the current playback/VFS hardening track.

The remaining major blocker to broader product validation is now **external current-frontend validation and continued playback/VFS hardening**, followed by deeper scrape/downloader realism.
