# Next Implementation Priorities

## Purpose

This is the current high-signal next-steps list for `filmu-python` after the latest audit of:

- current source implementation
- the original TS `riven` backend
- current frontend readiness needs
- FilmuVFS-first architecture requirements

This document is intentionally short and prioritized.

## Strategic quality bar

These priorities should now be read together with [`ENTERPRISE_GRADE_GAP_MATRIX.md`](ENTERPRISE_GRADE_GAP_MATRIX.md).

Filmu is expected to become **enterprise-grade and state-of-the-art across all major areas**, not just locally parity-clean with current `riven-ts`.

---

## Priority 1 - Close the remaining playback parity gap

Detailed playback/readiness breakdown: [`LOCAL_FRONTEND_TESTING_READINESS.md`](../LOCAL_FRONTEND_TESTING_READINESS.md) and [`PHASE_C_ROUTE_SURFACE_MATRIX.md`](PHASE_C_ROUTE_SURFACE_MATRIX.md).

The active next step is hardening the now-real playback surface for the current frontend:

1. keep the merged GitHub-hosted playback workflow green on real PR traffic and verify the live protected-branch policy with [`../scripts/check_github_main_policy.ps1`](../scripts/check_github_main_policy.ps1) from an admin-authenticated host; do not treat repo-settings state as proven from an unauthenticated workspace
   - the stricter policy profile is now explicit through `proof:playback:policy:enterprise` and `proof:playback:policy:enterprise:validate`, which add minimum-review and admin-enforcement expectations plus explicit provider/Windows proof-profile expectations to the canonical policy printout/validation path
2. keep the GitHub-hosted Linux runner prerequisites, external frontend checkout variables, and media-server secrets intentionally configured for that workflow (`FILMU_FRONTEND_REPOSITORY` when overriding the default frontend checkout, plus `PLEX_TOKEN` / `EMBY_API_KEY` when provider parity should auto-run), and validate them with [`../scripts/check_playback_gate_runner.ps1`](../scripts/check_playback_gate_runner.ps1); Windows dev hosts are expected to fail the Linux `/dev/fuse` prerequisite
3. continue using the optional Plex-compatible stub proof, the real Jellyfin/frontend proof stack, the native Windows WinFSP gate, the now-working Docker Plex path, and the local Emby container as regression layers beneath the merged workflow
4. keep the newly explicit Docker Plex proof checks green in repeated runs: host-binary freshness, entry-id refresh-identity visibility, and foreground-fetch/coalescing visibility in the proof artifact bundle
5. keep the new thresholded Windows soak profiles green in repeated operator runs: `continuous`, `seek`, `concurrent`, and `full` now exist on top of [`../scripts/run_windows_vfs_soak.ps1`](../scripts/run_windows_vfs_soak.ps1), [`../scripts/run_windows_vfs_soak_stability.ps1`](../scripts/run_windows_vfs_soak_stability.ps1) now aggregates repeated runs across named environment classes, they now also capture backend `/api/v1/stream/status` VFS governance snapshots plus `backend_vfs_governance_delta`, live/peak prefetch-depth, and chunk-coalescing evidence when the backend is reachable, and the next step is repeatability across more than one environment class rather than inventing another soak harness
   - the stricter local wrapper now also exists as `proof:windows:vfs:soak:enterprise`, requiring runtime-status capture, backend-status capture, and zero tolerated reconnect/provider-pressure/fatal incidents for the selected repeated profile set
1. finish promoting the now-green playback harnesses into real playback-path merge gates, starting from the new GitHub-hosted CI workflow in [`.github/workflows/playback-gate.yml`](../../.github/workflows/playback-gate.yml), [`../package.json`](../package.json) `proof:playback:gate`, [`../package.json`](../package.json) `proof:playback:providers:gate`, and the wrappers under [`../scripts/`](../scripts/)
2. provision the GitHub-hosted Linux runner prerequisites, external frontend checkout variables, and media-server secrets for that workflow (`FILMU_FRONTEND_REPOSITORY`, `PLEX_TOKEN`, `EMBY_API_KEY`), validate them with [`../scripts/check_playback_gate_runner.ps1`](../scripts/check_playback_gate_runner.ps1), validate the exact GitHub `main` policy with [`../scripts/check_github_main_policy.ps1`](../scripts/check_github_main_policy.ps1), then make the gates required for playback-path changes; Windows dev hosts are expected to fail the Linux `/dev/fuse` prerequisite
3. continue using the optional Plex-compatible stub proof, the real Jellyfin/frontend proof stack, the native Windows WinFSP gate, the now-working Docker Plex path, and the local Emby container as regression layers beneath the enforced gate
4. keep the newly explicit Docker Plex proof checks green in repeated runs: host-binary freshness, entry-id refresh-identity visibility, and foreground-fetch/coalescing visibility in the proof artifact bundle
5. keep the new thresholded Windows soak profiles green in repeated operator runs: `continuous`, `seek`, `concurrent`, and `full` now exist on top of [`../scripts/run_windows_vfs_soak.ps1`](../scripts/run_windows_vfs_soak.ps1), [`../scripts/run_windows_vfs_soak_stability.ps1`](../scripts/run_windows_vfs_soak_stability.ps1) now aggregates repeated runs across named environment classes, they now also capture backend `/api/v1/stream/status` VFS governance snapshots plus `backend_vfs_governance_delta`, live/peak prefetch-depth, and chunk-coalescing evidence when the backend is reachable, and the next step is repeatability across more than one environment class rather than inventing another soak harness
6. continue hardening HLS/direct-play governance beyond the new production-grade HLS baseline, especially policy tuning, operator ergonomics, and any remaining control-plane refinements
7. continue hardening direct-play governance beyond the now-shipped shared chunk-engine HTTP path, degraded-direct fallback awareness, provider-backed direct-source ranking, lease-freshness-aware provider-backed ordering, related-entry recovery via provider file identity, explicit active-stream-winner authority rules, same-file sibling collapse for non-active direct entries, richer-identity-first different-file fallback ordering, generated-local HLS cache/reference integrity checks, explicit malformed-manifest governance signals, route-level malformed-manifest observability/signaling, normalized HLS failure-reason counters, generated-missing/upstream-failure HLS route visibility, upstream-playlist structural validation, and explicit remote-HLS timeout/transport failure policy

Why first:

- Full current-frontend local playback proof is now present, the merged GitHub-hosted workflow has already gone green on the last playback PR, and the next risk is drift or policy mismatch rather than missing workflow wiring.
- Local Plex and Emby containers are now provisioned in Compose, native Windows Jellyfin plus Emby are already validated on `C:\FilmuCoreVFS`, the isolated Docker Plex path reran green through repeatable proof coverage against the shared WSL host mount on April 9, 2026, and the repo now has a dedicated native Windows provider gate for Jellyfin/Emby/Plex in [`../scripts/run_windows_media_server_gate.ps1`](../scripts/run_windows_media_server_gate.ps1). Native Windows Plex is now green too, so the next parity step is no longer bring-up; it is keeping the now-explicit Docker evidence checks and the native Windows provider gate green while promoting both into CI/merge policy.
- It aligns with the backend-as-motor principle without reopening already-delivered Phase C breadth.
- The playback harness now has live-green preferred-client proof plus repeated green local runs, the selected direct stale-link repair now persists refreshed leases across session boundaries, selected media-entry-backed remote-HLS winners can now also self-heal once inline on real upstream playlist/segment failure, that inline repair path is now explicitly visible on `/api/v1/stream/status`, and the GitHub-hosted CI workflow now fail-fast validates runner readiness, conditionally runs the provider gate, uploads artifacts, and covers `main`/`merge_group` traffic. The fastest next step is therefore repeated stability evidence plus explicit validation of live GitHub policy state, not more harness design.
- The playback harness now has live-green preferred-client proof plus repeated green local runs, the selected direct stale-link repair now persists refreshed leases across session boundaries, selected media-entry-backed remote-HLS winners can now also self-heal once inline on real upstream playlist/segment failure, that inline repair path is now explicitly visible on `/api/v1/stream/status`, and the GitHub-hosted CI workflow now fail-fast validates runner readiness, enforces the provider gate, uploads artifacts, and covers `main`/`merge_group` traffic. The fastest next step is therefore runner provisioning plus branch-protection / merge-policy promotion rather than more harness design.
- Mounted/VFS operator visibility is also no longer split between the Python bridge and sidecar-local artifacts: `/api/v1/stream/status` now reports FilmuVFS catalog watch-session churn, reconnect-delta behavior, supplier failure classes, provider-backed refresh outcomes, explicit inline refresh request outcomes from the gRPC bridge, and additive `vfs_runtime_*` counters ingested from the Rust sidecar snapshot, so the next observability step is failure-taxonomy refinement and repeated soak evidence rather than first cross-process status exposure.
- The Rust sidecar still emits a structured `filmuvfs-runtime-status.json` snapshot under the managed Windows-native state directory, and the soak/status scripts still consume it directly. With the Python API now ingesting the same file, the soak gate now using runtime-derived checks for upstream provider pressure, cold-fetch churn, unrecovered stale reads, fatal mounted-read failures, live prefetch scheduler depth, and chunk-coalescing wait behavior, the next mounted observability step is no longer first convergence between Rust and Python status vocabularies, first threshold attachment, first startup-latency exposure, or first coalescing/prefetch-depth surfacing; it is repeated real-environment proof plus any remaining active-stream/data-plane classes that still cannot be diagnosed from the current snapshots alone.
- Backend HTTP fallback behavior is no longer one of those remaining blind spots either: the mounted runtime now reports fallback attempts/success/failure by reason through `vfs_runtime_backend_fallback_*`, so the next mounted observability step is narrower still: repeated live soak evidence under sustained load, threshold tuning against the richer runtime surfaces, and only then any additional data-plane counters that remain genuinely missing.

---

## Priority 2 — Expand the item/domain model beyond the current minimal media-state baseline — ✅ Largely Complete

Detailed model-expansion breakdown: [`DOMAIN_MODEL_EXPANSION_MATRIX.md`](DOMAIN_MODEL_EXPANSION_MATRIX.md).

The Python persistence layer now includes 15+ ORM models covering:

- ✅ `ItemRequestORM` — separate request intent from media lifecycle
- ✅ `MovieORM`/`ShowORM`/`SeasonORM`/`EpisodeORM` — media specialization hierarchy
- ✅ `StreamORM`/`StreamBlacklistRelationORM`/`StreamRelationORM` — stream candidate graph
- ✅ `MediaEntryORM` + `ActiveStreamORM` — file-level identity and active stream ownership
- ✅ `OutboxEventORM` — transactional outbox
- ✅ Stats and calendar projections backed by intentional domain models

Remaining domain gaps:

- none at the planned entity level; remaining work is read-model depth and richer projection consumption

---

## Priority 3 — Strengthen orchestration breadth without copying TS complexity — ✅ Largely Complete

Detailed orchestration breakdown: [`ORCHESTRATION_BREADTH_MATRIX.md`](ORCHESTRATION_BREADTH_MATRIX.md).

The following orchestration semantics are now implemented:

- ✅ `parse_scrape_results` — dedicated parse stage
- ✅ `rank_streams` — RTN-compatible ranking stage
- ✅ `select_stream_candidate` — container selection
- ✅ `debrid_item` — provider-backed download execution
- ✅ `recover_incomplete_library` — stage-aware retry-library recovery
- ✅ `publish_outbox_events` — transactional outbox drain

Remaining orchestration gaps:

- massive-torrent download performance optimization (current TS baseline now centers on `find-valid-torrent` plus sandboxed `map-items-to-files` / `validate-torrent-files` breadth) - see `RIVEN_TS_RUST_VFS_BRANCH_AUDIT.md` for more details on upstream orchestration
- richer request-intake provenance and policy beyond the now-working partial-request compatibility path (`items/add`, Overseerr intake, and the `scrape/auto` upsert bridge all create tenant-aware request-intent rows, but source attribution and quota policy are still intentionally light)
- stronger stage-idempotency, enqueue-dedup, and queue-lag/operator visibility across the broader queue graph
- add more orchestration stages only where they improve correctness and recovery breadth (see `ORCHESTRATION_BREADTH_MATRIX.md` for more details)

---

## Priority 4 — Deepen the real plugin platform beyond the new runtime baseline

Detailed plugin-platform breakdown: [`PLUGIN_CAPABILITY_MODEL_MATRIX.md`](PLUGIN_CAPABILITY_MODEL_MATRIX.md).

Current plugin support now includes packaged entry-point discovery, plugin-scoped settings, datasource-aware `PluginContext` construction, runtime capability registration across scraper/downloader/indexer/content-service/notification/event-hook capabilities, namespaced publishable-event governance, built-in Torrentio plus real MDBList/webhook-notification/StremThru integrations, runtime visibility through `/api/v1/plugins` plus `/api/v1/plugins/events`, and an explicit trust/policy baseline for `publisher`, `release_channel`, `trust_level`, and `permission_scopes`.
Current plugin support now also includes provenance/isolation metadata (`source_sha256`, `signature`, `signing_key_id`, `sandbox_profile`, quarantine fields), loader-side digest verification, quarantine refusal, and operator-visible provenance fields on `/api/v1/plugins`.

Next capability layers should include:

- richer external-author packaging/distribution guidance via [`../PLUGIN_DISTRIBUTION_POLICY.md`](../PLUGIN_DISTRIBUTION_POLICY.md)
- ✅ MDBList list sync — real ContentServicePlugin implementation
- ✅ Webhook notifications — real NotificationPlugin + PluginEventHookWorker implementation
- ✅ Overseerr webhook intake at `/api/v1/webhook/overseerr`
- deepen the now-real StremThru integration with stronger operator-facing health/compatibility policy rather than treating it as a remaining stub; the first additive readiness metadata now ships on [`GET /api/v1/plugins`](../../filmu_py/api/routes/default.py)
- cryptographically verify signatures instead of only surfacing `signature` metadata, and add revocation/trust-store policy above the now-landed provenance/quarantine baseline
- decide whether hook execution should remain in-process or grow into a durable/queued model
- continue broadening capabilities only where they improve real platform breadth (see `PLUGIN_CAPABILITY_MODEL_MATRIX.md` for more details)

Why fourth:

- the TS platform’s plugin system is a major competitive advantage today
- Python can beat it only by building a cleaner, more explicit platform model and then deepening policy/distribution/real integrations rather than stopping at the first runtime baseline (see `PLUGIN_CAPABILITY_MODEL_MATRIX.md` for more details)

---

## Priority 5 — Continue the FilmuVFS-first byte-serving platform from the shared substrate and new chunk engine

Detailed FilmuVFS/platform breakdown: [`FILMUVFS_BYTE_SERVING_PLATFORM_MATRIX.md`](FILMUVFS_BYTE_SERVING_PLATFORM_MATRIX.md).
Linux-host validation gate runbook: [`../FILMUVFS_LINUX_HOST_VALIDATION_RUNBOOK.md`](../FILMUVFS_LINUX_HOST_VALIDATION_RUNBOOK.md).

This is not the first local-frontend blocker, but it is strategically crucial.

Recent baseline completed here:

- mounted show output now normalizes to `Show Title (Year)/Season XX/<sanitized source filename>`
- provider-path season inference now handles `S05x08`-style filenames for show-level media entries
- catalog deltas now emit removals when an existing `entry_id` changes visible path, preventing stale root-level mount entries during naming-policy upgrades
- the Rust sidecar now parses mounted media-semantic path metadata (`path_type`, `tmdb_id`/`tvdb_id`/`imdb_id`, `season`, `episode`), carries it on mounted `getattr` / `readdir` / `open` / `read` surfaces, and resolves mounted semantic aliases like external-ref show folders plus season/episode names onto canonical catalog paths
- the Rust sidecar now also surfaces those semantic aliases as discoverable mounted directory entries (`tvdb-*`, `tmdb-*`, `Season 01`, `Episode 01.mkv`) instead of keeping them as resolution-only affordances
- the Rust sidecar now also deduplicates concurrent inline stale-link refreshes per catalog entry and reuses already-refreshed catalog URLs for later mounted stale reads instead of fanning out duplicate refresh RPCs
- the Rust sidecar now also enforces a first explicit per-handle background-prefetch fairness cap, and the runtime snapshot exposes that operator policy alongside live prefetch depth so multi-reader pressure is diagnosable instead of implicit

Build toward:

1. integrate the implemented shared chunk/range planning and caching engine across mounted FilmuVFS reads
2. keep deepening the optional disk/persistent cache and smarter prefetch policy above the now-hardened Rust cache/control plane; the new per-handle background-prefetch fairness cap is the first explicit operator policy, not the end state
3. deepen mounted `open`/`read`/`readdir`/`getattr`/`release` behavior for long-running operational behavior, soak testing, and backpressure handling
4. keep the WatchCatalog/runtime and stale-link refresh story hardened now that reconnect deltas and inline refresh are in place
5. deeper HLS and serving-lifecycle governance
6. mounted data-plane metrics and cross-process VFS observability
7. research and implement more VFS capabilities to improve the backend's VFS breadth (see `FILMUVFS_BYTE_SERVING_PLATFORM_MATRIX.md` for more details)

### Priority 5 operational hardening criteria

The active FilmuVFS/playback question is no longer "can it work once?".
It is now "under what measured conditions is it stable enough to trust?".

Recent Windows-native update:

- `proof:windows:vfs:gate` is now green on `C:\FilmuCoreVFS`
- native Jellyfin playback is proven on WinFSP
- native Emby playback/probe/stream-open checks now pass through the current provider proof on `C:\FilmuCoreVFS`
- in-flight foreground chunk-fetch coalescing in [`../rust/filmuvfs/src/chunk_engine.rs`](../rust/filmuvfs/src/chunk_engine.rs) reduced duplicate upstream fetches and improved Emby buffering
- the Docker Plex parity path is now working on the Linux/WSL topology after the WSL host-mount visibility and refresh-collision fixes, and the direct provider gate reran green on April 9, 2026
- the next Plex step is no longer first playback bring-up; it is keeping the now-explicit Docker evidence checks green while promoting the gates into CI/merge policy, and only then adding separate native Windows PMS evidence if available

The current hardening slice should therefore be driven by explicit validation scenarios:

1. **Continuous playback soak**
   - one mounted playback session
   - 60 minutes minimum
   - no mount crash, sidecar restart, or unrecovered stale-read failure
2. **Interactive seek soak**
   - one mounted playback session with repeated forward/backward seeks
   - 15 minutes minimum
   - no broken handle state, no stuck read loop, no stale path that requires manual recovery
3. **Concurrent read/backpressure soak**
   - at least 3 concurrent mounted readers against the same stack
   - 15 minutes minimum
   - no unbounded reconnect churn, no runaway cache growth beyond configured limits, no worker/backend collapse under read pressure

Target pass/fail thresholds for this slice:

- mounted-read fatal error rate: `0` during the defined soak runs
- sidecar crash/restart count: `0` during the defined soak runs
- stale-link recovery: successful recovery should complete without user-visible playback termination whenever the provider refresh succeeds
- reconnect churn after steady state: no repeating reconnect loop and no more than one isolated reconnect incident per soak run without subsequent instability
- cache/backpressure behavior: memory/disk cache behavior must stay within configured bounds and must not degrade into repeated unbounded miss/re-fetch loops during sequential playback

Operator-facing failure classes to track explicitly:

- stale-link refresh could not recover
- upstream transport timeout / open failure
- mount reconnect churn
- cache churn / repeated cold re-fetch behavior
- limiter / provider backpressure denial
- HLS generation or remote-HLS governance failure

Readiness rubric for the mounted data plane:

- **Experimental**
  - single proof runs work
  - known reconnect noise or recovery gaps remain
  - soak criteria are not yet consistently green
- **Local-stable**
  - all three soak scenarios pass locally on at least one maintainer machine
  - no fatal mounted-read failures appear in those runs
  - failure classes are visible in metrics/logs/status surfaces without ad hoc debugging
- **Rollout-ready**
  - soak scenarios are repeatable across more than one environment class
  - reconnect churn and stale-refresh behavior stay within the thresholds above
  - mounted metrics and cross-process signals are sufficient for operators to diagnose failures without code inspection
  - the HTTP playback path and mounted VFS path tell a consistent operational story

The immediate next FilmuVFS work should be judged against this rubric instead of adding new VFS breadth by default.

Why fifth instead of first:

- VFS is a product differentiator and long-term requirement
- but current frontend route breadth is the faster local-enablement win
- the VFS implementation should be deliberate, not rushed
- VFS should be implemented in a way that is compatible with the current frontend via a dual compatibility model while also extending the backend's VFS breadth for a future rewriting of the frontend to consume the GraphQL VFS API (see `FILMUVFS_BYTE_SERVING_PLATFORM_MATRIX.md` for more details)

---

## Priority 6 — Keep observability first-class while the system grows

Detailed observability breakdown: [`OBSERVABILITY_MATURITY_MATRIX.md`](OBSERVABILITY_MATURITY_MATRIX.md).

After the shipped layer-1 baseline, deepen:

- shipper/search workflow above the now-landed durable structured logs
- queue/backlog/control-plane lag history and alerting are now baseline through `/api/v1/workers/queue`, `/api/v1/workers/queue/history`, and bounded alert classification; the remaining work is broader lag/backlog history depth, replay taxonomy, and stronger operator automation rather than first instrumentation
- mounted stream/VFS data-plane metrics
- correlation across API, workers, plugins, and stream control-plane events

---

## Priority 7 — Turn interim auth context into a real enterprise identity plane

Current implementation now carries additive actor/tenant/role/scope context through authenticated API requests and emits structured audit logs for privileged settings/API-key mutations.

That is not yet a finished enterprise identity system.

Next identity/authz work should include:

- persisted principals, service accounts, and tenant/org models
- OIDC/SSO readiness instead of header-carried operator metadata
- RBAC/ABAC policy enforcement across API, workers, plugins, and VFS control-plane actions
- tenant-aware quotas, audit retention, and operator-visible access policy state

Why continuously:

- this is easiest to maintain if added as surfaces land
- retrofitting observability late is much harder

---

## Practical next sequence

If implementing immediately, the next concrete development sequence should be:

1. ✅ VFS mounted `read()` → chunk engine integration (Slice E)
2. ✅ GraphQL compat subscription layer (Slice E)
3. ✅ Partial season filtering in parse/rank stages (Slice E)
4. ✅ StremThru real DownloaderPlugin implementation (Slice E)

Next:

1. ✅ Adaptive prefetching in Rust (Slice F)
2. ✅ Optional L2 disk cache (Slice F)
3. ✅ Hidden path guard (Slice F)
4. ✅ GraphQL mutations: `requestItem`, `itemAction`, `updateSetting` (Slice F)
5. ✅ Hidden path guard and GraphQL mutation breadth closed the last remaining Slice E follow-ups (Slice F)

Next:

1. ✅ Plugin trust/policy baseline: publisher, release-channel, trust-level, and permission-scope validation plus operator-facing visibility
2. ✅ Durable structured logging baseline: rotating ECS/NDJSON-style file output with correlation filters
3. ✅ Rate-limiter observability baseline: allow/deny/remaining/retry-after metrics by bounded bucket class
4. ✅ Queue/control-plane baseline: `GET /api/v1/workers/queue` plus queue depth/lag/retry/dead-letter gauges
5. ✅ GraphQL operation observability baseline: operation counters and duration histograms by operation type and root field

Next 5 slices after that:

1. Admin-authenticated GitHub branch-policy validation and merge-policy enforcement for the playback gate.
2. Multi-environment Windows/Linux mounted-soak hardening with threshold tuning and stronger mount data-plane diagnostics.
3. Queue-backed resolver/orchestration breadth beyond today’s inline dedup, especially replay-safe lag history and operator alerting.
4. Plugin provenance/signing/sandboxing for non-builtin plugins, including quarantine/revocation policy.
5. Enterprise identity/tenancy/authz foundation so the control plane stops depending on a single shared API key.
1. CI/merge-policy promotion for the now-green `proof:playback:gate` and `proof:playback:providers:gate` surfaces
2. Keep the newly explicit Docker Plex proof checks green in repeated runs: host-binary freshness, entry-id refresh-identity evidence, and foreground-fetch/coalescing evidence in the playback-proof bundle
3. Keep the thresholded Windows soak profiles green and repeatable across maintainers now that they preserve live prefetch-depth and chunk-coalescing evidence, then decide whether one of them should become a formal release gate alongside the shorter existing `proof:windows:vfs:gate`
4. Decide whether FilmuVFS should stop at discoverable alias entries or add a fully separate id-keyed browse tree, and how much broader queue-backed/orchestrated resolver workflow is still needed beyond the new mount-side inline refresh dedup
5. GraphQL rich field expansion — add extended fields to compat subscription types
6. Dedicated index-item worker stage (TMDB enrichment as ARQ stage, not request-time)
7. Plex library scan trigger after item reaches `COMPLETED` state
8. keep native Windows Plex parity green against `C:\FilmuCoreVFS` through repeatable proof reruns and CI promotion

---

## First local frontend/backend run findings

The first real containerized frontend + backend run found three important things:

1. the local infrastructure stack is now healthy enough to exercise real frontend behavior
2. `POST /api/v1/items/add` and TMDB/TVDB-backed detail lookups were immediate contract gaps and have now been implemented
3. the next meaningful blocker has moved back to playback parity plus deeper request/scrape/downloader realism, not basic stack startup or missing request routes

That means the current next slice after this request/detail compatibility fix is still the playback track, with downstream orchestration realism close behind it.

## Scrape pipeline fix update (March 2026)

Three bugs were found and fixed that blocked the end-to-end pipeline from ever running after item requests:

1. **`retry_library` did not pick up `REQUESTED` items** — it only scanned `INDEXED`/`SCRAPED` states, so new items never got a scrape job from the cron. Fixed in [`workers/tasks.py`](../filmu_py/workers/tasks.py).
2. **`POST /api/v1/items/add` never triggered a worker** — items were persisted but no `scrape_item` ARQ job was enqueued. Fixed in [`api/routes/items.py`](../filmu_py/api/routes/items.py) to immediately enqueue a `scrape_item` job when an item is created. Requires `FILMU_PY_ARQ_ENABLED=true`.
3. **VFS proto bindings crashed backend startup in Docker** — `app.py` top-level imported `vfs_server.py` which imports the generated `filmuvfs` protobuf module not installed in the image. Fixed in [`app.py`](../filmu_py/app.py) by making VFS imports lazy/conditional.
4. **The frontend sent brand-new TV requests with selected seasons to `POST /api/v1/scrape/auto`** — missing items now upsert through the shared [`request_item()`](../filmu_py/services/media.py) path before scrape enqueue, so the broken routing no longer returns `404` for that flow.

The Docker stack also required these corrections:

- The worker must use `python -c "from filmu_py.workers.tasks import run_worker_entrypoint; run_worker_entrypoint()"` — `python -m` triggers a `runpy` `sys.modules` conflict that silently exits the process.
- `FILMU_PY_API_KEY` must be exactly 32+ characters (Pydantic validates this at startup).
- The backend healthcheck using `curl -H x-api-key:` inside the compose healthcheck block does not work reliably with env expansion, causing the backend to be marked `unhealthy` and blocking the worker. The backend healthcheck was removed; the worker now only depends on `postgres` and `redis` being healthy.


Latest playback update:

- the HLS route family can now also transcode from a `remote-direct` winner when no explicit HLS/local-file source exists
- generated-local HLS cache reuse is now tied to the effective source input, reducing one stale-cache failure mode when the winner changes for the same item id
- the next playback slice should therefore stay focused on end-to-end player validation and any remaining playback-surface polish rather than reopening that earlier unsupported-transcode seam

Latest tooling/runtime update:

- the dev toolchain now includes `pytest-cov`
- built-in Real-Debrid / AllDebrid clients now have explicit `httpx` connection-pool limits, which is a low-risk future-proofing step before real provider fan-out becomes hot-path traffic

Latest platform slice update:

- the plugin runtime now includes plugin-scoped settings, datasource injection, typed event hooks, publishable-event governance, real MDBList polling, real webhook notifications, and runtime visibility through `/api/v1/plugins/events`
- the FilmuVFS control plane now reuses generation ids for unchanged catalogs, serves reconnect deltas when possible, exposes `RefreshCatalogEntry`, retries stale mounted reads inline through the Rust sidecar, uses `moka::future::Cache`, and preserves stable assigned inodes with collision fallback
- TMDB intake now performs a secondary external-ID lookup when primary metadata lacks `imdb_id`, and the worker now exposes a manual `backfill_imdb_ids` task to repair previously persisted scrape-failing items
- `retry` and `reset` now perform IMDb enrichment before re-queueing, immediately enqueue `scrape_item`, and are exposed on both the REST and GraphQL surfaces
- the one-shot `backfill_imdb_ids` startup hook is now deployed behind the Redis sentinel key `backfill:imdb_ids:enqueued`
- the first observability layer is now live across route, worker, cache, and plugin surfaces, and the current full Python verification gate now passes at `628 passed`

---

## Success checkpoint

The next milestone should be considered reached when:

- the current frontend can exercise end-to-end playback against `filmu-python` without contract workarounds
- direct-play source resolution is stable enough that playback failures are real product issues rather than missing-surface issues
- HLS routes behave as production-governed playback endpoints rather than partial compatibility shims
- the shared serving registry is feeding real mount-style operations rather than only HTTP routes
- the roadmap can then shift more aggressively toward plugin breadth, orchestration depth, and FilmuVFS performance work

## Serving-core update (March 2026)

- The playback track now includes a real shared serving substrate plus an internal serving-status surface.
- That substrate now also includes registered paths, directory semantics, mount-facing path/handle helpers, explicit VFS-facing `getattr`/`readdir` wrappers, and owner-aware stale-runtime cleanup.
- Playback source resolution now also distinguishes typed local-file, remote-direct, and remote-HLS candidates instead of treating all URLs as equivalent fallback values.
- Remote proxy streaming now also uses explicit handle/path accounting through the shared serving core, reducing the gap between HTTP playback and future mount-oriented serving semantics.
- Playback resolution now also preserves attachment-level metadata such as provider, provider download identifiers, filenames, and file sizes so future debrid-services-backed file attachments have a clearer path into both HTTP playback and FilmuVFS.
- Playback attachment resolution now also lives behind a reusable boundary in [`../filmu_py/api/playback_resolution.py`](../filmu_py/api/playback_resolution.py) instead of remaining route-local.
- Playback attachment resolution now also has a service-layer boundary in [`../filmu_py/services/playback.py`](../filmu_py/services/playback.py), reducing API-layer coupling.
- A first persisted playback attachment model now exists, and the service layer prefers it over metadata-derived fallbacks when available.
- Persisted playback attachments now also carry lifecycle/refresh-ready fields and preferred-vs-fallback ordering semantics, which gives the playback service a first explicit model for expired unrestricted-link fallback.
- Persisted playback attachments now also carry an explicit refresh state, which lets the service distinguish ready, stale, refreshing, and failed records rather than inferring everything from timestamps alone.
- Persisted playback attachments now also expose explicit refresh transition/update helpers in [`../filmu_py/services/playback.py`](../filmu_py/services/playback.py), giving the playback layer a first intentional state-transition path instead of only passive fallback semantics.
- Persisted playback attachments now also expose a refresh request/result boundary in [`../filmu_py/services/playback.py`](../filmu_py/services/playback.py), which is the first clean seam for future provider-backed refresh execution.
- Persisted playback attachments now also have explicit planning/request helpers in [`../filmu_py/services/playback.py`](../filmu_py/services/playback.py), so refreshable attachments can be selected and transitioned into `refreshing` in a deterministic service-layer path.
- Persisted playback attachments now also have a provider-facing orchestration boundary in [`../filmu_py/services/playback.py`](../filmu_py/services/playback.py), so planned refresh work can be executed and applied through one domain/service seam.
- Persisted playback attachments now also support a first provider-client-backed `unrestrict_link(...)` execution path in [`../filmu_py/services/playback.py`](../filmu_py/services/playback.py), which moves refresh execution beyond pure provider-agnostic callbacks.
- A first built-in Real-Debrid playback client now exists in [`../filmu_py/services/debrid.py`](../filmu_py/services/debrid.py), and [`../filmu_py/services/playback.py`](../filmu_py/services/playback.py) can resolve it from runtime settings for persisted refresh execution.
- Built-in playback refresh coverage now also includes AllDebrid and Debrid-Link in [`../filmu_py/services/debrid.py`](../filmu_py/services/debrid.py).
- A first Real-Debrid provider-download-id-driven refresh path now also exists on top of the built-in provider clients.
- Persisted provider/file identity now also extends beyond filename/filesize heuristics, which reduces ambiguity during provider-driven refresh matching.
- The playback refresh layer now also has a first provider-side attachment projection model, which lets projection-aware refresh execution persist matched provider file identity instead of repeating filename/filesize heuristics on every refresh.
- The item-detail route now also exposes persisted playback attachment projections, so the next playback/VFS step is no longer basic provider/file identity expansion, the first projection model, or the first detail-read-model exposure; it is broadening those projections into fuller VFS-facing read models, expanding download-id refresh support across providers, hardening production-grade HLS governance, and wiring a real mount worker on top of the shared substrate.
- The item-detail route now also exposes a resolved direct/HLS playback snapshot, so the next playback/VFS step is no longer simply showing the currently best candidate on the details page; it is lifting that resolved-playback model into fuller VFS-facing and active-stream read models, expanding download-id refresh support across providers, hardening production-grade HLS governance, and wiring a real mount worker on top of the shared substrate.
- The playback service and item-detail route now both prefer persisted `media_entries`, persisted `active_stream` selections, durable media-entry lease state, provider-backed lease refresh orchestration, a first fail-closed `503` lease-failure policy, stronger HLS timeout/failure cleanup behavior, a simplified HLS `503`/`404` route mapping, richer `/api/v1/stream/status` visibility, Prometheus stream/playback counters, first latency histograms, abort telemetry, request-shape counters, read-size proxy metrics, first session-level read-amplification proxy metrics, pre-chunk seek/scan-pattern telemetry, degraded-direct fallback awareness, provider-backed direct-source ranking, lease-freshness-aware provider-backed ordering, related-entry recovery via provider file identity, explicit active-stream-winner authority rules, same-file sibling collapse for non-active direct entries, richer-identity-first different-file fallback ordering, generated-local HLS cache/reference integrity checks, and explicit malformed-manifest governance signals, so the next playback/VFS step is no longer first persistence, first resolver adoption, first lease-state persistence, first provider-backed lease execution, first route-level risk policy, first HLS lifecycle guardrail, first client-facing HLS mapping simplification, first internal playback-governance status view, first route-level stream metrics, first latency instrumentation, first abort telemetry, first full/range/suffix request-shape telemetry, first read-size proxy metrics, first session-level read-amplification proxy, first pre-chunk seek/scan classifier, first degraded-direct fallback-ranking correction, first provider-backed direct-vs-generic direct ranking, first lease-freshness-aware provider-backed direct ranking, first related-media-entry recovery rule, first explicit active-stream-authority guardrail, first non-active same-file sibling collapse rule, first different-file fallback tie-break rule, first generated-local HLS integrity guardrail, or first malformed-manifest governance step; it is adding richer chunk/cache visibility, hardening production-grade HLS behavior, extending provider refresh/projection coverage, and wiring a real mount worker on top of the shared substrate.
- Route-level malformed-manifest `503` signaling plus explicit status-surface manifest counters are now also in place, so the next playback/VFS step is no longer first manifest-structure validation or first stale-manifest observability; it is deeper production-grade HLS lifecycle/governance work or a deliberate pivot into the RTN-backed ranking stage.
- Normalized HLS failure-reason counters are now also in place, so the next playback/VFS step is no longer first route-level reason taxonomy either; it is deeper production-grade HLS lifecycle/governance work or a deliberate pivot into the RTN-backed ranking stage.
- Generated-missing and upstream-failed HLS route counters are now also in place, so the next playback/VFS step is no longer first non-`503` HLS route taxonomy coverage either; it is deeper production-grade HLS lifecycle/governance work or a deliberate pivot into the RTN-backed ranking stage.
- Upstream-playlist structural validation is now also in place, so the next playback/VFS step is no longer first remote-playlist manifest-shape protection either; it is deeper production-grade HLS lifecycle/governance work or a deliberate pivot into the RTN-backed ranking stage.
- Remote-HLS timeout/transport hardening is now also in place, so the next playback/VFS step is no longer first upstream transport-failure containment either; it is deeper production-grade HLS lifecycle/governance work or a deliberate pivot into the RTN-backed ranking stage.
- A backend-side frontend playback contract harness is now also in place for the current BFF/player direct-range and HLS-query-parameter patterns, so the next playback/VFS step is no longer first current-frontend contract-shape regression coverage either; it is deeper production-grade HLS lifecycle/governance work or a deliberate pivot into the RTN-backed ranking stage.
- Explicit playlist/segment cache-control policy is now also in place, so the next playback/VFS step is no longer first HLS freshness-header governance either; it is deeper production-grade HLS lifecycle/governance work or a deliberate pivot into the RTN-backed ranking stage.
- Bounded remote-HLS retry/cooldown recovery is now also in place, so the next playback/VFS step is no longer first transient remote-HLS retry policy either; it is deeper production-grade HLS lifecycle/governance work, broader direct-play source/link resolution, or a deliberate pivot into the RTN-backed ranking stage.
- Shared direct-play source authority/local-file precedence is now also in place, so the next playback/VFS step is no longer first unification of active-source authority with local-file preference either; it is deeper direct-play link abstraction work, richer provider/link lifecycle modeling, or deeper production-grade HLS lifecycle/governance work.
- Named direct-play source classification is now also in place, so the next playback/VFS step is no longer first explicit source-class modeling either; it is richer provider/link lifecycle modeling, broader direct-link abstraction, or deeper production-grade HLS lifecycle/governance work.
- Direct-play lease-health source classes are now also in place, so the next playback/VFS step is no longer first explicit ready/stale/refreshing/failed/degraded direct-link class modeling either; it is richer provider/link lifecycle execution, broader direct-link abstraction, or deeper production-grade HLS lifecycle/governance work.
- Explicit direct-play serve/fail/refresh-intent decisions are now also in place, so the next playback/VFS step is no longer first policy-seam extraction above source classes either; it is richer provider/link lifecycle execution, deliberate refresh triggering policy, or deeper production-grade HLS lifecycle/governance work.
- Explicit direct-play refresh recommendations are now also in place, so the next playback/VFS step is no longer first recommendation-payload modeling above route decisions either; it is deliberate execution of those recommendations, richer provider/link lifecycle execution, or deeper production-grade HLS lifecycle/governance work.
- Direct-play refresh dispatch is now also in place, so the next playback/VFS step is no longer first translation of recommendations into existing refresh-request models either; it is deliberate execution policy, provider-aware backpressure/limiter integration, or deeper production-grade HLS lifecycle/governance work.
- One-shot direct-play refresh-dispatch execution is now also in place, so the next playback/VFS step is no longer first execution of translated refresh dispatches outside the request path either; it is provider-aware backpressure/limiter integration, deliberate background scheduling, or deeper production-grade HLS lifecycle/governance work.
- Provider-aware limiter/backpressure on those one-shot direct-play refresh executions is now also in place, so the next playback/VFS step is no longer first refresh-rate-limit denial/retry-after semantics either; it is deliberate background scheduling, richer provider-pressure/circuit-breaker policy, or deeper production-grade HLS lifecycle/governance work.
- A small background scheduling seam now also exists above the limiter-aware one-shot direct-play refresh path, so the next playback/VFS step is no longer first non-blocking run-later scheduling semantics either; it is deciding whether to wire that seam into a deliberate broader control-plane path, deepen provider-pressure/circuit-breaker policy, or continue production-grade HLS governance work.
- A small in-process control-plane caller now also exists above that scheduling seam, so the next playback/VFS step is no longer first service-layer invocation of background direct-play refresh work either; it is deciding whether to attach that caller to a narrow route-adjacent trigger, deepen provider-pressure/circuit-breaker policy, or continue production-grade HLS governance work.
- That in-process caller is now also attached at the app-resource/runtime boundary, so the next playback/VFS step is no longer first app-scoped ownership/lifecycle wiring either; it is deciding whether to attach that app-scoped controller to one narrow route-adjacent trigger, deepen provider-pressure/circuit-breaker policy, or continue production-grade HLS governance work.
- A small helper now also exists above that app-scoped controller attachment, so the next playback/VFS step is no longer first service-boundary trigger helper either; it is deciding whether to attach that helper to one narrow route-adjacent trigger, deepen provider-pressure/circuit-breaker policy, or continue production-grade HLS governance work.
- A narrow route-adjacent non-blocking trigger now also exists in the direct-play route, so the next playback/VFS step is no longer first HTTP-surface attachment to the refresh control-plane either; it is deciding whether to deepen provider-pressure/circuit-breaker policy, expand similar non-blocking triggers to adjacent playback surfaces, or continue production-grade HLS governance work.
- The direct-play trigger path now also has its first duplicate-trigger/backoff guardrails and status-surface governance counters, so the next playback/VFS step is no longer first provider-pressure observability on that route-adjacent path either; it is extending similar behavior to adjacent playback surfaces or continuing deeper HLS governance work.
- The HLS route family now also has a first narrow selected-failed-lease refresh controller plus route-adjacent non-blocking trigger path, selected media-entry-backed remote-HLS winners can now also force one inline repair on real upstream playlist/segment failure, and `/api/v1/stream/status` now reports whether that inline repair path is attempting, recovering, failing, or no-oping, so the next playback/VFS step is no longer first HLS attachment to the durable lease-refresh control plane or first observability for that repair seam either; it is deciding whether to consolidate adjacent HLS trigger/retry policy, deepen provider-pressure/circuit-breaker policy, or continue broader production-grade HLS lifecycle governance work.
- That HLS failed-lease trigger path now also has duplicate-trigger/backoff guardrails and status-surface governance counters, so the next playback/VFS step is no longer first HLS route-surface provider-pressure observability either; it is extending similar behavior to adjacent remote-HLS source classes or continuing deeper HLS governance work.
- The HLS route family now also has a second narrow controller plus route-adjacent trigger path for selected stale or refreshing remote-HLS restricted-fallback winners, broader media-entry-backed remote-HLS winners now also have one-shot inline repair when the resolved upstream playlist/segment URL actually breaks, failed inline media-entry repair now also hands back into that restricted-fallback controller by persisting the selected winner stale before scheduling follow-up work, the two selected-HLS background refresh paths now also share one provider-pressure-aware execution core plus explicit `/api/v1/stream/status` counters for rate-limit vs provider-circuit deferrals, and the direct background refresh plane now also follows the same deferred provider-pressure contract plus matching status counters, so the next playback/VFS step is no longer first narrow HLS winner-state attachment, first repair-handoff unification, first selected-HLS provider-pressure observability, or first widening of those pressure semantics beyond selected-HLS only; it is consolidating the remaining route-adjacent matrix into cleaner policy helpers and keeping the repeated playback/provider proof gates green as stability criteria rather than adding more one-off route patches.
- That restricted-fallback HLS trigger path now also has its own duplicate-trigger/backoff guardrails and parallel status-surface governance counters, so the next playback/VFS step is no longer first separate observability for stale/refreshing HLS backoff pressure either; it is broadening or consolidating adjacent HLS trigger policy deliberately rather than introducing new taxonomy casually.
- The direct-file route now also preserves a stable inline filename contract for resolved direct-play attachments while respecting upstream-provided `content-disposition` on proxied remote responses, so the next playback/VFS step is no longer first filename-preserving direct-file contract hardening either; it is deepening the link resolver abstraction and service-layer direct-file serving descriptor rather than only patching individual headers.
- The playback service now also builds its internal `DirectFileLinkResolution` model from the explicit direct-play decision seam rather than raw attachment shape alone, so the next playback/VFS step is no longer first separation of direct-file classification policy from serving-descriptor synthesis either; it is deciding how far to deepen provider/link lifecycle read models without widening into new persistence concepts or route surface changes.
- That internal direct-file provenance now also carries a debrid-first lifecycle snapshot built from already-persisted playback attachments and media entries, so the next playback/VFS step is no longer first persisted owner/link-state projection behind the descriptor boundary either; it is deciding how to extend that lifecycle read model across adjacent playback/VFS surfaces without leaking it into the route contract.
- That same lifecycle model now also projects across the internal resolved direct/HLS playback snapshot used by adjacent playback read paths, so the next playback/VFS step is no longer first internal non-direct adoption either; it is deciding which future VFS/open-path consumers can reuse that model without requiring new reads, schema changes, or public-contract widening.
- A separate mount-worker boundary module now also defines the explicit media-entry query contract for those future VFS/open-path consumers and already implements the concrete persisted-query executor against the existing `media_entries` + `active_streams` model, so the next playback/VFS step is no longer first mount-boundary/query-contract or first executor definition either; it is wiring later FUSE operations on top of that boundary without collapsing it back into [`../filmu_py/services/playback.py`](../filmu_py/services/playback.py).
- A proto-first WatchCatalog contract now also exists at [`../proto/filmuvfs/catalog/v1/catalog.proto`](../proto/filmuvfs/catalog/v1/catalog.proto), and the Python-side supplier/runtime in [`../filmu_py/services/vfs_catalog.py`](../filmu_py/services/vfs_catalog.py) plus [`../filmu_py/services/vfs_server.py`](../filmu_py/services/vfs_server.py) now already serve reconnect deltas and `RefreshCatalogEntry` refresh requests.
- The Rust runtime now also exists at [`../rust/filmuvfs`](../rust/filmuvfs), and the mounted runtime already passes Linux-target compile validation, WSL/Linux mount lifecycle tests, manual mount/read smoke, and Plex/Emby playback validation.
- The Rust sidecar now also ships chunk-engine-backed mounted reads, inline stale-read refresh, `moka::future` caching, opt-in hybrid L2 cache support, adaptive velocity-based prefetch, hidden-path guards, and stable assigned inodes with collision fallback, so the active FilmuVFS frontier is no longer first mount bring-up or first read-path implementation.
- That means the current FilmuVFS frontier is longer-running soak/backpressure hardening, richer mounted data-plane observability, rollout controls, and continued semantic/operational convergence across the HTTP and VFS playback paths.









