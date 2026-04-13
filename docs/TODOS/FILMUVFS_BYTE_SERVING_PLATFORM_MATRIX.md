# FilmuVFS Byte-Serving Platform Matrix

## Purpose

Turn the active FilmuVFS/platform track from [`../STATUS.md`](../STATUS.md) and [`../EXECUTION_PLAN.md`](../EXECUTION_PLAN.md) into an executable planning artifact.

This document maps the full FilmuVFS-first byte-serving platform into concrete capability areas, showing:

- what is currently present in `filmu-python`
- what the original TS backend already has
- what is still needed to make FilmuVFS a first-class product capability
- how HTTP streaming and HLS should complement, not replace, the VFS path

The concrete product goal is to serve Real-Debrid-backed movie/show files through FUSE so Plex, Emby, and Jellyfin users can browse and play them as if they were ordinary files in the mounted library tree.

---

## Core architectural rule

FilmuVFS is a **product path**, not an optional add-on.

That means the design center should be:

1. FilmuVFS mount/runtime behavior for Plex/Emby and other filesystem-oriented consumers
2. a shared byte-serving engine used by both VFS and HTTP paths
3. HTTP direct file/HLS compatibility as companion surfaces for frontend and non-mount clients

The backend should not build two unrelated streaming systems.

---

## Current Python baseline

What exists today:

- control-plane logs/history SSE baseline
- minimal notifications SSE baseline
- hardened direct-file route with explicit byte-range handling under `/api/v1/stream/file/*`
- partial HLS baseline for local file-backed generation and upstream-HLS proxying under `/api/v1/stream/hls/*`
- shared serving core extracted in [`filmu_py/core/byte_streaming.py`](../../filmu_py/core/byte_streaming.py), including local byte-range serving, remote proxy support, safe child-path resolution, baseline HLS cleanup/concurrency controls, serving-session accounting, file handles, and registered serving paths
- registered path hierarchy, category-aware directory registration, path classification, path attributes, directory listing, explicit mount-facing path/handle helpers, explicit VFS-facing `getattr`/`readdir` wrappers, and owner-aware stale-runtime cleanup are now part of the serving substrate
- internal serving-status surface exposed at [`/api/v1/stream/status`](../../filmu_py/api/routes/stream.py) for current session/handle/path/governance visibility
- FilmuVFS-first architecture direction documented in [`VFS.md`](../VFS.md)
- control-plane constraints for future event backplane documented in [`HYBRID_EVENT_BACKPLANE_RESEARCH.md`](../HYBRID_EVENT_BACKPLANE_RESEARCH.md)
- item details now also expose persisted `media_entries`, which is the first durable filesystem/media-entry domain bridge between the current playback model and future mount-facing work
- item details now also expose additive `active_stream` ownership/readiness plus per-entry active flags on top of `resolved_playback`, persisted `media_entries`, and a persisted active-stream relation, which gives the current API an explicit bridge from selected playback to mount-facing entry projections without relying only on attachment-matching heuristics
- the playback service now also prefers that persisted active-stream/media-entry relation for direct and HLS selection, so the durable playback-domain slice is no longer only a detail-read-model improvement
- media-entry lease/refresh state is now also persisted and preferred during playback resolution, so stale/expired selected entries can degrade through durable lease state rather than only through attachment-local heuristics
- provider-backed lease refresh orchestration now also updates those persisted media-entry leases directly, so the durable playback-domain slice has its first real control-plane execution path above the current resolver model
- selected failed leases now also fail closed with explicit `503` playback-risk behavior, so the route layer has its first intentional reliability policy above the durable lease model
- local generated-HLS lifecycle now also has explicit timeout/failure cleanup behavior plus richer governance counters in the shared serving layer, so partial ffmpeg output leakage is better controlled than the earlier baseline
- the route layer now also uses the simpler client-facing HLS policy: generation and lease-risk failures collapse to `503`, while true missing generated child files remain `404`
- `/api/v1/stream/status` now also surfaces playback-governance counters for tracked media entries, selected-stream failures, and lease-refresh pressure, so the internal stream-control view is no longer limited to serving-runtime counters alone
- Prometheus metrics now also exist for HLS generation outcomes, playback lease-refresh failures, selected-lease playback-risk events, and direct/HLS route outcomes, so stream observability is no longer status-snapshot-only
- latency histograms now also exist for HLS generation duration, remote proxy open latency, and playback-resolution duration, so the stream platform now measures some timing behavior and not only event counts
- abort/cancellation telemetry now also exists in the shared serving core and status snapshot, so interrupted local/remote streaming is visible even before the shared chunk engine is wired into live HTTP and mount reads
- request-shape telemetry now also exists for full-file, range, suffix-range, and partial-content outcomes, so the shared serving core has its first explicit view of how HTTP clients are exercising the byte path before route/mount execution is delegated through the shared chunk engine
- per-read size histograms and lightweight small/medium/large buckets now also exist, so the serving core has its first read-shape proxy beneath the request level before true chunk/read-amplification metrics exist
- session-level read-operations-per-open and bytes-per-read proxy histograms now also exist, so the serving core can approximate read amplification trends even before the shared chunk engine is wired into live HTTP and mount reads
- pre-chunk seek/scan-pattern telemetry now also exists at the request boundary, so the serving core can distinguish head probes, tail probes, seek probes, and larger stream windows alongside the newer chunk-engine-native read-type detection
- direct-file resolution now also demotes degraded restricted-link fallbacks below ready local-file and non-degraded direct candidates, so the shared playback resolver is less likely to choose a stale restricted direct source when a stronger direct source is already available
- direct-file resolution now also prefers provider-backed unrestricted direct URLs over generic direct URLs, so the shared playback resolver keeps more provider-native identity and lease context when multiple good direct candidates coexist
- provider-backed direct ranking now also uses lease freshness while keeping local files ahead of remote direct links, so the shared playback resolver is less likely to choose the shorter-lived provider direct source when multiple provider-backed direct candidates coexist
- internal resolved playback snapshots now also carry the same debrid-first lifecycle projection used by direct-file provenance, so adjacent playback/VFS-facing service consumers can reuse persisted owner/link-state context without changing current route contracts or adding new reads in this slice
- a separate mount-worker boundary module now also exists for future VFS query planning, defining a playback-snapshot supplier seam plus an explicit persisted media-entry query contract and concrete executor instead of collapsing that responsibility into the HTTP playback service
- selected degraded direct media entries now also recover through sibling entries that share provider file identity, so active-stream selection is less brittle when one row for the same provider-backed file has gone stale ahead of another
- non-active provider-backed direct siblings now also collapse only within same-file groups keyed by `provider_file_id`/`provider_file_path`, so same-file duplicate entries compete intentionally while different-file groups remain separate
- different-file provider-backed direct groups now also break ties by richer provider identity first and lease freshness second, so fallback ordering is deliberate instead of depending on row order once same-file sibling collapse is finished
- generated-local HLS playlist caching now also validates that all referenced child files exist on disk before reuse, regenerates incomplete cached directories instead of serving partial leftovers, and exposes explicit referenced-file inspection helpers
- generated-local HLS segment serving now also restricts served child files to those actually referenced by the generated playlist, so unreferenced leftover files under the output directory are no longer implicitly servable
- generated-local HLS manifests now also fail explicit structural validation when empty, malformed, or segmentless, and the serving substrate now surfaces `hls_manifest_invalid` / `hls_manifest_regenerated` governance counters for that state
- malformed generated-local HLS manifests now also surface through the HLS routes using the same simplified client-facing `503` policy, and `/api/v1/stream/status` now exposes those manifest counters directly for operator-facing stale-manifest observability
- normalized HLS route failure counters now also classify simplified `503` responses by reason (`generation_failed`, `generation_timeout`, `generator_unavailable`, `lease_failed`, `manifest_invalid`) and expose that breakdown through `/api/v1/stream/status`, so the serving platform now has its first route-level HLS reason taxonomy above raw status codes
- HLS route failure counters now also include `generated_missing` and `upstream_failed`, so missing generated child files and remote playlist/segment failures participate in the same operator-facing route taxonomy rather than living only in raw status-code counts
- remote upstream playlists are now also structurally validated before rewrite/proxy handoff, and failures in that validation surface through a dedicated `upstream_manifest_invalid` route counter instead of being treated as ordinary successful playlist rewrites
- remote upstream playlist fetches and segment proxy opens now also fail with explicit `504` / `502` transport policy, so the remote HLS data path no longer depends on framework-level exception leakage for timeout/transport failures
- remote-HLS playlist fetches and segment proxy opens now also retry one transient timeout/transport failure and then enter a short per-playlist cooldown window with `Retry-After`, so the current HTTP playback path has its first bounded upstream backoff policy before fuller circuit-breaker work lands
- no-behavior-change route decomposition is now also in progress for that remote-HLS policy seam: retry/cooldown governance, inline-repair counters, and HLS failure taxonomy helpers now live in [`../../filmu_py/api/routes/runtime_hls_governance.py`](../../filmu_py/api/routes/runtime_hls_governance.py), while [`../../filmu_py/api/routes/stream.py`](../../filmu_py/api/routes/stream.py) keeps compatibility wrappers
- the HLS route family can now also treat a `remote-direct` playback winner as an ffmpeg transcode source rather than failing immediately when no explicit HLS/local-file winner exists
- generated-local HLS cache reuse is now source-aware via a small marker file in the generated directory, so cached HLS output is regenerated when the effective input source changes for the same item id
- a proto-first WatchCatalog contract now exists at [`../../proto/filmuvfs/catalog/v1/catalog.proto`](../../proto/filmuvfs/catalog/v1/catalog.proto), and the Python-side catalog supplier now exists at [`../../filmu_py/services/vfs_catalog.py`](../../filmu_py/services/vfs_catalog.py), reusing the existing playback snapshot and persisted mount-query boundaries instead of inventing a second VFS-only read model
- a Rust runtime now exists at [`../../rust/filmuvfs`](../../rust/filmuvfs), including proto generation in [`../../rust/filmuvfs/build.rs`](../../rust/filmuvfs/build.rs), the reconnecting WatchCatalog client in [`../../rust/filmuvfs/src/catalog/client.rs`](../../rust/filmuvfs/src/catalog/client.rs), in-memory catalog state in [`../../rust/filmuvfs/src/catalog/state.rs`](../../rust/filmuvfs/src/catalog/state.rs), bootstrap in [`../../rust/filmuvfs/src/main.rs`](../../rust/filmuvfs/src/main.rs), and the mounted `fuse3` data path in [`../../rust/filmuvfs/src/mount.rs`](../../rust/filmuvfs/src/mount.rs)
- the Rust scaffold now also includes explicit generated-binding guards in [`../../rust/filmuvfs/src/proto.rs`](../../rust/filmuvfs/src/proto.rs) for `session_id`, `handle_key`, `provider_file_id`, and `provider_file_path`, so Cargo validation can catch proto/binding drift before mount lifecycle work starts
- those binding guards are now also the documented extension point for any future cross-process observability fields added to the proto contract; when new correlation fields appear, the guard coverage in [`../../rust/filmuvfs/src/proto.rs`](../../rust/filmuvfs/src/proto.rs) should expand in the same change
- the Rust scaffold has now passed the validation gate on this Windows host: [`rust:fmt`](../../package.json) ✅, [`rust:check`](../../package.json) ✅, and [`rust:test`](../../package.json) ✅; `fuse3` is now correctly Unix-only in [`../../rust/filmuvfs/Cargo.toml`](../../rust/filmuvfs/Cargo.toml) and [`../../rust/filmuvfs/src/mount.rs`](../../rust/filmuvfs/src/mount.rs)
- a first mount-facing lifecycle layer now also exists in [`../../rust/filmuvfs/src/mount.rs`](../../rust/filmuvfs/src/mount.rs), covering catalog-backed `getattr`, `readdir`, `open`, `read`, and `release` behavior with hierarchy-preserving tests and real WSL/Linux mounted execution
- a first Unix-only `fuse3` adapter now also exists in [`../../rust/filmuvfs/src/mount.rs`](../../rust/filmuvfs/src/mount.rs), including deterministic inode mapping from catalog entry IDs, inode/name lookup, a `Session::mount(...)` bootstrap seam, and real WSL/Linux validation of that mounted lifecycle while still deferring chunk-engine-backed byte serving
- a Linux-target Cargo compile check now also passes for [`../../rust/filmuvfs`](../../rust/filmuvfs) with `--target x86_64-unknown-linux-gnu`, so the Unix-only adapter is now compile-validated against the intended production target from this Windows host
- the Python control plane now reuses generation ids for unchanged snapshots, can serve reconnect deltas for known generations, and exposes `RefreshCatalogEntry` for forced provider-link refresh through [`../../filmu_py/services/vfs_catalog.py`](../../filmu_py/services/vfs_catalog.py) and [`../../filmu_py/services/vfs_server.py`](../../filmu_py/services/vfs_server.py)
- the Rust sidecar now retries stale mounted reads inline through that refresh RPC, uses `moka::future::Cache` in [`../../rust/filmuvfs/src/chunk_engine.rs`](../../rust/filmuvfs/src/chunk_engine.rs), and preserves stable assigned inodes with collision fallback in [`../../rust/filmuvfs/src/catalog/state.rs`](../../rust/filmuvfs/src/catalog/state.rs)
- the Rust sidecar now also parses mounted media-semantic path metadata, carries it on mounted `getattr` / `readdir` / `open` / `read` surfaces, uses it for alias-aware mounted traversal, exposes discoverable alias browse entries (`tvdb-*`, `tmdb-*`, `Season 01`, `Episode 01.mkv`), and deduplicates concurrent inline stale-refresh RPCs per entry
- the enterprise operator surface now also consumes the same mounted-runtime snapshot as [`/api/v1/stream/status`](../../filmu_py/api/routes/stream.py): [`/api/v1/operations/governance`](../../filmu_py/api/routes/default.py) now exposes live `vfs_runtime_rollout_readiness`, rollout reasons, cache/fallback/prefetch ratios, provider/fairness pressure incidents, explicit cache/chunk-coalescing/upstream-wait/refresh pressure classes with bounded reason lists, and runtime-snapshot availability inside the `vfs_data_plane` slice instead of limiting that enterprise view to static capability posture
- mounted runtime telemetry in `/api/v1/stream/status` is now tenant-safe for active-handle summaries: request-scoped visibility, hidden-count accounting, and visible tenant rollups are emitted instead of unfiltered cross-tenant summaries
- mount-vs-HTTP chunk behavior now has a dedicated parity harness gate via [`../../scripts/check_mount_http_chunk_parity.ps1`](../../scripts/check_mount_http_chunk_parity.ps1), backed by Python and Rust contract tests that validate equivalent range/chunk coverage semantics plus HTTP-side cache/read classification
- Windows VFS soak promotion now runs as a scheduled multi-environment program via [`../../scripts/run_windows_vfs_soak_program.ps1`](../../scripts/run_windows_vfs_soak_program.ps1), with trend-regression blocking through [`../../scripts/check_windows_vfs_soak_trends.ps1`](../../scripts/check_windows_vfs_soak_trends.ps1) and workflow [`../../.github/workflows/windows-vfs-soak-program.yml`](../../.github/workflows/windows-vfs-soak-program.yml)
- mounted foreground reads now also inherit explicit cancellation and release semantics: released handles cancel in-flight foreground reads, cancelled reads no longer repopulate chunk-engine handle tracking after release, ProjFS command cancellation now aborts async callback work, and runtime status plus `/api/v1/stream/status` now expose cancelled mounted-read, handle-startup, and ProjFS callback outcomes

What does **not** exist yet:

- ~~live Rust-mount adoption of the shared chunk/range engine~~ → ✅ Done — [`mount.rs`](../../rust/filmuvfs/src/mount.rs) `read()` now uses `ChunkEngine` + `moka::future` cache (Slice E).
- ~~Hidden path guard~~ → ✅ Done — [`is_hidden_path()`](../../rust/filmuvfs/src/hidden_paths.rs) + [`is_ignored_path()`](../../rust/filmuvfs/src/hidden_paths.rs) in FUSE lookup/readdir (Slice F).
- ~~Adaptive prefetch~~ → ✅ Done — [`VelocityTracker`](../../rust/filmuvfs/src/prefetch.rs) per handle, TCP-slow-start-style window scaling (Slice F).
- ~~L2 disk cache~~ → ✅ Done — [`HybridCache`](../../rust/filmuvfs/src/cache.rs) trait implementation, opt-in via [`config.cache.l2_enabled`](../../rust/filmuvfs/src/config.rs) (Slice F).
- ~~GraphQL mutation breadth~~ → ✅ Done — `requestItem`, `itemAction`, and `updateSetting` now ship on [`/graphql`](../../filmu_py/graphql/schema.py) (Slice F).
- broader long-running soak/backpressure hardening of the Unix-only `fuse3` runtime now that reconnect deltas, inline stale-link refresh, and mounted semantic path metadata are in place
- later HLS governance deepening beyond the new production-grade HTTP baseline, especially any Rust/mount-side reuse and broader resource-policy tuning
- deeper governance around remote-direct-backed HLS generation remains necessary even after the new transcode fallback baseline, especially around ffmpeg failure policy and end-to-end player validation
- real multi-environment mounted soak evidence collection above the new repo-level aggregation gate, plus cross-process correlation once the shared chunk engine is driving real mounted reads
- repeatable Docker Plex playback-proof coverage on top of the now-working `/mnt/filmuvfs` path, with the new `plex-wsl-evidence.json` artifact and per-check summary fields driven to green
- keep native Windows Plex parity evidence green against `C:\FilmuCoreVFS` through repeatable reruns now that the local PMS path is live-green

---

## Reference breadth from the TS backend

The original TS backend already has a real native VFS path, including:

- mount lifecycle bootstrap
- path parsing for movie/show/season/episode navigation
- `open`, `read`, `readdir`, `getattr`, `release` operations
- chunk calculation and discrete byte-range fetching
- read-type detection (body-read vs cache-hit vs scans)
- file handle maps and chunk caches
- stream-link request integration through plugin queues

For FilmuCore, the equivalent Python target is specifically a FUSE mount that can surface Real-Debrid-backed content as browseable/readable files for media-server clients.

The local Compose stack now also provisions isolated real Plex and real Emby containers for parity testing on top of that mount. Docker Plex parity is now materially working on the Linux/WSL topology after fixing host-mount visibility, stale host-binary reuse, entry-id refresh collisions, and duplicate foreground fetches, and the direct provider gate reran green on April 9, 2026. Native Windows validation is now live-green for Jellyfin, Emby, and Plex on `C:\FilmuCoreVFS`, with Plex now proven through the real local PMS rather than just a planned target.

This is the implementation breadth Python still needs to plan toward if the goal is to outperform it. The Docker/WSL Plex proof path now also emits explicit artifacted mount-visibility, host-binary-freshness, refresh-identity, and foreground-fetch evidence checks, and those reran green twice on April 9, 2026.

---

## Byte-serving platform matrix

| Capability area                        | Current Python state           | TS reference breadth                                          | Why it matters                                                                                            | Priority |
| -------------------------------------- | ------------------------------ | ------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- | -------- |
| **FilmuVFS mount lifecycle**           | gRPC bridge + Rust sidecar + WSL-validated mount lifecycle/read smoke + passed Plex/Emby playback gate | Implemented in TS                                             | Core product path for Plex/Emby-style consumers                                                           | **P1** (runtime hardening) |
| **Path model / directory semantics**   | Implemented baseline with typed browse semantics, aliases, and season grouping | TS has movie/show/season/episode path typing                  | Needed for human-usable mount behavior and stable browse semantics                                        | Done baseline / deepen |
| **Shared chunk/range engine**          | Implemented in Python + adopted in HTTP direct range route | TS has chunk-calculation and discrete fetch utilities | Critical for performance, cache efficiency, and avoiding duplicate logic across VFS/HTTP                  | **P1** (mount adoption) |
| **Direct file byte-range serving**     | Implemented baseline + parity harness gate | TS has mature read/open path concepts                         | Needed for frontend playback parity and shared stream engine validation                                   | ✅ Done baseline / operational hardening |
| **HLS serving**                        | Implemented baseline           | TS references richer stream platform expectations             | Needed for web playback compatibility and future frontend playback                                        | Done     |
| **Link resolver abstraction**          | Implemented                      | TS integrates stream-link request flow with plugins           | Required to turn Real-Debrid/provider state into file-like mount reads cleanly                            | Done     |
| **File handle / stream session state** | Implemented natively           | TS has explicit file-handle maps and read-position logic      | Needed for performant mount reads and safe seek/scan behavior                                             | Done     |
| **Chunk caching strategy**             | Implemented in-memory shared cache + HTTP route reuse | TS has chunk caches and read-type detection          | Needed to outperform TS rather than just match it                                                         | **P1** (mount wiring + disk cache) |
| **Cancellation / abort behavior**      | Stronger baseline              | TS explicitly handles aborted read requests                   | Foreground mount reads now cancel cleanly on handle release/ProjFS command cancellation, but broader soak coverage and more Windows parity evidence still remain | **P1**   |
| **FilmuVFS control-plane events**      | Implemented baseline via reconnect deltas + `RefreshCatalogEntry` + inline stale-read recovery | TS integrates plugin queues for stream-link requests          | Needed for lease refresh, invalidation, provider pressure handling                                        | Done baseline |
| **Stream/VFS metrics**                 | Strong HTTP + joined cross-process baseline; mount telemetry now also yields cache/chunk-coalescing/upstream-wait/refresh pressure classes on API governance surfaces and Windows soak artifacts | TS has richer operational behavior even if metrics are uneven | Needed for performance tuning and proving superiority                                                     | **P1** (mount telemetry + wider evidence breadth) |
| **Canary/rollback controls**           | Missing                        | Needed regardless of TS                                       | Required before rollout to real users                                                                     | **P2**   |

---

## Shared-engine principle

The most important design rule is:

> Build one shared byte-serving engine, then expose it through multiple surfaces.

That shared engine should own:

- link resolution handoff
- range planning
- chunk sizing
- chunk fetch and reuse
- read-ahead policy
- cache-hit vs scan detection
- lease/refresh coordination
- cancellation handling

The surfaces built on top of it should be:

1. FilmuVFS mount operations
2. HTTP direct file/range routes
3. HLS generation/segment serving

This is the main way Python can beat the TS backend: one better-designed serving core instead of duplicated logic.

---

## Control plane vs data plane

## Data plane — must stay direct

These operations must remain off the event backbone hot path:

- byte reads
- chunk fetches
- file-handle read progression
- HLS segment serving

## Control plane — may use orchestration/events

These can use orchestration and future backplane support:

- stream-link request/refresh
- lease invalidation
- cache invalidation signals
- provider backpressure/circuit-breaker updates
- session lifecycle notifications
- replay/rebuild of control-plane projections

This keeps FilmuVFS fast while still allowing a strong event architecture around it.
The performance of the system is very important, so we need to make sure that we are not adding any unnecessary overhead.

---

## Recommended implementation sequence

### Stage 1 — Shared foundations

1. link resolver abstraction
2. stream/file attachment model in the domain layer
   - the current API and playback resolver now both prefer persisted `media_entries`, persisted active-stream selection, durable media-entry lease state, provider-backed lease refresh orchestration, a first fail-closed route policy, stronger HLS lifecycle cleanup, a simplified HLS client-facing mapping, richer internal status visibility, Prometheus stream/playback counters, first latency histograms, abort telemetry, request-shape counters, read-size proxy metrics, first session-level read-amplification proxies, pre-chunk seek/scan-pattern telemetry, degraded-direct fallback awareness, provider-backed direct-source ranking, lease-freshness-aware provider-backed ordering, related-entry recovery via provider file identity, same-file sibling collapse for non-active direct entries, richer-identity-first different-file fallback ordering, generated-local HLS cache/reference integrity checks, and explicit malformed-manifest governance counters, but broader stream/file lifecycle semantics, richer route governance, and mount-facing resolver flows still need to be built on top of those relations
3. integrate the implemented shared chunk/range engine into live HTTP serving and later mount open/read consumers
   - the new lifecycle projection is already available on internal resolved playback snapshots, but reusing it inside true FilmuVFS open/path-resolution workflows is still a separate deliberate step because that adjacent adoption may require broader persistence-query work or mount-specific read-model boundaries
4. wire later mount/open-path consumers onto the new mount-worker query executor before any FUSE operation wiring

### Stage 2 — Direct file path hardening

4. stronger direct file source/link resolution and contract hardening
5. strict header and partial-content contract tests

### Stage 3 — FilmuVFS core

6. harden the new Unix-only `fuse3` trait wiring and OS mount lifecycle integration for longer-running recovery scenarios
7. deepen the current mounted `open`/`read`/`readdir`/`getattr`/`release` layer into higher-performance mounted operations for Real-Debrid-backed files
8. keep the on-disk cache and fair prefetch queue in the Rust sidecar, while reusing or porting the proven shared chunk-engine semantics from Python rather than inventing a second geometry/classification model
    - the priority-aware prefetch scheduler is a deliberate differentiator over `riven-ts`: first chunk highest priority, later prefetch chunks lower priority, and fair round-robin sharing across concurrent sessions
9. deepen path typing beyond the current generic registered-path model
10. file-handle/session state tracking, read-position policy, and cancellation behavior

### Stage 4 — HLS and advanced serving

10. HLS playlist/segment pipeline
11. ffmpeg process governance
12. shared-engine reuse across HLS and direct file paths

### Stage 5 — Operational maturity

13. stream/VFS metrics
14. cancellation, lease refresh, provider pressure handling
15. canary/rollback controls

---

## What not to do

- Do **not** build HTTP direct-file serving and FilmuVFS as separate logic stacks.
- Do **not** defer FilmuVFS until after all HTTP work is “done”.
- Do **not** push byte-serving reads onto a future JetStream/Redis control plane.
- Do **not** treat HLS as independent from the shared byte-serving engine.

---

## Success checkpoint

Priority 5 should be considered meaningfully advanced when:

- a real FilmuVFS mount worker exists
- direct file streaming works through the same serving core
- chunk planning/caching is shared across VFS and HTTP paths
- Plex/Emby can traverse the mounted tree and read Real-Debrid-backed content as if it were ordinary filesystem media
- VFS and stream metrics exist to prove performance and reliability
- the design is clearly better factored than the TS implementation, not just equivalent in behavior

Current checkpoint:

- Reached for first-class mounted playback proof, shared chunk/cache execution, and cross-process governance visibility.
- Still open for canary/rollback controls, broader long-running soak evidence, and deeper mounted telemetry/rollout hardening.

## Current serving-core update (March 2026)

- The shared substrate in [`filmu_py/core/byte_streaming.py`](../../filmu_py/core/byte_streaming.py) now covers local byte-range serving, remote proxying, safe child-path resolution, generated-HLS cleanup/concurrency controls, serving-session accounting, file handles, and registered serving paths.
- The substrate now also exposes category-aware directory hierarchy registration, path classification, path attributes, directory listing, explicit mount-facing path/handle helpers, explicit VFS-facing `getattr`/`readdir` wrappers, and owner-aware stale-runtime cleanup.
- Remote proxy streaming now also participates in explicit handle/path accounting rather than only session-level tracking.
- The proto-first WatchCatalog contract, Python-side catalog supplier, and Rust runtime scaffold now all exist, so the next FilmuVFS step is no longer contract or bootstrap design. It is validating and hardening the mounted runtime deliberately without flattening the hierarchical path model or weakening the catalog protocol lifecycle.
- That Cargo validation gate now also includes explicit generated-binding verification for the correlation-key fields required by cross-process observability, so mount lifecycle work does not begin on top of silently drifted Rust bindings.
- That validation gate has now passed on Windows, so the active FilmuVFS frontier has moved beyond scaffold/validation work and into runtime hardening on top of the validated mount path.
- That first mount-facing lifecycle slice now also exists, and the catalog-backed operations are already attached to the real `fuse3` trait boundary and OS mount lifecycle without flattening `/movies` / `/shows` or leaking provider ownership out of Python.
- That Unix-only `fuse3` trait boundary now also exists in code, now passes the automated WSL/Linux [`mount_lifecycle`](../../rust/filmuvfs/tests/mount_lifecycle.rs) gate plus the manual WSL mount/list/stat/read smoke path, and now serves chunk-engine-backed reads with stale-read refresh, adaptive prefetch, and selectable memory/hybrid caching.
- That means the next frontier is no longer first mount bring-up, compile-only validation, or first media-server playback success; it is longer-running runtime/data-plane hardening, mounted observability, and deliberate rollout control.
- A shared Python chunk engine now also exists in [`../../filmu_py/core/chunk_engine.py`](../../filmu_py/core/chunk_engine.py), covering header/footer/body chunk geometry, 6-way read-type classification, byte-weighted in-memory cache, ordered chunk resolution, validated range fetch/stitch behavior, and `filmu_chunk_*` Prometheus metrics, with focused regression coverage in [`../../tests/test_chunk_engine.py`](../../tests/test_chunk_engine.py).
- The Python control plane now also serves reconnect deltas and `RefreshCatalogEntry`, while the Rust sidecar now retries stale reads inline, uses `moka::future::Cache`, and preserves stable assigned inodes with collision fallback.
- The direct-play HTTP route now also reuses that shared chunk engine for known-size remote-proxy range requests in [`../../filmu_py/api/routes/stream.py`](../../filmu_py/api/routes/stream.py), while the older remote proxy streaming path remains the deliberate fallback for unknown-size upstream resources.
- Generated-local HLS caching now also validates playlist completeness (all referenced child files present on disk) before reuse, regenerates incomplete cached directories instead of serving stale leftovers, and restricts served child files to those actually referenced by the generated playlist.
- Generated-local HLS malformed-manifest counters are now also exposed through [`/api/v1/stream/status`](../../filmu_py/api/routes/stream.py), and malformed generated-local manifests now surface through the HLS routes as simplified `503` playback-risk responses.
- Normalized HLS route failure counters are now also exposed through [`/api/v1/stream/status`](../../filmu_py/api/routes/stream.py), so timeout, lease-failed, malformed-manifest, and similar HLS route failures have a first operator-facing reason taxonomy even while the client contract stays simplified.
- Generated-missing and upstream-failed route counters are now also exposed through [`/api/v1/stream/status`](../../filmu_py/api/routes/stream.py), broadening that taxonomy beyond normalized `503` cases.
- Upstream-playlist structural validation now also protects the remote HLS path before playlist rewrite, and `upstream_manifest_invalid` is exposed through [`/api/v1/stream/status`](../../filmu_py/api/routes/stream.py) for that class of defect.
- Remote-HLS timeout/transport policy now also protects playlist fetch and segment proxy opens, while the shared serving metrics surface records timeout/error upstream open outcomes for that path.
- Generated-local HLS governance is now also production-grade on the HTTP path: [`../../filmu_py/core/byte_streaming.py`](../../filmu_py/core/byte_streaming.py) tracks active ffmpeg processes, rejects saturated generation immediately with `503`, force-terminates cancelled/timed-out ffmpeg processes to avoid zombies, records stderr/speed observability, reaps stalled generated segment directories, enforces high-water/low-water disk quota cleanup, and runs a background governance loop under app lifespan ownership.
- The HLS route family now also keeps generated playlists/segments alive while they are actively being served and exposes the new `generation_capacity_exceeded` failure class through [`/api/v1/stream/status`](../../filmu_py/api/routes/stream.py).
- Internal session/handle/path/governance visibility is now exposed through [`/api/v1/stream/status`](../../filmu_py/api/routes/stream.py).
- This is still below full FilmuVFS readiness, but it is now a real reusable serving substrate rather than only route-local helpers.




