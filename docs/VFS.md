# Virtual File System (VFS) Plan

## Scope

Document how VFS/streaming compatibility will be preserved while modernizing internals.

The primary product objective is not an abstract mount feature; it is a real mounted filesystem that exposes Real-Debrid-backed movie and show files so Plex, Emby, and Jellyfin users can browse and play them as if they were normal files under the mount path.

Detailed comparison notes for the current gap versus the audited TypeScript implementation:

- [`RIVEN_TS_AUDIT.md`](RIVEN_TS_AUDIT.md)

## Current status

- A real FilmuVFS mount path now exists across the Python catalog/control plane and the Rust host-adapter sidecar. Linux hosts use the `fuse3` adapter and Windows hosts now go through an explicit native adapter boundary with both ProjFS and WinFSP backends available, while all paths continue to share the same catalog watch, inode, and chunk-read control plane.
- The current backend already has orchestration/state/cache/rate-limit foundations, a hardened [`/api/v1/stream/file/*`](../filmu_py/api/routes/stream.py) baseline with explicit byte-range handling, and a partial [`/api/v1/stream/hls/*`](../filmu_py/api/routes/stream.py) baseline for local file-backed generation and upstream-HLS proxying.
- Playback attachment/source resolution now also lives behind [`filmu_py/api/playback_resolution.py`](../filmu_py/api/playback_resolution.py) rather than remaining embedded in [`filmu_py/api/routes/stream.py`](../filmu_py/api/routes/stream.py).
- Internal resolved playback snapshots in [`filmu_py/services/playback.py`](../filmu_py/services/playback.py) now also carry the same debrid-first lifecycle projection used by direct-file provenance, so adjacent playback/VFS-facing service consumers can reuse persisted owner/link-state context without changing the current route contracts.
- Provider-backed media-entry lease resolution now also sits behind a reusable service-layer [`LinkResolver`](../filmu_py/services/playback.py) boundary in [`filmu_py/services/playback.py`](../filmu_py/services/playback.py), so the current HTTP compatibility surface and the future mount worker can share the same persisted-lease, rate-limit, and circuit-breaker resolution path without importing route internals.
- That resolver now also owns inline provider refresh governance for persisted media entries, including the existing `ratelimit:{provider}:stream_link_refresh` bucket, per-provider circuit-open backoff, and cached-lease fallback semantics when refresh is denied or temporarily blocked.
- The HLS route family now also reuses that same resolver boundary for `media-entry`-backed `remote-direct` transcode inputs before ffmpeg generation starts, so the future mount path can rely on one shared persisted-lease validation story rather than diverging direct-file and transcode-input refresh behavior.
- A separate mount-worker boundary now exists in [`../filmu_py/services/mount_worker.py`](../filmu_py/services/mount_worker.py), defining a playback-snapshot supplier protocol plus an explicit media-entry query contract and concrete persisted-query executor for future mount-facing provider-file identity resolution.
- A proto-first FilmuVFS catalog contract now exists at [`../proto/filmuvfs/catalog/v1/catalog.proto`](../proto/filmuvfs/catalog/v1/catalog.proto), and the Python-side supplier that projects current media-entry/playback state into that contract now exists at [`../filmu_py/services/vfs_catalog.py`](../filmu_py/services/vfs_catalog.py).
- A Rust runtime now also exists at [`../rust/filmuvfs`](../rust/filmuvfs), including vendored proto generation, `tonic` client bindings, a reconnecting WatchCatalog client, in-memory catalog state application, mounted read execution, and tracing/OTLP bootstrap.
- A first Rust mount-facing lifecycle layer now also exists in [`../rust/filmuvfs/src/mount.rs`](../rust/filmuvfs/src/mount.rs), covering catalog-backed `getattr`, `readdir`, `open`, `read`, and `release` behavior behind a host-adapter-aware `Session::mount(...)` boundary.
- A Linux `fuse3` adapter and a Windows adapter boundary now exist under [`../rust/filmuvfs/src/mount.rs`](../rust/filmuvfs/src/mount.rs), [`../rust/filmuvfs/src/windows_host.rs`](../rust/filmuvfs/src/windows_host.rs), and [`../rust/filmuvfs/src/windows_projfs.rs`](../rust/filmuvfs/src/windows_projfs.rs), with stable inode mapping over the shared catalog hierarchy.
- The Windows ProjFS adapter now also keeps mounted file handles keyed by ProjFS `DataStreamId` and releases them on native file-handle-close notifications instead of reopening on every `GetFileData` callback.
- The raw Windows WinFSP adapter in [`../rust/filmuvfs/src/windows_winfsp.rs`](../rust/filmuvfs/src/windows_winfsp.rs) now also survives real Jellyfin probe/read pressure on the canonical `C:\FilmuCoreVFS` folder-mount path after fixing raw file-context ownership and aligning `UmFileContextIsUserContext2` with the working wrapper behavior.
- The Windows ProjFS adapter now also emits native read-path metrics through [`../rust/filmuvfs/src/telemetry.rs`](../rust/filmuvfs/src/telemetry.rs), including:
  - `filmuvfs_windows_projfs_callbacks_total`
  - `filmuvfs_windows_projfs_callback_duration_seconds`
  - `filmuvfs_windows_projfs_stream_handle_events_total`
  - `filmuvfs_windows_projfs_notifications_total`
- When no OTLP collector is configured, the Windows adapter now also provides a low-friction operator fallback:
  - slow `GetFileData` callbacks emit structured warnings from [`../rust/filmuvfs/src/windows_projfs.rs`](../rust/filmuvfs/src/windows_projfs.rs)
  - sidecar shutdown emits a `windows projfs adapter summary` line from [`../rust/filmuvfs/src/telemetry.rs`](../rust/filmuvfs/src/telemetry.rs) with callback counts/latency, stream-handle lifecycle totals, and close-notification totals
  - long-running Windows sessions also emit that same `windows projfs adapter summary` line periodically; the interval defaults to 300 seconds on Windows, stays disabled on non-Windows hosts, and can be overridden or disabled with `FILMUVFS_WINDOWS_PROJFS_SUMMARY_INTERVAL_SECONDS` / `--windows-projfs-summary-interval-seconds`
- The Python control plane now reuses generation ids for unchanged catalogs, can build reconnect deltas for known generations, and exposes `RefreshCatalogEntry` so the Rust sidecar can force provider-link refresh through [`../filmu_py/services/vfs_catalog.py`](../filmu_py/services/vfs_catalog.py) and [`../filmu_py/services/vfs_server.py`](../filmu_py/services/vfs_server.py) instead of falling back to full repolls for every reconnect.
- The Rust sidecar now retries stale upstream reads inline through that refresh RPC, uses async `moka::future::Cache` in [`../rust/filmuvfs/src/chunk_engine.rs`](../rust/filmuvfs/src/chunk_engine.rs), and preserves stable assigned inodes with collision fallback in [`../rust/filmuvfs/src/catalog/state.rs`](../rust/filmuvfs/src/catalog/state.rs).
- The Python catalog supplier now also normalizes mounted show layout to `Show Title (Year)/Season XX/<sanitized source filename>` while preserving the source filename shape after sanitization rather than forcing a synthetic rename.
- Provider-path season inference in [`../filmu_py/services/vfs_catalog.py`](../filmu_py/services/vfs_catalog.py) now also understands `S05x08`-style patterns for show-level media entries, so real mounted libraries such as `Stranger Things (2016)` are grouped under season folders instead of flattening every file at the show root.
- Catalog deltas now emit removals when an existing catalog `entry_id` changes visible path, preventing stale root-level paths from surviving after naming-policy changes or better season inference becomes available.
- A shared serving core in [`filmu_py/core/byte_streaming.py`](../filmu_py/core/byte_streaming.py) now centralizes safe child-path resolution, remote byte-range proxying, baseline HLS cleanup/concurrency controls, serving-session accounting, file-handle tracking, registered-path tracking, category-aware directory hierarchy/listing, path classification, explicit mount-facing handle/path helpers, explicit VFS-facing `getattr`/`readdir` wrappers, and owner-aware stale-runtime cleanup.
- An internal status surface at [`/api/v1/stream/status`](../filmu_py/api/routes/stream.py) exposes the current serving-runtime/governance state.
- The status surface now also includes a first cross-process VFS governance layer from [`../filmu_py/services/vfs_server.py`](../filmu_py/services/vfs_server.py): watch-session lifecycle, reconnect-delta churn, supplier snapshot/delta failures, provider-backed refresh outcomes, and explicit inline refresh request outcomes are additive counters on [`/api/v1/stream/status`](../filmu_py/api/routes/stream.py) rather than log-only signals.
- That same status surface now also ingests the Rust sidecar's structured `filmuvfs-runtime-status.json` snapshot when it is available through the managed Windows stack state or an explicit `FILMU_PY_VFS_RUNTIME_STATUS_PATH` override, so mounted-read result counts, upstream fetch totals/durations, chunk-cache activity, prefetch pressure, inline refresh outcomes, and Windows callback failures are visible through the same operator-facing API instead of only through separate sidecar artifacts.
- Cross-process correlation is no longer only a proto intention: `session_id` and `handle_key` already flow through the gRPC path, and [`/api/v1/stream/status`](../filmu_py/api/routes/stream.py) now exposes joined active-session and active-handle summaries so Python control-plane state and Rust mounted-read activity can be correlated from one operator surface.
- The Rust runtime snapshot now also carries first-class upstream failure and retryable-pressure classifications rather than only aggregate read/error totals: invalid URL, request-build, network, stale-status, unexpected-status, read-body, and retryable 429/5xx transport-pressure events are captured in the sidecar snapshot and surfaced on [`/api/v1/stream/status`](../filmu_py/api/routes/stream.py) as additive `vfs_runtime_upstream_*` counters.
- Mounted backend fallback behavior is now first-class too: the Rust sidecar snapshot records backend HTTP fallback attempts, successes, and failures split by `direct_read_failure`, `inline_refresh_unavailable`, and `post_inline_refresh_failure`, and [`/api/v1/stream/status`](../filmu_py/api/routes/stream.py) now exposes those as additive `vfs_runtime_backend_fallback_*` counters instead of leaving fallback behavior implicit in log text.
- Mounted startup latency is now first-class on that same runtime surface: the Rust sidecar snapshot records handle-open to first completed read counts and latency (`handle_startup.total/ok/error/estale`, average/max ms), [`/api/v1/stream/status`](../filmu_py/api/routes/stream.py) now exposes those as additive `vfs_runtime_handle_startup_*` counters, and the Windows soak artifact bundle now preserves the same startup metrics in `runtime_status_delta` / `runtime_diagnostics`.
- Mounted cache-layer visibility is now first-class on that same runtime surface too: the Rust sidecar snapshot records the active cache backend, memory/disk bytes and limits, memory-vs-disk hit/miss totals, and disk write / write-error / eviction counters inside `chunk_cache`, [`/api/v1/stream/status`](../filmu_py/api/routes/stream.py) exposes those as additive `vfs_runtime_chunk_cache_*` fields, and the Windows soak artifact now preserves the same layer-level cache evidence in `runtime_diagnostics`.
- The Windows soak runner now also treats artifact durability as part of the operational contract: once the per-run artifact directory exists, preflight or scenario failures still emit `summary.json` and the tail-log bundle before returning a non-zero exit, so failure diagnosis no longer depends on catching a still-live process before it disappears.
- Windows-native runtime visibility is now also structured on the Rust side itself instead of being only an OTEL/log concern: [`../rust/filmuvfs/src/telemetry.rs`](../rust/filmuvfs/src/telemetry.rs) can now emit a `filmuvfs-runtime-status.json` snapshot with mounted-read result counts, upstream fetch totals, chunk-cache and prefetch summaries, inline-refresh outcomes, current open-handle/active-read/cache gauges, and Windows ProjFS callback summaries, and the Windows stack scripts wire that file into the managed state/evidence directory.
- What is still missing is longer-running soak/backpressure validation across more than one environment class, broader rollout hardening, and continued HTTP/VFS semantic convergence above the now-shared API vocabulary rather than first mounted-read bring-up. On Windows specifically, the WinFSP path is now validated through the native soak/remux gate, Jellyfin software-transcode playback, and sampled native Emby playback/probe/stream-open checks across multiple titles. Recent validation also showed that some apparent direct-stream failures were stale Jellyfin codec metadata mismatches rather than VFS byte corruption, so operator guidance now treats a full item metadata refresh as the first check before reopening the WinFSP read path. The new Windows-native soak runner in [../scripts/run_windows_vfs_soak.ps1](../scripts/run_windows_vfs_soak.ps1) is the current pressure/backpressure gate for that hardening phase, and it now captures both backend `stream/status` snapshots and the sidecar `filmuvfs-runtime-status.json` delta when those files are available, and it now promotes runtime-derived provider-pressure, cold-fetch, stale-read, and fatal-failure checks into thresholded PASS/FAIL gates instead of depending only on log scraping, while the chunk engine continues coalescing in-flight foreground chunk fetches to reduce duplicate upstream work during native-media-server playback.
- The local Docker/WSL validation stack now also provisions isolated Plex and Emby containers for parity testing on top of the same mounted tree. That Docker Plex path is now materially working after fixing WSL host-mount visibility, stale host-binary reuse, refresh-by-provider-file-id collisions, and duplicate foreground fetches, and the proof harness now records Docker/WSL Plex quality signals as explicit artifacted checks in `summary.json` and `plex-wsl-evidence.json` rather than warning-only notes. Those explicit Docker/WSL checks reran green twice on April 9, 2026 after adding the WSL mount preflight plus ANSI-safe evidence parsing. Native Windows media-center support is now treated as a first-class target set for Jellyfin, Emby, and Plex on `C:\FilmuCoreVFS`: the repo exposes `proof:windows:vfs:providers` / `proof:windows:vfs:providers:gate` through [`../scripts/run_windows_media_server_gate.ps1`](../scripts/run_windows_media_server_gate.ps1), current evidence is live-green for Emby and previously validated for Jellyfin, and native Windows Plex is now also live-green through the real local PMS with its local admin token.

## Upstream `feat/rust-vfs` implication

- The upstream `riven-ts` `feat/rust-vfs` branch validates that a split backend/daemon model with an explicit transport contract is worth taking seriously.
- It does **not** currently replace the local TS backend as the behavior baseline, because the audited branch is still thinner in path modeling, read heuristics, chunk reuse, and config/doc alignment.
- Filmu should therefore borrow the **process-boundary idea** without regressing from the current shared serving-core strategy.
- See [`RIVEN_TS_AUDIT.md`](RIVEN_TS_AUDIT.md) for the verified branch-vs-local findings and the current baseline interpretation.

## Compatibility targets

- Preserve stream URL contract under `/api/v1/stream/*`.
- Preserve HLS playlist and segment endpoint behavior.
- Preserve range-request support and streaming headers.
- Preserve a mount-oriented path model that can present Real-Debrid-backed files to Plex/Emby/Jellyfin as ordinary filesystem content on both Linux and Windows hosts.

## Runtime design

1. **FilmuVFS Rust sidecar — first-class requirement**
   - Primary product capability for Plex/Emby/Jellyfin and other filesystem-oriented consumers.
   - Runs as a separate Rust process that now owns the host-filesystem adapter loop (`open` / `read` / `readdir` / `getattr` / `release`), chunk-engine-backed mounted reads, cache engine selection, hidden-path guards, and velocity-aware prefetch while consuming Python-supplied catalog state and stale-link refresh over gRPC.
   - The HTTP path currently uses the Python chunk/range engine, while mounted reads now use a Rust chunk-engine implementation with the same product goal; the remaining work is convergence and hardening rather than first byte-read wiring.
   - Communicates with the Python backend through a WatchCatalog-style bidirectional gRPC catalog channel.
   - Rust is chosen for deterministic memory management with no GC pauses during active reads — critical for smooth playback under cache pressure when serving large media files to Plex/Emby/Jellyfin/Kodi. Rust stack: `tokio` + `hyper` + `tonic` + `bytes` + `moka` + `dashmap` + `tracing` + `opentelemetry-otlp` + `tokio-util`, with `fuse3` on Linux, ProjFS on Windows, and a priority-aware prefetch scheduler for fair multi-session bandwidth sharing.
2. **Link resolver service**
   - Resolves playable links from provider/debrid state, especially Real-Debrid-backed file resources that the FUSE layer will present as files.
3. **Shared chunk/range engine**
   - Common byte-serving core used by both FilmuVFS-oriented reads and HTTP streaming paths.
   - Implemented today in [`../filmu_py/core/chunk_engine.py`](../filmu_py/core/chunk_engine.py) with chunk geometry, read classification, in-memory caching, validated range fetching, and stitch behavior.
4. **HTTP stream compatibility path**
   - Supports frontend and non-mount clients without becoming the primary design center.
5. **Transcode/HLS service**
   - Controlled ffmpeg process model with bounded concurrency.

Boundary ownership:

- The **Python backend** owns provider clients, link resolution, lease refresh, rate limiting, domain state, orchestration, API routes, the catalog-supplier side of the gRPC contract, and the current shared chunk/range engine implementation.
- The **Rust sidecar** owns mount/runtime behavior and will later either call into the shared chunk engine across the process boundary or carry a Rust port of the same semantics.
- The boundary is explicit: Python tells the Rust sidecar what files exist and what URLs serve them via the WatchCatalog-style gRPC channel.
- The Rust sidecar never calls debrid APIs directly.
- The protobuf/gRPC contract must include the correlation-key model from the start: shared session ID, handle key, and provider file identity fields, so cross-process observability does not need to be retrofitted after the Go module exists.

Adapter model:

- Linux hosts mount through the `fuse3` adapter in [`../rust/filmuvfs/src/mount.rs`](../rust/filmuvfs/src/mount.rs).
- Windows hosts mount through the native Windows adapter boundary in [`../rust/filmuvfs/src/windows_host.rs`](../rust/filmuvfs/src/windows_host.rs), with ProjFS in [`../rust/filmuvfs/src/windows_projfs.rs`](../rust/filmuvfs/src/windows_projfs.rs) and WinFSP in [`../rust/filmuvfs/src/windows_winfsp.rs`](../rust/filmuvfs/src/windows_winfsp.rs).
- `FILMUVFS_MOUNT_ADAPTER` and `--mount-adapter` now accept `auto`, `fuse`, `projfs`, and `winfsp`, with `auto` resolving to `fuse` on Linux and `projfs` on Windows in the current build.
- The canonical helper-managed Windows folder mount used in current playback validation is `C:\FilmuCoreVFS`; drive-letter aliases are intentionally not part of the managed path.
- `FILMUVFS_WINDOWS_PROJFS_SUMMARY_INTERVAL_SECONDS` and `--windows-projfs-summary-interval-seconds` control the periodic Windows ProjFS operator summary cadence. The default is `300` on Windows and `0` elsewhere; setting it to `0` disables the background summary task.
- The WSL UNC path (`\\wsl.localhost\...`) is not a product-grade Windows host path. It remains useful for debugging and ad hoc access, but Windows-hosted media servers should target the native Windows adapter mount instead of crossing the WSL UNC bridge.

Current proto-first boundary implementation:

- The contract source of truth is [`../proto/filmuvfs/catalog/v1/catalog.proto`](../proto/filmuvfs/catalog/v1/catalog.proto).
- The current Python-side supplier is [`../filmu_py/services/vfs_catalog.py`](../filmu_py/services/vfs_catalog.py), and the current Python gRPC bridge is [`../filmu_py/services/vfs_server.py`](../filmu_py/services/vfs_server.py).
- The current Rust-side runtime consumer lives under [`../rust/filmuvfs`](../rust/filmuvfs) and now implements the full subscribe/heartbeat/ack/snapshot/delta/removal lifecycle plus the `RefreshCatalogEntry` stale-link refresh seam.
- That supplier already reuses [`../filmu_py/services/playback.py`](../filmu_py/services/playback.py) and [`../filmu_py/services/mount_worker.py`](../filmu_py/services/mount_worker.py) so the future Rust runtime consumes the same persisted media-entry and playback-lifecycle decisions the HTTP surface already uses.
- The Python side no longer stops at snapshot/delta generation alone: it also serves reconnect deltas to known generations and refreshes provider-backed links for mounted stale-read recovery.
- The Rust scaffold now also includes a first mount-facing lifecycle layer plus the Linux `fuse3` adapter and a Windows adapter boundary whose current backend is ProjFS, and the Linux adapter has now been exercised in WSL/Linux through both an automated mount-lifecycle pass and a manual mounted-read smoke pass in this workspace.
- The Rust crate now gates `fuse3` behind Linux-only boundaries and ProjFS behind Windows-only boundaries in [`../rust/filmuvfs/Cargo.toml`](../rust/filmuvfs/Cargo.toml), [`../rust/filmuvfs/src/mount.rs`](../rust/filmuvfs/src/mount.rs), and [`../rust/filmuvfs/src/windows_projfs.rs`](../rust/filmuvfs/src/windows_projfs.rs), while keeping the catalog/runtime/telemetry path shared.
- A Linux-target Cargo compile check now also passes for [`../rust/filmuvfs`](../rust/filmuvfs) with `--target x86_64-unknown-linux-gnu`, which gives the Linux `fuse3` adapter compile-time validation against the intended production platform from this Windows host.
- The shared chunk/range engine now also exists in [`../filmu_py/core/chunk_engine.py`](../filmu_py/core/chunk_engine.py) and is covered by focused regression tests in [`../tests/test_chunk_engine.py`](../tests/test_chunk_engine.py).

Pre-mount validation gate:

- Before any mount lifecycle or FUSE operation work begins, a Cargo-capable host must run [`cargo fmt --manifest-path ./rust/filmuvfs/Cargo.toml --all --check`](../package.json), [`cargo check --manifest-path ./rust/filmuvfs/Cargo.toml`](../package.json), and [`cargo test --manifest-path ./rust/filmuvfs/Cargo.toml`](../package.json) through the workspace scripts in [`package.json`](../package.json).
- That validation must confirm the generated bindings expose the correlation-key model from the proto contract, especially `session_id`, `handle_key`, `provider_file_id`, and `provider_file_path`, which are guarded in [`rust/filmuvfs/src/proto.rs`](../rust/filmuvfs/src/proto.rs).
- When later proto revisions add new cross-process observability or correlation fields, those same guard tests in [`rust/filmuvfs/src/proto.rs`](../rust/filmuvfs/src/proto.rs) should be extended in the **same change** so the validation gate grows with the contract rather than lagging behind it.

Current validation status:

- The Windows contract/compile gate has passed: [`rust:fmt`](../package.json) ✅, [`rust:check`](../package.json) ✅, [`rust:test`](../package.json) ✅, and the stricter local `cargo clippy --manifest-path ./rust/filmuvfs/Cargo.toml --all-targets --all-features -- -D warnings` gate is also clean for [`../rust/filmuvfs`](../rust/filmuvfs).
- The Windows-native adapter boundary now compiles and passes the Rust test gate on this host, so Windows no longer relies on `\\wsl.localhost` as its intended mounted playback topology. The current build still defaults `auto` to ProjFS, while the native WinFSP folder-mount path has now also been validated through direct `ffprobe`, the native soak/remux gate, sustained Jellyfin reads, successful software transcode, native Emby playback/probe/stream-open checks, and native Windows Plex through the real local PMS on `http://127.0.0.1:32400`.
- Focused Windows adapter coverage now also verifies per-stream handle reuse and cleanup inside [`../rust/filmuvfs/src/windows_projfs.rs`](../rust/filmuvfs/src/windows_projfs.rs), which is the first step toward smoother sequential-read behavior on Windows-hosted media servers.
- Those Windows-specific metrics are intended to answer the next operational questions directly: whether Jellyfin/Plex/Emby is reusing stream handles, how long `GetFileData` callbacks take, and whether native close notifications arrive as expected under sequential playback.
- The slow-callback warning path is intended for the common Windows-host case where operators have logs but not a full OTLP collector. It is a debugging fallback, not a replacement for exported metrics.
- The periodic Windows summary path is intended for the same operator case: long-running Jellyfin/Plex/Emby sessions can now surface callback-latency and handle-reuse drift without waiting for a clean sidecar shutdown.
- The automated WSL/Linux mount lifecycle gate now also passes through [`../rust/filmuvfs/tests/mount_lifecycle.rs`](../rust/filmuvfs/tests/mount_lifecycle.rs) and the helper script [`../rust/filmuvfs/scripts/run_mount_lifecycle_gate.sh`](../rust/filmuvfs/scripts/run_mount_lifecycle_gate.sh).
- A manual WSL mounted-read smoke path now also passes through [`../rust/filmuvfs/scripts/run_manual_mount_smoke.sh`](../rust/filmuvfs/scripts/run_manual_mount_smoke.sh): the sidecar mounts `/movies` and `/shows`, `stat` succeeds on real catalog-backed files, and a mounted `head -c 1000` read returns bytes.
- The Plex/Emby playback-validation leg has now also passed in this workspace, so the Linux-host validation gate in [`FILMUVFS_LINUX_HOST_VALIDATION_RUNBOOK.md`](FILMUVFS_LINUX_HOST_VALIDATION_RUNBOOK.md) should now be treated as a regression checklist rather than an outstanding blocker.
- The Python VFS tests in [`../tests/test_vfs_catalog.py`](../tests/test_vfs_catalog.py) and [`../tests/test_vfs_server.py`](../tests/test_vfs_server.py) now cover generation reuse, reconnect deltas, and forced provider refresh, while Rust coverage in [`../rust/filmuvfs/tests/read_path.rs`](../rust/filmuvfs/tests/read_path.rs) and [`../rust/filmuvfs/tests/catalog_state.rs`](../rust/filmuvfs/tests/catalog_state.rs) covers inline stale-link refresh and inode-collision fallback.
- [`../tests/test_vfs_catalog.py`](../tests/test_vfs_catalog.py) now also covers normalized show directory output, season-folder grouping, `S05x08` provider-path inference, and delta removals for path changes on existing catalog entries.
- The Python gRPC bridge now also refreshes stale provider-backed direct links at serve time in [`../filmu_py/services/vfs_server.py`](../filmu_py/services/vfs_server.py), and the Rust sidecar now consumes that refresh path inline rather than surfacing immediate `ESTALE` failures for those reads.
- The Windows/WSL testing path now also has one-command orchestration through [`../start_local_stack.ps1`](../start_local_stack.ps1), [`../status_local_stack.ps1`](../status_local_stack.ps1), and [`../stop_local_stack.ps1`](../stop_local_stack.ps1), plus stale-mount detection/recovery in [`../rust/filmuvfs/scripts/start_persistent_mount.sh`](../rust/filmuvfs/scripts/start_persistent_mount.sh), [`../rust/filmuvfs/scripts/stop_persistent_mount.sh`](../rust/filmuvfs/scripts/stop_persistent_mount.sh), and [`../rust/filmuvfs/scripts/persistent_mount_status.sh`](../rust/filmuvfs/scripts/persistent_mount_status.sh).
- That local stack now also exposes Plex on `http://localhost:32401/web` and Emby on `http://localhost:8097`, and the playback-proof harness can source their local URLs/auth tokens from [`.env`](../.env) the same way it now does for Jellyfin.

## What is missing now (implementation checklist)

1. Continue converging mounted-read behavior with the HTTP serving model: read-pattern semantics, failure taxonomy, provider-pressure handling, and cache observability.
2. Harden and operationally validate the now-implemented optional disk/persistent cache and adaptive prefetch policy under longer-running media workloads.
3. Deepen soak/backpressure validation and failure recovery around the existing inode-based `open`/`read`/`readdir`/`getattr`/`release` adapters on both Linux and Windows hosts.
4. Extend stream/VFS-focused metrics into the mounted data plane (`active_streams`, `stream_failures`, cache-hit ratio, mount latency, fallback counters, reconnect delta health`); startup latency is now exposed through `vfs_runtime_handle_startup_*`.
5. Keep refining the shared link-resolver/control-plane story so mounted stale-read recovery, reconnect deltas, and provider-pressure governance stay aligned with the HTTP path.
6. Production-grade HLS playlist/segment pipeline with bounded ffmpeg worker pool, cleanup, and lifecycle controls.
7. End-to-end integration tests for FilmuVFS + range/HLS parity under longer-running workloads.
8. Keep the now-validated WinFSP playback path green while the policy/default Windows path still resolves to ProjFS, keep the now-working Docker Plex path and its explicit evidence checks green through repeatable proof coverage, keep the now-green native Windows Plex provider gate green through repeatable proof coverage, and promote those gates into CI/merge policy.

## Current scope note for lifecycle projection

- The new lifecycle projection now reaches the internal resolved direct/HLS playback snapshot in [`filmu_py/services/playback.py`](../filmu_py/services/playback.py) without introducing new persistence reads or schema changes.
- It has **not** been pushed into true mount/open-path consumers yet.
- [`../filmu_py/services/mount_worker.py`](../filmu_py/services/mount_worker.py) now marks the correct separate boundary for that future work, defines the explicit query contract the mount worker will need, and already implements the concrete persisted-query executor against the existing `media_entries` + `active_streams` model.
- The Rust sidecar now also has a first mount-facing lifecycle layer in [`../rust/filmuvfs/src/mount.rs`](../rust/filmuvfs/src/mount.rs) that consumes the already-materialized catalog state without flattening the hierarchy.
- The remaining gap is no longer first Linux-side mount bring-up, first Windows-native mount bring-up, first media-server playback success, or first mounted byte-read wiring; it is hardening the existing Rust read path, validating cache/prefetch behavior under load, and deepening cross-process observability and failure handling.

## Current open risk

- The earlier reconnect/repoll control-plane issue has now been materially reduced by generation-aware reconnect deltas in [`../filmu_py/services/vfs_catalog.py`](../filmu_py/services/vfs_catalog.py) and [`../filmu_py/services/vfs_server.py`](../filmu_py/services/vfs_server.py), plus inline stale-link refresh in [`../rust/filmuvfs/src/mount.rs`](../rust/filmuvfs/src/mount.rs). The remaining risk is long-running operational behavior of the already-live mounted read path, especially cache/prefetch behavior, backpressure, cross-process observability under sustained playback load, and the fact that the current Windows ProjFS backend still shows probe-hostile oversized callback patterns for media workloads. Treat VFS production readiness as a soak-validation and data-plane-hardening problem rather than a first-control-plane-contract problem.

## Reliability constraints

- Enforce per-provider and per-stream concurrency limits.
- Add timeout budgets for upstream read and transcode startup.
- Emit metrics for active streams, failures, and fallback behavior.
- Keep FilmuVFS byte-serving reads off any future durable event backbone; events may coordinate control-plane behavior only.

## Future implementation notes

- Keep VFS internals decoupled from REST handlers.
- Treat FilmuVFS as the primary streaming product path for Real-Debrid-backed library consumption; HTTP stream support should complement it, not replace it.
- Add contract tests for key stream response headers.
- Add canary mode for stream path migration.
- Treat FilmuVFS as a required system constraint when choosing future event backplane architecture; see [`HYBRID_EVENT_BACKPLANE_RESEARCH.md`](HYBRID_EVENT_BACKPLANE_RESEARCH.md).






