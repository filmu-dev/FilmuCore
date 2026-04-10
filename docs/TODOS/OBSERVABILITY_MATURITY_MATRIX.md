# Observability Maturity Matrix

## Purpose

Turn Priority 6 from [`NEXT_IMPLEMENTATION_PRIORITIES.md`](NEXT_IMPLEMENTATION_PRIORITIES.md) into an executable planning artifact.

This document maps the current `filmu-python` observability baseline against the broader visibility needed for:

- current frontend support
- worker/retry safety
- plugin platform growth
- FilmuVFS and streaming performance work
- eventual durable orchestration growth

---

## Current Python observability baseline

Already present in the Python backend:

- structured logging in `filmu_py/logging.py`
- durable rotating ECS/NDJSON-style file output in `logs/ecs.json` via [`../../filmu_py/logging.py`](../../filmu_py/logging.py)
- request correlation via `RequestIdMiddleware`
- route-level compatibility counters and latency histograms in [`../../filmu_py/api/router.py`](../../filmu_py/api/router.py)
- Prometheus/OpenTelemetry bootstrap in `filmu_py/observability.py`
- worker stage/retry/DLQ metrics plus `structlog.contextvars` correlation in [`../../filmu_py/workers/retry.py`](../../filmu_py/workers/retry.py) and [`../../filmu_py/workers/tasks.py`](../../filmu_py/workers/tasks.py)
- cache hit/miss/invalidation/stale counters in [`../../filmu_py/core/cache.py`](../../filmu_py/core/cache.py)
- rate-limiter allow/deny/remaining/retry-after metrics in [`../../filmu_py/core/rate_limiter.py`](../../filmu_py/core/rate_limiter.py)
- plugin load and hook execution/duration metrics in [`../../filmu_py/plugins/loader.py`](../../filmu_py/plugins/loader.py) and [`../../filmu_py/plugins/hooks.py`](../../filmu_py/plugins/hooks.py)
- GraphQL operation duration/outcome metrics in [`../../filmu_py/graphql/observability.py`](../../filmu_py/graphql/observability.py)
- ARQ queue lag/backlog gauges plus operator route visibility in [`../../filmu_py/core/queue_status.py`](../../filmu_py/core/queue_status.py) and [`../../filmu_py/api/routes/default.py`](../../filmu_py/api/routes/default.py)
- extensive HLS status/governance metrics
- latency histograms for reads, resolutions, and ffmpeg generation
- abort and request-shape (range/seek/EOF) counters
- session-level read amplification proxies
- logs/history SSE compatibility path
- bounded in-memory log broker for `/api/v1/logs`, SSE logging, and GraphQL `logStream`

Metric naming contract:

- Python-backend-wide observability now uses the `filmu_py_*` namespace.
- Shared streaming and chunk-engine families that are already intentionally product-scoped remain under their existing `filmu_*` names.
- Historical misspelled legacy metric names are deprecated and should not be extended.

What this baseline is good at:

- basic request-level diagnostics
- route-level compatibility visibility
- first worker and plugin runtime visibility
- startup/boot visibility
- internal counters across cache, worker, plugin, and serving layers
- current frontend log/history compatibility

What it does **not** yet provide sufficiently:

- operator-ready local log shipping and search comparable to the current `riven-ts` Elastic/Filebeat path
- richer trace/span adoption across every log-producing path
- mounted Rust data-plane telemetry and cross-process traceability
- queue lag / DLQ age-size history, alerting thresholds, and broader replay-taxonomy visibility
- durable event/control-plane observability

To check progressively and update as we go.

- rtn
- torrent ranking
- subsitles from subtitle providers
- performance metrics
- error metrics
- error recovery
- error handling
- error reporting
- error logging
- BFF

---

## Observability maturity matrix

| Observability area                    | Current Python state           | Missing maturity                                                                                                              | Why it matters                                                       | Priority |
| ------------------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- | -------- |
| **HTTP request correlation**          | Implemented baseline           | Broaden into deeper service/control-plane correlation without high-cardinality labels                                          | Needed for route debugging and frontend integration support          | **P1**   |
| **Route-level compatibility metrics** | Implemented baseline           | Deeper failure taxonomy, auth/contract drift breakdown, and broader surface coverage                                           | Needed to know which `/api/v1/*` surfaces are unstable or incomplete | **P1**   |
| **Worker correlation**                | Implemented baseline           | Broader multi-stage lifecycle correlation across API -> queue -> worker -> playback/VFS paths                                   | Needed for reliable debugging of orchestration failures              | **P1**   |
| **Retry/DLQ visibility**              | Improved baseline              | retry counts over time, DLQ age/size/reason history, and replay-taxonomy rollups                                               | Needed for D1 and future D2 maturity                                 | **P1**   |
| **Plugin load telemetry**             | Implemented baseline           | richer startup health rollups and longer-lived plugin health summaries                                                         | Needed as plugin platform grows beyond GraphQL                       | **P1**   |
| **Plugin runtime telemetry**          | Implemented in-process baseline | per-plugin error/timeout health rollups, and queue lag only if/when hook execution becomes durable/queued                     | Needed for safe extensibility                                        | **P1**   |
| **Structured log pipeline**           | Implemented baseline            | shipper/search workflow, environment-specific shipping policy, and stronger end-to-end trace/span adoption                       | Current `riven-ts` now has a materially stronger operator log pipeline | **P1**   |
| **GraphQL observability**             | Implemented baseline           | richer error taxonomy, schema diff governance, and subscription/control-plane visibility                                        | Needed for strategic parity and regression detection                 | **P1**   |
| **Cache observability**               | Implemented baseline           | hit/miss split by layer, richer invalidation reasons, and longer-lived stale-serve ratios                                      | Needed for correctness and provider safety                           | **P1**   |
| **Rate limiter observability**        | Implemented baseline           | provider-refresh/control-plane correlation and alerting thresholds                                                              | Needed for provider safety and FilmuVFS control-plane tuning         | **P1**   |
| **Stream/VFS observability**          | Strong HTTP baseline + limited mount baseline | deepen the current chunk-engine metrics into route/mount-driven amplification, plus richer lease-refresh and prefetch time series | Needed to outperform TS in FilmuVFS/product streaming                | **P1**   |
| **Control-plane/event observability** | Improved baseline              | event publish lag, bridge lag, replay metrics, lease refresh events, and queue-history/alerting surfaces                       | Needed for future backplane work                                     | **P1**   |
| **Durable workflow observability**    | Not started                    | workflow progress, signal/query visibility, compensation/failure telemetry                                                    | Needed only once D2 begins                                           | **P3**   |

---

## Recommended observability layers

### Layer 1 — Current platform safety

This layer is now largely landed:

- route-level compatibility metrics
- worker correlation IDs plus stage timing/retry/DLQ metrics
- cache hit/miss/invalidation/stale metrics

Remaining Layer 1 gap:

- shipper/search workflow above the now-landed durable structured logs
- wider trace/span propagation across API, workers, plugin paths, and Rust-sidecar correlation

This is now the active baseline for the current expanding backend rather than a purely future plan.

### Layer 2 — Plugin platform visibility

This layer now has its first baseline too:

- plugin discovery/load/failure metrics
- plugin hook execution counts and latency
- timeout/error outcomes for in-process hook execution

Still missing here:

- richer per-plugin health rollups
- per-plugin queue lag only if/when hook execution becomes durable or queued

This should continue deepening before the plugin platform becomes much broader.

### Layer 3 — FilmuVFS / byte-serving visibility

Add as VFS/stream work progresses:

- **Rust sidecar-emitted metrics** should cover:
  - active mount sessions
  - open file handles
  - chunk cache hit ratios
  - read amplification
  - seek/scan patterns
  - upstream range latency per chunk fetch
  - prefetch queue depth
- The current Rust sidecar baseline now already emits `filmuvfs_mounted_read_duration_seconds`, `filmuvfs_chunk_cache_events_total`, `filmuvfs_chunk_read_patterns_total`, `filmuvfs_prefetch_events_total`, `filmuvfs_inline_refresh_total`, and `filmuvfs_chunk_cache_weighted_bytes`.
- The Windows-native soak runner in [`../../scripts/run_windows_vfs_soak.ps1`](../../scripts/run_windows_vfs_soak.ps1) is now the operator-facing evidence bundle for those signals on `C:\FilmuCoreVFS`.
- The Windows-native ProjFS path now also has an operator fallback when no OTLP collector is present:
  - slow `GetFileData` callbacks log structured warnings
  - sidecar shutdown logs a `windows projfs adapter summary` with callback counts/latency, stream-handle reuse/open/release totals, and ProjFS notification totals
  - long-running Windows hosts now also emit that same summary periodically by default every 300 seconds via `FILMUVFS_WINDOWS_PROJFS_SUMMARY_INTERVAL_SECONDS` / `--windows-projfs-summary-interval-seconds`, and `0` disables it
- The Windows WinFSP path is now also an active observability target, because the current Windows-host playback validation runs through the raw WinFSP folder mount at `C:\FilmuCoreVFS` rather than through WSL UNC. The current gap there is no longer first playback reachability or first metrics: the Rust sidecar now emits mounted-read duration, chunk-cache events, read-pattern, prefetch, inline-refresh, and cache-size telemetry there too, and the native soak/regression gate is now green. The remaining gap is turning those signals into richer operator-facing summaries for cache pressure, in-flight chunk coalescing, upstream wait behavior, and longer-run multi-media-server parity.
- The Linux/WSL host-mount path is now also a real observability target for Docker Plex parity. Recent work proved that stale host-binary reuse, host-mount visibility, and entry-id refresh collisions can all masquerade as "Plex playback failures", so the next observability slice should make those classes first-class in the evidence bundle instead of requiring ad hoc log forensics.
- **Cross-process correlation** will be required between the Python backend and the Rust sidecar.
  At minimum, a shared session/handle key must propagate through the gRPC catalog channel so Python-side lease events and Rust-side read events can be correlated in the same trace or metric dimension.
- `tracing` + `opentelemetry-otlp` in the Rust sidecar should plug into the Python backend's existing OpenTelemetry pipeline so those shared session/handle keys become visible across process boundaries without manual trace stitching.
- Before any mount lifecycle work begins, Cargo validation on a Rust-capable host should explicitly prove the generated bindings still expose `CatalogCorrelationKeys.session_id`, `CatalogCorrelationKeys.handle_key`, `CatalogCorrelationKeys.provider_file_id`, and `CatalogCorrelationKeys.provider_file_path`; the guard tests now live in [`../../rust/filmuvfs/src/proto.rs`](../../rust/filmuvfs/src/proto.rs).
- The **Python backend** remains responsible for:
  - lease refresh success/failure rates
  - playback-risk events
  - route outcome counters
  - HLS metrics
  - all existing Prometheus counters already emitted today
- active mount sessions
- open file handles
- route outcome counters for direct/HLS playback
- HLS generation result counters
- playback lease failure / playback-risk counters
- HLS generation duration histograms
- remote proxy latency histograms
- playback-resolution duration histograms
- normalized HLS route failure counters by reason
- abort/cancellation counters by serving category
- request-shape counters for full/range/suffix traffic and full/partial outcomes
- per-read size histograms and lightweight small/medium/large read buckets
- session-level read-operations-per-open and bytes-per-read proxy histograms
- pre-chunk seek/scan-pattern buckets at the request boundary
- chunk read-type classification counters
- chunk cache hits, misses, evictions, and current bytes
- chunk fetch bytes and latency histograms
- chunk cache hit ratios and end-to-end read amplification once the engine is wired into live reads
- seek/scan patterns
- upstream range latency
- lease refresh success/failure rates
- HLS startup and segment timing

This is crucial if the goal is to exceed the TS backend rather than just match it.

### Logging delta vs current `riven-ts`

Current upstream `riven-ts` logging is materially broader than the current Filmu baseline:

- Winston transport stack in `apps/riven/lib/utilities/logger/logger.ts`
- ECS NDJSON output to `logs/ecs.json`
- optional durable local `combined.log`, `error.log`, and `exceptions.log`
- Sentry trace/span enrichment via `sentry-meta.format.ts`
- source-tagged console output with `riven.log.source` and `riven.worker.id`
- local Elastic/Filebeat/Kibana stack under `elastic-local/`

Filmu currently has:

- stdlib logging + `structlog` JSON rendering in [`../../filmu_py/logging.py`](../../filmu_py/logging.py)
- compatibility-oriented bounded in-memory log history and live fan-out in [`../../filmu_py/core/log_stream.py`](../../filmu_py/core/log_stream.py)

That means Filmu still needs a real operator log pipeline, not just a compatibility log surface.

Minimum bar for closing that gap:

- file-backed structured logs outside process memory
- explicit retention/rotation policy
- shipper-friendly JSON or ECS-like log format
- trace/span/request/worker/plugin correlation fields in every emitted record
- documented local operator flow for searching and shipping logs

### Layer 4 — Durable event/workflow visibility

Add later if/when the system adopts a stronger event/workflow backbone:

- event publish lag
- replay lag
- bridge lag
- consumer lag
- workflow checkpoint progression
- compensation/failure metrics

---

## Correlation model to adopt

Observability should be anchored on explicit correlation keys rather than only human-readable logs.

Recommended correlation dimensions:

- request ID
- item ID
- item request ID
- workflow/job ID
- plugin name
- provider/downloader name
- stream/session ID
- FilmuVFS handle/session key

These keys should be propagated consistently across:

- API logs
- worker logs
- plugin logs
- stream/VFS events
- metrics labels (selectively, to avoid cardinality explosions)

---

## What not to do

- Do **not** treat logs alone as observability.
- Do **not** add metrics with uncontrolled label cardinality.
- Do **not** grow the plugin/runtime/VFS platform faster than its visibility.
- Do **not** defer stream/VFS metrics until after the byte-serving path is “done”.

---

## Minimum implementation sequence for Priority 6

1. route-level compatibility metrics — delivered
2. worker correlation + retry/DLQ metrics — delivered
3. cache + rate-limiter visibility by operation class — delivered
4. plugin discovery/runtime metrics — delivered for the current in-process runtime
5. GraphQL operation metrics plus queue/control-plane lag visibility — delivered baseline
6. stream/VFS metrics as soon as byte-serving implementation starts — ongoing on the HTTP path; mount data-plane visibility still remaining
7. event/workflow observability only when the corresponding platform layer becomes real

---

## Success checkpoint

Priority 6 should be considered meaningfully advanced when:

- missing/unstable route families can be identified from metrics, not guesswork
- retries and DLQ behavior are visible enough to support D1 hardening confidently
- plugin failures can be isolated and attributed cleanly
- FilmuVFS/stream performance can be measured objectively rather than inferred from anecdotal playback behavior

## Layer-1 implementation update (March 2026)

- Route-level metrics now live in [`../../filmu_py/api/router.py`](../../filmu_py/api/router.py) and use route templates to avoid uncontrolled path-label cardinality.
- Worker observability now lives in [`../../filmu_py/workers/retry.py`](../../filmu_py/workers/retry.py) plus [`../../filmu_py/workers/tasks.py`](../../filmu_py/workers/tasks.py), including stage duration, retry, and dead-letter counters plus `structlog.contextvars` binding for `item_id`, `item_request_id`, `worker_stage`, and `job_id`.
- Cache observability now lives in [`../../filmu_py/core/cache.py`](../../filmu_py/core/cache.py), including hit/miss/invalidation/stale-serve counters.
- Rate-limiter observability now lives in [`../../filmu_py/core/rate_limiter.py`](../../filmu_py/core/rate_limiter.py), including allow/deny counts, remaining-token histograms, and retry-after histograms by bounded bucket class.
- Plugin observability now lives in [`../../filmu_py/plugins/loader.py`](../../filmu_py/plugins/loader.py) and [`../../filmu_py/plugins/hooks.py`](../../filmu_py/plugins/hooks.py), including load outcomes plus hook invocation/duration telemetry with success/error/timeout outcomes.
- GraphQL observability now lives in [`../../filmu_py/graphql/observability.py`](../../filmu_py/graphql/observability.py), including operation counters and duration histograms by operation type and bounded root-field labels.
- Queue/control-plane visibility now lives in [`../../filmu_py/core/queue_status.py`](../../filmu_py/core/queue_status.py) plus [`../../filmu_py/api/routes/default.py`](../../filmu_py/api/routes/default.py), including queue depth, ready-vs-deferred lag, retry/result counts, dead-letter counts, and exported gauges.
- Structured logging now also has a durable file-backed baseline in [`../../filmu_py/logging.py`](../../filmu_py/logging.py), with rotating ECS/NDJSON-style output, correlation filters, and operator-ready local retention settings.
- Dedicated coverage now exists in [`../../tests/test_observability.py`](../../tests/test_observability.py).
- The full Python verification gate for this layer is currently green at `628 passed`, with the observability surfaces still covered by `ruff check .`, `mypy --strict filmu_py/`, and `pytest -q`.

## Serving-status update (March 2026)

- The backend now has a richer internal visibility surface at [`/api/v1/stream/status`](../../filmu_py/api/routes/stream.py) for serving sessions, HLS governance counters, tracked media-entry lease pressure, and selected-stream failure/refresh counts.
- The backend now also emits first Prometheus counters for HLS generation outcomes, lease-refresh failure classes, playback-risk events, and direct/HLS route outcomes.
- The backend now also emits first latency histograms for HLS generation, remote proxy open latency, and playback-resolution duration.
- The backend now also emits abort/cancellation counters and exposes aggregate abort counts through the serving-governance snapshot.
- The backend now also emits request-shape counters for full-file, range, suffix-range, and partial-content outcomes in the shared serving core.
- The backend now also emits per-read size histograms and small/medium/large read buckets, which gives the platform its first lightweight read-shape proxy below the request level.
- The backend now also emits first session-level read-amplification proxy histograms based on read operations and bytes served per serving session.
- The backend now also emits first pre-chunk seek/scan-pattern buckets, which makes the request boundary more informative before true chunk/read-type logic exists.
- The backend now also emits first chunk-engine-native Prometheus metrics from [`../../filmu_py/core/chunk_engine.py`](../../filmu_py/core/chunk_engine.py), including read-type classification counters plus chunk-cache hit/miss/eviction/bytes and chunk-fetch byte/duration telemetry.
- The backend now also exposes normalized HLS route failure counters by reason through [`/api/v1/stream/status`](../../filmu_py/api/routes/stream.py), which is the first operator-facing taxonomy above raw HLS `503` route outcomes.
- The backend now also exposes `generated_missing` and `upstream_failed` in that HLS route taxonomy, so status-surface observability now covers both normalized `503` cases and the main remaining non-`503` HLS route failures.
- The backend now also exposes `upstream_manifest_invalid` in that HLS route taxonomy, so malformed remote playlists have a dedicated operator-facing classification instead of being hidden inside generic upstream failures.
- The backend now also records explicit timeout/error upstream open outcomes for remote proxy playback and maps remote-HLS playlist fetch / segment proxy transport failures to explicit `504` / `502` responses, which makes that transport layer more observable and predictable.
- The backend now also exposes remote-HLS retry/cooldown counters through [`/api/v1/stream/status`](../../filmu_py/api/routes/stream.py), including retry attempts, cooldown starts, cooldown hits, and currently active cooldown windows.
- This is a stronger baseline than the original serving-only status view, and the Rust sidecar now also emits mounted-read cache/pattern/prefetch/refresh metrics plus cache-layer backend/memory/disk breakdown from live WinFSP/ProjFS reads. It is still below the fuller stream/VFS observability model described in this matrix because provider-pressure rollups, prefetch-depth visibility, and true end-to-end amplification summaries still remain.




