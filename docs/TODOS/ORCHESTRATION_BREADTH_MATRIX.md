# Orchestration Breadth Matrix

## Purpose

Turn the active orchestration track from [`../STATUS.md`](../STATUS.md) and [`../EXECUTION_PLAN.md`](../EXECUTION_PLAN.md) into an executable planning artifact.

This document maps:

- the **current Python orchestration baseline**
- the **broader TS-origin execution model**
- the **missing stages, recovery paths, and plugin execution capabilities**

It is intentionally focused on orchestration breadth, not long-term durable workflow adoption.

---

## Current Python orchestration baseline

Today the Python backend has:

### Domain state

- deterministic item state transitions in [`filmu_py/state/item.py`](../../filmu_py/state/item.py)
- persisted lifecycle events in [`filmu_py/db/models.py`](../../filmu_py/db/models.py)

### Worker baseline

- real provider-backed worker pipeline in [`filmu_py/workers/tasks.py`](../../filmu_py/workers/tasks.py)
- retry/dead-letter baseline in [`filmu_py/workers/retry.py`](../../filmu_py/workers/retry.py)

### Implemented stages

- `scrape_item` — resolves plugin registry (filesystem + built-in Prowlarr/RARBG/Torrentio scrapers), fan-out search, persists raw candidates to `ScrapeCandidateORM`, enqueues parse stage
- `parse_scrape_results` — parses raw torrent names, validates content compatibility, persists parsed candidates
- `rank_streams` — RTN-compatible Levenshtein + multi-axis scoring, fetch-check filtering, durable score persistence
- `select_stream_candidate` — deterministic container selection by rank_score/lev_ratio/stream_id
- `debrid_item` — resolves enabled provider, calls provider download-pipeline client, persists `MediaEntryORM` rows with BIGINT file sizes after migration `20260318_0018`
- `finalize_item` — completes the lifecycle, emits outbox events
- `recover_incomplete_library` — scheduled cron (every 15 min), scans failed/stale items, re-enters correct pipeline stage
- `publish_outbox_events` — scheduled cron (every 30 sec), drains unpublished outbox rows

### What this baseline is good at

- real provider-backed pipeline execution end-to-end
- validating state transitions and durable stream/media-entry persistence
- RTN-compatible ranking with configurable quality policy
- transactional outbox for publish consistency
- stage-aware retry/recovery with deduplication

### What it does **not** yet cover well

- queue-backed link-resolver dedup for VFS beyond the new mount-side inline refresh dedup baseline
- broader queue-lag/operator visibility and stronger enqueue-dedup/idempotency boundaries across the widened worker graph
- deeper process/sandbox isolation beyond the current bounded heavy-stage executor budgets

---

## Reference breadth from the original TS backend

The current local TS working tree includes a much broader execution model with distinct flow families and stage boundaries, including:

- explicit `xstate` machine hierarchy across program lifecycle, bootstrap, plugin registration, and main-runner steady state
- request-content-services intake
- index-item flow
- scrape-item flow
- **parse-scrape-results step** — calls `parse(rawTitle)` via `@repo/util-rank-torrent-name` on every scraper result, validates Zod `ParsedDataSchema` (40+ fields), then runs `validateTorrent()` for content-level checks (country, year, season/episode matching against the requested `MediaItem`)
- download-item flow
- **rank-streams step** — instantiates the RTN class with a configurable `Settings` + `RankingModel`, calls `rankTorrent()` per stream (hash validation → Levenshtein title similarity → additive scoring → 8-stage fetch-check pipeline), then bucket-sorts results by resolution
- **find-valid-torrent step**
- retry-library recovery
- sandboxed heavy-stage jobs on disk for parse/map/validate work
- queue-backed `stream-link-requested` handling on the VFS side
- downloader provider-list / cache-check hook families before final download resolution
- plugin hook workers on typed events
- publishable-event governance for plugin fan-out

The parse/rank/validation stages are backed by the `@repo/util-rank-torrent-name` package (see [`rtn_research_report.md`](../rtn_research_report.md) for full details).

This is the reference breadth the Python roadmap should explicitly account for.

---

## Orchestration breadth matrix

| Capability                           | Current Python state                                  | TS reference breadth                                   | Why it matters                                                            | Status                 |
| ------------------------------------ | ----------------------------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------------------- | ---------------------- |
| **Request-content-services intake**  | Overseerr/Jellyseerr webhook intake plus scheduled content-service polling now exist, with partial-range tracking on `ItemRequestORM` | Explicit TS flow exists                                | Separates inbound request ingestion from later lifecycle stages           | ✅ Done — Overseerr webhook at `/api/v1/webhook/overseerr` + `poll_content_services` ARQ cron (30 min) + partial season/episode range on `ItemRequestORM` (migration 0019) |
| **Index stage**                      | Dedicated `index_item` worker stage exists, item intake/recovery now re-enter through it, and metadata enrichment no longer hides inside `scrape_item` | Explicit TS flow exists                                | Required for metadata enrichment and correct lifecycle progression        | ✅ Done / deepen       |
| **Formal runtime lifecycle graph**   | Explicit runtime lifecycle state now tracks bootstrap, plugin registration, steady state, degraded startup, and shutdown on `/api/v1/operations/runtime` | TS `program` / `bootstrap` / `plugin-registrar` / `main-runner` hierarchy exists | Keeps boot, plugin registration, and runtime transitions explicit and observable | ✅ Done baseline       |
| **Scrape stage**                     | ✅ Real plugin-backed provider stage with `ScrapeCandidateORM` persistence and built-in Prowlarr/RARBG/Torrentio scrapers | Explicit TS flow exists                                | Core discovery stage                                                      | ✅ Done                |
| **Parse-scrape-results stage**       | ✅ Dedicated `parse_scrape_results` ARQ stage parses torrent names, validates content compatibility, persists parsed candidates to `StreamORM` | Separate TS step (`parse-scrape-results.processor`)    | Keeps scrape collection and parse/validate separated                      | ✅ Done                |
| **Torrent-file validation / selection** | ✅ `select_stream_candidate()` chooses best passing candidate by rank_score/lev_ratio/part-aware tie-break, persists `StreamORM.selected` | TS now centers on `find-valid-torrent` plus sandboxed `validate-torrent-files` work | Important for correctness and provider/debrid decoupling                  | ✅ Done baseline       |
| **Ranking stage**                    | ✅ `rank_streams` ARQ stage with RTN-compatible Levenshtein + multi-axis scoring, fetch-check filtering, configurable `RankingModel`, and partial-scope coverage bonus | Separate TS step (`rank-streams.processor`)            | Needed for repeatable selection logic and configurable quality policy      | ✅ Done / deepen       |
| **Download fan-out / orchestration** | ✅ `debrid_item` calls enabled provider download-pipeline client, persists `MediaEntryORM` rows | TS has broader fan-out behavior                        | Supports partial success and richer downloader selection                   | ✅ Done (single-provider) |
| **Retry-library recovery**           | ✅ Scheduled `recover_incomplete_library` cron (15 min), stage-aware re-entry, deduplication | TS has explicit actor/flow                             | Crucial for restart recovery and incomplete-library progression           | ✅ Done                |
| **Scheduled reindex / reconciliation** | `index_item` exists, but there is no first-class scheduled reindex program or operator-facing reconciliation surface yet | TS `schedule-reindex` actor exists                     | Keeps metadata fresh without relying only on ad hoc item-triggered indexing | 🔶 Partial             |
| **Queue-backed stream-link resolution** | Mount-side inline stale-link refresh and dedup still exist in Rust, and the Python control plane now has an optional queued refresh dispatch path for direct-play and HLS refresh work | TS VFS `open` publishes `stream-link-requested` and dedups at queue level | Matters if link resolution pressure needs to be separated from read/open latency | ✅ Done baseline / deepen |
| **Plugin hook workers**              | Implemented baseline (in-process typed hook dispatch with timeout isolation) | TS has queue-backed plugin hook workers                | Required for plugin platform parity beyond capability-protocol contributions | Done baseline / deepen |
| **Publishable-event governance**     | Implemented baseline                                  | TS tracks publishable events explicitly                | Prevents queue buildup and undefined plugin-event fan-out                 | Done baseline          |
| **Transactional outbox**             | ✅ `OutboxEventORM` + scheduled `publish_outbox_events` cron (30 sec) | Not identical in TS, but needed for Python correctness | Needed for publish consistency and replay-safe growth                     | ✅ Done                |
| **Idempotency boundaries**           | Stable job ids, stage-idempotency counters, DLQ reason codes, and queue-history DLQ taxonomy now exist across the widened worker graph | Needed regardless of TS                                | Required for safe retries, replays, and recovery                          | ✅ Done baseline / deepen |
| **Heavy-stage isolation**            | `index_item`, `parse_scrape_results`, and `rank_streams` now run under bounded isolated stage budgets and explicit timeouts | TS keeps sandboxed heavy jobs on disk for parse/map/validate work | Needed for crash containment, bounded CPU pressure, and enterprise workload isolation | ✅ Done baseline / deepen |

---

## Recommended stage model for Python

The Python backend should not clone the TS actor/queue topology, but it should recover the missing execution semantics in a simpler explicit model.

Recommended execution stages:

1. **Request intake**
   - ingest content-service results or direct user request intent

Current update:

- The backend now has a first persisted request-intent row via [`ItemRequestORM`](../../filmu_py/db/models.py), and [`request_item()`](../../filmu_py/services/media.py) now upserts it on repeated external-reference requests.
- That improves the domain boundary between "request happened" and "media lifecycle item exists".
- The backend now also has additive specialization rows for `movie` / `show` / `season` / `episode`, so future intake/orchestration work no longer needs to assume the flat `MediaItemORM.attributes` blob is the only place where media shape can live.
- The backend now also has additive persisted stream-candidate rows plus blacklist and parent/child relation tables, so future parse/rank/container-selection work no longer needs to invent its first durable candidate graph.
- The backend now also has an Overseerr/Jellyseerr webhook receiver at `/api/v1/webhook/overseerr` and a scheduled `poll_content_services` ARQ cron that fans out to registered `ContentServicePlugin` implementations including `MDBListContentService`. Partial season/episode range tracking is persisted on `ItemRequestORM` (migration `20260320_0019`).
- The backend now also routes broken frontend TV season requests on [`/api/v1/scrape/auto`](../../filmu_py/api/routes/scrape.py) through that same shared [`request_item()`](../../filmu_py/services/media.py) seam before scrape enqueue, which keeps the future GraphQL `requestItem` boundary aligned with the REST compatibility layer.
- It does **not** yet constitute the broader TS-style request-content-services intake stage or queue decomposition described in this matrix, but the request/recovery boundary itself is no longer missing.

2. **Index**
   - enrich item metadata and resolve canonical identifiers

3. **Scrape**
   - collect raw scrape/provider results

4. **Parse + validate scrape results** *(maps to TS `parse-scrape-results`)*
   - parse raw torrent names into structured `ParsedData` (via RTN parser or equivalent)
   - content-level validation: year/country/season/episode matching against the requested item
   - reject content mismatches early (wrong movie, wrong season, etc.) before ranking

Current update:

- The backend now has a first parse-stage persistence seam in [`persist_parsed_stream_candidates()`](../../filmu_py/services/media.py), which parses selected manual-scrape filenames with `guessit`, validates basic content compatibility against the requested item, and persists the parsed pre-ranking rows into [`StreamORM`](../../filmu_py/db/models.py).
- The current manual scrape-session completion path in [`../../filmu_py/api/routes/scrape.py`](../../filmu_py/api/routes/scrape.py) now exercises that seam.
- This seam is now also exercised automatically by the scraped-item ARQ worker path when persisted candidates still lack parsed payloads.
- `parse_scrape_results` is now a real dedicated worker stage in the Wave 3 baseline, with bounded isolation/timeouts and standalone queue observability.
- Remaining work is deeper parser-configuration breadth, stricter content-validation depth, and future platform hardening above that now-landed stage baseline.

5. **Rank/select candidate** *(maps to TS `rank-streams`)*
   - title similarity check (Levenshtein ratio against canonical title + aliases)
   - additive quality scoring across categories (quality, codec, HDR, audio, channels, flags)
   - preferred-pattern / preferred-language boosting (+10,000 binary)
   - fetch-check pipeline (trash/adult/exclude/language/resolution/attribute filtering)
   - bucket-limited sorting by resolution to maintain variety
   - tiebreak by `ResolutionRank` when scores are equal
   - _Python does not need to clone the exact TS `RankingModel` values, but the dual-axis fetch+rank model and configurable settings/model structure are worth adopting_

Current update:

- The backend now has a first persisted service-layer ranking seam in [`rank_stream_candidates()`](../../filmu_py/services/media.py), which reads parsed candidates from [`StreamORM`](../../filmu_py/db/models.py), computes the TS-compatible Levenshtein ratio formula from persisted data, filters below-threshold candidates, ranks surviving rows by resolution tier, and writes the durable score back onto `StreamORM.rank`.
- This stage deliberately reads the persisted `parsed_title`, `resolution`, and `raw_title` fields instead of reparsing filenames again.
- The same seam now also supports the first additive RTN-style multi-axis scoring layer for quality/source, codec, HDR, and audio through an overridable `RankingModel`, still reading only persisted parsed payload fields.
- The same seam now also includes the first post-score fetch-check layer: attribute-level `fetch: false`, trash hard-fail rules, `remove_ranks_under`, and `require` override support.
- The same seam now also has a first deterministic container-selection boundary via [`select_stream_candidate()`](../../filmu_py/services/media.py) and [`MediaService.select_stream_candidate()`](../../filmu_py/services/media.py), choosing the best passing candidate by `rank_score`, then `lev_ratio`, then multipart `part` ordering, then `stream_id`, and persisting the durable winner on [`StreamORM.selected`](../../filmu_py/db/models.py).
- Partial show requests now also receive a coverage-aware rank bonus in [`rank_streams`](../../filmu_py/workers/tasks.py), so broader season coverage outranks narrow single-episode hits when multiple candidates satisfy the requested scope.
- The scraped-item ARQ worker now runs the persisted parse → rank → select sequence automatically when an item transitions into `scraped`, moving successful items to `downloaded` and failed selections to `failed`.
- That scraped-item seam is now explicitly decomposed into [`parse_scrape_results`](../../filmu_py/workers/tasks.py) and [`rank_streams`](../../filmu_py/workers/tasks.py), so the worker graph no longer hides parse and rank inside one combined stage even though the original frontend still only needs compatibility behavior from the outside.
- A standalone RTN compatibility package now also exists under [`../../filmu_py/rtn`](../../filmu_py/rtn/__init__.py), deserializing the original snake_case `settings.json` ranking block directly and separating parser/schema/default/fetch-check/sort behavior from the current worker/service seams so later job-time settings wiring can reuse it without another extraction step.
- The debrid stage is no longer only a placeholder transition: [`../../filmu_py/workers/tasks.py`](../../filmu_py/workers/tasks.py) now resolves the enabled downloader provider, calls the provider download-pipeline client methods, persists provider-backed media entries with BIGINT size support, and then hands off to `finalize_item`.
- Retry-library recovery is now also stage-aware in the worker layer: `indexed` items re-enter through `scrape_item`, `scraped` items re-enter through `parse_scrape_results`, and already-queued jobs for the current stage are skipped for idempotency.
- It is still intentionally below the broader TS breadth visible in the current clean checkout: downloader execution now exists through the real [`debrid_item`](../../filmu_py/workers/tasks.py) stage, but there is still no separate worker stage devoted only to torrent-file validation/selection, no TS-style persisted `failedChecks` propagation, and no torrent-content completeness validation equivalent to TS `validate-torrent-files`.

6. **Torrent-file validation + download/debrid path** *(maps to the current TS `find-valid-torrent` + sandboxed validation/download breadth)*
   - receive ranked results with scores, fetch status, and `failedChecks` sets
   - perform container selection + downloader execution

Current update:

- The backend now persists one durable selected container per item by flipping [`StreamORM.selected`](../../filmu_py/db/models.py) through [`MediaService.select_stream_candidate()`](../../filmu_py/services/media.py).
- The new scraped-item ARQ task now drives the persisted parse → rank → select sequence automatically from the existing lifecycle transition hook.
- This is now above the earlier selection-only seam: [`debrid_item`](../../filmu_py/workers/tasks.py) resolves the enabled provider, adds the selected magnet, polls provider state, selects files, persists [`MediaEntryORM`](../../filmu_py/db/models.py) rows, and then hands off to [`finalize_item`](../../filmu_py/workers/tasks.py). The remaining gap is broader multi-container download orchestration, richer provider fan-out, and a more durable workflow model above the current single-selected-container path.

7. **Finalize/persist side effects**
   - persist chosen outcome, emit control-plane events, publish notifications

Current update:

- [`transition_item()`](../../filmu_py/services/media.py) now writes transactional outbox rows for state-change publication in the same commit as the lifecycle transition and transition-event record.
- The ARQ worker now also exposes a scheduled [`publish_outbox_events`](../../filmu_py/workers/tasks.py) cron task every 30 seconds, draining unpublished rows and persisting `published_at` / `failed_at` / `attempt_count` outcomes on [`OutboxEventORM`](../../filmu_py/db/models.py).
- This is still intentionally below a broader durable broker/backplane: the outbox currently feeds the existing process-local [`EventBus`](../../filmu_py/core/event_bus.py), not a cross-process event system.

8. **Recovery/retry-library**
   - explicit restart/incomplete-state recovery path

Current update:

- The backend now has a first retry-library recovery seam in [`recover_incomplete_library()`](../../filmu_py/services/media.py), scanning failed items past cooldown windows plus scraped items whose `process_scraped_item` job is no longer in flight.
- The ARQ worker now also exposes a scheduled [`recover_incomplete_library`](../../filmu_py/workers/tasks.py) cron task every 15 minutes, reusing the same deduplicated [`enqueue_process_scraped_item()`](../../filmu_py/workers/tasks.py) path and incrementing durable recovery-attempt counters on [`MediaItemORM`](../../filmu_py/db/models.py).
- The recovery model now also persists `next_retry_at` on [`MediaItemORM`](../../filmu_py/db/models.py) and exposes additive `next_retry_at`, `recovery_attempt_count`, and `is_in_cooldown` fields on [`/api/v1/items`](../../filmu_py/api/routes/items.py) and [`/api/v1/items/{id}`](../../filmu_py/api/routes/items.py), so UI/GraphQL surfaces can show cooldown timing without adding a new state string.
- It is still intentionally below a fuller TS-style recovery model: there is no content-service intake recovery and no downloader-stage retry-library breadth yet, but the first transactional outbox seam now exists.

This is broader than the current worker graph used at project start, but still simpler than reproducing the full TS state-machine stack.

---

## Recovery semantics to add deliberately

The most important missing breadth item is **recovery semantics**.

Python needs explicit logic for:

- restart with in-flight items
- incomplete library requeue
- deduplicated re-entry into the correct stage
- avoiding repeated side effects after crashes

This is where:

- idempotency keys
- durable event/state records
- outbox semantics
- retry-library orchestration

must converge.

---

## Plugin execution implications

The TS backend’s plugin model is not only about GraphQL resolvers.

Its broader orchestration implications include:

- plugin reactions to typed events
- queue-backed isolation
- worker registration per event/hook
- datasource and settings injection during plugin execution

For Python, that means the plugin roadmap should eventually include:

1. event schema governance
2. capability registration for hook execution
3. datasource/context injection model
4. failure isolation per plugin hook
5. explicit publishability rules for events

Current update:

- event-hook capability registration now exists in [`../../filmu_py/plugins/registry.py`](../../filmu_py/plugins/registry.py)
- datasource/context injection now exists through [`../../filmu_py/plugins/context.py`](../../filmu_py/plugins/context.py)
- failure isolation and timeout handling now exist in [`../../filmu_py/plugins/hooks.py`](../../filmu_py/plugins/hooks.py)
- explicit publishability rules now exist in [`../../filmu_py/core/event_bus.py`](../../filmu_py/core/event_bus.py)

The remaining orchestration question is no longer first hook execution or first event governance; it is whether plugin-event execution needs a durable queued model beyond the current in-process runtime.

---

## Minimum next implementation sequence for Priority 3

1. Define request-intake stage semantics
2. Deepen parse/rank/select stage observability and worker decomposition if needed
3. Add idempotency boundaries and outbox strategy around the now-real recovery path
4. Widen retry-library recovery beyond the scraped-item stage into broader pipeline breadth
5. Only then decide whether plugin hook execution needs a durable queue-backed model beyond the current in-process baseline

This sequence keeps correctness and restart behavior ahead of extensibility.

---

## What not to do

- Do **not** treat “more queue stages” as automatically better.
- Do **not** copy the TS actor hierarchy blindly.
- Do **not** add queued plugin hook execution without preserving the now-implemented publishability and idempotency rules.
- Do **not** add recovery behavior as route-local or operator-only logic.

---

## Success checkpoint

Priority 3 should be considered meaningfully advanced when:

- the Python backend has a clearly staged execution model with 7+ real provider-backed stages
- recovery/retry-library behavior is explicit, stage-aware, and scheduled
- retries are safe because outbox, deduplication, and recovery-attempt counters are defined
- plugin execution can grow beyond capability protocols without collapsing orchestration clarity

Current checkpoint:

- Reached for the core scrape -> parse -> rank -> debrid -> finalize pipeline and baseline recovery.
- Wave 3 is materially deeper than the original baseline, but the closed-PR audit shows one meaningful orchestration gap remains above the landed repo surface: Filmu still needs a first-class scheduled reindex/reconciliation program instead of relying only on item-triggered indexing plus ad hoc governance reminders.

## Serving-core update (March 2026)

- The backend now has a shared serving substrate in [`filmu_py/core/byte_streaming.py`](../../filmu_py/core/byte_streaming.py) plus internal status visibility at [`/api/v1/stream/status`](../../filmu_py/api/routes/stream.py).
- Future orchestration and control-plane work can now attach to real serving-session and governance data instead of opaque route-local behavior.

## RTN reference update (March 2026)

- A full audit of the TS `@repo/util-rank-torrent-name` package is documented in [`rtn_research_report.md`](../rtn_research_report.md).
- Key architectural takeaways for Python orchestration:
  - **Two-phase separation**: Parse + content-validation (scrape-time) is decoupled from rank + fetch-checking (download-time). Parsed data is persisted and reused without re-parsing.
  - **Dual-axis filtering**: `fetch` (boolean per attribute) controls whether a torrent is *allowed*; `rank` (numeric) controls *preference*. This is more granular than a single score threshold and should inform the Python ranking stage design.
  - **Required override**: `require` patterns act as a whitelist overriding all fetch failures — a useful escape hatch for edge cases.
  - **Configurable at enqueue time**: The TS app passes concrete `Settings` + `RankingModel` into each BullMQ job (see `enqueue-download-item.ts`), allowing per-request tuning. Python should support equivalent configurability.
  - **Schema-driven boundaries**: Every RTN input/output uses Zod schemas. The Python equivalent should use Pydantic or equivalent validation at stage boundaries for observability and contract stability.

## Scrape pipeline fix update (March 2026)

Three bugs were blocking the end-to-end scrape pipeline from ever running:

1. **`retry_library` did not process `REQUESTED` state items.** The cron only re-enqueued items in `INDEXED` or `SCRAPED` states, so newly requested items stalled indefinitely waiting for a scrape job that would never arrive. Fixed by including `ItemState.REQUESTED` in the `retry_library` state list in [`filmu_py/workers/tasks.py`](../../filmu_py/workers/tasks.py).

2. **`POST /api/v1/items/add` never enqueued a `scrape_item` job.** The route created the item in the DB but returned without triggering any worker. New items now immediately enqueue a `scrape_item` ARQ job via the `arq_redis` pool, bypassing the 15-minute `retry_library` cron wait. This requires `FILMU_PY_ARQ_ENABLED=true` on the backend container. See [`filmu_py/api/routes/items.py`](../../filmu_py/api/routes/items.py).

3. **VFS proto imports crashed backend startup in Docker.** `app.py` did a top-level import of `vfs_server.py`, which imports the generated `filmuvfs` protobuf bindings. Those bindings are not installed in the Docker image (they require `grpcio-tools` + `proto_codegen` to generate). Fixed by making the `FilmuVfsCatalogSupplier` and `FilmuVfsCatalogGrpcServer` imports lazy/conditional in `app.py` — the VFS server is optional and the backend starts cleanly without it.
