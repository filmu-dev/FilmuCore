# FilmuCore — Full Source Code Audit Report

> Historical audit note
>
> This report is a point-in-time audit snapshot and should not be treated as the canonical current-status document.
> Some counts, gaps, and maturity statements below have been superseded by later implementation work.
> For current status and active priorities, use [`STATUS.md`](STATUS.md) and the active docs under [`docs/TODOS/`](TODOS).

> **Generated**: March 2026 (post-scrape-candidate BIGINT fix)
> **Scope**: Complete cross-reference of source code against all existing documentation

---

## Addendum: Pipeline Fixes Applied (2026-03-25)

A second audit pass identified and resolved the following issues:

| Priority | Fix | File | Lines |
|---|---|---|---|
| 🔴 Critical | Episode satisfaction now uses `MediaEntryORM` (download URL) instead of `ActiveStreamORM` (active playback session). The old query caused all shows to loop infinitely between `finalize_item → scrape_item` since no stream rows exist at initial library setup. | `services/media.py` | 2712–2719 |
| 🟠 Medium | `retry_library` now recovers `DOWNLOADED`-state orphans by re-enqueuing `finalize_item` | `workers/tasks.py` | 1438 |
| 🟠 Medium | `recover_incomplete_library` recovery routing is stage-aware for `FAILED` and `SCRAPED`, while `retry_library` owns `DOWNLOADED -> finalize_item` orphan recovery | `services/media.py` / `workers/tasks.py` | superseded by later recovery ownership split |
| 🟠 Medium | Debrid poll loop: `asyncio.sleep(0)` → `asyncio.sleep(2.0)` (was busy-spinning) | `workers/tasks.py` | 1788 |
| 🟠 Medium | `_resolve_item_type` now normalizes `"tv"` and `"series"` → `"show"` | `workers/tasks.py` | 2181 |
| 🟡 Low | `poll_ongoing_shows` now checks `is_scrape_item_job_active` before enqueuing | `workers/tasks.py` | 2382 |

**Test suite after fixes**: `590 passed, 0 failed`.

Tests updated: `tests/test_show_completion.py` — `_FakeShowSession` now matches
`from media_entries`; `_FakeRedis` in the `poll_ongoing_shows` test now stubs `pipeline()`
to satisfy `arq.Job.status()` (which uses `async with redis.pipeline()`).

**Docs updated**: `ORCHESTRATION.md`, `RIVEN_TS_SHOW_COMPLETION_BEHAVIOR.md`.

---

## Executive Summary

This audit examined every major module, ORM model, worker task, plugin, route, and documentation file in `FilmuCore`. The codebase is substantially more mature than several documentation files reflect. This report captures the real current state and highlights the specific discrepancies requiring documentation updates.

**Key findings:**

1. The codebase now has **16 ORM models** across 17 Alembic migrations — docs cite "15+"
2. **`ScrapeCandidateORM`** (migrations 0016/0017) is fully functional but missing from `DOMAIN_MODEL_EXPANSION_MATRIX.md`
3. **3 built-in scraper plugins** (Prowlarr, RARBG, Torrentio) exist and are worker-integrated, not documented beyond passing mention
4. **6 typed capability protocols** exist in `interfaces.py` — docs cite "5"
5. The `scrape_item` worker task is now a real provider-backed stage with plugin registry resolution — docs describe it as a scaffold
6. `retry_library` correctly handles `REQUESTED` state, confirmed in source — partially documented

---

## 1. Project Inventory

### Source File Counts

| Area | Files | Lines (approx) |
|------|-------|-----------------|
| `filmu_py/db/models.py` | 1 | 736 |
| `filmu_py/workers/tasks.py` | 1 | 1,483 |
| `filmu_py/services/` | 9 modules | ~6,000+ |
| `filmu_py/api/routes/` | 6 route modules | ~2,500+ |
| `filmu_py/plugins/` | 10+ files | ~1,800+ |
| `filmu_py/core/` | 7 modules | ~3,000+ |
| `filmu_py/rtn/` | 8 modules | ~2,000+ |
| `filmu_py/graphql/` | 5+ modules | ~1,000+ |
| `tests/` | 34 test files | ~8,000+ |
| `rust/filmuvfs/` | 7+ Rust files | ~2,000+ |
| `docs/` | 27 markdown files | ~6,000+ |
| `docs/TODOS/` | 8 matrix files | ~2,000+ |

### ORM Models (16 total)

| # | Model | Table | Migration | Status in Docs |
|---|-------|-------|-----------|----------------|
| 1 | `SettingsORM` | `settings` | 0015 | ✅ Documented |
| 2 | `MediaItemORM` | `media_items` | 0001 | ✅ Documented |
| 3 | `ItemStateEventORM` | `item_state_events` | 0001 | ✅ Documented |
| 4 | `PlaybackAttachmentORM` | `playback_attachments` | 0002–0005 | ✅ Documented |
| 5 | `ItemRequestORM` | `item_requests` | 0009 | ✅ Documented |
| 6 | `MovieORM` | `movies` | 0010 | ✅ Documented |
| 7 | `ShowORM` | `shows` | 0010 | ✅ Documented |
| 8 | `SeasonORM` | `seasons` | 0010 | ✅ Documented |
| 9 | `EpisodeORM` | `episodes` | 0010 | ✅ Documented |
| 10 | `StreamORM` | `streams` | 0011 | ✅ Documented |
| 11 | `StreamBlacklistRelationORM` | `stream_blacklist_relations` | 0011 | ✅ Documented |
| 12 | `StreamRelationORM` | `stream_relations` | 0011 | ✅ Documented |
| 13 | `MediaEntryORM` | `media_entries` | 0006 | ✅ Documented |
| 14 | `ActiveStreamORM` | `active_streams` | 0007 | ✅ Documented |
| 15 | `OutboxEventORM` | `outbox_events` | 0014 | ✅ Documented |
| 16 | **`ScrapeCandidateORM`** | `scrape_candidates` | 0016+0017 | ❌ **Missing from DOMAIN_MODEL matrix** |

### Alembic Migrations (17 total)

```
0001  initial_media_state_schema
0002  playback_attachments
0003  playback_attachment_lifecycle
0004  playback_attachment_refresh_state
0005  playback_attachment_provider_file_identity
0006  media_entries
0007  active_streams
0008  media_entry_lease_state
0009  item_requests
0010  media_specializations
0011  stream_graph
0012  stream_selection
0013  item_recovery_attempts
0014  outbox_events
0015  settings_table
0016  scrape_candidates          ← NEW, not in STATUS.md
0017  scrape_candidates_size_bigint ← NEW, standalone doc only
```

---

## 2. Worker Pipeline — Actual State

The worker tasks in [`workers/tasks.py`](../filmu_py/workers/tasks.py) (1,483 lines) implement a complete **8-stage pipeline**:

| Stage | Function | Status | Rate Limited |
|-------|----------|--------|------|
| 1. Scrape | `scrape_item()` | ✅ Real provider-backed | ✅ `worker:scrape` |
| 2. Parse | `parse_scrape_results()` | ✅ Dedicated ARQ stage | — |
| 3. Rank | `rank_streams()` | ✅ RTN-compatible | — |
| 4. Select | via `select_stream_candidate()` | ✅ Deterministic | — |
| 5. Debrid | `debrid_item()` | ✅ Provider-backed | ✅ `worker:debrid` |
| 6. Finalize | `finalize_item()` | ✅ Lifecycle completion | — |
| 7. Recovery | `retry_library()` + `recover_incomplete_library()` | ✅ Scheduled crons | — |
| 8. Outbox | `publish_outbox_events()` | ✅ 30-sec cron | — |

**Key doc gap**: `scrape_item()` is now a **real provider-backed stage** that:
- Resolves the plugin registry from worker context
- Loads both filesystem and built-in plugins 
- Calls `_scrape_with_plugins()` to fan out across registered scrapers
- Persists results to `ScrapeCandidateORM`
- Enqueues `parse_scrape_results` as the next stage

This is significantly more mature than the docs describe.

---

## 3. Plugin System — Actual State

### Capability Protocols (6, not 5)

The docs consistently cite "5 typed capability protocols." The actual count is **6**:

| Protocol | File | Description |
|----------|------|-------------|
| `ScraperPlugin` | `interfaces.py:181` | Search + normalize torrent candidates |
| `DownloaderPlugin` | `interfaces.py:188` | `add_magnet` + `get_status` + `get_download_links` |
| `IndexerPlugin` | `interfaces.py:199` | Metadata enrichment |
| `ContentServicePlugin` | `interfaces.py:206` | Content request polling |
| `NotificationPlugin` | `interfaces.py:213` | Event notification delivery |
| **`PluginInitializer`** | `interfaces.py:174` | Shared initialization contract (base protocol) |

> **Note**: `PluginInitializer` is a runtime-checkable base protocol. Whether it counts as a "capability" protocol is debatable, but it is a distinct `Protocol` class with `@runtime_checkable`.

### Built-in Scraper Plugins (3)

| Plugin | File | Lines | API Style |
|--------|------|-------|-----------|
| **Prowlarr** | `builtin/prowlarr.py` | 138 | REST API (`/api/v1/search`) |
| **RARBG** | `builtin/rarbg.py` | 174 | HTML scraping |
| **Torrentio** | `builtin/torrentio.py` | ~160 | REST API |

All three:
- Implement `ScraperPlugin` protocol via duck typing
- Accept `PluginContext` via `initialize()`
- Use `httpx.AsyncClient` with configurable transport/timeout
- Honor rate limiting via `ctx.rate_limiter.acquire()`
- Read configuration from compatibility settings dict

### Plugin Context Provider

[`plugins/context.py`](../filmu_py/plugins/context.py) provides a `PluginContextProvider` that injects:
- Compatibility settings dict
- `EventBus` reference
- `RateLimiter` reference
- `CacheManager` reference
- Per-plugin logger factory

The worker creates its own plugin context provider at [`tasks.py:1209`](../filmu_py/workers/tasks.py), confirming plugin system extends beyond API scope into worker scope.

---

## 4. Documentation Discrepancies

### 4.1 STATUS.md Gaps

| Finding | Source | Doc status |
|---------|--------|------------|
| `ScrapeCandidateORM` + migration 0016 | `db/models.py:436` | ❌ Not mentioned |
| Migration 0017 (BIGINT fix) | `db/alembic/versions/` | ❌ Not mentioned (standalone doc only) |
| Built-in Prowlarr/RARBG/Torrentio scrapers | `plugins/builtin/` | ❌ Not mentioned |
| Worker `scrape_item` is now provider-backed | `workers/tasks.py:918` | ❌ Described as scaffold |
| 6 capability protocols (not 5) | `plugins/interfaces.py` | ❌ Says "5" |
| Worker-side plugin registry resolution | `workers/tasks.py:1230` | ❌ Not mentioned |
| 34 test files | `tests/` | ❌ Test inventory not tracked |
| `services/scrapers/` directory exists (empty `__pycache__` only) | `filmu_py/services/scrapers/` | N/A (dead directory) |

### 4.2 DOMAIN_MODEL_EXPANSION_MATRIX.md Gaps

- Row for `ScrapeCandidateORM` is **completely missing** from the domain expansion matrix
- The matrix should list it as: persisted raw scrape-stage candidate with `UniqueConstraint(item_id, info_hash)`, `BigInteger` size_bytes, provider tracking, and relationship to `MediaItemORM`

### 4.3 ORCHESTRATION_BREADTH_MATRIX.md Gaps

- `scrape_item` stage description does not reflect that it now invokes plugin-backed scrapers and persists `ScrapeCandidateORM` rows
- The matrix still describes scrape as a "real stage exists" without mentioning plugin registry integration or `ScrapeCandidateORM` persistence

### 4.4 PLUGIN_CAPABILITY_MODEL_MATRIX.md Gaps

- References "5 typed capability protocols" — should be updated to reflect `PluginInitializer` as a base protocol
- Does not mention the 3 built-in scraper plugins (Prowlarr, RARBG, Torrentio)
- Does not mention worker-side plugin resolution or `register_builtin_plugins()` integration
- Missing `PluginContextProvider` documentation

### 4.5 ARCHITECTURE.md Gaps

- Does not mention `ScrapeCandidateORM` or the scrape persistence layer
- Does not mention built-in scraper plugins
- Plugin capability protocol count is not specified (so not technically wrong, but incomplete)

### 4.6 Dead/Stale Directories

- `filmu_py/services/scrapers/` — contains only `__pycache__`, no source files. Either dead or placeholder.

---

## 5. API Route Surface — Current State

| Route Module | Endpoints | Status |
|-------------|-----------|--------|
| `routes/settings.py` | `/api/v1/settings/*` (6+ endpoints) | ✅ Production |
| `routes/items.py` | `/api/v1/items/*` (add, list, detail, reset, retry, remove) | ✅ Production |
| `routes/stream.py` | `/api/v1/stream/*` (file, HLS, status, SSE) | ✅ Production |
| `routes/scrape.py` | `/api/v1/scrape/*` (sessions, manual scrape) | ✅ Production |
| `routes/default.py` | `/api/v1/{stats,services,calendar,logs,health,...}` | ✅ Production |
| `routes/triven.py` | Legacy `/api/v1/triven` aliases | ✅ Compatibility |

---

## 6. Service Layer — Current State

| Service | File | Key Responsibilities |
|---------|------|---------------------|
| `media.py` | `services/media.py` | Item CRUD, state transitions, stream candidates, stats, calendar, outbox, recovery |
| `playback.py` | `services/playback.py` | Playback resolution, attachment management, lease refresh, direct/HLS selection |
| `debrid.py` | `services/debrid.py` | Real-Debrid + AllDebrid + Debrid-Link provider clients |
| `tmdb.py` | `services/tmdb.py` | TMDB metadata enrichment |
| `vfs_catalog.py` | `services/vfs_catalog.py` | VFS catalog projection supplier |
| `vfs_server.py` | `services/vfs_server.py` | gRPC catalog watch server |
| `mount_worker.py` | `services/mount_worker.py` | Mount-worker boundary, media-entry query contract |
| `settings.py` | `services/settings.py` | Settings persistence, compatibility translation |

---

## 7. Rust VFS Sidecar — Current State

| Component | File | Status |
|-----------|------|--------|
| Bootstrap | `rust/filmuvfs/src/main.rs` | ✅ Scaffold |
| Runtime | `rust/filmuvfs/src/runtime.rs` | ✅ WatchCatalog lifecycle |
| Catalog client | `rust/filmuvfs/src/catalog/client.rs` | ✅ Reconnecting |
| Catalog state | `rust/filmuvfs/src/catalog/state.rs` | ✅ In-memory |
| Mount adapter | `rust/filmuvfs/src/mount.rs` | ✅ Unix-only fuse3 |
| Proto bindings | `rust/filmuvfs/src/proto.rs` | ✅ Guard tests |
| Proto contract | `proto/filmuvfs/catalog/v1/catalog.proto` | ✅ Source of truth |

**Status**: Cargo check passes on both Windows and Linux targets. The Unix-only `fuse3` adapter now also passes the automated WSL/Linux mount lifecycle gate, the manual WSL mount/list/stat/read smoke path, and the Plex/Emby playback gate. The next step is no longer first validation; it is mount-side chunk-engine adoption plus runtime/control-plane hardening (especially WatchCatalog reconnect behavior and repeated serve-time link refresh pressure).

---

## 8. Test Coverage Inventory

34 test files under `tests/`:

| Category | Files |
|----------|-------|
| Worker/pipeline | `test_worker_tasks.py`, `test_worker_retry.py`, `test_debrid_worker.py`, `test_scrape_worker.py` |
| Routes | `test_settings_routes.py`, `test_stream_routes.py`, `test_items_routes.py`, `test_scrape_routes.py` |
| Services | `test_media_service.py`, `test_playback_service.py`, `test_debrid_service.py` |
| Core | `test_byte_streaming.py`, `test_chunk_engine.py`, `test_rate_limiter.py`, `test_event_bus.py`, `test_cache.py` |
| Plugins | `test_plugin_loader.py`, `test_prowlarr_plugin.py`, `test_rarbg_plugin.py`, `test_torrentio_plugin.py` |
| RTN | `test_rtn_*.py` (multiple) |
| State | `test_state_machine.py` |
| Config | `test_config.py`, `test_settings_persistence.py` |
| VFS/Proto | `test_vfs_catalog.py`, `test_proto_codegen.py` |

---

## 9. Recommendations

### Immediate Documentation Updates Needed

1. **STATUS.md**: Add sections for ScrapeCandidateORM, migrations 0016/0017, built-in scrapers, worker plugin integration
2. **DOMAIN_MODEL_EXPANSION_MATRIX.md**: Add `ScrapeCandidateORM` row to the domain expansion matrix table
3. **ORCHESTRATION_BREADTH_MATRIX.md**: Update scrape stage description to reflect plugin-backed provider execution and ScrapeCandidateORM persistence
4. **PLUGIN_CAPABILITY_MODEL_MATRIX.md**: Update protocol count, document built-in scrapers, add PluginContextProvider details
5. **ARCHITECTURE.md**: Add scrape persistence and built-in plugin sections

### Code Cleanup

1. **`filmu_py/services/scrapers/`**: Dead directory — remove or document as intentional placeholder
2. **Test file naming**: Some test files don't follow consistent naming (e.g., `test_debrid_worker.py` vs `test_worker_tasks.py`)

### Structural Observations

1. The codebase is **well-organized** — services, routes, plugins, workers each have clear boundaries
2. The RTN package is properly isolated under `filmu_py/rtn/` with its own test coverage
3. The plugin system's `PluginContext`→`PluginContextProvider` pattern is solid but underdocumented
4. The worker pipeline with per-stage retry policies, dead-letter routing, and rate limiting is production-grade

---

*End of audit report.*
