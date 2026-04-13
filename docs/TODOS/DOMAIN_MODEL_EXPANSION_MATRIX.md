# Domain Model Expansion Matrix

## Purpose

Turn the active domain-model track from [`../STATUS.md`](../STATUS.md) and [`../EXECUTION_PLAN.md`](../EXECUTION_PLAN.md) into an executable architecture artifact.

This document maps:

- the **current Python domain model**
- the **missing entities/relationships/projections**
- the **frontend routes and platform capabilities** that depend on them

It is intentionally implementation-aware but remains a planning document.

---

## Current Python baseline

The current persistence baseline in [`filmu_py/db/models.py`](../../filmu_py/db/models.py) has grown significantly since the initial minimal layer and now includes **17 first-class ORM models** across 20 Alembic migrations.

Current first-class persisted concepts:

1. `MediaItemORM`
   - `id`, `external_ref`, `title`, `state`, JSONB `metadata`, timestamps

2. `ItemStateEventORM`
   - immutable transition/event record per media item

3. `ItemRequestORM` ✅ *(migration 0009)*
   - separates inbound request intent from enriched media lifecycle state

4. `MovieORM` / `ShowORM` / `SeasonORM` / `EpisodeORM` ✅ *(migration 0010)*
   - media-type specialization layer with hierarchy relationships

5. `StreamORM` / `StreamBlacklistRelationORM` / `StreamRelationORM` ✅ *(migration 0011)*
   - stream candidate graph with ranking, selection, blacklisting, and parent/child relationships

6. `MediaEntryORM` ✅ *(migration 0006)*
   - connects media items to actual playable files with provider/file identity
   - `size_bytes` now promoted to `BIGINT` via migration `20260318_0018`

7. `ActiveStreamORM` ✅ *(migration 0007)*
   - active stream ownership/readiness relation keyed to media entries

8. `PlaybackAttachmentORM`
   - persisted playback attachment with lifecycle/refresh fields, provider identity
   - `file_size` now promoted to `BIGINT` via migration `20260318_0018`

9. `OutboxEventORM` ✅ *(migration 0014)*
   - transactional outbox for publish-consistency and replay-safe events

10. `SettingsORM` ✅ *(migration 0015)*
    - persisted settings model

11. `ScrapeCandidateORM` ✅ *(migration 0016 + 0017 BIGINT fix)*
    - raw scrape-stage candidate with provider tracking, `UniqueConstraint(item_id, info_hash)`, `BigInteger` size_bytes

Current strengths of this baseline:

- enough for full lifecycle state progression, request intake, and specialization
- enough for parse → rank → debrid → finalize pipeline with durable stream candidates
- enough for provider-backed media entry persistence and active-stream relations
- stats, calendar, item-detail, and playback surfaces all consume intentional domain models
- transactional outbox gives publish-consistency for domain events

Remaining limitations of this baseline:

- Domain model gaps are now closed across 20 migrations. All planned entity types are persisted.
- Remaining evolution is read-model deepening for VFS-facing surfaces, item-list shaping, and the remaining compatibility consumers beyond the now-landed graph-first specialization adoption in calendar and detail projections.

### Current status summary

- Done now: planned entities and core projections are persisted and wired into real route/service surfaces.
- Partial only: VFS-facing, item-list, and remaining compatibility read-model adoption still need deepening.
- Still missing: no new first-class entity family is currently missing from the original planning set; the remaining work is read-model and control-plane consumption depth.

---

## Reference breadth from the original TS platform

The original TS backend has a much richer entity graph, including concepts like:

- item requests
- media items with richer specializations
- movies
- shows
- seasons
- episodes
- streams
- media entries/filesystem entries
- subtitle entries

That richer model supports:

- dashboard stats
- library views and actions
- item details
- stream/VFS behaviors
- queue/orchestration semantics

---

## Domain expansion matrix

| Missing model/projection                                              | Why it is needed                                                                                    | Depends-on surfaces                                                                                | Status |
| --------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- | ------ |
| **ItemRequest**                                                       | Separate inbound request intent from enriched/persisted media lifecycle state                       | request-content-services intake, retry-library, user-request semantics, future UI request surfaces | ✅ Done — `ItemRequestORM` (migration 0009) |
| **Retry cooldown metadata**                                           | Needed to surface scheduled scrape re-entry timing without inventing a new lifecycle state          | `/api/v1/items`, `/api/v1/items/{id}`, retry-library diagnostics, future GraphQL request surfaces  | ✅ Done — `MediaItemORM.next_retry_at` (migration 0021) + additive item response fields |
| **Media item specializations** (`Movie`, `Show`, `Season`, `Episode`) | Current single generic item model is too thin for calendar, detail, stream, and lifecycle behaviors | `/api/v1/items`, `/api/v1/items/{id}`, calendar, FilmuVFS, stream parity                           | ✅ Done — `MovieORM`/`ShowORM`/`SeasonORM`/`EpisodeORM` (migration 0010) |
| **Streams / active stream relation**                                  | Needed for ranking, blacklisting, playback selection, download fan-out, item detail parity          | item details, retry/reindex/recovery, playback, stream routes, future VFS parity                   | ✅ Done — `StreamORM`/`StreamBlacklistRelationORM`/`StreamRelationORM` (migration 0011) + `ActiveStreamORM` (migration 0007) |
| **Filesystem/media entry model**                                      | Needed to connect media items to actual playable files and future FilmuVFS pathing                  | direct file streaming, HLS, FilmuVFS mount, item detail parity                                     | ✅ Done — `MediaEntryORM` (migration 0006, `size_bytes` BIGINT in 0018) |
| **Scrape candidates**                                                 | Needed for durable raw torrent candidate persistence before RTN parse/validation, with provider tracking and BIGINT size_bytes | scrape pipeline, parse stage, ranking stage, retry-library                                          | ✅ Done — `ScrapeCandidateORM` (migration 0016 + 0017 BIGINT fix) |
| **Subtitle entry model**                                              | Needed for stream/VFS completeness and richer media detail surfaces                                 | playback parity, VFS parity, detail surfaces                                                       | ✅ Done — `SubtitleEntryORM` (migration 0020), service helpers, subtitles projected on item detail |
| **Provider/downloader account projection**                            | Needed for `/api/v1/downloader_user_info` and dashboard capability/status views                     | dashboard, service visibility, operational diagnostics                                             | ✅ Done — `DownloaderAccountService`, real `/api/v1/downloader_user_info` (5-min cached) |
| **Service registry/status projection**                                | Needed for `/api/v1/services` and future operational/admin visibility                               | dashboard, health/service reporting                                                                | ✅ Done — real `/api/v1/services` from runtime `DownloadersSettings` |
| **Calendar projection**                                               | Calendar should not depend on ad hoc item shaping; it needs a stable projection/query path          | `/api/v1/calendar`                                                                                 | ✅ Done — `MediaService.get_calendar()` backed by `EpisodeORM` + season relations |
| **Stats projection**                                                  | Stats should be produced from clear aggregates, not improvised route logic only                     | `/api/v1/stats`, dashboard                                                                         | ✅ Done — `MediaService.get_stats()` backed by ORM + specialization rows |
| **Stream/VFS lease/control projection**                               | Useful for FilmuVFS and direct stream control-plane state                                           | FilmuVFS, stream refresh, control-plane events, hybrid backplane evolution                         | 🔶 Partial — persisted lease state, active-stream relations, and mount-worker boundary exist; VFS-facing read models still evolving |

---

## Minimum expansion required to properly back the delivered Phase C route surface

The core Phase C routes already exist. The minimum domain additions below are needed so those delivered routes can evolve from compatibility-first baselines into intentional models and projections rather than remaining thin shims.

### 1. Item request model

Needed for:

- explicit request intake from content services
- separating requested-but-not-enriched state from persisted media lifecycle state
- retry/recovery semantics that do not overload `MediaItemORM`

Current update:

- A first persisted [`ItemRequestORM`](../../filmu_py/db/models.py) slice now exists and is upserted from [`request_item()`](../../filmu_py/services/media.py).
- That means repeated external-reference requests no longer live only as incidental `MediaItemORM` writes.
- That same shared request seam now also backs the missing-item compatibility path on [`/api/v1/scrape/auto`](../../filmu_py/api/routes/scrape.py), including partial season/episode request ranges for broken frontend TV flows.
- The remaining gap is broader request-source semantics, richer linkage to content-service intake/orchestration, and read-model exposure where that becomes product-relevant.

### 2. Media item specialization layer

Needed for:

- calendar
- item detail surfaces
- richer item list representations
- eventual FilmuVFS pathing rules by movie/show/season/episode

Current update:

- A first additive persistence layer now exists via [`MovieORM`](../../filmu_py/db/models.py), [`ShowORM`](../../filmu_py/db/models.py), [`SeasonORM`](../../filmu_py/db/models.py), and [`EpisodeORM`](../../filmu_py/db/models.py).
- Those rows are created or updated from [`request_item()`](../../filmu_py/services/media.py) without changing the current route contracts, which means the backend no longer relies only on `MediaItemORM.attributes` to remember the requested media shape.
- The remaining gap is read-model adoption: the shared service layer and richer GraphQL detail/calendar consumers now consume those specialization rows deliberately, but item-list shaping, VFS pathing, and the remaining compatibility consumers still need the same treatment.

### 3. Stream + file attachment layer

Needed for:

- direct file streaming
- HLS parity
- ranking/download selection
- playback selection visibility

Current note:

- The compatibility stream layer now has an in-memory attachment abstraction behind [`../filmu_py/api/playback_resolution.py`](../filmu_py/api/playback_resolution.py), but it still derives that shape from flexible metadata rather than persisted first-class file/link entities.
- Attachment selection now also has a service-layer boundary in [`../filmu_py/services/playback.py`](../filmu_py/services/playback.py), but that service still reads flexible metadata rather than intentional persisted attachment entities.
- A first persisted playback attachment record now exists in [`../filmu_py/db/models.py`](../filmu_py/db/models.py), and the playback service now prefers it when present.
- The persisted layer now also includes lifecycle/refresh-ready fields, but it still does not perform real debrid-services unrestriction or persisted attachment refresh workflows.
- The persisted layer now also includes explicit attachment refresh state, but it still stops short of a real refresh/unrestriction workflow engine.
- The service layer now has explicit refresh transition/update helpers, but those transitions are still local state changes rather than real provider-backed refresh execution.
- The service layer now also has an explicit refresh request/result boundary, which is a good integration seam for future provider-backed refresh work even though the provider execution itself is still missing.
- The service layer now also has explicit refresh planning/request helpers, but the actual provider-backed execution path still needs to be implemented on top of that planning seam.
- The service layer now also has a provider-facing refresh orchestration boundary plus a first provider-client-backed `unrestrict_link(...)` execution path, but built-in debrid-services integrations and download-id-driven refresh workflows are still future work.
- The first built-in Real-Debrid playback client now exists, but the domain still lacks broader built-in provider coverage, download-id-driven refresh workflows, and the fuller torrent/debrid projection model the long-term VFS wants.
- Built-in provider coverage now also includes AllDebrid and Debrid-Link for playback-link refreshes, but the domain still lacks provider-download-id-driven refresh workflows, richer provider state projections, and the fuller torrent/debrid model the long-term VFS wants.
- The first provider-download-id-driven refresh path now exists for Real-Debrid, but the domain still lacks broader download-id refresh coverage, richer persisted provider/file identity fields, and the fuller torrent/debrid projection model the long-term VFS wants.
- Persisted provider/file identity now covers more than filename/filesize heuristics, but the domain still lacks broader provider-download-id refresh coverage and the fuller torrent/debrid projection model the long-term VFS wants.
- The playback service and built-in Real-Debrid client now also share a first provider-side attachment projection model, so provider-download-id refreshes can select projected files intentionally and persist the matched provider-side file identity back onto attachments.
- The remaining gap is no longer the first projection model itself; it is broader provider-download-id refresh coverage across providers plus exposing richer provider-side projections to item-detail and VFS-facing read models.
- The playback service now also projects debrid-first lifecycle context onto internal resolved direct/HLS playback snapshots, so adjacent playback read paths can reuse persisted owner/link-state semantics without changing current route contracts.
- The remaining gap is no longer first internal snapshot-level lifecycle projection either; it is deciding which true VFS-facing or public read models should reuse that projection and whether those specific consumers can do so without additional persistence-query breadth.
- A separate mount-worker boundary now also exists in [`../filmu_py/services/mount_worker.py`](../filmu_py/services/mount_worker.py), defining the explicit persisted media-entry query contract for future VFS-facing provider-file identity resolution without moving mount ownership into [`../filmu_py/services/playback.py`](../filmu_py/services/playback.py).
- That boundary now also implements the concrete executor against the existing `media_entries` + `active_streams` model, so the remaining gap is no longer first mount-boundary/query-contract or first executor definition either; it is deciding which later VFS/public read models can reuse that executor without requiring broader schema/query expansion and then wiring real mount operations on top of it.
- The item-detail surface now also exposes persisted playback attachment projections on the current extended detail response, which gives the frontend and future read-model work a stable compatibility seam above raw `metadata` blobs.
- The item-detail surface now also exposes a resolved direct/HLS playback snapshot that reuses the same playback-selection rules as the stream routes, so detail consumers can see current readiness without reconstructing attachment priority client-side.
- The item-detail surface now also exposes a persisted `media_entries` projection with an explicit `source_attachment_id` relationship in the domain, which gives the current API a stable compatibility seam for filename/URL/provider state above raw attachment rows.
- The item-detail surface now also exposes an additive `active_stream` ownership/readiness projection backed by a persisted active-stream relation keyed to those `media_entries`, and each projected media entry now advertises whether it backs direct playback, HLS playback, or both.
- The playback service now also prefers those persisted `media_entries` + `active_stream` relations plus durable media-entry lease state when resolving direct/HLS playback for HTTP routes, and it now has a first provider-backed refresh seam that updates those durable lease rows directly.
- The remaining gap is no longer whether the domain slice influences real playback at all; it is widening those relations into fuller stream/filesystem/media-entry lifecycle entities, broader route-level playback-risk handling, and richer provider-backed resolver workflows.
- Real long-term VFS and debrid-services-backed playback readiness still requires those attachment concepts to exist intentionally in the domain model rather than only at route-resolution time.

Current stream-candidate update:

- A first additive persisted stream graph now exists via [`StreamORM`](../../filmu_py/db/models.py), [`StreamBlacklistRelationORM`](../../filmu_py/db/models.py), and [`StreamRelationORM`](../../filmu_py/db/models.py).
- A first parse-stage writer now also exists via [`persist_parsed_stream_candidates()`](../../filmu_py/services/media.py), so selected manual-scrape filenames can become persisted parsed stream candidates instead of staying only in ephemeral route/session state.
- This gives later parse/rank/container-selection work a real persistence target for ranked candidates, blacklist semantics, and parent/child candidate relationships.
- The remaining gap is no longer first persistence for stream candidates themselves; it is orchestration-stage adoption, selection logic, and read-model exposure.

### 4. Service/downloader projections

Needed for:

- dashboard
- `/api/v1/services`
- `/api/v1/downloader_user_info`

### 5. Stats/calendar query models

Needed for:

- `/api/v1/stats`
- `/api/v1/calendar`

These are not optional if the existing Phase C compatibility routes are meant to evolve beyond superficial route shims into durable product-facing backend surfaces.

Current update:

- [`MediaService.get_stats()`](../../filmu_py/services/media.py) now exposes a first typed stats query projection backed by persisted `MediaItemORM` plus specialization rows instead of the earlier improvised route-side aggregation.
- [`MediaService.get_calendar()`](../../filmu_py/services/media.py) now exposes a first episode-air-date projection backed by [`EpisodeORM`](../../filmu_py/db/models.py) and the season relationship, ordered deterministically by air date.
- [`/api/v1/stats`](../../filmu_py/api/routes/default.py) and [`/api/v1/calendar`](../../filmu_py/api/routes/default.py) now consume those projection methods directly, so the remaining gap is no longer first query-model extraction for those two surfaces.
- The richer GraphQL surface now also consumes the same service-layer projections, and `mediaItem` detail additionally exposes specialization lineage (`imdbId`, parent ids, show/season/episode fields) from that shared domain seam instead of recomputing those values from metadata blobs.
- The next Priority 2 gap after this slice is broader read-model adoption across item-list shaping, the remaining compatibility consumers, and VFS-facing surfaces.

## Relationship model to add deliberately

The Python backend should not copy the TS entity graph blindly, but it should restore the missing relationship categories intentionally.

Recommended relationship families:

1. **Request -> Media lifecycle**
   - request record exists separately from enriched media item state

2. **Show -> Season -> Episode hierarchy**
   - required for calendar, FilmuVFS, and episode-specific playback behavior

3. **Media item -> Streams**
   - multiple candidate streams
   - one selected/active stream where applicable

4. **Media item -> Media/file entries**
   - links lifecycle entities to actual playable media/file state

5. **Media item -> Subtitle entries**
   - supports stream/VFS completeness and richer playback UX

---

## Projection strategy

Not every surface should read the raw entity graph directly.

Recommended split:

### Write model

- request intake
- lifecycle state transitions
- stream selection/ranking results
- download/finalization side effects

### Read models / projections

- dashboard stats
- calendar entries
- item list summaries
- item detail view model
- service/downloader visibility

This keeps the backend motor strong and avoids coupling frontend read shapes too tightly to the core persisted entity layout.

---

## What not to do

- Do **not** keep stretching `MediaItemORM.metadata` to carry every missing concept.
- Do **not** implement Phase C routes as route-local shape fabrication without domain backing.
- Do **not** let stream/VFS-specific state live only in transient worker memory.
- Do **not** force dashboard, calendar, and playback features to share the same query model if their access patterns differ.

---

## Recommended implementation sequence

1. ~~Deepen the new `ItemRequest` model into explicit content-service intake semantics and richer request-source attribution.~~ ✅ Done — `ItemRequestORM` upserted from `request_item()`.
2. ~~Add media specialization boundaries (`Movie`, `Show`, `Season`, `Episode`) or equivalent typed layering.~~ ✅ Done — `MovieORM`/`ShowORM`/`SeasonORM`/`EpisodeORM` with hierarchy.
3. ~~Add stream and file-entry relationships.~~ ✅ Done — `StreamORM` graph + `MediaEntryORM` + `ActiveStreamORM`.
4. ~~Add stats and calendar projections.~~ ✅ Done — `MediaService.get_stats()` and `MediaService.get_calendar()`.
5. ~~Add service/downloader/account visibility projections.~~ ✅ Done — `DownloaderAccountService` plus real `/api/v1/services` and `/api/v1/downloader_user_info`.
6. ~~Add subtitle and stream/VFS control-plane models as playback capability grows.~~ ✅ Done — `SubtitleEntryORM` (migration 0020) plus subtitle detail projection.

---

## Success checkpoint

Priority 2 should be considered meaningfully advanced when:

- Phase C routes can be implemented against real domain structures rather than compatibility shims alone
- dashboard/library/calendar responses are backed by intentional models/projections
- orchestration can evolve without abusing free-form JSON metadata fields
- FilmuVFS planning has real entities to attach stream/file/subtitle behavior to

Current checkpoint:

- Reached for the entity layer.
- Not fully closed for read-model depth, especially VFS-facing projections, item-list shaping, and the remaining compatibility consumers beyond the now-landed graph-first specialization reads.

## Serving-session note (March 2026)

- The shared serving core now tracks basic serving sessions and governance counters, but those are still runtime records rather than intentional domain entities.
- Real VFS readiness still requires durable stream/file/session models so the serving substrate does not remain only a transient runtime layer.
