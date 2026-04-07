# FilmuCore — Worker Orchestration

This document describes the ARQ worker pipeline stages, cron jobs, recovery mechanics,
and behavioral guarantees.

---

## Worker Pipeline (8 stages)

```
scrape_item
    └─► parse_scrape_results
            └─► rank_streams
                    └─► debrid_item  (polls every 2s until ready or timeout)
                            └─► finalize_item
```

Recovery crons feed back into this pipeline for stuck items.

### Stage details

| Stage | Function | Key behavior |
|---|---|---|
| 1 Scrape | `scrape_item` | Fan-out across registered plugin scrapers; persists `ScrapeCandidateORM` rows |
| 2 Parse | `parse_scrape_results` | Deduplicates candidates by `info_hash`; builds `StreamORM` rows |
| 3 Rank | `rank_streams` | RTN-compatible scoring plus partial-request scope handling; exponential backoff on no-winner |
| 4 Debrid | `debrid_item` | Adds magnet to provider; polls **every 2 s** until `downloaded`/`ready`; circuit-breaker on failure |
| 5 Finalize | `finalize_item` | Evaluates show completion scope; transitions to`COMPLETE`, `ONGOING`, `PARTIALLY_COMPLETED`, or re-queues scrape |
| 6 Recovery | `retry_library` (cron) | Re-enqueues `REQUESTED`, `INDEXED`, `SCRAPED`, and **`DOWNLOADED`** orphans |
| 7 Recovery | `recover_incomplete_library` (cron) | Scans `FAILED` and `SCRAPED`; advances cooldown failures or dead-letters after max attempts |
| 8 Outbox | `publish_outbox_events` | 30-second cron; publishes transactional outbox rows to the event bus |

---

## Show Completion Logic

See [RIVEN_TS_SHOW_COMPLETION_BEHAVIOR.md](./RIVEN_TS_SHOW_COMPLETION_BEHAVIOR.md) for full details.

Key points:
- Episode satisfaction = `MediaEntryORM` row exists for that episode's `media_item_id`
  (download URL persisted by debrid pipeline).
- `ActiveStreamORM` is used **only** for VFS playback-role tagging — not for completion evaluation.
- Shows with zero satisfied released episodes are re-queued for scrape without any state transition.

### Partial request ranking semantics

Current FilmuCore behavior is now intentionally split into two layers:

1. **Compatibility behavior for the current frontend**
   - Partial season requests remain accepted through the current REST intake/routes.
   - Parse/rank workers still operate behind the same compatibility surface.

2. **Richer internal scope semantics for future graph/frontend work**
   - [`rank_streams()`](../filmu_py/workers/tasks.py) now applies a partial-scope coverage bonus so broader season coverage outranks narrow single-episode hits for multi-season partial requests.
   - Season packs receive a stronger bonus than multi-episode batches, and multi-episode batches receive a stronger bonus than single episodes.
   - This preserves compatibility while moving the selection behavior closer to the stricter `riven-ts` intent.

Important boundary:

- FilmuCore still does **not** fully replicate the TS download/container validation path yet.
- The current improvement is rank-time scope awareness, not full torrent-content completeness validation.

---

## Cron Jobs (6 registered)

| Job | Schedule | Purpose |
|---|---|---|
| `retry_library` | Configurable (default: daily at 00:00) | Re-enqueue REQUESTED/INDEXED/SCRAPED/DOWNLOADED orphans |
| `recover_incomplete_library` | Configurable | Advance or dead-letter items at max attempts |
| `publish_outbox_events` | Every 30 s | Drain transactional outbox to event bus |
| `poll_unreleased_items` | Configurable | Transition UNRELEASED → INDEX → REQUESTED when aired |
| `poll_ongoing_shows` | Configurable | Re-scrape PARTIALLY_COMPLETED/ONGOING shows with new missing episodes |
| `vacuum_and_analyze` (optional) | Configurable | DB maintenance |

### `poll_ongoing_shows` double-enqueue guard

Before enqueuing `scrape_item` for a show, `poll_ongoing_shows` checks
`is_scrape_item_job_active()`. If a scrape job is already active for that item, the show
is skipped. This prevents duplicate scrape pressure on busy systems.

---

## Debrid Polling

`debrid_item` polls the provider's torrent status on a **2-second interval** via
`asyncio.sleep(2.0)`. The previous `asyncio.sleep(0)` was busy-spinning and could
issue hundreds of API calls per second for slow torrents.

Timeout behavior: configurable via `settings.downloaders.debrid_poll_timeout_seconds`.
After timeout, the item is dead-lettered via `route_dead_letter`.

---

## Item Type Resolution

`_resolve_item_type()` maps raw `item_type` attribute strings to canonical pipeline types:

| Input | Canonical |
|---|---|
| `"movie"` | `"movie"` |
| `"show"` | `"show"` |
| `"tv"`, `"series"` | `"show"` (normalized) |
| `"season"` | `"season"` |
| `"episode"` | `"episode"` |
| `tmdb:`-prefixed ref (fallback) | `"movie"` |
| anything else | `"show"` |

---

## DOWNLOADED State Recovery

Items stuck in `DOWNLOADED` state (common after a worker crash between debrid completion and
finalize enqueue) are recovered by `retry_library`. `recover_incomplete_library` does not scan
`DOWNLOADED`; it owns cooldown/orphan recovery for `FAILED` and `SCRAPED` items.

- `retry_library`: checks if `finalize_item` job is already active; if not, re-enqueues it.
- `recover_incomplete_library`: only scans `FAILED` and `SCRAPED`, using scrape-candidate presence to choose whether a failed item should return to `scrape_item` or `parse_scrape_results`.

## Recovery Ownership Model

Keep the ownership split below stable unless both worker behavior and the compatibility contract
change together:

- `retry_library` owns orphaned stage re-entry.
- It repairs pipeline gaps for `REQUESTED`, `INDEXED`, `SCRAPED`, and `DOWNLOADED`.
- It maps those states to the next missing stage: `scrape_item`, `parse_scrape_results`, or `finalize_item`.

- `recover_incomplete_library` owns failed-item cooldown recovery.
- It scans `FAILED` items after cooldown and `SCRAPED` items that no longer have an active parse job.
- It can send failed items back to `parse_scrape_results` when scrape candidates already exist.
- It can send failed items back to `scrape_item` when no scrape candidates exist.

The shared recovery planner in the backend expresses this as item intent for Graph and future
frontend work, but the current REST/frontend compatibility layer should continue treating recovery
as item-level state plus retry diagnostics rather than exposing cron ownership.
