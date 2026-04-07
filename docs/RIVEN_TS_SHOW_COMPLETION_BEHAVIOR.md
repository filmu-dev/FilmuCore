# Show completion behavior — design and current implementation

This document describes how FilmuCore evaluates show completion and how it differs
from the legacy Riven Python backend and the Riven-TS reference implementation.

---

## Executive summary

The old Python/Riven backend promoted a show to `completed` as soon as **one episode
obtained a key** — far too coarse for multi-episode requests.

FilmuCore implements full scope-aware evaluation via `_evaluate_show_completion()`, which
compares *requested*, *released*, and *satisfied* episode tuples before deciding the terminal
state.

---

## Satisfaction definition (updated 2026-03-25)

An episode is considered **satisfied** when a `MediaEntryORM` row exists for that episode's
`media_item_id`. This means a download URL has been persisted from the debrid provider for
that specific episode.

> **Why not `ActiveStreamORM`?**
>
> `ActiveStreamORM` rows are written by the **playback service** only when a user initiates a
> playback session. Using them as the satisfaction signal caused an infinite
> `finalize_item → scrape_item` loop for all newly-fetched shows because no stream rows
> exist at initial library setup time. The fix swaps to `MediaEntryORM` (download URL present)
> as the canonical satisfaction signal.

---

## State outcomes from `finalize_item`

| Condition | Outcome |
|---|---|
| Item is a movie | Always → `COMPLETE` |
| Show: all released/requested episodes satisfied, no future episodes | → `COMPLETE` |
| Show: all released episodes satisfied, future episodes exist | → `ONGOING` |
| Show: some released episodes satisfied, some missing | → `PARTIALLY_COMPLETED` |
| Show: zero released episodes satisfied | Re-queue `scrape_item`, no state transition |

---

## Scope evaluation

`_evaluate_show_completion()` computes four sets:

| Set | Description |
|---|---|
| `requested_scope` | `(season, episode)` tuples derived from `ItemRequestORM` |
| `released_scope` | Subset of `requested_scope` where `aired_at ≤ now` |
| `future_scope` | Subset of `requested_scope` where `aired_at > now` |
| `satisfied_scope` | Episodes in `released_scope` with a `MediaEntryORM` row |

`missing_released = released_scope - satisfied_scope`

A show is complete only if `missing_released == []` and `future_scope == []`.
A show is ongoing if `missing_released == []` and `future_scope != []`.

---

## Recovery for stuck items

`retry_library` now also recovers items stuck in `DOWNLOADED` state (worker crash between
debrid completion and finalize enqueue) by re-enqueuing `finalize_item`.
`recover_incomplete_library` does not scan `DOWNLOADED`; it handles cooldown/orphan recovery
for `FAILED` and `SCRAPED` items.

---

## Ongoing show polling

`poll_ongoing_shows` re-evaluates `PARTIALLY_COMPLETED` and `ONGOING` shows periodically.
It now guards against double-enqueuing: if a `scrape_item` job is already active for a show
it skips that show to prevent duplicate scrape pressure.

---

## Legacy reference: what old Riven did

The legacy Python backend in `Triven_backend` treated a show as completed after
`show.seasons[0].episodes[0].set("key", "some_key")` — i.e., one episode key → complete show.
This behavior is explicitly **not** replicated in FilmuCore.

## Riven-TS reference

Riven-TS derives parent show state from child completion counts in
`media-item-state.subscriber.ts`. The April 2026 upstream refresh confirms that current
`main` also carries partial-request-aware `requestedSeasons` wiring plus scheduled re-index
follow-up, but the parent-state subscriber is still fundamentally child-count driven.
FilmuCore goes further by reasoning about **requested scope** and **release dates** rather
than raw child counts, enabling more accurate ONGOING vs PARTIALLY_COMPLETED disambiguation.
