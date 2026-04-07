# Scrape Candidate Size Persistence

## Why this exists

Large torrent results from providers such as [`prowlarr`](../filmu_py/plugins/builtin/prowlarr.py:1) can report `size_bytes` values far above PostgreSQL 32-bit `INTEGER` range.

Examples observed in live worker failures:

- `33265696768`
- `45851508736`
- `195738042368`

Those values previously crashed [`scrape_item()`](../filmu_py/workers/tasks.py:913) while [`MediaService.persist_scrape_candidates()`](../filmu_py/services/media.py:2230) attempted to insert rows into [`ScrapeCandidateORM`](../filmu_py/db/models.py:435).

## Fix

[`ScrapeCandidateORM.size_bytes`](../filmu_py/db/models.py:455) now uses `BIGINT` instead of `INTEGER`.

The schema upgrade is captured in [`20260318_0017_scrape_candidates_size_bigint.py`](../filmu_py/db/alembic/versions/20260318_0017_scrape_candidates_size_bigint.py:1).

## Follow-up: downloader/media-entry size overflow

Once scrape candidate persistence was fixed, the next live failure surfaced in the downloader stage: provider-backed media entries were still persisting large file sizes into 32-bit columns.

That follow-up is addressed by promoting:

- [`MediaEntryORM.size_bytes`](../filmu_py/db/models.py:574)
- [`PlaybackAttachmentORM.file_size`](../filmu_py/db/models.py:150)

through [`20260318_0018_media_entry_sizes_bigint.py`](../filmu_py/db/alembic/versions/20260318_0018_media_entry_sizes_bigint.py:1).

## Operational note

If the worker is still throwing `value out of int32 range` during scrape persistence, the running database has not yet applied [`20260318_0017`](../filmu_py/db/alembic/versions/20260318_0017_scrape_candidates_size_bigint.py:1).

In that case, restart the local stack so migrations run before retrying scrape jobs.
