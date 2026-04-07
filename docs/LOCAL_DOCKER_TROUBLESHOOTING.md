# Local Docker Troubleshooting

This document captures the recurring local-stack issues that recently affected [`docker-compose.local.yml`](../docker-compose.local.yml) and the concrete recovery steps that worked.

## Fast path (preferred)

Before using manual recovery commands, prefer the stack scripts:

- [`../start_local_stack.ps1`](../start_local_stack.ps1)
- [`../status_local_stack.ps1`](../status_local_stack.ps1)
- [`../stop_local_stack.ps1`](../stop_local_stack.ps1)

The startup script now includes mount-root preflight auto-repair for `/mnt/filmuvfs` and one automatic `docker compose` retry when the first startup fails due to stale mount-root state.

## 1. Backend/worker crash loops on startup

### Symptom

- [`filmu-python`](../docker-compose.local.yml) or [`arq-worker`](../docker-compose.local.yml) restarts repeatedly
- `docker compose ps` shows restart loops or `unhealthy`

### Known causes

#### API key too short

[`Settings`](../filmu_py/config.py:513) enforces a minimum 32-character [`FILMU_PY_API_KEY`](../filmu_py/config.py:513).

If the container receives a shorter value, startup fails before the app can boot.

#### Compose variable expansion produced an incomplete DSN

If `POSTGRES_DB`, `POSTGRES_USER`, or `POSTGRES_PASSWORD` are missing, the backend can boot with a broken DSN.

The local stack now provides default compose values in [`docker-compose.local.yml`](../docker-compose.local.yml), but this is still worth checking if the file is edited.

#### Wrong ARQ worker command

The worker should start through [`run_worker_entrypoint()`](../filmu_py/workers/tasks.py:1458) rather than an `arq` CLI invocation that expects a different worker-settings shape.

### Recovery steps

1. Check container logs:

```powershell
docker logs filmu-python
docker logs filmu-arq-worker
```

2. Recreate the local stack after fixing env/compose values. On Windows + WSL, run the command from WSL so [`filmuvfs`](../docker-compose.local.yml) can resolve `/mnt/filmuvfs` correctly:

```bash
docker compose -f docker-compose.local.yml down --remove-orphans
docker compose -f docker-compose.local.yml up -d postgres redis filmu-python arq-worker filmuvfs frontend
```

3. If [`filmuvfs`](../docker-compose.local.yml) fails with `bind source path does not exist: /mnt/filmuvfs` or `it is not a shared mount`, prepare the WSL mountpoint first and then retry:

```bash
sudo mkdir -p /mnt/filmuvfs
sudo mount --bind /mnt/filmuvfs /mnt/filmuvfs
sudo mount --make-rshared /mnt/filmuvfs
mountpoint -q /mnt/filmuvfs && echo MOUNTPOINT_OK
```

4. If `MOUNTPOINT_OK` is already true but [`docker compose`](../docker-compose.local.yml) still reports `bind source path does not exist: /mnt/filmuvfs`, the command is probably still being launched from Windows rather than from inside WSL. In that case, invoke Compose through [`wsl.exe`](../docker-compose.local.yml) and change into the Linux path first:

```powershell
wsl.exe -e sh -lc "cd /mnt/e/Dev/Filmu/FilmuCore && docker compose -f docker-compose.local.yml up -d filmuvfs"
```

This was the working recovery path during the latest live verification: the same [`filmuvfs`](../docker-compose.local.yml) service failed from the Windows shell with `/mnt/filmuvfs` bind-path validation, then started successfully when the identical compose file was launched from WSL.

### New stale mount-root failure class (ENOTCONN / transport endpoint not connected)

#### Symptom

- startup fails with WSL errors such as `Transport endpoint is not connected`
- `/mnt/filmuvfs` exists but cannot be listed (`ls` fails)
- Compose may fail with bind-source validation for `/mnt/filmuvfs`

#### Cause

The mount root itself can become stale/corrupted after interrupted FUSE/mount teardown, so simple `mkdir` or bind checks fail even before the sidecar mount starts.

#### Current behavior

[`../start_local_stack.ps1`](../start_local_stack.ps1) now auto-recovers this class by running a preflight repair routine before compose startup and retrying compose once after repair if needed.

#### Manual fallback (if needed)

```powershell
wsl.exe -d Ubuntu-22.04 -u root -- bash -lc "set +e; umount -l /mnt/filmuvfs 2>/dev/null; fusermount3 -uz /mnt/filmuvfs 2>/dev/null; cd /mnt; rm -rf filmuvfs 2>/dev/null; mkdir -p filmuvfs; timeout 2 ls filmuvfs >/dev/null"
```

If the path still cannot be listed, reset WSL once, then re-run [`../start_local_stack.ps1`](../start_local_stack.ps1):

```powershell
wsl.exe --shutdown
```

## 2. Backend shows `unhealthy` even though the app is up

### Symptom

- [`filmu-python`](../docker-compose.local.yml) is running but Compose marks it `unhealthy`
- logs show repeated `401` responses against the health probe

### Cause

[`/api/v1/health`](../filmu_py/api/routes/default.py:53) is behind the shared API-key dependency from [`create_api_router()`](../filmu_py/api/router.py:14).

Even a healthy app can be marked unhealthy if the probe key does not match the persisted runtime key.

### Fix

The compose healthcheck now probes [`/openapi.json`](../filmu_py/app.py:287), which is a safer unauthenticated liveness check for the local stack.

## 3. Frontend shows `Unable to fetch stats data!`

### Symptom

- frontend loads
- stats/dashboard requests fail with a `500`
- direct backend requests return `401 Unauthorized`

### Cause

The frontend sends [`BACKEND_API_KEY`](../docker-compose.local.yml), but the backend runtime key may have been replaced by the persisted compatibility settings row loaded through [`Settings.from_compatibility_dict()`](../filmu_py/config.py:727).

That means these two values can drift apart:

- frontend [`BACKEND_API_KEY`](../docker-compose.local.yml)
- persisted settings `api_key` stored in the database

### Detection

If this happens, compare:

- frontend container env
- backend container env
- persisted settings JSON row

If the DB `api_key` differs from the frontend key, the frontend will fail auth even when containers are otherwise healthy.

### Recovery

Either:

1. update the persisted settings `api_key` to match the frontend local-dev key, or
2. update the frontend env to match the persisted key, then restart the affected containers

After syncing the values, recreate [`filmu-python`](../docker-compose.local.yml) and [`frontend`](../docker-compose.local.yml).

## 4. Library posters disappear

### Symptom

- library items render with `poster_path: null`
- some items only show numeric TMDB titles or placeholder titles

### Cause

The item row may have:

- a `tmdb_id`
- no persisted `poster_path`
- no runtime [`TMDB_API_KEY`](../filmu_py/config.py:514) available to the backend

Without a backend TMDB key, request-time or read-time enrichment cannot fill the poster.

### Fix

The backend now backfills missing poster/title metadata on library reads through [`MediaService.search_items()`](../filmu_py/services/media.py:2575) and [`MediaService._hydrate_summary_records()`](../filmu_py/services/media.py:2627).

For that to work live, the backend container must receive a real [`TMDB_API_KEY`](../filmu_py/config.py:514), which is forwarded through [`docker-compose.local.yml`](../docker-compose.local.yml).

### Validation

After recreating the stack with a real TMDB key:

- query [`/api/v1/items`](../filmu_py/api/routes/items.py:182)
- verify that `poster_path` is an absolute `image.tmdb.org` URL

### Additional recovery behavior now present

On startup, the backend now enqueues one-shot repair jobs for stale library rows when ARQ is enabled:

- [`backfill_imdb_ids`](../filmu_py/workers/tasks.py:1436)
- [`recover_incomplete_library`](../filmu_py/workers/tasks.py:1271)
- [`retry_library`](../filmu_py/workers/tasks.py:1314)

This means a restart can legitimately change existing library rows from posterless/failed placeholders into enriched or requeued items without any frontend action.

## 5. Worker scraper path moved to plugin runtime

### What changed

[`scrape_item()`](../filmu_py/workers/tasks.py:913) no longer uses the deleted legacy scraper registry path.

It now resolves [`ScraperPlugin`](../filmu_py/plugins/interfaces.py:180) implementations through the runtime [`PluginRegistry`](../filmu_py/plugins/registry.py:59).

### Related helpers

- [`_resolve_plugin_registry()`](../filmu_py/workers/tasks.py:1225)
- [`_scrape_with_plugins()`](../filmu_py/workers/tasks.py:1344)
- [`register_builtin_plugins()`](../filmu_py/plugins/builtins.py:12)

### Operational note

If scrape behavior regresses, inspect plugin loading first, not the removed [`filmu_py/services/scrapers`](../filmu_py/services/scrapers) path.

### Current known upstream issue

If TV shows are still failing after metadata enrichment succeeds, inspect [`torrentio`](../filmu_py/plugins/builtin/torrentio.py) responses next. The local stack previously hit `403 Forbidden` against Torrentio even after `tmdb_id`, `imdb_id`, and `poster_path` were populated correctly. The backend now normalizes the official host to HTTPS, but an upstream `403` can still leave items with no candidates and therefore in `failed`.

### Torrentio configuration behavior

- The compatibility settings model already exposes [`scraping.torrentio`](../filmu_py/config.py#L454) through [`TorrentioConfig`](../filmu_py/config.py#L388).
- The current frontend settings page is schema-driven rather than hardcoded, so the enable/disable/configure controls come from the backend settings schema instead of a bespoke frontend form.
- The backend now treats Torrentio as disabled when it is unconfigured: [`TorrentioScraper.initialize()`](../filmu_py/plugins/builtin/torrentio.py#L70) defaults `enabled` to `False` unless the settings explicitly enable it.

### Scraped rows that never progress

If rows remain in `Scraped` with no `media_entries`, inspect Redis stale-result keys before assuming the parser/ranker never ran. Stable ARQ stage job ids can leave old `arq:result:*` entries behind, which can silently block re-enqueueing the same stage later.

The backend now clears those stale result keys inside the enqueue helpers before trying to enqueue parse/rank/debrid/finalize again:

- [`enqueue_parse_scrape_results()`](../filmu_py/workers/tasks.py#L314)
- [`enqueue_scrape_item()`](../filmu_py/workers/tasks.py#L352)
- [`enqueue_rank_streams()`](../filmu_py/workers/tasks.py#L380)
- [`enqueue_debrid_item()`](../filmu_py/workers/tasks.py#L392)
- [`enqueue_finalize_item()`](../filmu_py/workers/tasks.py#L404)

### Settings-save behavior for scrapers

Backend settings saves now update the in-memory plugin settings payload and worker plugin runtime without requiring frontend changes:

- [`_persist_runtime_settings()`](../filmu_py/api/routes/settings.py#L58)
- [`_resolve_plugin_registry()`](../filmu_py/workers/tasks.py#L1961)

This specifically fixes the case where a scraper like [`torrentio`](../filmu_py/plugins/builtin/torrentio.py#L60) was disabled in settings but workers kept using a stale startup-scoped plugin registry.

### Current remaining live blockers after retry/rebuild

After retrying all incomplete items in the local stack and monitoring worker logs, the dominant remaining blockers are:

- [`rank_streams.no_winner`](../filmu_py/workers/tasks.py#L1184) with `failure_reason=no_candidates_passing_fetch`
- [`debrid_item.rate_limited`](../filmu_py/workers/tasks.py#L1729) against Real-Debrid

Those are now the main operational limits preventing some items from reaching `Completed` after the settings-refresh and stale-result fixes.
