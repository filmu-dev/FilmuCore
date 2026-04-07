Here is a prompt for Roo Code / ChatGPT 5.4, to continue addressing the remainder of the issues found in the Docker Logs.

Prompt:
----------
Hey Roo/ChatGPT, we are working on the Filmu media management stack (a Python FastAPI backend in `FilmuCore` and a SvelteKit frontend in `Triven_frontend`).

I just ran a complete codebase audit and fixed several critical data-type bugs that were causing crashes in the pipeline. Here is what has ALREADY been fixed:
1. **32-Bit Integer Overflows (Postgres)**: Fixed by promoting `ScrapeCandidateORM.size_bytes`, `MediaEntryORM.size_bytes`, and `PlaybackAttachmentORM.file_size` to `BIGINT` (Migrations 0017 and 0018).
2. **UUID Identifier Routing Error**: The `GET /api/v1/items/{id}` route now natively handles namespaced text identifiers like `tvdb:12345` (saving against the `external_ref` column) natively instead of crashing asyncpg by casting them to UUIDs.
3. **Detail Page State Desync**: The Backend DB stores states completely in lowercase (e.g. `completed`), but the frontend expects cases like `Completed`. The backend's `ItemDetailResponse` and `ItemSummaryResponse` mappings have been updated to .title() the output state, fixing the frontend "Play" button block.

Here is what I need you to address NEXT:

1. **Worker Scraper Dependency Conflicts**
The pipeline runs fine because we use `asyncio.gather(return_exceptions=True)`, but the logs are throwing persistent `RequestsDependencyWarning` errors during the scrape stage due to broken library pinnings. The `pyproject.toml` has been heavily audited. Please analyze the worker logs (specifically around `chardet`, `urllib3`, and `charset-normalizer` conflicts) and ensure the Python 3.12+ async environments start cleanly without those scrape warnings.

2. **Ensure File Size Limits on Proxy Fetching**
Now that we accept `BIGINT` file sizes downstream for Real Debrid items, we need to ensure the upstream proxies (e.g. streaming chunks) correctly handle multi-gigabyte offsets. Review `filmu_py/core/byte_streaming.py` and ensure the cache/chunking engines gracefully handle files up to 100GB without attempting to pull them fully into RAM.

3. **Check Frontend Drizzle Migrations**
Although frontend code shouldn't be altered heavily, we need to ensure the local SQLite DB isn't throwing setup errors. Please verify the Drizzle setup inside `Triven_frontend/src/lib/server/db.ts` runs a clean migration sync against the recent API changes (run `pnpm run generate-api` to sync the new backend schemas).

Start by running the stack with `docker compose -f docker-compose.local.yml --env-file .env.local up -d` and checking the logs to confirm the previously noisy warnings are gone, then tackle the dependency warnings inside the worker container.
