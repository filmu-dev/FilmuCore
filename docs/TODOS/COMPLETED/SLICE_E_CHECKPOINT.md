## E1 complete — 2026-03-20T23:18:22+00:00

Mounted VFS reads now flow through the Rust chunk engine in [`rust/filmuvfs/src/mount.rs`](../rust/filmuvfs/src/mount.rs), with `ChunkEngine` wired through [`rust/filmuvfs/src/runtime.rs`](../rust/filmuvfs/src/runtime.rs) and read-path coverage extended in [`rust/filmuvfs/tests/read_path.rs`](../rust/filmuvfs/tests/read_path.rs).

- `read()` now uses chunk planning + `moka::future` cache-backed resolution.
- Stale provider/CDN links trigger `RefreshCatalogEntry` and retry once before returning `ESTALE`.
- Rust targeted gate: `cargo test --manifest-path ./rust/filmuvfs/Cargo.toml -- --nocapture` ✅
- Python full gate remained unchanged at this step: `473 passed` ✅

## E2 complete — 2026-03-21T00:10:09+00:00

GraphQL compat subscriptions landed in [`filmu_py/graphql/schema.py`](../filmu_py/graphql/schema.py), [`filmu_py/graphql/types.py`](../filmu_py/graphql/types.py), and [`filmu_py/graphql/deps.py`](../filmu_py/graphql/deps.py), with subscription regression coverage in [`tests/test_graphql_subscriptions.py`](../tests/test_graphql_subscriptions.py).

- Subscriptions added: `itemStateChanged`, `logStream`, `notifications`.
- `itemStateChanged` and `notifications` mirror the existing SSE payloads intentionally.
- `logStream` has since become the richer structured GraphQL log surface while the REST/SSE logging routes remain the frozen compatibility surfaces.
- `/graphql` supports both `graphql-transport-ws` and `graphql-ws`.
- Targeted GraphQL + stream regression suite: `194 passed` ✅

## E3 complete — 2026-03-21T00:30:51+00:00

Partial-season request scope now threads from [`scrape_item()`](../filmu_py/workers/tasks.py) into [`parse_scrape_results()`](../filmu_py/workers/tasks.py) and filters parsed candidates through [`filmu_py/services/media.py`](../filmu_py/services/media.py) before `StreamORM` persistence.

- Latest partial request seasons are forwarded as `partial_seasons` ARQ job kwargs.
- Parsed candidates with no season info are kept; mismatched season-only candidates are dropped.
- Helper and worker coverage expanded in [`tests/test_partial_show_requests.py`](../tests/test_partial_show_requests.py) and [`tests/test_scraped_item_worker.py`](../tests/test_scraped_item_worker.py).
- Python full gate: `486 passed` ✅

## E4 complete — 2026-03-21T00:42:38+00:00

[`filmu_py/plugins/builtin/stremthru.py`](../filmu_py/plugins/builtin/stremthru.py) is now a real DownloaderPlugin implementation backed by the StremThru v0 store API, with config support in [`filmu_py/config.py`](../filmu_py/config.py) and DTO normalization in [`filmu_py/plugins/interfaces.py`](../filmu_py/plugins/interfaces.py).

- `add_magnet()` performs authenticated magnet creation.
- `get_status()` maps StremThru status values into normalized downloader status values.
- `get_download_links()` resolves downloadable file links and supports file-id filtering.
- Coverage added in [`tests/test_stremthru_plugin.py`](../tests/test_stremthru_plugin.py).
- Python full gate: `494 passed` ✅

## SLICE E COMPLETE — 2026-03-21T00:49:56+00:00

Tracks closed:

- E1 — VFS chunk-engine-backed mounted reads
- E2 — GraphQL compat subscriptions
- E3 — Partial season filtering in parse stage
- E4 — StremThru real downloader integration

Final quality summary:

- Python full gate: `494 passed`
- Rust cargo test gate: green
- Targeted GraphQL/stream suite: `194 passed`

GraphQL strategy locked:

- `REST /api/v1/*` → frozen compatibility surface
- `SSE /api/v1/stream/*` → frozen compatibility surface
- `GraphQL /graphql` → future-facing growth surface, compat subscriptions first

Next-session scope:

- adaptive Rust prefetching
- optional L2 disk cache
- broader GraphQL mutation coverage
- future rich-field expansion of current compat subscription types

Final gate confirmation:

- `ruff check .` ✅
- `mypy --strict filmu_py/` ✅
- `pytest -q --tb=short` ✅ (`494 passed in 19.69s`)
- `cargo test --manifest-path ./rust/filmuvfs/Cargo.toml` ✅
