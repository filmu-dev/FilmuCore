# March 2026 Completed Slice Closeout — VFS Path Normalization and Local Zilean Stack

## Scope

This note records the now-completed work around:

- normalized mounted show layout in [`../../filmu_py/services/vfs_catalog.py`](../../filmu_py/services/vfs_catalog.py)
- catalog delta behavior for path migrations
- local Zilean availability inside the real [`../../docker-compose.local.yml`](../../docker-compose.local.yml) FilmuCore stack

## Completed work

### 1. Mounted show layout normalization

The Python FilmuVFS catalog supplier now projects show files under:

- `Show Title (Year)/Season XX/<sanitized source filename>`

Key points:

- show roots are normalized with year when available
- season folders are now inferred correctly for more provider filename shapes, including `S05x08`
- filenames intentionally preserve the underlying source naming shape after sanitization rather than force a synthetic `sXXeYY` rename

Relevant implementation:

- [`../../filmu_py/services/vfs_catalog.py`](../../filmu_py/services/vfs_catalog.py)
- [`../../tests/test_vfs_catalog.py`](../../tests/test_vfs_catalog.py)

### 2. Catalog path-migration deltas

[`../../filmu_py/services/vfs_catalog.py`](../../filmu_py/services/vfs_catalog.py) now emits removals when an existing catalog `entry_id` changes visible path.

This prevents stale root-level paths from surviving in the mounted tree after naming-policy changes or improved season inference.

### 3. Live mounted validation

Live verification against `Stranger Things (2016)` confirmed that the mounted tree now exposes:

- `Season 01`
- `Season 02`
- `Season 03`
- `Season 04`
- `Season 05`

instead of flattening all entries directly under the show root.

### 4. Local Zilean stack integration

The real FilmuCore local stack in [`../../docker-compose.local.yml`](../../docker-compose.local.yml) now includes:

- `zilean-postgres`
- `zilean`

Operator note:

- backend-facing scraper settings must use `http://zilean:8181`
- `http://localhost:8181` is only the host-facing URL and is incorrect from inside [`filmu-python`](../../docker-compose.local.yml)

## Validation snapshot

- targeted VFS catalog test suite passed after the normalization/path-migration changes
- live mounted filesystem confirmed season-folder grouping for a real show
- local FilmuCore Docker stack now runs a healthy `zilean` service in the same compose project as `filmu-python`

## Follow-on work

What remains is no longer first readable mounted show layout.
The next VFS-related work should focus on:

- deeper incoming path semantics in the mount/runtime layer
- mounted data-plane metrics and observability
- longer-running soak/backpressure validation
