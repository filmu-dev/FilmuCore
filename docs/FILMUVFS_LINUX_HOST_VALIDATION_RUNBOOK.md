# FilmuVFS Linux-host validation runbook

This runbook is the explicit gate between the first Unix-only mount lifecycle slice and the next chunk-engine implementation slice.

Do **not** begin chunk geometry planning, `moka` in-memory cache wiring, `tokio::sync::Semaphore`-based prefetch scheduling, or the `hyper` HTTP/2 pool work until every automated and manual check below passes on a real Linux host with FUSE support.

## Scope of this gate

- The automated Linux gate centers on [`rust/filmuvfs/tests/mount_lifecycle.rs`](../rust/filmuvfs/tests/mount_lifecycle.rs), which validates the real `fuse3` mount lifecycle against a mock catalog supplier.
- The sidecar command-line contract used in the manual steps below comes from [`rust/filmuvfs/src/config.rs`](../rust/filmuvfs/src/config.rs).
- The Python backend's default gRPC bind address is `127.0.0.1:50051` in [`filmu_py/config.py`](../filmu_py/config.py).
- Important scope note: [`rust/filmuvfs/tests/mount_lifecycle.rs`](../rust/filmuvfs/tests/mount_lifecycle.rs) currently proves mount + list + stat + clean unmount. It does **not yet** assert mounted file-byte reads end-to-end, so the manual mounted-read validation below is mandatory before the chunk-engine slice starts.
- The current source tree now also includes two Linux/WSL helper scripts: [`../rust/filmuvfs/scripts/run_mount_lifecycle_gate.sh`](../rust/filmuvfs/scripts/run_mount_lifecycle_gate.sh) and [`../rust/filmuvfs/scripts/run_manual_mount_smoke.sh`](../rust/filmuvfs/scripts/run_manual_mount_smoke.sh).

## Current verified status in this workspace

- The automated WSL/Linux gate now passes through [`../rust/filmuvfs/tests/mount_lifecycle.rs`](../rust/filmuvfs/tests/mount_lifecycle.rs) and [`../rust/filmuvfs/scripts/run_mount_lifecycle_gate.sh`](../rust/filmuvfs/scripts/run_mount_lifecycle_gate.sh).
- Manual WSL mount/list/stat/read smoke now also passes through [`../rust/filmuvfs/scripts/run_manual_mount_smoke.sh`](../rust/filmuvfs/scripts/run_manual_mount_smoke.sh).
- Plex/Emby playback validation on the mounted path has now also passed in this workspace, so this runbook's gate is currently satisfied.
- The next work after this runbook is no longer “first successful playback.” It is chunk-engine adoption in mounted reads plus operational hardening of the mount/control-plane path.

## A. Pre-validation setup

Run all commands from the repository root on a Linux host that exposes `/dev/fuse`.

```bash
# Install FUSE userspace packages (or the distro-equivalent packages)
sudo apt-get update
sudo apt-get install -y fuse3 libfuse3-dev

# Confirm the Linux host actually exposes the FUSE device required by the test
ls -l /dev/fuse

# Build the sidecar and the Linux-gated integration tests
cargo build --manifest-path rust/filmuvfs/Cargo.toml --features integration-tests
```

If `/dev/fuse` is missing, do not continue until the host/kernel/container configuration exposes it.

## B. Integration test commands

Run the gated mount lifecycle integration test:

```bash
cargo test --manifest-path rust/filmuvfs/Cargo.toml \
  --features integration-tests \
  mount_lifecycle -- --nocapture
```

Equivalent convenience command from Linux/WSL:

```bash
bash ./rust/filmuvfs/scripts/run_mount_lifecycle_gate.sh
```

Expected automated validation checklist:

- [ ] The test starts only on Linux with FUSE available and does not abort on `/dev/fuse` absence.
- [ ] Mount succeeds at the temporary mountpoint created by the test (typically under `/tmp`).
- [ ] The initial catalog snapshot is received from the mock gRPC catalog supplier.
- [ ] Directory listing through the mounted root returns `movies` and `shows`.
- [ ] `stat` on the sample movie/show paths succeeds through the mounted filesystem.
- [ ] Unmount/shutdown completes cleanly without hanging.

Supporting note:

- Byte-read correctness is **not yet** asserted by [`rust/filmuvfs/tests/mount_lifecycle.rs`](../rust/filmuvfs/tests/mount_lifecycle.rs). Keep the manual mounted-read checks in Section C as part of the required gate.

## C. Manual Plex/Emby validation

### 1. Start the Python backend (catalog supplier)

```bash
uv run python -m filmu_py.main
```

By default, this exposes the FilmuVFS gRPC supplier on `127.0.0.1:50051`.

If you are using the local Docker stack instead of a host-native Python process, [`../docker-compose.local.yml`](../docker-compose.local.yml) now supports two practical validation modes:

- a host-native Rust sidecar process against the published backend gRPC supplier on `http://127.0.0.1:50051`
- the Dockerized [`filmuvfs`](../docker-compose.local.yml) sidecar against [`filmu-python`](../docker-compose.local.yml) on the Compose network at `http://filmu-python:50051`

### 2. Start the Rust sidecar on the Linux host

In a second terminal:

```bash
./rust/filmuvfs/target/debug/filmuvfs \
  --mountpoint /mnt/filmuvfs \
  --grpc-server http://127.0.0.1:50051
```

### 3. Verify the mount and enumerate real paths

In a third terminal:

```bash
ls -la /mnt/filmuvfs
ls -la /mnt/filmuvfs/movies
ls -la /mnt/filmuvfs/shows

# Pick a real mounted media path for the next checks
find /mnt/filmuvfs/movies -type f | head -n 5
find /mnt/filmuvfs/shows -type f | head -n 5
```

### 4. Validate metadata and mounted byte reads

Replace `/mnt/filmuvfs/movies/<path-to-file>.mkv` with an actual mounted file discovered above.

```bash
stat /mnt/filmuvfs/movies/<path-to-file>.mkv
head -c 1000 /mnt/filmuvfs/movies/<path-to-file>.mkv | wc -c
```

The byte-read check must return `1000` for a sufficiently large media file. If the file is smaller, confirm that the command still returns non-zero bytes without `EIO` or `ESTALE`.

Equivalent convenience command from Linux/WSL:

```bash
bash ./rust/filmuvfs/scripts/run_manual_mount_smoke.sh
```

That helper mounts the sidecar, prints the discovered tree, performs `stat`, reads `1000` bytes from the first discovered media file, and emits `NO_MEDIA_FILES_FOUND` if the catalog currently has no file entries.

### 5. Validate Plex/Emby traversal and playback

1. Add `/mnt/filmuvfs/movies` as a movie library path in Plex and/or Emby.
2. Add `/mnt/filmuvfs/shows` as a show library path if show traversal is part of the validation pass.
3. Trigger a scan.
4. Open a discovered item and attempt playback for at least 30 seconds.

### 6. Inspect error surfaces while the validation is running

```bash
# Kernel/FUSE errors
dmesg | tail -n 100

# Sidecar stderr/log output should stay free of mount/read failures
# Backend logs should stay free of catalog-stream errors
```

Check these sources during browse and playback:

- Plex logs
- Emby logs
- sidecar stderr / service logs
- Python backend logs
- `dmesg` for FUSE-level failures

### 7. Clean unmount

```bash
fusermount3 -u /mnt/filmuvfs
```

Treat lazy or forced cleanup as a failure that requires hardening work before the next implementation slice begins.

## D. Validation checklist

- [ ] Mount succeeds without errors.
- [ ] `ls /mnt/filmuvfs` shows `movies` and `shows`.
- [ ] `ls /mnt/filmuvfs/movies` shows catalog entries.
- [ ] `stat` against a mounted media file succeeds.
- [ ] `head -c 1000 /mnt/filmuvfs/movies/<path-to-file>.mkv | wc -c` returns bytes without `EIO` or `ESTALE`.
- [ ] Plex detects the mounted library and shows files.
- [ ] Plex and/or Emby can play at least 30 seconds without buffering or fatal read errors.
- [ ] No `EIO`, `ESTALE`, transport, or catalog-stream errors appear in sidecar/backend/media-server logs.
- [ ] `fusermount3 -u /mnt/filmuvfs` succeeds cleanly.

## Release gate

This gate has now passed in the current workspace. The next slice can begin, but this checklist should still be reused as the regression gate before future FilmuVFS claims:

- chunk geometry planning
- `moka` in-memory cache wiring
- `tokio::sync::Semaphore`-based read-ahead / priority scheduling
- `hyper` HTTP/2 upstream pool integration

## Current known open issue

- Even with playback validation passing, the current WatchCatalog control-plane can still fall back into reconnect/repoll behavior after the initial snapshot in some sessions, which causes repeated serve-time link refresh attempts from [`../filmu_py/services/vfs_server.py`](../filmu_py/services/vfs_server.py). Treat that as an active runtime hardening item rather than a passed quality gate.
