# FilmuCore for Windows Hosts

This guide is for Windows users running Jellyfin, Plex, or Emby on Windows.

The supported Windows topology is:

- Docker Compose runs the FilmuCore backend services.
- `filmuvfs.exe` runs on the Windows host with a native Windows mount adapter.
- FilmuVFS mounts into a normal Windows folder chosen by the operator, with `C:\FilmuCoreVFS` as the canonical helper-managed path.
- The media server uses `C:\FilmuCoreVFS\movies` and `C:\FilmuCoreVFS\shows` as library paths.

Do not use `\\wsl.localhost\...\mnt\filmuvfs` as the intended playback path for Windows-hosted media servers. That path is still useful for debugging, but it is not the product-grade Windows playback topology.

Current build status:

- the local Docker/WSL stack now also provisions isolated real Plex at `http://localhost:32401/web` and real Emby at `http://localhost:8097` for parity testing against the same mounted library tree
- `winfsp` is now the preferred Windows-native adapter for playback because it avoids ProjFS file hydration on the mount path.
- `auto` resolves to `winfsp` on Windows in the current runtime.
- `projfs` remains available for diagnostics and compatibility, but it hydrates file data into the virtualization root and can therefore consume host disk space over time.
- The raw WinFSP folder-mount path in [`rust/filmuvfs/src/windows_winfsp.rs`](/E:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/windows_winfsp.rs) is now validated beyond first bring-up: root enumeration works, direct `ffprobe` works, the native Windows soak/remux gate passes on `C:\FilmuCoreVFS`, Jellyfin reaches sustained mounted reads and software transcode, and sampled native Emby playback/probe/stream-open checks now succeed across multiple titles.
- Recent Windows read-path hardening also includes in-flight foreground chunk-fetch coalescing in [`rust/filmuvfs/src/chunk_engine.rs`](/E:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/chunk_engine.rs), which reduced duplicate upstream fetches and noticeably improved Emby buffering behavior.
- Native Windows Plex should still be treated as a separate parity target unless a real local Plex Media Server is installed against `C:\FilmuCoreVFS`. Docker Plex on `32401` is valuable parity evidence, but it is the Linux/WSL validation topology rather than native Windows PMS evidence.
- Native Windows media-center support is now treated as a first-class FilmuVFS contract for Jellyfin, Emby, and Plex on `C:\FilmuCoreVFS`. The repo now exposes a native Windows provider-gate wrapper in [`scripts/run_windows_media_server_gate.ps1`](/E:/Dev/Filmu/FilmuCore/scripts/run_windows_media_server_gate.ps1) plus package entrypoints `proof:windows:vfs:providers` and `proof:windows:vfs:providers:gate`. Current recorded evidence is live-green for Emby and previously validated for Jellyfin; native Windows Plex uses the same gate surface once a real local Plex Media Server is configured against the mount.

## Prerequisites

- Windows with the Projected File System feature available
- Docker Desktop
- PowerShell
- Rust toolchain if you are building `filmuvfs.exe` from source
- a writable mount folder, for example `C:\FilmuCoreVFS`
- a configured `.env` file copied from [`.env.example`](/E:/Dev/Filmu/FilmuCore/.env.example)

If the effective mount adapter is `projfs` and the ProjFS Windows feature is not enabled yet, `start_windows_stack.ps1` now attempts to enable it automatically before the backend stack starts. The script will request Windows elevation through UAC when needed, and Windows will usually still require one reboot before the native mount can start.

Manual fallback:

```powershell
dism.exe /online /Enable-Feature /FeatureName:Client-ProjFS /All
```

Then reboot if Windows asks for it.

## 1. Configure the backend

Copy the backend env template if you have not already:

```powershell
Copy-Item .env.example .env
```

At minimum, make sure your `.env` has:

- `FILMU_PY_API_KEY`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- one debrid provider key such as `REAL_DEBRID_API_KEY`
- `FRONTEND_AUTH_SECRET`
- `FRONTEND_ORIGIN`

`TMDB_API_KEY` is strongly recommended.

## 2. Preferred startup path: auto-detect the host OS

From [FilmuCore](/E:/Dev/Filmu/FilmuCore), the preferred startup command is:

```powershell
pnpm run stack:start
```

That launcher resolves `FILMU_STACK_VFS_MODE=auto` against the host OS and dispatches to the Windows-native startup path on Windows hosts. On Windows, that means it:

- starts the backend services from `docker-compose.windows.yml`
- waits for the backend API and gRPC catalog supplier
- starts the native `filmuvfs.exe` host process for the configured mount path

If you set `FILMU_STACK_VFS_MODE=windows`, the same launcher is forced onto the Windows-native path explicitly.

## 3. Backend-only compose path

If you run Compose manually:

```powershell
docker compose -f docker-compose.windows.yml up -d
```

that starts only the backend containers and exposes the FilmuVFS gRPC catalog supplier on `localhost:50051`.

It does **not** start the native Windows FilmuVFS mount automatically. The compose file is backend-only by design on Windows because the mount must run as a host-native `filmuvfs.exe` process, not as a Linux container.

Optional verification:

```powershell
pnpm run stack:status
.\status_windows_stack.ps1
.\check_windows_stack.ps1
pnpm run stack:validate
```

You should see the backend on `http://localhost:8000`, the frontend on `http://localhost:3000`, and the catalog supplier on `localhost:50051`.

## 4. Start the native Windows FilmuVFS mount

Build the host binary if you do not already have it:

```powershell
cargo build --release --manifest-path .\rust\filmuvfs\Cargo.toml
```

Create the mount folder once. The folder path is fully configurable; `C:\FilmuCoreVFS` is the canonical example used by the helper scripts and current Windows-host playback validation:

```powershell
New-Item -ItemType Directory -Path C:\FilmuCoreVFS -Force
```

Then start the Windows-native FilmuVFS adapter:

```powershell
.\rust\filmuvfs\target\release\filmuvfs.exe `
  --mountpoint C:\FilmuCoreVFS `
  --mount-adapter auto `
  --grpc-server http://127.0.0.1:50051
```

The helper and the binary accept these Windows adapter values:

- `winfsp` (recommended)
- `auto` (resolves to `winfsp`)
- `projfs` (diagnostic/compatibility path; hydrated reads consume local disk space)

By default, use `winfsp` (or `auto`). `projfs` should only be used when you specifically need to debug the Projected File System path, because hydrated reads persist data under the mount root and do not behave like the bounded temporary chunk cache used by the Linux path.

Optional operator tuning:

- `--windows-projfs-summary-interval-seconds 300`
- `0` disables the periodic Windows ProjFS summary task
- `--prefetch-min-chunks 4`
- `--prefetch-max-chunks 16`
- `--prefetch-startup-chunks 8`
- `--chunk-size-scan-kb 1024`

Startup warmup now stages an initial contiguous read window based on the dedicated startup prefetch setting. In practice, raising `--prefetch-startup-chunks` increases how much media is warmed into cache as soon as the file handle opens, which helps playback start faster and reduces early buffering. `--prefetch-min-chunks` and `--prefetch-max-chunks` still control the steady-state adaptive prefetch behavior after playback has started.

Equivalent environment variables are also supported:

- `FILMUVFS_MOUNTPOINT`
- `FILMUVFS_MOUNT_ADAPTER=projfs|winfsp|auto`
- `FILMUVFS_WINFSP_USE_WRAPPER=1` (forces the `winfsp_wrs` wrapper backend for diagnostics)
- `FILMUVFS_WINFSP_ALLOW_WRAPPER_FALLBACK=1` (re-enables fallback to wrapper if raw WinFSP startup fails; default is disabled)
- `FILMUVFS_WINDOWS_PROJFS_SUMMARY_INTERVAL_SECONDS`
- `FILMUVFS_PREFETCH_MIN_CHUNKS`
- `FILMUVFS_PREFETCH_MAX_CHUNKS`
- `FILMUVFS_PREFETCH_STARTUP_CHUNKS`
- `FILMUVFS_CHUNK_SIZE_SCAN_KB`

Or use the helper script:

```powershell
.\start_windows_stack.ps1 -MountPath C:\FilmuCoreVFS
```

Or use the unified launcher with Windows selected automatically:

```powershell
pnpm run stack:start
```

To force the choice from config, set `FILMU_STACK_VFS_MODE=windows` in `.env` or the host environment.

That records both the requested adapter (`auto` by default) and the effective adapter selected by policy/runtime.

To select the Windows adapter explicitly:

```powershell
.\start_windows_stack.ps1 -MountPath C:\FilmuCoreVFS -MountAdapter projfs
```

The helper exposes the same playback-start tuning knobs:

```powershell
.\start_windows_stack.ps1 `
  -MountPath C:\FilmuCoreVFS `
  -PrefetchStartupChunks 8 `
  -PrefetchMinChunks 4 `
  -PrefetchMaxChunks 16 `
  -ScanChunkSizeKb 1024
```

If `auto`/`projfs` is selected and Client-ProjFS is missing, that helper will:

- detect the missing feature before starting Docker services
- request elevation automatically and attempt to enable `Client-ProjFS`
- stop with an explicit reboot-required message when Windows applies the feature

## 5. Configure your media server

Use normal Windows folder paths. Replace `C:\FilmuCoreVFS` with whatever mount folder you configured:

- movies: `C:\FilmuCoreVFS\movies`
- shows: `C:\FilmuCoreVFS\shows`

Examples:

- Jellyfin: add `C:\FilmuCoreVFS\movies` and `C:\FilmuCoreVFS\shows`
- Emby: add `C:\FilmuCoreVFS\movies` and `C:\FilmuCoreVFS\shows`
- Plex: add `C:\FilmuCoreVFS\movies` and `C:\FilmuCoreVFS\shows`

For local parity testing through the Docker/WSL stack, the helper scripts now also expose:

- Plex web UI: `http://localhost:32401/web`
- Emby web UI: `http://localhost:8097`

Jellyfin note:

- If playback fails with `h264_amf` / AMF initialization errors, disable hardware transcoding first and re-test with software encoding. That encoder failure is separate from the FilmuVFS read path; software transcode is the currently verified Windows-host playback gate.
- If Jellyfin direct-stream fails with a bitstream-filter mismatch such as `h264_mp4toannexb` against a file that ffmpeg probes as a different codec (for example `av1`), force a full metadata refresh for that item before treating it as VFS corruption. The proof harness reports this as `metadata_mismatch`.

## 5. Stop the stack

If you started the native mount through the helper script, stop it with:

```powershell
pnpm run stack:stop
.\stop_windows_stack.ps1
```

If you started `filmuvfs.exe` manually, stop that process directly with `Ctrl+C`.

Then stop Docker services when you are done:

```powershell
docker compose -f docker-compose.windows.yml down
```

## Notes

- The Windows mount is folder-based.
- Drive-letter aliases are intentionally ignored in the managed helper path.
- Files only appear after FilmuCore completes the acquisition pipeline for the requested item.
- The backend stack is shared with Linux. The difference is only the mounted filesystem adapter.
- The Windows helper scripts are:
  - [start_windows_stack.ps1](/E:/Dev/Filmu/FilmuCore/scripts/start_windows_stack.ps1)
  - [check_windows_stack.ps1](/E:/Dev/Filmu/FilmuCore/scripts/check_windows_stack.ps1)
  - [status_windows_stack.ps1](/E:/Dev/Filmu/FilmuCore/scripts/status_windows_stack.ps1)
  - [stop_windows_stack.ps1](/E:/Dev/Filmu/FilmuCore/scripts/stop_windows_stack.ps1)
- The repository-level split-stack validation helper is [validate_platform_stack.ps1](/E:/Dev/Filmu/FilmuCore/scripts/validate_platform_stack.ps1).

