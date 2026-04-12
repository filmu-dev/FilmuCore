# FilmuCore quick start

Choose the guide that matches the host running your media server.

## Windows hosts

Use [WINDOWS_README.md](/E:/Dev/Filmu/FilmuCore/WINDOWS_README.md).

Summary:

- use `pnpm run stack:start` as the canonical startup path; it auto-detects Windows vs Unix-like hosts and dispatches to the correct stack launcher
- `docker-compose.windows.yml` is backend-only on Windows and does **not** start the native `filmuvfs.exe` mount by itself
- the Windows launcher then runs `filmuvfs.exe` natively on Windows with a folder mount such as `C:\FilmuCoreVFS`
- point Jellyfin, Plex, or Emby at `C:\FilmuCoreVFS\movies` and `C:\FilmuCoreVFS\shows`
- `auto` now prefers `winfsp` on Windows when the current build includes the WinFSP backend, and falls back to `projfs` otherwise
- `winfsp` is the verified native playback path for current Windows-host Jellyfin validation and avoids ProjFS hydration consuming the mount volume
- use `pnpm run stack:validate` to sanity-check the split stack files and helper scripts
- set `FILMU_STACK_VFS_MODE=auto|windows|unix` and use `pnpm run stack:start`, `pnpm run stack:status`, and `pnpm run stack:stop` for the unified launcher

## Linux and Unix-like hosts

Use [LINUX_UNIX_README.md](/E:/Dev/Filmu/FilmuCore/LINUX_UNIX_README.md).

Summary:

- use `pnpm run stack:start` as the canonical startup path; it auto-detects Windows vs Unix-like hosts and dispatches to the correct stack launcher
- the Unix/Linux path then runs the default `docker-compose.yml` stack, including the Linux `filmuvfs` sidecar
- mount into `/mnt/filmuvfs`
- point Jellyfin, Plex, or Emby at `/mnt/filmuvfs/movies` and `/mnt/filmuvfs/shows`
- use `pnpm run stack:validate` to sanity-check the split stack files and helper scripts
- `FILMU_STACK_VFS_MODE=auto` resolves to the Unix `/mnt/filmuvfs` launcher on Unix-like hosts

## Legacy WSL helper scripts

The existing PowerShell helpers are still useful for the Linux/FUSE validation path:

- [start_local_stack.ps1](/E:/Dev/Filmu/FilmuCore/start_local_stack.ps1)
- [status_local_stack.ps1](/E:/Dev/Filmu/FilmuCore/status_local_stack.ps1)
- [stop_local_stack.ps1](/E:/Dev/Filmu/FilmuCore/stop_local_stack.ps1)

They remain oriented around the WSL/Linux mount workflow and should not be treated as the primary Windows-host playback path now that the native Windows adapters exist.
