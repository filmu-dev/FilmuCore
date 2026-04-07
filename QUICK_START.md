# FilmuCore quick start

Choose the guide that matches the host running your media server.

## Windows hosts

Use [WINDOWS_README.md](/E:/Dev/Filmu/FilmuCore/WINDOWS_README.md).

Summary:

- run `docker-compose.windows.yml` for the backend services
- run `filmuvfs.exe` natively on Windows with a folder mount such as `C:\FilmuCoreVFS`
- point Jellyfin, Plex, or Emby at `C:\FilmuCoreVFS\movies` and `C:\FilmuCoreVFS\shows`
- `auto` still resolves to `projfs` by default on Windows
- `winfsp` still requires explicit opt-in (`FILMUVFS_ENABLE_EXPERIMENTAL_WINFSP=1`), but it is now the verified native playback path for current Windows-host Jellyfin validation
- use `pnpm run stack:validate` to sanity-check the split stack files and helper scripts

## Linux and Unix-like hosts

Use [LINUX_UNIX_README.md](/E:/Dev/Filmu/FilmuCore/LINUX_UNIX_README.md).

Summary:

- run the default `docker-compose.yml` stack, including the Linux `filmuvfs` sidecar
- mount into `/mnt/filmuvfs`
- point Jellyfin, Plex, or Emby at `/mnt/filmuvfs/movies` and `/mnt/filmuvfs/shows`
- use `pnpm run stack:validate` to sanity-check the split stack files and helper scripts

## Legacy WSL helper scripts

The existing PowerShell helpers are still useful for the Linux/FUSE validation path:

- [start_local_stack.ps1](/E:/Dev/Filmu/FilmuCore/start_local_stack.ps1)
- [status_local_stack.ps1](/E:/Dev/Filmu/FilmuCore/status_local_stack.ps1)
- [stop_local_stack.ps1](/E:/Dev/Filmu/FilmuCore/stop_local_stack.ps1)

They remain oriented around the WSL/Linux mount workflow and should not be treated as the primary Windows-host playback path now that the native Windows adapters exist.
