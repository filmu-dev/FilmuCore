# FilmuCore for Linux and Unix-like Hosts

This guide is for Linux and Unix-like users running Jellyfin, Plex, or Emby on the same host as the FilmuVFS mount.

The supported Linux/Unix topology is:

- Docker Compose runs the FilmuCore backend services.
- The `filmuvfs` sidecar uses the Linux `fuse3` adapter.
- FilmuVFS mounts into a normal host path such as `/mnt/filmuvfs`.
- The media server uses `/mnt/filmuvfs/movies` and `/mnt/filmuvfs/shows`.

## Prerequisites

- Linux or another Unix-like host with FUSE support
- Docker and Docker Compose
- `/dev/fuse` available on the host
- a writable mount root such as `/mnt/filmuvfs`
- a configured `.env` file copied from [`.env.example`](/E:/Dev/Filmu/FilmuCore/.env.example)

If you use `allow_other`, make sure `user_allow_other` is enabled in `/etc/fuse.conf`.

## 1. Configure the backend

Copy the backend env template if you have not already:

```bash
cp .env.example .env
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

## 2. Prepare the mount root

Create the mount root once:

```bash
sudo mkdir -p /mnt/filmuvfs
```

For the local Docker stack used in this repo, prepare the shared bind mount once so the containerized sidecar can project the mounted filesystem back to the host:

```bash
sudo mount --bind /mnt/filmuvfs /mnt/filmuvfs
sudo mount --make-shared /mnt/filmuvfs
```

Persist that bind mount in `/etc/fstab`:

```fstab
/mnt/filmuvfs /mnt/filmuvfs none bind 0 0
```

## 3. Start the stack

From [FilmuCore](/E:/Dev/Filmu/FilmuCore), start the normal local stack:

```bash
docker compose up -d --build
```

This includes the Linux `filmuvfs` sidecar container and mounts FilmuVFS at `/mnt/filmuvfs`.

Or use the unified launcher:

```bash
pnpm run stack:start
```

`FILMU_STACK_VFS_MODE=auto` resolves to the Unix `/mnt/filmuvfs` launcher on Unix-like hosts. You can also force `FILMU_STACK_VFS_MODE=unix`.

## 4. Configure your media server

Use normal local filesystem paths:

- movies: `/mnt/filmuvfs/movies`
- shows: `/mnt/filmuvfs/shows`

Examples:

- Jellyfin: add `/mnt/filmuvfs/movies` and `/mnt/filmuvfs/shows`
- Emby: add `/mnt/filmuvfs/movies` and `/mnt/filmuvfs/shows`
- Plex: add `/mnt/filmuvfs/movies` and `/mnt/filmuvfs/shows`

## 5. Verify the mount

Basic verification:

```bash
ls -la /mnt/filmuvfs
ls -la /mnt/filmuvfs/movies
ls -la /mnt/filmuvfs/shows
pnpm run stack:validate
```

Mounted-read smoke:

```bash
find /mnt/filmuvfs/movies -type f | head -n 5
```

If you want the deeper Linux validation flow, use [docs/FILMUVFS_LINUX_HOST_VALIDATION_RUNBOOK.md](/E:/Dev/Filmu/FilmuCore/docs/FILMUVFS_LINUX_HOST_VALIDATION_RUNBOOK.md).

## 6. Stop the stack

```bash
pnpm run stack:stop
docker compose down
```

Remove volumes too if needed:

```bash
docker compose down -v
```

## Notes

- This is the traditional Compose-mounted FilmuVFS path.
- Files only appear after FilmuCore completes the acquisition pipeline for the requested item.
- The Linux and Windows backends share the same control plane. The platform-specific difference is the mounted filesystem adapter.
