"""Container-local bridge for FilmuVFS host-side polling and inline refresh."""

from __future__ import annotations

import argparse
import sys
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import cast

from filmuvfs.catalog.v1 import catalog_pb2

_DEFAULT_BASE_URL = "http://127.0.0.1:8000/internal/vfs"
_DEFAULT_TIMEOUT_SECONDS = 30.0


def _read_url(url: str, *, key: str, timeout_seconds: float) -> bytes:
    request = urllib.request.Request(url, headers={"x-filmu-vfs-key": key})
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return cast(bytes, response.read())


def _post_proto(
    url: str,
    *,
    key: str,
    payload: bytes,
    timeout_seconds: float,
) -> bytes:
    request = urllib.request.Request(
        url,
        data=payload,
        headers={
            "x-filmu-vfs-key": key,
            "content-type": "application/x-protobuf",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return cast(bytes, response.read())


def _watch_event(args: argparse.Namespace) -> int:
    query = ""
    if args.last_applied_generation_id:
        query = "?" + urllib.parse.urlencode(
            {"last_applied_generation_id": args.last_applied_generation_id}
        )
    payload = _read_url(
        f"{args.base_url.rstrip('/')}/watch-event.pb{query}",
        key=args.key,
        timeout_seconds=args.timeout_seconds,
    )
    sys.stdout.buffer.write(payload)
    return 0


def _refresh_entry(args: argparse.Namespace) -> int:
    payload = catalog_pb2.RefreshCatalogEntryRequest(
        provider_file_id=args.provider_file_id,
        handle_key=args.handle_key,
        entry_id=args.entry_id,
    ).SerializeToString()
    response = _post_proto(
        f"{args.base_url.rstrip('/')}/refresh-entry.pb",
        key=args.key,
        payload=payload,
        timeout_seconds=args.timeout_seconds,
    )
    sys.stdout.buffer.write(response)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=_DEFAULT_BASE_URL)
    parser.add_argument("--key", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=_DEFAULT_TIMEOUT_SECONDS)
    subparsers = parser.add_subparsers(dest="command", required=True)

    watch_parser = subparsers.add_parser("watch-event")
    watch_parser.add_argument("--last-applied-generation-id")
    watch_parser.set_defaults(handler=_watch_event)

    refresh_parser = subparsers.add_parser("refresh-entry")
    refresh_parser.add_argument("--provider-file-id", required=True)
    refresh_parser.add_argument("--handle-key", required=True)
    refresh_parser.add_argument("--entry-id", required=True)
    refresh_parser.set_defaults(handler=_refresh_entry)

    args = parser.parse_args(argv)
    handler = cast(Callable[[argparse.Namespace], int], args.handler)
    return handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
