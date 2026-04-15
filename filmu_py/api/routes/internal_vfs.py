"""Internal FilmuVFS transport routes used by local sidecar hosts."""

from __future__ import annotations

import os

from fastapi import APIRouter, HTTPException, Query, Request
from google.protobuf.message import DecodeError
from starlette.responses import Response

from filmuvfs.catalog.v1 import catalog_pb2

router = APIRouter(prefix="/internal/vfs", tags=["internal-vfs"])


def _verify_internal_vfs_key(request: Request) -> None:
    resources = request.app.state.resources
    expected = os.environ.get("FILMU_PY_API_KEY", "").strip()
    if not expected:
        expected = resources.settings.api_key.get_secret_value().strip()
    provided = request.headers.get("x-filmu-vfs-key", "").strip()
    if not expected or not provided or provided != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing internal VFS key")


@router.get("/watch-event.pb", operation_id="internal_vfs.watch_event_protobuf")
async def get_vfs_catalog_watch_event_protobuf(
    request: Request,
    last_applied_generation_id: str | None = Query(default=None),
) -> Response:
    """Return one protobuf catalog event for sidecar HTTP polling fallback clients."""

    _verify_internal_vfs_key(request)
    resources = request.app.state.resources
    if resources.vfs_catalog_server is None:
        raise HTTPException(status_code=503, detail="FilmuVFS catalog server is unavailable")

    event = await resources.vfs_catalog_server.build_poll_event(
        last_applied_generation_id=last_applied_generation_id
    )
    return Response(content=event.SerializeToString(), media_type="application/x-protobuf")


@router.post("/refresh-entry.pb", operation_id="internal_vfs.refresh_entry_protobuf")
async def post_vfs_catalog_refresh_entry_protobuf(request: Request) -> Response:
    """Resolve one protobuf inline-refresh request for sidecar HTTP fallback clients."""

    _verify_internal_vfs_key(request)
    resources = request.app.state.resources
    if resources.vfs_catalog_server is None:
        raise HTTPException(status_code=503, detail="FilmuVFS catalog server is unavailable")

    body = await request.body()
    proto_request = catalog_pb2.RefreshCatalogEntryRequest()
    try:
        proto_request.ParseFromString(body)
    except DecodeError as exc:
        raise HTTPException(status_code=400, detail="invalid protobuf request payload") from exc

    proto_response = await resources.vfs_catalog_server.refresh_catalog_entry_message(
        provider_file_id=proto_request.provider_file_id,
        handle_key=proto_request.handle_key,
        entry_id=proto_request.entry_id,
    )
    return Response(content=proto_response.SerializeToString(), media_type="application/x-protobuf")
