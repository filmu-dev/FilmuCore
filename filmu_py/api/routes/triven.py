"""Legacy `triven/*` compatibility routes still referenced by the frontend."""

from __future__ import annotations

from typing import Annotated, Literal, cast

from fastapi import APIRouter, Depends, HTTPException, Path, status
from pydantic import BaseModel

from filmu_py.api.deps import get_media_service
from filmu_py.services.media import MediaService

router = APIRouter(prefix="/triven", tags=["triven"])


class TrivenItemResponse(BaseModel):
    """Minimal legacy item payload used by the public watch redirect flow."""

    id: str
    type: Literal["movie", "show", "season", "episode"]


@router.get("/item/{id}", operation_id="triven.item", response_model=TrivenItemResponse)
async def get_triven_item(
    id: Annotated[str, Path(min_length=1)],
    media_service: Annotated[MediaService, Depends(get_media_service)],
) -> TrivenItemResponse:
    """Return the smallest legacy item payload needed by the watch redirect.

    The current frontend only uses this route to decide whether an item should be
    redirected to `movie` or `tv` details. We therefore resolve the identifier across
    the current item surfaces and normalize `tv` items to the legacy `show` type.
    """

    for media_type, normalized_type in (
        ("tv", "show"),
        ("movie", "movie"),
        ("item", None),
    ):
        result = await media_service.get_item_detail(id, media_type=media_type, extended=False)
        if result is None:
            continue

        resolved_result = result
        item_type = cast(
            Literal["movie", "show", "season", "episode"],
            normalized_type or resolved_result.type,
        )
        if item_type not in {"movie", "show", "season", "episode"}:
            item_type = "movie"

        return TrivenItemResponse(id=resolved_result.id, type=item_type)

    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")
