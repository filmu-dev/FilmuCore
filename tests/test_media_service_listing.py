from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from filmu_py.services.media import MediaItemRecord, MediaService
from filmu_py.state.item import ItemState


class _FakeScalarResult:
    def __init__(self, values: list[object]) -> None:
        self._values = values

    def scalars(self) -> _FakeScalarResult:
        return self

    def all(self) -> list[object]:
        return list(self._values)


class _FakeSession:
    def __init__(self, rows: list[object]) -> None:
        self._rows = rows

    async def execute(self, _statement: object) -> _FakeScalarResult:
        return _FakeScalarResult(self._rows)


class _FakeDatabaseRuntime:
    def __init__(self, rows: list[object]) -> None:
        self._rows = rows

    @asynccontextmanager
    async def session(self) -> AsyncIterator[_FakeSession]:
        yield _FakeSession(self._rows)


def _media_row(*, item_id: str, tenant_id: str) -> Any:
    return type(
        "_Row",
        (),
        {
            "id": item_id,
            "external_ref": f"tmdb:{item_id}",
            "title": f"Title {item_id}",
            "state": ItemState.REQUESTED.value,
            "tenant_id": tenant_id,
            "attributes": {"tmdb_id": item_id},
            "created_at": datetime(2026, 4, 10, 12, 0, tzinfo=UTC),
            "updated_at": datetime(2026, 4, 10, 12, 0, tzinfo=UTC),
        },
    )()


def test_list_items_preserves_tenant_id_from_rows() -> None:
    service = object.__new__(MediaService)
    service._db = _FakeDatabaseRuntime([_media_row(item_id="1", tenant_id="tenant-a")])

    records = asyncio.run(MediaService.list_items(service))

    assert records == [
        MediaItemRecord(
            id="1",
            external_ref="tmdb:1",
            title="Title 1",
            state=ItemState.REQUESTED,
            tenant_id="tenant-a",
            attributes={"tmdb_id": "1"},
        )
    ]


def test_list_items_in_states_preserves_tenant_id_from_rows() -> None:
    service = object.__new__(MediaService)
    service._db = _FakeDatabaseRuntime([_media_row(item_id="2", tenant_id="tenant-b")])

    records = asyncio.run(
        MediaService.list_items_in_states(service, states=[ItemState.REQUESTED])
    )

    assert records == [
        MediaItemRecord(
            id="2",
            external_ref="tmdb:2",
            title="Title 2",
            state=ItemState.REQUESTED,
            tenant_id="tenant-b",
            attributes={"tmdb_id": "2"},
        )
    ]
