from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from filmu_py.core.event_bus import EventBus
from filmu_py.db.models import MediaItemORM, SubtitleEntryORM
from filmu_py.services.media import MediaService, _build_detail_record


def _build_item(*, item_id: str = "item-1") -> MediaItemORM:
    item = MediaItemORM(
        id=item_id,
        external_ref=f"tmdb:{item_id}",
        title="Example Movie",
        state="completed",
        attributes={"item_type": "movie", "tmdb_id": item_id},
    )
    item.subtitle_entries = []
    return item


class _ScalarResult:
    def __init__(self, rows: list[SubtitleEntryORM]) -> None:
        self._rows = rows

    def scalars(self) -> _ScalarResult:
        return self

    def all(self) -> list[SubtitleEntryORM]:
        return self._rows


@dataclass
class _Storage:
    items: dict[str, MediaItemORM] = field(default_factory=dict)
    subtitles: dict[str, SubtitleEntryORM] = field(default_factory=dict)


class _Session:
    def __init__(self, storage: _Storage) -> None:
        self.storage = storage

    def add(self, value: object) -> None:
        if isinstance(value, SubtitleEntryORM):
            self.storage.subtitles[value.id] = value
            item = self.storage.items.get(value.item_id)
            if item is not None:
                item.subtitle_entries.append(value)

    async def commit(self) -> None:
        return None

    async def refresh(self, _value: object) -> None:
        return None

    async def execute(self, statement: object) -> _ScalarResult:
        where_criteria = list(getattr(statement, "_where_criteria", ()))
        if getattr(statement, "table", None) is not None and statement.table.name == "subtitle_entries":
            target_id = where_criteria[0].right.value if where_criteria else None
            if isinstance(target_id, str) and target_id in self.storage.subtitles:
                entry = self.storage.subtitles.pop(target_id)
                item = self.storage.items.get(entry.item_id)
                if item is not None:
                    item.subtitle_entries = [candidate for candidate in item.subtitle_entries if candidate.id != target_id]
            return _ScalarResult([])

        target_item_id = where_criteria[0].right.value if where_criteria else None
        rows = [
            entry
            for entry in self.storage.subtitles.values()
            if target_item_id is None or entry.item_id == target_item_id
        ]
        rows.sort(key=lambda entry: (not entry.is_default, entry.language, entry.id))
        return _ScalarResult(rows)

    async def get(self, model: type[object], primary_key: str) -> object | None:
        if model is SubtitleEntryORM:
            return self.storage.subtitles.get(primary_key)
        if model is MediaItemORM:
            return self.storage.items.get(primary_key)
        return None

    async def delete(self, value: object) -> None:
        if isinstance(value, SubtitleEntryORM):
            self.storage.subtitles.pop(value.id, None)
            item = self.storage.items.get(value.item_id)
            if item is not None:
                item.subtitle_entries = [entry for entry in item.subtitle_entries if entry.id != value.id]
        if isinstance(value, MediaItemORM):
            self.storage.items.pop(value.id, None)
            for subtitle_id in [entry.id for entry in value.subtitle_entries]:
                self.storage.subtitles.pop(subtitle_id, None)


@dataclass
class _Runtime:
    storage: _Storage

    @asynccontextmanager
    async def session(self) -> AsyncIterator[_Session]:
        yield _Session(self.storage)


def test_add_and_get_subtitle_entry() -> None:
    item = _build_item()
    storage = _Storage(items={item.id: item})
    service = MediaService(db=_Runtime(storage), event_bus=EventBus())  # type: ignore[arg-type]

    asyncio.run(
        service.add_subtitle_entry(
            item.id,
            language="en",
            format="srt",
            source="opensubtitles",
            url="https://example.com/subtitles/en.srt",
            is_default=True,
        )
    )
    results = asyncio.run(service.get_subtitle_entries(item.id))

    assert len(results) == 1
    assert results[0].language == "en"
    assert results[0].format == "srt"
    assert results[0].source == "opensubtitles"


def test_remove_subtitle_entry() -> None:
    item = _build_item()
    storage = _Storage(items={item.id: item})
    service = MediaService(db=_Runtime(storage), event_bus=EventBus())  # type: ignore[arg-type]

    entry = asyncio.run(service.add_subtitle_entry(item.id, language="en", format="srt"))
    asyncio.run(service.remove_subtitle_entry(entry.id))

    assert storage.subtitles == {}


def test_item_detail_no_subtitles() -> None:
    item = _build_item()

    detail = _build_detail_record(item, extended=False)

    assert detail.subtitles == []


def test_item_detail_with_subtitles() -> None:
    item = _build_item()
    item.subtitle_entries = [
        SubtitleEntryORM(
            item_id=item.id,
            language="en",
            format="srt",
            source="opensubtitles",
            url="https://example.com/subtitles/en.srt",
            is_default=True,
        ),
        SubtitleEntryORM(
            item_id=item.id,
            language="pt",
            format="vtt",
            source="local",
            url=None,
            is_default=False,
        ),
    ]

    detail = _build_detail_record(item, extended=False)

    assert len(detail.subtitles) == 2
    assert detail.subtitles[0].language == "en"
    assert detail.subtitles[0].format == "srt"
    assert detail.subtitles[0].source == "opensubtitles"
    assert detail.subtitles[0].url == "https://example.com/subtitles/en.srt"
    assert detail.subtitles[0].is_default is True


def test_subtitle_cascade_delete() -> None:
    item = _build_item()
    storage = _Storage(items={item.id: item})
    service = MediaService(db=_Runtime(storage), event_bus=EventBus())  # type: ignore[arg-type]

    asyncio.run(service.add_subtitle_entry(item.id, language="en", format="srt"))
    session = _Session(storage)
    asyncio.run(session.delete(item))

    assert storage.subtitles == {}
