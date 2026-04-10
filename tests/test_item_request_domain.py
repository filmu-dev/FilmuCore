from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from filmu_py.db.models import ItemRequestORM
from filmu_py.services.media import (
    MediaService,
    _infer_request_media_type,
    build_item_request_record,
    update_item_request_record,
)


def test_infer_request_media_type_prefers_item_type_metadata() -> None:
    assert (
        _infer_request_media_type(
            external_ref="tmdb:123",
            attributes={"item_type": "movie"},
        )
        == "movie"
    )
    assert (
        _infer_request_media_type(
            external_ref="tvdb:456",
            attributes={"item_type": "show"},
        )
        == "show"
    )


def test_infer_request_media_type_falls_back_to_external_ref_namespace() -> None:
    assert _infer_request_media_type(external_ref="tmdb:123", attributes={}) == "movie"
    assert _infer_request_media_type(external_ref="tvdb:456", attributes={}) == "show"
    assert _infer_request_media_type(external_ref="custom:789", attributes={}) == "unknown"


def test_build_item_request_record_sets_initial_request_intent_fields() -> None:
    requested_at = datetime(2026, 3, 14, 12, 0, tzinfo=UTC)

    record = build_item_request_record(
        external_ref="tmdb:123",
        media_item_id="item-1",
        requested_title="Example Movie",
        media_type="movie",
        requested_at=requested_at,
    )

    assert isinstance(record, ItemRequestORM)
    assert record.external_ref == "tmdb:123"
    assert record.media_item_id == "item-1"
    assert record.requested_title == "Example Movie"
    assert record.media_type == "movie"
    assert record.request_source == "api"
    assert record.request_count == 1
    assert record.first_requested_at == requested_at
    assert record.last_requested_at == requested_at


def test_update_item_request_record_increments_request_count_and_refreshes_fields() -> None:
    created_at = datetime(2026, 3, 14, 12, 0, tzinfo=UTC)
    updated_at = datetime(2026, 3, 14, 13, 0, tzinfo=UTC)
    record = build_item_request_record(
        external_ref="tvdb:456",
        media_item_id="item-old",
        requested_title="Old Show",
        media_type="show",
        requested_at=created_at,
    )

    updated = update_item_request_record(
        record,
        media_item_id="item-new",
        requested_title="New Show",
        media_type="show",
        requested_at=updated_at,
    )

    assert updated is record
    assert record.media_item_id == "item-new"
    assert record.requested_title == "New Show"
    assert record.media_type == "show"
    assert record.request_count == 2
    assert record.first_requested_at == created_at
    assert record.last_requested_at == updated_at


def test_item_request_orm_scopes_external_ref_uniqueness_by_tenant() -> None:
    constraint_names = {
        constraint.name
        for constraint in ItemRequestORM.__table__.constraints
        if constraint.name is not None
    }

    assert "uq_item_requests_tenant_external_ref" in constraint_names
    assert "uq_item_requests_external_ref" not in constraint_names


class _FakeScalarResult:
    def __init__(self, value: object | None) -> None:
        self._value = value

    def scalar_one_or_none(self) -> object | None:
        return self._value


class _RecordingSession:
    def __init__(self) -> None:
        self.statement: object | None = None
        self.added: list[object] = []

    async def execute(self, statement: object) -> _FakeScalarResult:
        self.statement = statement
        return _FakeScalarResult(None)

    def add(self, obj: object) -> None:
        self.added.append(obj)


async def _call_upsert_with_recording_session(session: _RecordingSession) -> ItemRequestORM:
    service = object.__new__(MediaService)
    return await MediaService._upsert_item_request(
        service,
        session,
        tenant_id="tenant-a",
        external_ref="tvdb:456",
        media_item_id="item-1",
        requested_title="Example Show",
        media_type="show",
    )


def test_upsert_item_request_queries_by_tenant_and_external_ref() -> None:
    import asyncio

    from sqlalchemy.dialects import postgresql

    session = _RecordingSession()
    record = asyncio.run(_call_upsert_with_recording_session(session))

    assert isinstance(record, ItemRequestORM)
    assert session.statement is not None

    compiled = session.statement.compile(dialect=postgresql.dialect())
    sql = str(compiled)
    params = cast(dict[str, Any], compiled.params)

    assert "item_requests.tenant_id" in sql
    assert "item_requests.external_ref" in sql
    assert "tenant-a" in params.values()
    assert "tvdb:456" in params.values()
