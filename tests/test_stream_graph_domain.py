from __future__ import annotations

from filmu_py.db.models import (
    MediaItemORM,
    StreamBlacklistRelationORM,
    StreamORM,
    StreamRelationORM,
)


def _build_item(*, external_ref: str, title: str = "Example") -> MediaItemORM:
    return MediaItemORM(external_ref=external_ref, title=title, state="requested", attributes={})


def test_stream_candidate_attaches_to_media_item() -> None:
    item = _build_item(external_ref="tmdb:stream-item")
    stream = StreamORM(
        media_item=item,
        infohash="abc123",
        raw_title="Example.2026.1080p.WEB-DL",
        parsed_title={"title": "Example", "resolution": "1080p"},
        rank=42,
        lev_ratio=0.97,
        resolution="1080p",
    )

    assert stream.media_item is item
    assert stream in item.streams
    assert stream.rank == 42
    assert stream.lev_ratio == 0.97


def test_stream_blacklist_relation_links_media_item_and_stream() -> None:
    item = _build_item(external_ref="tmdb:blacklist-item")
    stream = StreamORM(
        media_item=item,
        infohash="deadbeef",
        raw_title="Blocked.Release.2026.2160p",
        parsed_title={"title": "Blocked Release"},
        rank=10,
        resolution="2160p",
    )
    relation = StreamBlacklistRelationORM(media_item=item, stream=stream)

    assert relation.media_item is item
    assert relation.stream is stream
    assert relation in item.blacklisted_stream_relations
    assert relation in stream.blacklist_relations


def test_stream_relation_links_parent_and_child_candidates() -> None:
    item = _build_item(external_ref="tmdb:relation-item")
    parent = StreamORM(
        media_item=item,
        infohash="parenthash",
        raw_title="Parent.Release.2026.1080p",
        parsed_title={"title": "Parent Release"},
        rank=100,
        resolution="1080p",
    )
    child = StreamORM(
        media_item=item,
        infohash="childhash",
        raw_title="Child.Release.2026.1080p.Remux",
        parsed_title={"title": "Child Release"},
        rank=110,
        resolution="1080p",
    )
    relation = StreamRelationORM(parent_stream=parent, child_stream=child)

    assert relation.parent_stream is parent
    assert relation.child_stream is child
    assert relation in parent.parent_relations
    assert relation in child.child_relations
