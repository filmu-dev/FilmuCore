from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

from filmu_py.db.rehearsal import capture_database_snapshot, compare_snapshot_payloads


def _build_sqlite_db(path: Path, *, media_items: int, streams: int, revision: str) -> str:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE alembic_version (version_num TEXT NOT NULL);
            CREATE TABLE settings (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE media_items (id TEXT PRIMARY KEY);
            CREATE TABLE streams (id TEXT PRIMARY KEY, media_item_id TEXT NOT NULL);
            """
        )
        connection.execute("INSERT INTO alembic_version (version_num) VALUES (?)", (revision,))
        connection.execute("INSERT INTO settings (id, payload) VALUES (1, ?)", ('{"ok":true}',))
        for index in range(media_items):
            connection.execute("INSERT INTO media_items (id) VALUES (?)", (f"item-{index}",))
        for index in range(streams):
            connection.execute(
                "INSERT INTO streams (id, media_item_id) VALUES (?, ?)",
                (f"stream-{index}", f"item-{index % max(media_items, 1)}"),
            )
        connection.commit()
    finally:
        connection.close()
    return f"sqlite:///{path.as_posix()}"


def test_capture_database_snapshot_reports_revision_and_counts(tmp_path: Path) -> None:
    dsn = _build_sqlite_db(
        tmp_path / "snapshot.db",
        media_items=2,
        streams=3,
        revision="rev-current",
    )

    snapshot = asyncio.run(capture_database_snapshot(dsn))

    assert snapshot.revision == "rev-current"
    assert snapshot.table_count == 4
    assert snapshot.total_rows == 7
    counts = {table.name: table.row_count for table in snapshot.tables}
    assert counts == {
        "alembic_version": 1,
        "media_items": 2,
        "settings": 1,
        "streams": 3,
    }


def test_compare_snapshot_payloads_flags_missing_tables_mismatches_and_revision_drift() -> None:
    source = {
        "revision": "rev-a",
        "tables": [
            {"name": "alembic_version", "row_count": 1},
            {"name": "settings", "row_count": 1},
            {"name": "media_items", "row_count": 2},
            {"name": "streams", "row_count": 3},
        ],
    }
    restored = {
        "revision": "rev-b",
        "tables": [
            {"name": "alembic_version", "row_count": 1},
            {"name": "settings", "row_count": 1},
            {"name": "media_items", "row_count": 1},
        ],
    }

    parity = compare_snapshot_payloads(
        source,
        restored,
        required_tables=["alembic_version", "settings", "media_items", "streams"],
    )

    assert parity["status"] == "failed"
    assert parity["missing_tables"] == ["streams"]
    assert parity["row_count_mismatches"] == [
        {
            "table_name": "media_items",
            "source_row_count": 2,
            "restored_row_count": 1,
        }
    ]
    assert parity["revision_observed"] is True
    assert parity["revision_match"] is False


def test_compare_snapshot_payloads_fails_when_required_source_table_is_missing() -> None:
    source = {
        "revision": "rev-a",
        "tables": [
            {"name": "alembic_version", "row_count": 1},
        ],
    }
    restored = {
        "revision": "rev-a",
        "tables": [
            {"name": "alembic_version", "row_count": 1},
            {"name": "item_workflow_checkpoints", "row_count": 0},
        ],
    }

    parity = compare_snapshot_payloads(
        source,
        restored,
        required_tables=["alembic_version", "item_workflow_checkpoints"],
    )

    assert parity["status"] == "failed"
    assert parity["missing_source_tables"] == ["item_workflow_checkpoints"]
    assert parity["missing_tables"] == []
    assert parity["restore_only_tables"] == ["item_workflow_checkpoints"]
