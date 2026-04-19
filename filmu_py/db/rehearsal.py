"""Backup, restore, and migration rehearsal helpers."""

from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, cast

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import create_async_engine

from .migrations import run_migrations, should_use_async_engine


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class DatabaseTarget:
    dialect: str
    driver: str
    host: str | None
    port: int | None
    database: str | None


@dataclass(frozen=True)
class TableSnapshot:
    name: str
    row_count: int


@dataclass(frozen=True)
class DatabaseSnapshot:
    captured_at: str
    target: DatabaseTarget
    revision: str | None
    table_count: int
    total_rows: int
    tables: list[TableSnapshot]

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


def _describe_target(dsn: str) -> DatabaseTarget:
    url = make_url(dsn)
    return DatabaseTarget(
        dialect=url.get_backend_name(),
        driver=url.get_driver_name(),
        host=url.host,
        port=url.port,
        database=str(url.database) if url.database is not None else None,
    )


def _capture_snapshot_from_connection(
    connection: Connection,
    *,
    target: DatabaseTarget,
    include_tables: Sequence[str] | None,
) -> DatabaseSnapshot:
    inspector = inspect(connection)
    include_filter = {name.strip() for name in include_tables or () if name.strip()}
    table_names = sorted(
        name
        for name in inspector.get_table_names()
        if not name.startswith("sqlite_") and (not include_filter or name in include_filter)
    )
    preparer = connection.dialect.identifier_preparer
    tables: list[TableSnapshot] = []
    total_rows = 0
    for table_name in table_names:
        quoted_name = preparer.quote(table_name)
        row_count = int(
            connection.execute(text(f"SELECT COUNT(*) FROM {quoted_name}")).scalar_one()
        )
        total_rows += row_count
        tables.append(TableSnapshot(name=table_name, row_count=row_count))
    revision: str | None = None
    if "alembic_version" in inspector.get_table_names():
        revision_value = connection.execute(
            text("SELECT version_num FROM alembic_version LIMIT 1")
        ).scalar_one_or_none()
        if revision_value is not None:
            revision = str(revision_value)
    return DatabaseSnapshot(
        captured_at=_utc_now_iso(),
        target=target,
        revision=revision,
        table_count=len(tables),
        total_rows=total_rows,
        tables=tables,
    )


async def capture_database_snapshot(
    dsn: str, *, include_tables: Sequence[str] | None = None
) -> DatabaseSnapshot:
    """Capture a row-count and revision snapshot for a database."""

    target = _describe_target(dsn)
    if should_use_async_engine(dsn):
        async_engine: AsyncEngine = create_async_engine(dsn, future=True)
        try:
            async with async_engine.connect() as connection:
                return await connection.run_sync(
                    lambda sync_connection: _capture_snapshot_from_connection(
                        sync_connection,
                        target=target,
                        include_tables=include_tables,
                    )
                )
        finally:
            await async_engine.dispose()
    sync_engine = create_engine(dsn, future=True)
    try:
        with sync_engine.connect() as connection:
            return _capture_snapshot_from_connection(
                connection,
                target=target,
                include_tables=include_tables,
            )
    finally:
        sync_engine.dispose()


def compare_snapshot_payloads(
    source_snapshot: Mapping[str, Any],
    restored_snapshot: Mapping[str, Any],
    *,
    required_tables: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Compare two snapshot payloads and report parity gaps."""

    def table_counts(snapshot: Mapping[str, Any]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for table in cast(Sequence[Mapping[str, Any]], snapshot.get("tables", ())):
            name = str(table["name"]).strip()
            if name:
                counts[name] = int(table["row_count"])
        return counts

    source_counts = table_counts(source_snapshot)
    restored_counts = table_counts(restored_snapshot)
    required = sorted({name.strip() for name in required_tables or () if name.strip()})
    expected_tables = required or sorted(source_counts)
    compared_tables = sorted(set(source_counts) & set(restored_counts))
    missing_source_tables = sorted(name for name in expected_tables if name not in source_counts)
    missing_tables = sorted(name for name in expected_tables if name not in restored_counts)
    row_count_mismatches = [
        {
            "table_name": name,
            "source_row_count": source_counts[name],
            "restored_row_count": restored_counts[name],
        }
        for name in compared_tables
        if source_counts[name] != restored_counts[name]
    ]
    source_only_tables = sorted(set(source_counts) - set(restored_counts))
    restore_only_tables = sorted(set(restored_counts) - set(source_counts))
    source_revision = source_snapshot.get("revision")
    restored_revision = restored_snapshot.get("revision")
    revision_observed = source_revision is not None and restored_revision is not None
    revision_match = bool(revision_observed and source_revision == restored_revision)
    failed = bool(missing_source_tables or missing_tables or row_count_mismatches)
    if revision_observed and not revision_match:
        failed = True
    return {
        "status": "failed" if failed else "passed",
        "required_tables": expected_tables,
        "compared_tables": compared_tables,
        "missing_source_tables": missing_source_tables,
        "missing_tables": missing_tables,
        "row_count_mismatches": row_count_mismatches,
        "source_only_tables": source_only_tables,
        "restore_only_tables": restore_only_tables,
        "source_revision": source_revision,
        "restored_revision": restored_revision,
        "revision_observed": revision_observed,
        "revision_match": revision_match,
    }


async def run_migration_rehearsal(dsn: str, *, revision: str = "head") -> dict[str, Any]:
    """Run migrations against a restore target and report revision movement."""

    before_snapshot = await capture_database_snapshot(dsn)
    try:
        await asyncio.to_thread(run_migrations, dsn, revision)
    except Exception as exc:
        return {
            "requested": True,
            "attempted": True,
            "requested_revision": revision,
            "status": "failed",
            "before_revision": before_snapshot.revision,
            "after_revision": None,
            "changed": False,
            "error": str(exc),
        }
    after_snapshot = await capture_database_snapshot(dsn)
    return {
        "requested": True,
        "attempted": True,
        "requested_revision": revision,
        "status": "passed",
        "before_revision": before_snapshot.revision,
        "after_revision": after_snapshot.revision,
        "changed": before_snapshot.revision != after_snapshot.revision,
        "error": None,
    }


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    snapshot_parser = subparsers.add_parser("snapshot", help="Capture a DB snapshot.")
    snapshot_parser.add_argument("--dsn", required=True)
    snapshot_parser.add_argument("--table", action="append", default=[])
    snapshot_parser.set_defaults(handler=_handle_snapshot)

    migrate_parser = subparsers.add_parser("migrate", help="Run migration rehearsal.")
    migrate_parser.add_argument("--dsn", required=True)
    migrate_parser.add_argument("--revision", default="head")
    migrate_parser.set_defaults(handler=_handle_migrate)

    return parser.parse_args(argv)


def _handle_snapshot(args: argparse.Namespace) -> int:
    snapshot = asyncio.run(capture_database_snapshot(args.dsn, include_tables=args.table))
    print(json.dumps(snapshot.to_payload(), indent=2, sort_keys=True))
    return 0


def _handle_migrate(args: argparse.Namespace) -> int:
    payload = asyncio.run(run_migration_rehearsal(args.dsn, revision=args.revision))
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
