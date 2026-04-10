from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from filmu_py.db.migrations import should_use_async_engine


def test_should_use_async_engine_detects_asyncpg_urls() -> None:
    assert should_use_async_engine("postgresql+asyncpg://postgres:postgres@postgres:5432/filmu")


def test_should_use_async_engine_leaves_sync_urls_on_sync_path() -> None:
    assert not should_use_async_engine("postgresql://postgres:postgres@postgres:5432/filmu")
    assert not should_use_async_engine("sqlite:///./filmu.db")


def _load_revision_module(filename: str) -> Any:
    revision_path = (
        Path(__file__).resolve().parents[1]
        / "filmu_py"
        / "db"
        / "alembic"
        / "versions"
        / filename
    )
    spec = importlib.util.spec_from_file_location(f"test_revision_{revision_path.stem}", revision_path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"unable to load revision module {filename}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_item_requests_tenant_uniqueness_downgrade_guards_cross_tenant_duplicates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_revision_module("20260410_0023_item_requests_tenant_uniqueness.py")
    events: list[tuple[str, str]] = []

    class _FakeResult:
        def mappings(self) -> _FakeResult:
            return self

        def first(self) -> dict[str, object]:
            return {"external_ref": "shared-ref", "duplicate_count": 2}

    fake_op = SimpleNamespace(
        get_bind=lambda: SimpleNamespace(execute=lambda _query: _FakeResult()),
        drop_constraint=lambda name, table_name, type_=None: events.append(
            ("drop_constraint", f"{name}:{table_name}:{type_}")
        ),
        create_unique_constraint=lambda name, table_name, columns: events.append(
            ("create_unique_constraint", f"{name}:{table_name}:{','.join(columns)}")
        ),
    )
    monkeypatch.setattr(module, "op", fake_op)

    with pytest.raises(RuntimeError, match="cannot restore global item_requests external_ref uniqueness"):
        module.downgrade()

    assert events == []


def test_identity_and_tenancy_downgrade_only_drops_indexes_created_by_upgrade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_revision_module("20260410_0022_identity_and_tenancy.py")
    dropped_indexes: list[str] = []

    fake_op = SimpleNamespace(
        drop_constraint=lambda *args, **kwargs: None,
        drop_index=lambda name, table_name=None: dropped_indexes.append(name),
        drop_column=lambda *args, **kwargs: None,
        drop_table=lambda *args, **kwargs: None,
        f=lambda name: name,
    )
    monkeypatch.setattr(module, "op", fake_op)

    module.downgrade()

    assert "ix_service_accounts_api_key_id" in dropped_indexes
    assert "ix_service_accounts_principal_id" not in dropped_indexes
