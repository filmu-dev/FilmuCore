"""Persistence helpers for the single-row settings blob."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, cast

from fastapi import Request
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from filmu_py.config import Settings, set_runtime_settings
from filmu_py.db.models import SettingsORM
from filmu_py.db.runtime import DatabaseRuntime


async def load_settings(db: DatabaseRuntime) -> dict[str, Any] | None:
    """Load the persisted compatibility settings blob when present."""

    async with db.session() as session:
        payload = (
            await session.execute(select(SettingsORM.data).where(SettingsORM.id == 1))
        ).scalar_one_or_none()

    if payload is None:
        return None
    return deepcopy(cast(dict[str, Any], payload))


async def save_settings(db: DatabaseRuntime, data: dict[str, Any]) -> None:
    """Upsert the persisted compatibility settings blob into the single-row table."""

    payload = deepcopy(data)
    statement = pg_insert(SettingsORM).values(id=1, data=payload, updated_at=func.now())
    statement = statement.on_conflict_do_update(
        index_elements=[SettingsORM.id],
        set_={"data": payload, "updated_at": func.now()},
    )

    async with db.session() as session:
        await session.execute(statement)
        await session.commit()


async def update_settings_path(
    *,
    request: Request,
    db: DatabaseRuntime,
    path: str,
    value: Any,
) -> Settings:
    """Update one dot-separated settings path and synchronize runtime state."""

    resources = request.app.state.resources
    payload = deepcopy(resources.settings.to_compatibility_dict())
    _path_set(payload, path, value)
    validated = Settings.from_compatibility_dict(payload)
    await save_settings(db, payload)
    resources.settings = validated
    resources.plugin_settings_payload = payload
    set_runtime_settings(validated)
    return validated


def _path_set(root: dict[str, Any], path: str, value: Any) -> None:
    """Set one existing dot-path value or raise when invalid."""

    if not path:
        raise ValueError("path must not be empty")

    *parents, leaf = path.split(".")
    current: object = root
    for segment in parents:
        if not isinstance(current, dict) or segment not in current:
            raise ValueError(f"invalid settings path: {path}")
        current = cast(dict[str, Any], current)[segment]

    if not isinstance(current, dict) or leaf not in current:
        raise ValueError(f"invalid settings path: {path}")

    cast(dict[str, Any], current)[leaf] = value
