"""Persisted operator-managed plugin governance overrides."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select

from filmu_py.db.models import PluginGovernanceOverrideORM
from filmu_py.db.runtime import DatabaseRuntime


@dataclass(frozen=True, slots=True)
class PluginGovernanceOverrideRecord:
    """One persisted plugin governance override row."""

    plugin_name: str
    state: str
    reason: str | None
    notes: str | None
    updated_by: str | None
    created_at: datetime
    updated_at: datetime


class PluginGovernanceService:
    """Persist quarantine/revocation/operator approval state above manifest defaults."""

    def __init__(self, db: DatabaseRuntime) -> None:
        self._db = db

    async def list_overrides(self) -> dict[str, PluginGovernanceOverrideRecord]:
        """Return current persisted plugin governance overrides keyed by plugin name."""

        async with self._db.session() as session:
            rows = (
                await session.execute(
                    select(PluginGovernanceOverrideORM).order_by(
                        PluginGovernanceOverrideORM.plugin_name.asc()
                    )
                )
            ).scalars()
            return {row.plugin_name: _record_from_orm(row) for row in rows}

    async def write_override(
        self,
        *,
        plugin_name: str,
        state: str,
        reason: str | None = None,
        notes: str | None = None,
        updated_by: str | None = None,
    ) -> PluginGovernanceOverrideRecord:
        """Create or update one plugin governance override."""

        plugin_key = plugin_name.strip()
        state_key = state.strip().lower()
        if not plugin_key:
            raise ValueError("plugin name must not be empty")
        if state_key not in {"approved", "quarantined", "revoked"}:
            raise ValueError("plugin governance state must be approved, quarantined, or revoked")
        now = datetime.now(UTC)
        async with self._db.session() as session:
            row = (
                await session.execute(
                    select(PluginGovernanceOverrideORM).where(
                        PluginGovernanceOverrideORM.plugin_name == plugin_key
                    )
                )
            ).scalar_one_or_none()
            if row is None:
                row = PluginGovernanceOverrideORM(
                    plugin_name=plugin_key,
                    state=state_key,
                    reason=_normalize(reason),
                    notes=_normalize(notes),
                    updated_by=_normalize(updated_by),
                    created_at=now,
                    updated_at=now,
                )
                session.add(row)
            else:
                row.state = state_key
                row.reason = _normalize(reason)
                row.notes = _normalize(notes)
                row.updated_by = _normalize(updated_by)
                row.updated_at = now
            await session.flush()
            record = _record_from_orm(row)
            await session.commit()
        return record


def _normalize(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _record_from_orm(row: PluginGovernanceOverrideORM) -> PluginGovernanceOverrideRecord:
    return PluginGovernanceOverrideRecord(
        plugin_name=row.plugin_name,
        state=row.state,
        reason=row.reason,
        notes=row.notes,
        updated_by=row.updated_by,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )
