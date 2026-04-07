"""Plugin-scoped settings registry and validation helpers."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any


def _normalize_plugin_name(plugin_name: str) -> str:
    normalized = plugin_name.strip()
    if not normalized:
        raise ValueError("plugin_name must not be empty")
    return normalized


@dataclass(slots=True)
class PluginSettingsRegistry:
    """Mutable registry of validated plugin-scoped settings loaded during bootstrap."""

    _store: dict[str, dict[str, Any]] = field(default_factory=dict)
    _schemas: dict[str, dict[str, Any]] = field(default_factory=dict)
    _locked: bool = False

    def register(self, plugin_name: str, schema: dict[str, Any], values: dict[str, Any]) -> None:
        """Register validated settings for one plugin before runtime lock-down."""

        if self._locked:
            raise RuntimeError("plugin settings registry is locked")

        normalized_name = _normalize_plugin_name(plugin_name)
        normalized_schema = deepcopy(schema)
        normalized_values = deepcopy(values)

        if not isinstance(normalized_schema, dict):
            raise TypeError("schema must be a dict")
        if not isinstance(normalized_values, dict):
            raise TypeError("values must be a dict")

        for key, definition in normalized_schema.items():
            if not isinstance(key, str) or not key.strip():
                raise ValueError("schema keys must be non-empty strings")
            if isinstance(definition, Mapping) and definition.get("required") and key not in normalized_values:
                raise ValueError(f"missing required plugin setting '{key}'")

        self._schemas[normalized_name] = normalized_schema
        self._store[normalized_name] = normalized_values

    def get(self, plugin_name: str) -> dict[str, Any]:
        """Return a defensive copy of the registered settings for one plugin."""

        normalized_name = _normalize_plugin_name(plugin_name)
        return deepcopy(self._store.get(normalized_name, {}))

    def has(self, plugin_name: str) -> bool:
        """Return whether settings were already registered for one plugin."""

        normalized_name = _normalize_plugin_name(plugin_name)
        return normalized_name in self._store

    def lock(self) -> None:
        """Prevent further mutation after bootstrap is complete."""

        self._locked = True

