"""Validated plugin manifest models for drop-in plugin discovery."""

from __future__ import annotations

import re
from pathlib import Path, PurePosixPath

from pydantic import BaseModel, ConfigDict, Field, field_validator

from filmu_py.graphql.plugin_registry import GraphQLResolverKind


def _version_parts(value: str) -> tuple[int, ...]:
    """Extract comparable numeric version parts from a version-like string."""

    return tuple(int(part) for part in re.findall(r"\d+", value))


class GraphQLResolverExports(BaseModel):
    """Manifest-declared GraphQL resolver exports contributed by a plugin."""

    model_config = ConfigDict(extra="forbid")

    query_resolvers: tuple[str, ...] = Field(default_factory=tuple)
    settings_resolvers: tuple[str, ...] = Field(default_factory=tuple)
    mutation_resolvers: tuple[str, ...] = Field(default_factory=tuple)
    subscription_resolvers: tuple[str, ...] = Field(default_factory=tuple)

    @field_validator(
        "query_resolvers",
        "settings_resolvers",
        "mutation_resolvers",
        "subscription_resolvers",
    )
    @classmethod
    def validate_resolver_symbols(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        """Ensure exported resolver names are unique Python identifiers."""

        normalized = tuple(symbol.strip() for symbol in value)
        if any(not symbol for symbol in normalized):
            raise ValueError("resolver export names must not be empty")
        if any(not symbol.isidentifier() for symbol in normalized):
            raise ValueError("resolver export names must be valid Python identifiers")
        if len(set(normalized)) != len(normalized):
            raise ValueError("resolver export names must be unique per resolver kind")
        return normalized

    def exports_for(self, kind: GraphQLResolverKind) -> tuple[str, ...]:
        """Return configured export names for one GraphQL root kind."""

        if kind is GraphQLResolverKind.QUERY:
            return self.query_resolvers
        if kind is GraphQLResolverKind.SETTINGS:
            return self.settings_resolvers
        if kind is GraphQLResolverKind.MUTATION:
            return self.mutation_resolvers
        return self.subscription_resolvers


class PluginManifest(BaseModel):
    """Validated plugin manifest loaded from a plugin directory."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    version: str = Field(min_length=1)
    api_version: str = Field(default="1", min_length=1)
    min_host_version: str | None = Field(default=None)
    max_host_version: str | None = Field(default=None)
    distribution: str = Field(default="filesystem", min_length=1)
    capabilities: frozenset[str] = Field(default_factory=frozenset)
    entry_module: str = Field(min_length=1)
    graphql: GraphQLResolverExports = Field(default_factory=GraphQLResolverExports)
    scraper: str | None = Field(default=None)
    downloader: str | None = Field(default=None)
    indexer: str | None = Field(default=None)
    content_service: str | None = Field(default=None)
    notification: str | None = Field(default=None)
    event_hook: str | None = Field(default=None)
    datasource: str | None = Field(default=None)
    publishable_events: tuple[str, ...] = Field(default_factory=tuple)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        """Require a stable plugin identifier with no whitespace."""

        normalized = value.strip()
        if not normalized:
            raise ValueError("plugin name must not be empty")
        if any(character.isspace() for character in normalized):
            raise ValueError("plugin name must not contain whitespace")
        return normalized

    @field_validator("capabilities")
    @classmethod
    def validate_capabilities(cls, value: frozenset[str]) -> frozenset[str]:
        """Normalize capability names and reject empty entries."""

        normalized = frozenset(capability.strip() for capability in value)
        if "" in normalized:
            raise ValueError("capabilities must not contain empty values")
        return normalized

    @field_validator("version", "api_version", "min_host_version", "max_host_version")
    @classmethod
    def validate_version_strings(cls, value: str | None) -> str | None:
        """Normalize optional version-like fields without imposing packaging deps yet."""

        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("version fields must not be empty when provided")
        return normalized

    @field_validator("distribution")
    @classmethod
    def validate_distribution(cls, value: str) -> str:
        """Restrict manifest distribution declarations to known host policies."""

        normalized = value.strip().lower()
        if normalized not in {"filesystem", "entry_point", "builtin"}:
            raise ValueError("distribution must be one of: filesystem, entry_point, builtin")
        return normalized

    @field_validator(
        "scraper",
        "downloader",
        "indexer",
        "content_service",
        "notification",
        "event_hook",
        "datasource",
    )
    @classmethod
    def validate_capability_export_symbol(cls, value: str | None) -> str | None:
        """Ensure declared non-GraphQL capability exports use valid Python symbols."""

        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("capability export names must not be empty")
        if not normalized.isidentifier():
            raise ValueError("capability export names must be valid Python identifiers")
        return normalized

    @field_validator("publishable_events")
    @classmethod
    def validate_publishable_events(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        """Normalize declared publishable event names and reject duplicates."""

        normalized = tuple(event.strip() for event in value)
        if any(not event for event in normalized):
            raise ValueError("publishable event names must not be empty")
        if len(set(normalized)) != len(normalized):
            raise ValueError("publishable event names must be unique")
        return normalized

    @field_validator("entry_module")
    @classmethod
    def validate_entry_module(cls, value: str) -> str:
        """Restrict entry modules to safe relative Python file paths."""

        normalized = value.replace("\\", "/").strip()
        candidate = PurePosixPath(normalized)
        if not normalized:
            raise ValueError("entry_module must not be empty")
        if candidate.is_absolute() or ".." in candidate.parts:
            raise ValueError("entry_module must be a relative path inside the plugin directory")
        if candidate.suffix != ".py":
            raise ValueError("entry_module must reference a Python file")
        return normalized

    def resolve_entry_module(self, plugin_dir: Path) -> Path:
        """Resolve the declared entry module path within one plugin directory."""

        return plugin_dir / Path(self.entry_module)

    def capability_exports(self) -> dict[str, str | None]:
        """Return manifest-declared non-GraphQL capability export symbols."""

        return {
            "scraper": self.scraper,
            "downloader": self.downloader,
            "indexer": self.indexer,
            "content_service": self.content_service,
            "notification": self.notification,
            "event_hook": self.event_hook,
        }

    def declared_non_graphql_capabilities(self) -> tuple[str, ...]:
        """Return declared non-GraphQL capabilities from either the capability set or export fields."""

        declared = {capability for capability in self.capabilities if capability != "graphql"}
        for capability, export_name in self.capability_exports().items():
            if export_name is not None:
                declared.add(capability)
        return tuple(sorted(declared))

    @staticmethod
    def _compare_versions(left: str, right: str) -> int:
        """Compare two version-like strings using numeric components only."""

        required = _version_parts(left)
        current = _version_parts(right)
        width = max(len(required), len(current))
        padded_required = (*required, *([0] * (width - len(required))))
        padded_current = (*current, *([0] * (width - len(current))))
        if padded_required > padded_current:
            return 1
        if padded_required < padded_current:
            return -1
        return 0

    def ensure_host_compatibility(
        self,
        host_version: str,
        *,
        supported_api_versions: tuple[str, ...] = ("1",),
    ) -> None:
        """Raise when the running host falls outside the declared compatibility contract."""

        if self.api_version not in supported_api_versions:
            raise ValueError("api_version_incompatible")
        if self.min_host_version is not None and self._compare_versions(
            self.min_host_version, host_version
        ) > 0:
            raise ValueError("host_version_incompatible")
        if self.max_host_version is not None and self._compare_versions(
            self.max_host_version, host_version
        ) < 0:
            raise ValueError("host_version_incompatible")

    def declares_publishable_event(self, event_type: str) -> bool:
        """Return whether one namespaced event was declared by this plugin."""

        return event_type in self.publishable_events
