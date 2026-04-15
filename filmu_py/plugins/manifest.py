"""Validated plugin manifest models for drop-in plugin discovery."""

from __future__ import annotations

import re
from pathlib import Path, PurePosixPath

from pydantic import BaseModel, ConfigDict, Field, field_validator

from filmu_py.graphql.plugin_registry import GraphQLResolverKind

_ALLOWED_DISTRIBUTIONS = {"filesystem", "entry_point", "builtin"}
_ALLOWED_RELEASE_CHANNELS = {"stable", "beta", "experimental", "builtin"}
_ALLOWED_TRUST_LEVELS = {"builtin", "trusted", "community"}
_ALLOWED_SANDBOX_PROFILES = {"host", "network", "restricted", "isolated"}
_ALLOWED_TENANCY_MODES = {"shared", "tenant", "control_plane"}
_SCOPE_PATTERN = re.compile(r"^[a-z][a-z0-9_-]*:[a-z0-9._-]+$")
_SHA256_PATTERN = re.compile(r"^[a-fA-F0-9]{64}$")
_REQUIRED_PERMISSION_SCOPES_BY_CAPABILITY: dict[str, frozenset[str]] = {
    "graphql": frozenset({"graphql:extend"}),
    "scraper": frozenset({"scrape:search"}),
    "downloader": frozenset({"download:transfer"}),
    "indexer": frozenset({"index:read"}),
    "content_service": frozenset({"content:ingest"}),
    "notification": frozenset({"notify:send"}),
    "event_hook": frozenset({"events:subscribe"}),
    "stream_control": frozenset({"playback:operate"}),
}


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
    publisher: str | None = Field(default=None)
    release_channel: str = Field(default="stable", min_length=1)
    trust_level: str = Field(default="community", min_length=1)
    source_sha256: str | None = Field(default=None)
    signature: str | None = Field(default=None)
    signing_key_id: str | None = Field(default=None)
    sandbox_profile: str = Field(default="restricted", min_length=1)
    tenancy_mode: str = Field(default="shared", min_length=1)
    quarantined: bool = Field(default=False)
    quarantine_reason: str | None = Field(default=None)
    capabilities: frozenset[str] = Field(default_factory=frozenset)
    permission_scopes: frozenset[str] = Field(default_factory=frozenset)
    entry_module: str = Field(min_length=1)
    graphql: GraphQLResolverExports = Field(default_factory=GraphQLResolverExports)
    scraper: str | None = Field(default=None)
    downloader: str | None = Field(default=None)
    indexer: str | None = Field(default=None)
    content_service: str | None = Field(default=None)
    notification: str | None = Field(default=None)
    event_hook: str | None = Field(default=None)
    stream_control: str | None = Field(default=None)
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
        if normalized not in _ALLOWED_DISTRIBUTIONS:
            raise ValueError("distribution must be one of: filesystem, entry_point, builtin")
        return normalized

    @field_validator("publisher")
    @classmethod
    def validate_publisher(cls, value: str | None) -> str | None:
        """Normalize optional publisher metadata."""

        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("publisher must not be empty when provided")
        if any(character.isspace() for character in normalized):
            raise ValueError("publisher must not contain whitespace")
        return normalized

    @field_validator("release_channel")
    @classmethod
    def validate_release_channel(cls, value: str) -> str:
        """Restrict release channels to the host policy vocabulary."""

        normalized = value.strip().lower()
        if normalized not in _ALLOWED_RELEASE_CHANNELS:
            raise ValueError("release_channel must be one of: stable, beta, experimental, builtin")
        return normalized

    @field_validator("trust_level")
    @classmethod
    def validate_trust_level(cls, value: str) -> str:
        """Restrict trust levels to the host policy vocabulary."""

        normalized = value.strip().lower()
        if normalized not in _ALLOWED_TRUST_LEVELS:
            raise ValueError("trust_level must be one of: builtin, trusted, community")
        return normalized

    @field_validator("source_sha256")
    @classmethod
    def validate_source_sha256(cls, value: str | None) -> str | None:
        """Normalize optional source digests and require full SHA-256 hex values."""

        if value is None:
            return None
        normalized = value.strip().lower()
        if not _SHA256_PATTERN.match(normalized):
            raise ValueError("source_sha256 must be a 64-character SHA-256 hex digest")
        return normalized

    @field_validator("signature", "signing_key_id", "quarantine_reason")
    @classmethod
    def validate_optional_metadata(cls, value: str | None) -> str | None:
        """Normalize optional provenance and quarantine metadata."""

        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("metadata fields must not be empty when provided")
        return normalized

    @field_validator("sandbox_profile")
    @classmethod
    def validate_sandbox_profile(cls, value: str) -> str:
        """Restrict sandbox-profile declarations to current host policy vocabulary."""

        normalized = value.strip().lower()
        if normalized not in _ALLOWED_SANDBOX_PROFILES:
            raise ValueError(
                "sandbox_profile must be one of: host, network, restricted, isolated"
            )
        return normalized

    @field_validator("tenancy_mode")
    @classmethod
    def validate_tenancy_mode(cls, value: str) -> str:
        """Restrict tenancy declarations to current host policy vocabulary."""

        normalized = value.strip().lower()
        if normalized not in _ALLOWED_TENANCY_MODES:
            raise ValueError("tenancy_mode must be one of: shared, tenant, control_plane")
        return normalized

    @field_validator(
        "scraper",
        "downloader",
        "indexer",
        "content_service",
        "notification",
        "event_hook",
        "stream_control",
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

    @field_validator("permission_scopes")
    @classmethod
    def validate_permission_scopes(cls, value: frozenset[str]) -> frozenset[str]:
        """Normalize declared permission scopes and reject invalid patterns."""

        normalized = frozenset(scope.strip().lower() for scope in value)
        if "" in normalized:
            raise ValueError("permission scopes must not contain empty values")
        invalid = sorted(scope for scope in normalized if not _SCOPE_PATTERN.match(scope))
        if invalid:
            raise ValueError(
                "permission scopes must match '<domain>:<action>' "
                f"(invalid: {', '.join(invalid)})"
            )
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
            "stream_control": self.stream_control,
        }

    def declared_non_graphql_capabilities(self) -> tuple[str, ...]:
        """Return declared non-GraphQL capabilities from either the capability set or export fields."""

        declared = {capability for capability in self.capabilities if capability != "graphql"}
        for capability, export_name in self.capability_exports().items():
            if export_name is not None:
                declared.add(capability)
        return tuple(sorted(declared))

    def declared_capabilities(self) -> tuple[str, ...]:
        """Return every declared capability including GraphQL resolver contribution."""

        declared = set(self.declared_non_graphql_capabilities())
        if any(self.graphql.exports_for(kind) for kind in GraphQLResolverKind):
            declared.add("graphql")
        return tuple(sorted(declared))

    def required_permission_scopes(self) -> frozenset[str]:
        """Return the minimum permission scopes implied by the manifest surface."""

        required: set[str] = set()
        for capability in self.declared_capabilities():
            required.update(_REQUIRED_PERMISSION_SCOPES_BY_CAPABILITY.get(capability, ()))
        if self.datasource is not None:
            required.add("datasource:host")
        if self.publishable_events:
            required.add("events:publish")
        return frozenset(required)

    def effective_permission_scopes(self) -> frozenset[str]:
        """Return the operator-visible effective scopes for this plugin."""

        return frozenset(sorted(self.permission_scopes | self.required_permission_scopes()))

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

    def uses_implicit_permission_scopes(self) -> bool:
        """Return whether the manifest relies on host-derived minimum scopes."""

        return not self.permission_scopes and bool(self.required_permission_scopes())

    def validate_policy(self) -> None:
        """Raise when plugin policy metadata violates current host rules."""

        if self.distribution == "builtin":
            if self.release_channel != "builtin":
                raise ValueError("builtin plugins must use release_channel='builtin'")
            if self.trust_level != "builtin":
                raise ValueError("builtin plugins must use trust_level='builtin'")
            if self.sandbox_profile != "host":
                raise ValueError("builtin plugins must use sandbox_profile='host'")
            if self.tenancy_mode != "control_plane":
                raise ValueError("builtin plugins must use tenancy_mode='control_plane'")

        if self.quarantined and self.quarantine_reason is None:
            raise ValueError("quarantined plugins must declare quarantine_reason")
        if not self.quarantined and self.quarantine_reason is not None:
            raise ValueError("quarantine_reason requires quarantined=true")
        if self.signature is not None and self.signing_key_id is None:
            raise ValueError("signature requires signing_key_id")
        if self.signature is not None and self.source_sha256 is None:
            raise ValueError("signature requires source_sha256")
        if self.trust_level == "community" and self.sandbox_profile == "host":
            raise ValueError("community plugins must not request sandbox_profile='host'")
        if self.trust_level == "community" and self.tenancy_mode == "control_plane":
            raise ValueError("community plugins must not request tenancy_mode='control_plane'")

        if self.publishable_events:
            expected_prefix = f"{self.name}."
            invalid = [
                event_type
                for event_type in self.publishable_events
                if not event_type.startswith(expected_prefix)
            ]
            if invalid:
                raise ValueError(
                    "publishable event names must use the plugin namespace "
                    f"prefix '{expected_prefix}'"
                )
