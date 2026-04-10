"""Filesystem plugin loader for safe GraphQL and capability discovery."""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from dataclasses import dataclass, field
from importlib.metadata import EntryPoint, entry_points
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import structlog
from prometheus_client import Counter

from filmu_py.graphql.plugin_registry import GraphQLPluginRegistry, GraphQLResolverKind
from filmu_py.plugins.context import PluginContextProvider
from filmu_py.plugins.manifest import PluginManifest
from filmu_py.plugins.registry import PluginCapabilityKind, PluginRegistry
from filmu_py.plugins.trust import (
    PluginTrustStore,
    load_plugin_trust_store,
    verify_plugin_signature,
)

logger = structlog.get_logger(__name__)

PLUGIN_LOAD_TOTAL = Counter(
    "filmu_py_plugin_load_total",
    "Plugin load outcomes",
    ["plugin_name", "result"],
)


def _plugin_load_result(reason: str) -> str:
    if reason == "host_version_incompatible":
        return "skipped_version"
    if reason == "api_version_incompatible":
        return "skipped_api_version"
    if reason == "plugin_quarantined":
        return "skipped_quarantined"
    if reason == "source_digest_mismatch":
        return "skipped_integrity"
    if reason.startswith("plugin_signature_") or reason in {
        "unsigned_plugin_disallowed",
        "trust_store_required",
    }:
        return "skipped_signature"
    if "plugin.json" in reason or "manifest" in reason:
        return "skipped_manifest"
    return "failed"


@dataclass(frozen=True, slots=True)
class PluginLoadSuccess:
    """Summary of one successfully processed plugin directory."""

    plugin_name: str
    version: str
    plugin_dir: Path
    source: str
    api_version: str
    distribution: str
    publisher: str | None
    release_channel: str
    trust_level: str
    min_host_version: str | None
    max_host_version: str | None
    permission_scopes: tuple[str, ...]
    source_sha256: str | None
    signing_key_id: str | None
    signature_present: bool
    signature_verified: bool
    signature_verification_reason: str | None
    trust_policy_decision: str | None
    trust_store_source: str | None
    sandbox_profile: str
    quarantined: bool
    quarantine_reason: str | None
    registered_query_resolvers: int
    registered_mutation_resolvers: int
    registered_subscription_resolvers: int
    registered_capabilities: tuple[str, ...] = ()
    skipped: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class PluginLoadFailure:
    """Failure record for one plugin directory that could not be loaded."""

    plugin_name: str
    plugin_dir: Path
    source: str
    reason: str


@dataclass(frozen=True, slots=True)
class PackagedPluginDefinition:
    """Packaged entry-point plugin definition resolved from an installed distribution."""

    manifest: PluginManifest | dict[str, Any]
    module: object


@dataclass(slots=True)
class PluginLoadReport:
    """Aggregate plugin load results for startup reporting and tests."""

    loaded: list[PluginLoadSuccess] = field(default_factory=list)
    failed: list[PluginLoadFailure] = field(default_factory=list)

    @property
    def discovered_count(self) -> int:
        """Return the number of plugin discovery results across all discovery paths."""

        return len(self.loaded) + len(self.failed)


def _module_name_for(plugin_name: str, entry_module: Path) -> str:
    """Create a stable synthetic module name for dynamic plugin imports."""

    sanitized_name = plugin_name.replace("-", "_").replace(".", "_")
    sanitized_stem = entry_module.stem.replace("-", "_").replace(".", "_")
    return f"filmu_py_plugin_{sanitized_name}_{sanitized_stem}"


def _load_plugin_module(plugin_dir: Path, manifest: PluginManifest) -> ModuleType:
    """Import a plugin entry module from a plugin directory."""

    entry_module = manifest.resolve_entry_module(plugin_dir)
    if not entry_module.is_file():
        raise FileNotFoundError(f"entry module does not exist: {entry_module}")

    module_name = _module_name_for(manifest.name, entry_module)
    spec = importlib.util.spec_from_file_location(module_name, entry_module)
    if spec is None or spec.loader is None:
        raise ImportError(f"unable to create import spec for {entry_module}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module


def _resolve_module_exports(
    module: object,
    export_names: tuple[str, ...],
) -> tuple[list[object], list[str]]:
    """Resolve manifest-declared export names from an imported plugin module."""

    resolved: list[object] = []
    skipped: list[str] = []

    module_name = getattr(module, "__name__", type(module).__name__)
    for export_name in export_names:
        if not hasattr(module, export_name):
            skipped.append(f"resolver '{export_name}' is missing from module '{module_name}'")
            continue
        resolved.append(getattr(module, export_name))

    return resolved, skipped


def _packaged_plugin_dir(entry_point_name: str) -> Path:
    """Return a stable synthetic source path for packaged plugins."""

    return Path("__entrypoint_plugins__") / entry_point_name


def _load_packaged_plugin(
    entry_point: EntryPoint,
) -> tuple[PluginManifest | dict[str, Any], object]:
    """Load one packaged plugin entry point into a manifest/module pair."""

    loaded = entry_point.load()
    if not callable(loaded):
        raise TypeError(f"entry point '{entry_point.name}' is not callable")

    definition = loaded()
    if isinstance(definition, PackagedPluginDefinition):
        return definition.manifest, definition.module
    if isinstance(definition, tuple) and len(definition) == 2:
        manifest, module = cast(tuple[PluginManifest | dict[str, Any], object], definition)
        return manifest, module
    raise TypeError("entry point factory must return a (manifest, module) pair")


def _register_plugin(
    *,
    manifest_source: Path,
    manifest_data: PluginManifest | dict[str, Any],
    module: object,
    registry: PluginRegistry,
    host_version: str,
    trust_store: PluginTrustStore | None = None,
    strict_signatures: bool = False,
    context_provider: PluginContextProvider | None = None,
    register_graphql: bool = True,
    register_capabilities: bool = True,
) -> PluginLoadSuccess:
    """Validate one manifest/module pair and register its declared exports."""

    manifest = (
        manifest_data
        if isinstance(manifest_data, PluginManifest)
        else PluginManifest.model_validate(manifest_data)
    )
    manifest.validate_policy()
    if manifest.quarantined:
        raise ValueError("plugin_quarantined")
    manifest.ensure_host_compatibility(host_version, supported_api_versions=("1",))
    source_path = _resolve_source_path(manifest_source=manifest_source, manifest=manifest, module=module)
    _validate_source_digest(manifest=manifest, source_path=source_path)
    signature_verification = verify_plugin_signature(
        source_sha256=manifest.source_sha256,
        signature=manifest.signature,
        signing_key_id=manifest.signing_key_id,
        distribution=manifest.distribution,
        trust_store=trust_store,
    )
    if strict_signatures and manifest.distribution != "builtin":
        if manifest.signature is None:
            raise ValueError("unsigned_plugin_disallowed")
        if trust_store is None:
            raise ValueError("trust_store_required")
        if not signature_verification.verified:
            raise ValueError(f"plugin_signature_{signature_verification.reason}")
    registry.register_manifest(manifest)

    skipped_messages: list[str] = []
    if manifest.uses_implicit_permission_scopes() and manifest.distribution != "builtin":
        skipped_messages.append(
            "permission scopes were inferred from declared capabilities; "
            "set explicit permission_scopes for stricter policy review"
        )
    if manifest.distribution != "builtin" and manifest.source_sha256 is None:
        skipped_messages.append("source digest is not declared; provenance is not fully pinned")
    if manifest.distribution != "builtin" and manifest.signature is None:
        skipped_messages.append("plugin is unsigned; provenance is not independently verifiable")
    if manifest.distribution != "builtin" and not signature_verification.verified:
        skipped_messages.append(
            f"signature verification state: {signature_verification.reason}"
        )
    registered_counts: dict[GraphQLResolverKind, int] = {
        GraphQLResolverKind.QUERY: 0,
        GraphQLResolverKind.MUTATION: 0,
        GraphQLResolverKind.SUBSCRIPTION: 0,
    }

    if register_graphql:
        for kind in GraphQLResolverKind:
            exported_resolvers, missing_exports = _resolve_module_exports(
                module,
                manifest.graphql.exports_for(kind),
            )
            skipped_messages.extend(missing_exports)
            registered, skipped = registry.safe_register_many(
                plugin_name=manifest.name,
                kind=kind,
                resolvers=exported_resolvers,
            )
            registered_counts[kind] = registered
            skipped_messages.extend(skipped)

    registered_capabilities: list[str] = []
    if register_capabilities:
        declared_capabilities = manifest.declared_non_graphql_capabilities()
        plugin_context = (
            context_provider.build(
                manifest.name,
                datasource_name=manifest.datasource,
            )
            if context_provider is not None
            else None
        )
        for capability_name in declared_capabilities:
            export_name = manifest.capability_exports().get(capability_name)
            if export_name is None:
                skipped_messages.append(
                    f"capability '{capability_name}' is declared but no export symbol is configured"
                )
                continue
            if plugin_context is None:
                skipped_messages.append(
                    f"capability '{capability_name}' skipped because no context provider is configured"
                )
                continue

            exported_capabilities, missing_exports = _resolve_module_exports(module, (export_name,))
            skipped_messages.extend(
                [f"capability '{capability_name}': {message}" for message in missing_exports]
            )
            if not exported_capabilities:
                continue

            registered, skipped = registry.safe_register_capability(
                plugin_name=manifest.name,
                kind=PluginCapabilityKind(capability_name),
                candidate=exported_capabilities[0],
                context=plugin_context,
            )
            if registered:
                registered_capabilities.append(capability_name)
            skipped_messages.extend(skipped)

    return PluginLoadSuccess(
        plugin_name=manifest.name,
        version=manifest.version,
        plugin_dir=manifest_source,
        source=manifest.distribution,
        api_version=manifest.api_version,
        distribution=manifest.distribution,
        publisher=manifest.publisher,
        release_channel=manifest.release_channel,
        trust_level=manifest.trust_level,
        min_host_version=manifest.min_host_version,
        max_host_version=manifest.max_host_version,
        permission_scopes=tuple(sorted(manifest.effective_permission_scopes())),
        source_sha256=manifest.source_sha256,
        signing_key_id=manifest.signing_key_id,
        signature_present=manifest.signature is not None,
        signature_verified=signature_verification.verified,
        signature_verification_reason=signature_verification.reason,
        trust_policy_decision=signature_verification.trust_policy_decision,
        trust_store_source=signature_verification.trust_store_source,
        sandbox_profile=manifest.sandbox_profile,
        quarantined=manifest.quarantined,
        quarantine_reason=manifest.quarantine_reason,
        registered_query_resolvers=registered_counts[GraphQLResolverKind.QUERY],
        registered_mutation_resolvers=registered_counts[GraphQLResolverKind.MUTATION],
        registered_subscription_resolvers=registered_counts[GraphQLResolverKind.SUBSCRIPTION],
        registered_capabilities=tuple(registered_capabilities),
        skipped=tuple(skipped_messages),
    )


def _resolve_source_path(*, manifest_source: Path, manifest: PluginManifest, module: object) -> Path | None:
    """Return the local source file used for optional provenance digest verification."""

    if manifest.distribution == "filesystem":
        candidate = manifest.resolve_entry_module(manifest_source)
        return candidate if candidate.is_file() else None

    module_file = getattr(module, "__file__", None)
    if isinstance(module_file, str):
        candidate = Path(module_file)
        if candidate.is_file():
            return candidate
    return None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(64 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_source_digest(*, manifest: PluginManifest, source_path: Path | None) -> None:
    """Verify one manifest-declared source digest when the source file is available."""

    if manifest.source_sha256 is None or source_path is None:
        return
    if _sha256_file(source_path) != manifest.source_sha256:
        raise ValueError("source_digest_mismatch")


def load_plugins(
    plugins_dir: Path,
    registry: PluginRegistry,
    *,
    context_provider: PluginContextProvider | None = None,
    host_version: str = "0.1.0",
    trust_store_path: Path | None = None,
    strict_signatures: bool = False,
    register_graphql: bool = True,
    register_capabilities: bool = True,
) -> PluginLoadReport:
    """Discover filesystem and packaged plugins and register declared capabilities safely."""

    resolved_plugins_dir = plugins_dir if plugins_dir.is_absolute() else (Path.cwd() / plugins_dir)
    report = PluginLoadReport()
    trust_store = load_plugin_trust_store(trust_store_path)

    if not resolved_plugins_dir.exists():
        logger.info(
            "graphql.plugins.discovery.skipped",
            plugins_dir=str(resolved_plugins_dir),
            reason="directory-missing",
        )

    if resolved_plugins_dir.exists():
        for plugin_dir in sorted(path for path in resolved_plugins_dir.iterdir() if path.is_dir()):
            manifest_path = plugin_dir / "plugin.json"
            if not manifest_path.is_file():
                continue

            try:
                manifest = PluginManifest.model_validate_json(
                    manifest_path.read_text(encoding="utf-8")
                )
                if manifest.distribution == "filesystem":
                    manifest = manifest.model_copy(update={"distribution": "filesystem"})
                module = _load_plugin_module(plugin_dir, manifest)
                success = _register_plugin(
                    manifest_source=plugin_dir,
                    manifest_data=manifest,
                    module=module,
                    registry=registry,
                    host_version=host_version,
                    trust_store=trust_store,
                    strict_signatures=strict_signatures,
                    context_provider=context_provider,
                    register_graphql=register_graphql,
                    register_capabilities=register_capabilities,
                )
            except Exception as exc:
                reason = str(exc)
                plugin_name = plugin_dir.name
                report.failed.append(
                    PluginLoadFailure(
                        plugin_name=plugin_name,
                        plugin_dir=plugin_dir,
                        source="filesystem",
                        reason=reason,
                    )
                )
                PLUGIN_LOAD_TOTAL.labels(
                    plugin_name=plugin_name,
                    result=_plugin_load_result(reason),
                ).inc()
                logger.warning(
                    "graphql.plugins.discovery.failed",
                    plugin_dir=str(plugin_dir),
                    reason=reason,
                    source="filesystem",
                )
                continue

            report.loaded.append(success)
            PLUGIN_LOAD_TOTAL.labels(plugin_name=success.plugin_name, result="success").inc()
            logger.info(
                "graphql.plugins.discovery.loaded",
                plugin=success.plugin_name,
                version=success.version,
                plugin_dir=str(success.plugin_dir),
                registered_query_resolvers=success.registered_query_resolvers,
                registered_mutation_resolvers=success.registered_mutation_resolvers,
                registered_subscription_resolvers=success.registered_subscription_resolvers,
                registered_capabilities=success.registered_capabilities,
                skipped=len(success.skipped),
                source="filesystem",
            )

    for entry_point in sorted(entry_points(group="filmu.plugins"), key=lambda item: item.name):
        synthetic_plugin_dir = _packaged_plugin_dir(entry_point.name)
        try:
            manifest_data, loaded_module = _load_packaged_plugin(entry_point)
            if isinstance(manifest_data, PluginManifest):
                manifest_data = manifest_data.model_copy(update={"distribution": "entry_point"})
            elif isinstance(manifest_data, dict):
                manifest_data = {**manifest_data, "distribution": manifest_data.get("distribution", "entry_point")}
            success = _register_plugin(
                manifest_source=synthetic_plugin_dir,
                manifest_data=manifest_data,
                module=loaded_module,
                registry=registry,
                host_version=host_version,
                trust_store=trust_store,
                strict_signatures=strict_signatures,
                context_provider=context_provider,
                register_graphql=register_graphql,
                register_capabilities=register_capabilities,
            )
        except Exception as exc:
            reason = str(exc)
            report.failed.append(
                PluginLoadFailure(
                    plugin_name=entry_point.name,
                    plugin_dir=synthetic_plugin_dir,
                    source="entry_point",
                    reason=reason,
                )
            )
            PLUGIN_LOAD_TOTAL.labels(
                plugin_name=entry_point.name,
                result=_plugin_load_result(reason),
            ).inc()
            logger.warning(
                "graphql.plugins.discovery.failed",
                plugin_dir=str(synthetic_plugin_dir),
                reason=reason,
                source="entrypoint",
            )
            continue

        report.loaded.append(success)
        PLUGIN_LOAD_TOTAL.labels(plugin_name=success.plugin_name, result="success").inc()
        logger.info(
            "graphql.plugins.discovery.loaded",
            plugin=success.plugin_name,
            version=success.version,
            plugin_dir=str(success.plugin_dir),
            registered_query_resolvers=success.registered_query_resolvers,
            registered_mutation_resolvers=success.registered_mutation_resolvers,
            registered_subscription_resolvers=success.registered_subscription_resolvers,
            registered_capabilities=success.registered_capabilities,
            skipped=len(success.skipped),
            source="entrypoint",
        )

    logger.info(
        "graphql.plugins.discovery.completed",
        plugins_dir=str(resolved_plugins_dir),
        loaded=len(report.loaded),
        failed=len(report.failed),
    )
    return report


def load_graphql_plugins(
    plugins_dir: Path,
    registry: GraphQLPluginRegistry,
    *,
    host_version: str = "0.1.0",
    trust_store_path: Path | None = None,
    strict_signatures: bool = False,
) -> PluginLoadReport:
    """Backward-compatible wrapper that preserves the GraphQL-only loader surface."""

    capability_registry = PluginRegistry(graphql_registry=registry)
    return load_plugins(
        plugins_dir,
        capability_registry,
        context_provider=None,
        host_version=host_version,
        trust_store_path=trust_store_path,
        strict_signatures=strict_signatures,
        register_graphql=True,
        register_capabilities=False,
    )
