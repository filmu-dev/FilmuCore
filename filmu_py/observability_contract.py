"""Cross-process observability header contract shared by Python and Rust."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

TRACEPARENT_HEADER = "traceparent"
TRACESTATE_HEADER = "tracestate"
BAGGAGE_HEADER = "baggage"
REQUEST_ID_HEADER = "x-request-id"
TENANT_ID_HEADER = "x-tenant-id"
VFS_SESSION_ID_HEADER = "x-filmu-vfs-session-id"
VFS_DAEMON_ID_HEADER = "x-filmu-vfs-daemon-id"
VFS_ENTRY_ID_HEADER = "x-filmu-vfs-entry-id"
VFS_PROVIDER_FILE_ID_HEADER = "x-filmu-vfs-provider-file-id"
VFS_HANDLE_KEY_HEADER = "x-filmu-vfs-handle-key"

TRACE_CONTEXT_HEADERS = (
    TRACEPARENT_HEADER,
    TRACESTATE_HEADER,
    BAGGAGE_HEADER,
)
CORRELATION_HEADERS = (
    REQUEST_ID_HEADER,
    TENANT_ID_HEADER,
    VFS_SESSION_ID_HEADER,
    VFS_DAEMON_ID_HEADER,
    VFS_ENTRY_ID_HEADER,
    VFS_PROVIDER_FILE_ID_HEADER,
    VFS_HANDLE_KEY_HEADER,
)
REQUIRED_CROSS_PROCESS_HEADERS = TRACE_CONTEXT_HEADERS + CORRELATION_HEADERS
STRUCTLOG_BINDING_TO_SPAN_ATTRIBUTE = (
    ("request_id", "request.id"),
    ("tenant_id", "tenant.id"),
    ("vfs_session_id", "vfs.session_id"),
    ("vfs_daemon_id", "vfs.daemon_id"),
    ("vfs_entry_id", "catalog.entry_id"),
    ("provider_file_id", "provider.file_id"),
    ("handle_key", "vfs.handle_key"),
)


def normalize_text_carrier(
    values: Mapping[str, Any] | Iterable[tuple[str, Any]],
) -> dict[str, str]:
    """Return one lower-cased text carrier suitable for OTEL extract/inject helpers."""

    items = values.items() if isinstance(values, Mapping) else values
    carrier: dict[str, str] = {}
    for key, value in items:
        if value is None:
            continue
        normalized_key = str(key).strip().lower()
        normalized_value = str(value).strip()
        if not normalized_key or not normalized_value:
            continue
        carrier[normalized_key] = normalized_value
    return carrier


def extract_structlog_bindings(carrier: Mapping[str, str]) -> dict[str, str]:
    """Return structlog bindings derived from the shared cross-process carrier."""

    bindings = {
        "request_id": carrier.get(REQUEST_ID_HEADER),
        "tenant_id": carrier.get(TENANT_ID_HEADER),
        "vfs_session_id": carrier.get(VFS_SESSION_ID_HEADER),
        "vfs_daemon_id": carrier.get(VFS_DAEMON_ID_HEADER),
        "vfs_entry_id": carrier.get(VFS_ENTRY_ID_HEADER),
        "provider_file_id": carrier.get(VFS_PROVIDER_FILE_ID_HEADER),
        "handle_key": carrier.get(VFS_HANDLE_KEY_HEADER),
    }
    return {key: value for key, value in bindings.items() if isinstance(value, str) and value}


def build_span_attributes(bindings: Mapping[str, str]) -> dict[str, str]:
    """Return normalized span attributes derived from shared correlation bindings."""

    attributes: dict[str, str] = {}
    for binding_key, attribute_key in STRUCTLOG_BINDING_TO_SPAN_ATTRIBUTE:
        value = bindings.get(binding_key)
        if not isinstance(value, str):
            continue
        normalized = value.strip()
        if normalized:
            attributes[attribute_key] = normalized
    return attributes
