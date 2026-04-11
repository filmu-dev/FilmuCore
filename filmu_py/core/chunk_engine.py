"""Shared chunk geometry, caching, and byte stitching for VFS and HTTP serving."""

from __future__ import annotations

import base64
import enum
import logging
import math
import threading
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass
from time import perf_counter

import httpx
from cachetools import LRUCache
from prometheus_client import Counter, Gauge, Histogram

logger = logging.getLogger(__name__)

CHUNK_READ_TYPE_TOTAL = Counter(
    "filmu_chunk_read_type_total",
    "Count of chunk-engine read classifications by read type.",
    labelnames=("read_type",),
)
CHUNK_CACHE_HITS_TOTAL = Counter(
    "filmu_chunk_cache_hits_total",
    "Count of chunk-cache hits.",
)
CHUNK_CACHE_MISSES_TOTAL = Counter(
    "filmu_chunk_cache_misses_total",
    "Count of chunk-cache misses.",
)
CHUNK_CACHE_EVICTIONS_TOTAL = Counter(
    "filmu_chunk_cache_evictions_total",
    "Count of chunk-cache evictions.",
)
CHUNK_CACHE_BYTES = Gauge(
    "filmu_chunk_cache_bytes",
    "Current byte footprint of the process-local chunk cache.",
)
CHUNK_FETCH_BYTES_TOTAL = Counter(
    "filmu_chunk_fetch_bytes_total",
    "Total upstream bytes fetched for missing chunks.",
)
CHUNK_FETCH_DURATION_SECONDS = Histogram(
    "filmu_chunk_fetch_duration_seconds",
    "Time spent fetching upstream chunk byte ranges.",
    buckets=(0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)


@dataclass(frozen=True, slots=True)
class ChunkConfig:
    """Static knobs for chunk geometry and read classification."""

    header_size: int = 131_072
    min_footer_size: int = 131_072
    max_footer_size: int = 2_097_152
    target_footer_pct: float = 0.02
    block_size: int = 4096
    chunk_size: int = 2_097_152
    scan_tolerance_bytes: int = 524_288
    sequential_read_tolerance_blocks: int = 8
    chunk_timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.header_size < 0:
            raise ValueError("header_size must be non-negative")
        if self.min_footer_size < 0:
            raise ValueError("min_footer_size must be non-negative")
        if self.max_footer_size < 0:
            raise ValueError("max_footer_size must be non-negative")
        if self.max_footer_size < self.min_footer_size:
            raise ValueError("max_footer_size must be greater than or equal to min_footer_size")
        if self.target_footer_pct < 0:
            raise ValueError("target_footer_pct must be non-negative")
        if self.block_size <= 0:
            raise ValueError("block_size must be positive")
        if self.chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if self.scan_tolerance_bytes < 0:
            raise ValueError("scan_tolerance_bytes must be non-negative")
        if self.sequential_read_tolerance_blocks < 0:
            raise ValueError("sequential_read_tolerance_blocks must be non-negative")
        if self.chunk_timeout_seconds <= 0:
            raise ValueError("chunk_timeout_seconds must be positive")


DEFAULT_CONFIG = ChunkConfig()


@dataclass(frozen=True, slots=True)
class ChunkDescriptor:
    """One concrete byte-range descriptor."""

    index: int
    start: int
    end: int
    size: int
    cache_key: str


@dataclass(frozen=True, slots=True)
class FileChunks:
    """Precomputed chunk geometry for one resource."""

    header: ChunkDescriptor
    footer: ChunkDescriptor
    body_chunks: tuple[ChunkDescriptor, ...]
    config: ChunkConfig
    file_size: int


@dataclass(frozen=True, slots=True)
class CacheStats:
    """Current chunk-cache counters and occupancy."""

    hits: int
    misses: int
    evictions: int
    current_bytes: int


class ReadType(enum.StrEnum):
    """Read classifications aligned to the TS comparator model."""

    HEADER_SCAN = "header-scan"
    FOOTER_SCAN = "footer-scan"
    FOOTER_READ = "footer-read"
    GENERAL_SCAN = "general-scan"
    BODY_READ = "body-read"
    CACHE_HIT = "cache-hit"


def _create_cache_key(resource_id: str, start: int, end: int) -> str:
    return base64.b64encode(f"{resource_id}-{start}-{end}".encode()).decode("ascii")


def _calculate_footer_size(file_size: int, config: ChunkConfig) -> int:
    percentage_size = file_size * config.target_footer_pct
    raw_footer_size = min(max(percentage_size, config.min_footer_size), config.max_footer_size)
    aligned_footer_size = math.floor(raw_footer_size / config.block_size) * config.block_size
    return abs(int(aligned_footer_size))


def _descriptor_size(start: int, end: int) -> int:
    if end < start:
        return 0
    return (end - start) + 1


def _build_descriptor(resource_id: str, *, index: int, start: int, end: int) -> ChunkDescriptor:
    return ChunkDescriptor(
        index=index,
        start=start,
        end=end,
        size=_descriptor_size(start, end),
        cache_key=_create_cache_key(resource_id, start, end),
    )


def calculate_file_chunks(
    resource_id: str,
    file_size: int,
    config: ChunkConfig = DEFAULT_CONFIG,
) -> FileChunks:
    """Calculate header, footer, and body chunk geometry for one resource."""

    if file_size < 0:
        raise ValueError("file_size must be non-negative")

    footer_size = _calculate_footer_size(file_size, config)
    header_range_end = min(config.header_size, file_size) - 1

    header = _build_descriptor(resource_id, index=0, start=0, end=header_range_end)

    if file_size <= header.size:
        footer_start = 0
        footer_end = -1
    else:
        footer_start = max(header.size, file_size - footer_size)
        footer_end = file_size - 1

    body_chunks: list[ChunkDescriptor] = []
    body_start = header.size
    body_end = footer_start - 1
    next_index = 1
    current_start = body_start
    while current_start <= body_end:
        current_end = min(current_start + config.chunk_size - 1, body_end)
        body_chunks.append(
            _build_descriptor(
                resource_id,
                index=next_index,
                start=current_start,
                end=current_end,
            )
        )
        current_start = current_end + 1
        next_index += 1

    footer = _build_descriptor(
        resource_id,
        index=next_index,
        start=footer_start,
        end=footer_end,
    )

    return FileChunks(
        header=header,
        footer=footer,
        body_chunks=tuple(body_chunks),
        config=config,
        file_size=file_size,
    )


def _record_read_type(read_type: ReadType) -> ReadType:
    CHUNK_READ_TYPE_TOTAL.labels(read_type=read_type.value).inc()
    return read_type


class _ObservableLRUCache(LRUCache[str, bytes]):
    """LRU cache that reports evictions back to the owner cache."""

    def __init__(
        self,
        *,
        maxsize: int,
        getsizeof: Callable[[bytes], int],
        on_evict: Callable[[str, bytes], None],
    ) -> None:
        super().__init__(maxsize=maxsize, getsizeof=getsizeof)
        self._on_evict = on_evict

    def popitem(self) -> tuple[str, bytes]:
        key, value = super().popitem()
        self._on_evict(key, value)
        return key, value


class ChunkCache:
    """Thread-safe byte-weighted LRU cache for chunk payloads."""

    def __init__(self, max_bytes: int = 256 * 1024 * 1024):
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        self._max_bytes = max_bytes
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._cache = _ObservableLRUCache(
            maxsize=max_bytes,
            getsizeof=len,
            on_evict=self._record_eviction,
        )
        CHUNK_CACHE_BYTES.set(0)

    def _record_eviction(self, key: str, value: bytes) -> None:
        del key
        del value
        self._evictions += 1
        CHUNK_CACHE_EVICTIONS_TOTAL.inc()

    def get(self, key: str) -> bytes | None:
        with self._lock:
            cached = self._cache.get(key)
            if cached is None:
                self._misses += 1
                CHUNK_CACHE_MISSES_TOTAL.inc()
                return None

            self._hits += 1
            CHUNK_CACHE_HITS_TOTAL.inc()
            return cached

    def put(self, key: str, data: bytes) -> None:
        if len(data) > self._max_bytes:
            return

        with self._lock:
            self._cache[key] = data
            CHUNK_CACHE_BYTES.set(self._cache.currsize)

    def has(self, key: str) -> bool:
        with self._lock:
            return key in self._cache

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            CHUNK_CACHE_BYTES.set(0)

    def max_bytes(self) -> int:
        """Return the configured byte budget for operator/runtime surfaces."""

        return self._max_bytes

    @property
    def stats(self) -> CacheStats:
        with self._lock:
            return CacheStats(
                hits=self._hits,
                misses=self._misses,
                evictions=self._evictions,
                current_bytes=int(self._cache.currsize),
            )


def resolve_chunks_for_read(
    offset: int, size: int, file_chunks: FileChunks
) -> list[ChunkDescriptor]:
    """Resolve the ordered chunk descriptors needed for one read request."""

    if offset < 0:
        raise ValueError("offset must be non-negative")
    if size < 0:
        raise ValueError("size must be non-negative")
    if size == 0 or file_chunks.file_size == 0 or offset >= file_chunks.file_size:
        return []

    request_end_inclusive = min(offset + size, file_chunks.file_size) - 1
    if request_end_inclusive <= file_chunks.header.end:
        return [file_chunks.header]

    if offset >= file_chunks.footer.start:
        if (
            file_chunks.header.start == file_chunks.footer.start
            and file_chunks.header.end == file_chunks.footer.end
        ):
            return [file_chunks.header]
        return [file_chunks.footer]

    resolved: list[ChunkDescriptor] = []
    if offset < file_chunks.header.size and file_chunks.header.size > 0:
        resolved.append(file_chunks.header)

    for chunk in file_chunks.body_chunks:
        if chunk.end < offset or chunk.start > request_end_inclusive:
            continue
        resolved.append(chunk)

    if request_end_inclusive >= file_chunks.footer.start and file_chunks.footer.size > 0:
        resolved.append(file_chunks.footer)

    return resolved


def detect_read_type(
    offset: int,
    size: int,
    file_chunks: FileChunks,
    cache: ChunkCache,
    previous_offset: int | None = None,
) -> ReadType:
    """Classify one request using the TS-inspired six-way decision tree."""

    chunks = resolve_chunks_for_read(offset, size, file_chunks)
    if not chunks:
        raise ValueError("Read request does not resolve to any chunks")

    if all(cache.has(chunk.cache_key) for chunk in chunks):
        return _record_read_type(ReadType.CACHE_HIT)

    start = offset
    end = min(offset + size, file_chunks.file_size) - 1

    if start <= file_chunks.header.end and end <= file_chunks.header.end:
        return _record_read_type(ReadType.HEADER_SCAN)

    if (
        previous_offset is not None
        and previous_offset
        < start
        - (file_chunks.config.sequential_read_tolerance_blocks * file_chunks.config.block_size)
        and file_chunks.footer.start <= start <= file_chunks.footer.end
    ):
        return _record_read_type(ReadType.FOOTER_SCAN)

    if (
        previous_offset is not None
        and abs(previous_offset - start) > file_chunks.config.scan_tolerance_bytes
        and start != file_chunks.header.size
        and size < file_chunks.config.block_size
    ) or (
        previous_offset is None
        and start > file_chunks.header.size
        and start < file_chunks.footer.start
    ):
        return _record_read_type(ReadType.GENERAL_SCAN)

    if start < file_chunks.footer.start:
        return _record_read_type(ReadType.BODY_READ)

    return _record_read_type(ReadType.FOOTER_READ)


def _parse_content_range(header_value: str) -> tuple[int, int]:
    unit, _, remainder = header_value.partition(" ")
    if unit.lower() != "bytes" or not remainder:
        raise ValueError("Invalid Content-Range header")

    range_part, _, _ = remainder.partition("/")
    start_text, separator, end_text = range_part.partition("-")
    if not separator:
        raise ValueError("Invalid Content-Range header")

    return int(start_text), int(end_text)


async def _fetch_chunk(
    *,
    resource_id: str,
    url: str,
    chunk: ChunkDescriptor,
    cache: ChunkCache,
    http_client: httpx.AsyncClient,
) -> bytes:
    started_at = perf_counter()
    payload = b""
    try:
        response = await http_client.get(
            url,
            headers={
                "Range": f"bytes={chunk.start}-{chunk.end}",
                "Accept-Encoding": "identity",
            },
            timeout=DEFAULT_CONFIG.chunk_timeout_seconds,
        )
        if response.status_code != httpx.codes.PARTIAL_CONTENT:
            raise httpx.HTTPStatusError(
                f"Expected 206 Partial Content, got {response.status_code}",
                request=response.request,
                response=response,
            )

        content_range = response.headers.get("Content-Range")
        if content_range is None:
            raise ValueError("Missing Content-Range header for chunk response")

        range_start, range_end = _parse_content_range(content_range)
        if range_start != chunk.start or range_end != chunk.end:
            raise ValueError("Chunk response Content-Range does not match requested range")

        content_encoding = response.headers.get("Content-Encoding")
        if content_encoding is not None and content_encoding.lower() != "identity":
            raise ValueError("Chunk response must not use compressed content encoding")

        payload = response.content
        if len(payload) != chunk.size:
            raise ValueError("Chunk response payload length does not match descriptor size")
    except Exception:
        logger.exception(
            "Failed to fetch chunk for resource_id=%s range=%s-%s from %s",
            resource_id,
            chunk.start,
            chunk.end,
            url,
        )
        raise
    finally:
        CHUNK_FETCH_DURATION_SECONDS.observe(perf_counter() - started_at)

    CHUNK_FETCH_BYTES_TOTAL.inc(len(payload))
    cache.put(chunk.cache_key, payload)
    return payload


async def iter_fetch_and_stitch(
    resource_id: str,
    url: str,
    offset: int,
    size: int,
    chunks: list[ChunkDescriptor],
    cache: ChunkCache,
    http_client: httpx.AsyncClient,
) -> AsyncGenerator[bytes, None]:
    """Fetch missing chunks and incrementally yield the exact requested byte window."""

    if offset < 0:
        raise ValueError("offset must be non-negative")
    if size < 0:
        raise ValueError("size must be non-negative")
    if size == 0:
        return
    if not chunks:
        raise ValueError("Chunk list must not be empty for a non-zero read")

    request_end = min(offset + size, max(chunk.end for chunk in chunks) + 1)
    covered_until = offset

    for chunk in chunks:
        payload = cache.get(chunk.cache_key)
        if payload is None:
            payload = await _fetch_chunk(
                resource_id=resource_id,
                url=url,
                chunk=chunk,
                cache=cache,
                http_client=http_client,
            )
        elif len(payload) != chunk.size:
            raise ValueError("Cached chunk payload length does not match descriptor size")

        overlap_start = max(offset, chunk.start)
        overlap_end = min(request_end, chunk.end + 1)
        if overlap_start >= overlap_end:
            continue
        if overlap_start > covered_until:
            raise ValueError("Chunk list does not fully cover the requested byte range")

        source_start = overlap_start - chunk.start
        source_end = overlap_end - chunk.start
        yield payload[source_start:source_end]
        covered_until = max(covered_until, overlap_end)

    if covered_until < request_end:
        raise ValueError("Chunk list does not fully cover the requested byte range")


async def fetch_and_stitch(
    resource_id: str,
    url: str,
    offset: int,
    size: int,
    chunks: list[ChunkDescriptor],
    cache: ChunkCache,
    http_client: httpx.AsyncClient,
) -> bytes:
    """Fetch missing chunks and return the exact requested byte window as one payload."""

    stitched = bytearray()
    async for payload in iter_fetch_and_stitch(
        resource_id=resource_id,
        url=url,
        offset=offset,
        size=size,
        chunks=chunks,
        cache=cache,
        http_client=http_client,
    ):
        stitched.extend(payload)
    return bytes(stitched)


__all__ = [
    "CHUNK_CACHE_BYTES",
    "CHUNK_CACHE_EVICTIONS_TOTAL",
    "CHUNK_CACHE_HITS_TOTAL",
    "CHUNK_CACHE_MISSES_TOTAL",
    "CHUNK_FETCH_BYTES_TOTAL",
    "CHUNK_FETCH_DURATION_SECONDS",
    "CHUNK_READ_TYPE_TOTAL",
    "DEFAULT_CONFIG",
    "CacheStats",
    "ChunkCache",
    "ChunkConfig",
    "ChunkDescriptor",
    "FileChunks",
    "ReadType",
    "calculate_file_chunks",
    "detect_read_type",
    "fetch_and_stitch",
    "iter_fetch_and_stitch",
    "resolve_chunks_for_read",
]
