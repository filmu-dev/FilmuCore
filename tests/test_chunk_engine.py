from __future__ import annotations

import asyncio

import httpx
import pytest

from filmu_py.core.chunk_engine import (
    CHUNK_FETCH_BYTES_TOTAL,
    CHUNK_FETCH_DURATION_SECONDS,
    ChunkCache,
    ChunkConfig,
    ReadType,
    calculate_file_chunks,
    detect_read_type,
    fetch_and_stitch,
    resolve_chunks_for_read,
)


def _make_source(size: int) -> bytes:
    return bytes(index % 251 for index in range(size))


def _chunk_bytes(source: bytes, *, start: int, end: int) -> bytes:
    return source[start : end + 1]


def test_calculate_file_chunks_for_large_file_produces_non_overlapping_ranges() -> None:
    file_size = 1024 * 1024 * 1024
    chunks = calculate_file_chunks("movie", file_size)

    assert chunks.header.start == 0
    assert chunks.header.end == chunks.header.size - 1
    assert chunks.header.size == chunks.config.header_size

    assert chunks.footer.size == chunks.config.max_footer_size
    assert chunks.footer.end == file_size - 1
    assert chunks.footer.start == file_size - chunks.config.max_footer_size

    assert chunks.body_chunks
    assert chunks.body_chunks[0].start == chunks.header.size
    assert chunks.body_chunks[-1].end == chunks.footer.start - 1
    assert chunks.header.end < chunks.body_chunks[0].start
    assert chunks.body_chunks[-1].end < chunks.footer.start

    previous_end = chunks.header.end
    for body_chunk in chunks.body_chunks:
        assert body_chunk.start == previous_end + 1
        previous_end = body_chunk.end


def test_calculate_file_chunks_for_small_file_handles_overlap_cleanly() -> None:
    chunks = calculate_file_chunks("small", 200_000)

    assert chunks.header.start == 0
    assert chunks.header.end == chunks.config.header_size - 1
    assert chunks.header.size == chunks.config.header_size
    assert chunks.footer.start == chunks.config.header_size
    assert chunks.footer.end == 199_999
    assert chunks.footer.size == 200_000 - chunks.config.header_size
    assert chunks.body_chunks == ()


def test_calculate_file_chunks_for_zero_byte_file_does_not_crash() -> None:
    chunks = calculate_file_chunks("empty", 0)

    assert chunks.file_size == 0
    assert chunks.header.size == 0
    assert chunks.footer.size == 0
    assert chunks.body_chunks == ()


def test_calculate_file_chunks_for_file_exactly_header_size() -> None:
    config = ChunkConfig()
    chunks = calculate_file_chunks("header-only", config.header_size)

    assert chunks.header.start == 0
    assert chunks.header.end == config.header_size - 1
    assert chunks.header.size == config.header_size
    assert chunks.footer.start == 0
    assert chunks.footer.end == -1
    assert chunks.footer.size == 0
    assert chunks.body_chunks == ()


@pytest.mark.parametrize(
    ("file_size", "expected_footer_size"),
    [
        (4 * 1024 * 1024, 131_072),
        (64 * 1024 * 1024, 1_339_392),
        (1024 * 1024 * 1024, 2_097_152),
    ],
)
def test_footer_size_clamping_and_alignment(file_size: int, expected_footer_size: int) -> None:
    chunks = calculate_file_chunks("footer", file_size)

    assert chunks.footer.size == expected_footer_size
    assert chunks.footer.size % chunks.config.block_size == 0


def test_detect_read_type_header_scan() -> None:
    file_chunks = calculate_file_chunks("movie", 1024 * 1024 * 1024)
    cache = ChunkCache()

    assert detect_read_type(0, 4096, file_chunks, cache) is ReadType.HEADER_SCAN


def test_detect_read_type_footer_read_without_previous_context() -> None:
    file_chunks = calculate_file_chunks("movie", 1024 * 1024 * 1024)
    cache = ChunkCache()

    assert (
        detect_read_type(file_chunks.footer.start, 4096, file_chunks, cache) is ReadType.FOOTER_READ
    )


def test_detect_read_type_footer_scan_with_previous_far_back() -> None:
    file_chunks = calculate_file_chunks("movie", 1024 * 1024 * 1024)
    cache = ChunkCache()

    assert (
        detect_read_type(
            file_chunks.footer.start,
            4096,
            file_chunks,
            cache,
            previous_offset=file_chunks.header.size,
        )
        is ReadType.FOOTER_SCAN
    )


def test_detect_read_type_sequential_body_read() -> None:
    file_chunks = calculate_file_chunks("movie", 1024 * 1024 * 1024)
    cache = ChunkCache()
    offset = file_chunks.header.size + 64 * 1024

    assert (
        detect_read_type(
            offset,
            128 * 1024,
            file_chunks,
            cache,
            previous_offset=file_chunks.header.size,
        )
        is ReadType.BODY_READ
    )


def test_detect_read_type_general_scan() -> None:
    file_chunks = calculate_file_chunks("movie", 1024 * 1024 * 1024)
    cache = ChunkCache()
    offset = file_chunks.header.size + (2 * file_chunks.config.chunk_size) + 4096

    assert (
        detect_read_type(
            offset,
            1024,
            file_chunks,
            cache,
            previous_offset=file_chunks.header.size,
        )
        is ReadType.GENERAL_SCAN
    )


def test_detect_read_type_cache_hit() -> None:
    file_chunks = calculate_file_chunks("movie", 1024 * 1024 * 1024)
    cache = ChunkCache()
    offset = file_chunks.header.size + 1000
    size = 4096

    chunks = resolve_chunks_for_read(offset, size, file_chunks)
    for chunk in chunks:
        cache.put(chunk.cache_key, bytes(chunk.size))

    assert detect_read_type(offset, size, file_chunks, cache) is ReadType.CACHE_HIT


def test_resolve_chunks_for_single_body_chunk() -> None:
    file_chunks = calculate_file_chunks("movie", 1024 * 1024 * 1024)
    offset = file_chunks.header.size + 1000
    size = 4096

    chunks = resolve_chunks_for_read(offset, size, file_chunks)

    assert chunks == [file_chunks.body_chunks[0]]


def test_resolve_chunks_for_multiple_body_chunks() -> None:
    file_chunks = calculate_file_chunks("movie", 1024 * 1024 * 1024)
    first_body = file_chunks.body_chunks[0]
    offset = first_body.end - 1024
    size = 4096

    chunks = resolve_chunks_for_read(offset, size, file_chunks)

    assert chunks == [file_chunks.body_chunks[0], file_chunks.body_chunks[1]]


def test_resolve_chunks_for_header_range() -> None:
    file_chunks = calculate_file_chunks("movie", 1024 * 1024 * 1024)

    chunks = resolve_chunks_for_read(0, 4096, file_chunks)

    assert chunks == [file_chunks.header]


def test_resolve_chunks_for_footer_range() -> None:
    file_chunks = calculate_file_chunks("movie", 1024 * 1024 * 1024)

    chunks = resolve_chunks_for_read(file_chunks.footer.start, 4096, file_chunks)

    assert chunks == [file_chunks.footer]


def test_resolve_chunks_for_header_body_boundary() -> None:
    file_chunks = calculate_file_chunks("movie", 1024 * 1024 * 1024)
    offset = file_chunks.header.end - 1024
    size = 4096

    chunks = resolve_chunks_for_read(offset, size, file_chunks)

    assert chunks == [file_chunks.header, file_chunks.body_chunks[0]]


def test_resolve_chunks_deduplicates_identical_header_footer_ranges() -> None:
    file_chunks = calculate_file_chunks("small", 100_000)

    chunks = resolve_chunks_for_read(0, 4096, file_chunks)

    assert chunks == [file_chunks.header]


def test_chunk_cache_put_and_get() -> None:
    cache = ChunkCache(max_bytes=1024)
    payload = b"chunk-data"

    cache.put("key", payload)

    assert cache.get("key") == payload


def test_chunk_cache_miss_returns_none() -> None:
    cache = ChunkCache(max_bytes=1024)

    assert cache.get("missing") is None


def test_chunk_cache_evicts_when_max_bytes_exceeded() -> None:
    cache = ChunkCache(max_bytes=5)

    cache.put("first", b"123")
    cache.put("second", b"456")

    assert cache.has("first") is False
    assert cache.has("second") is True
    assert cache.stats.evictions == 1
    assert cache.stats.current_bytes == 3


def test_chunk_cache_stats_track_hits_and_misses() -> None:
    cache = ChunkCache(max_bytes=1024)
    cache.put("key", b"data")

    assert cache.get("key") == b"data"
    assert cache.get("missing") is None
    assert cache.stats.hits == 1
    assert cache.stats.misses == 1


def test_chunk_cache_rejects_non_positive_capacity() -> None:
    with pytest.raises(ValueError):
        ChunkCache(max_bytes=0)


def test_fetch_and_stitch_uses_cache_and_returns_exact_requested_bytes() -> None:
    config = ChunkConfig(
        header_size=16,
        min_footer_size=16,
        max_footer_size=16,
        target_footer_pct=0.25,
        block_size=4,
        chunk_size=8,
    )
    source = _make_source(64)
    file_chunks = calculate_file_chunks("movie", len(source), config)
    offset = 12
    size = 28
    chunks = resolve_chunks_for_read(offset, size, file_chunks)
    cache = ChunkCache(max_bytes=256)

    cached_chunk = chunks[0]
    cache.put(
        cached_chunk.cache_key, _chunk_bytes(source, start=cached_chunk.start, end=cached_chunk.end)
    )

    seen_ranges: list[tuple[int, int]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        range_header = request.headers["Range"]
        start_text, end_text = range_header.removeprefix("bytes=").split("-", maxsplit=1)
        start = int(start_text)
        end = int(end_text)
        seen_ranges.append((start, end))
        return httpx.Response(
            206,
            content=source[start : end + 1],
            headers={"Content-Range": f"bytes {start}-{end}/{len(source)}"},
        )

    async def run_test() -> bytes:
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            return await fetch_and_stitch(
                resource_id="movie",
                url="https://example.invalid/movie.mkv",
                offset=offset,
                size=size,
                chunks=chunks,
                cache=cache,
                http_client=client,
            )

    before_fetch_bytes = CHUNK_FETCH_BYTES_TOTAL._value.get()
    before_fetch_duration = CHUNK_FETCH_DURATION_SECONDS._sum.get()

    result = asyncio.run(run_test())

    assert result == source[offset : offset + size]
    assert seen_ranges == [(chunk.start, chunk.end) for chunk in chunks[1:]]
    assert CHUNK_FETCH_BYTES_TOTAL._value.get() - before_fetch_bytes == sum(
        chunk.size for chunk in chunks[1:]
    )
    assert CHUNK_FETCH_DURATION_SECONDS._sum.get() > before_fetch_duration

    for chunk in chunks[1:]:
        assert cache.has(chunk.cache_key) is True


def test_fetch_and_stitch_rejects_full_body_200_for_range_request() -> None:
    config = ChunkConfig(
        header_size=16,
        min_footer_size=16,
        max_footer_size=16,
        target_footer_pct=0.25,
        block_size=4,
        chunk_size=8,
    )
    source = _make_source(64)
    file_chunks = calculate_file_chunks("movie", len(source), config)
    chunks = resolve_chunks_for_read(20, 8, file_chunks)
    cache = ChunkCache(max_bytes=256)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=source)

    async def run_test() -> None:
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            await fetch_and_stitch(
                resource_id="movie",
                url="https://example.invalid/movie.mkv",
                offset=20,
                size=8,
                chunks=chunks,
                cache=cache,
                http_client=client,
            )

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(run_test())


def test_fetch_and_stitch_rejects_short_payload() -> None:
    config = ChunkConfig(
        header_size=16,
        min_footer_size=16,
        max_footer_size=16,
        target_footer_pct=0.25,
        block_size=4,
        chunk_size=8,
    )
    source = _make_source(64)
    file_chunks = calculate_file_chunks("movie", len(source), config)
    chunks = resolve_chunks_for_read(20, 8, file_chunks)
    cache = ChunkCache(max_bytes=256)

    def handler(request: httpx.Request) -> httpx.Response:
        range_header = request.headers["Range"]
        start_text, end_text = range_header.removeprefix("bytes=").split("-", maxsplit=1)
        start = int(start_text)
        end = int(end_text)
        return httpx.Response(
            206,
            content=source[start:end],
            headers={"Content-Range": f"bytes {start}-{end}/{len(source)}"},
        )

    async def run_test() -> None:
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            await fetch_and_stitch(
                resource_id="movie",
                url="https://example.invalid/movie.mkv",
                offset=20,
                size=8,
                chunks=chunks,
                cache=cache,
                http_client=client,
            )

    with pytest.raises(ValueError):
        asyncio.run(run_test())


def test_fetch_and_stitch_rejects_invalid_cached_payload_length() -> None:
    config = ChunkConfig(
        header_size=16,
        min_footer_size=16,
        max_footer_size=16,
        target_footer_pct=0.25,
        block_size=4,
        chunk_size=8,
    )
    file_chunks = calculate_file_chunks("movie", 64, config)
    chunks = resolve_chunks_for_read(20, 8, file_chunks)
    cache = ChunkCache(max_bytes=256)
    cache.put(chunks[0].cache_key, b"bad")

    async def run_test() -> None:
        transport = httpx.MockTransport(lambda request: httpx.Response(500))
        async with httpx.AsyncClient(transport=transport) as client:
            await fetch_and_stitch(
                resource_id="movie",
                url="https://example.invalid/movie.mkv",
                offset=20,
                size=8,
                chunks=chunks,
                cache=cache,
                http_client=client,
            )

    with pytest.raises(ValueError):
        asyncio.run(run_test())


def test_fetch_and_stitch_clamps_to_end_of_file() -> None:
    config = ChunkConfig(
        header_size=16,
        min_footer_size=16,
        max_footer_size=16,
        target_footer_pct=0.25,
        block_size=4,
        chunk_size=8,
    )
    source = _make_source(64)
    file_chunks = calculate_file_chunks("movie", len(source), config)
    chunks = resolve_chunks_for_read(60, 16, file_chunks)
    cache = ChunkCache(max_bytes=256)

    def handler(request: httpx.Request) -> httpx.Response:
        range_header = request.headers["Range"]
        start_text, end_text = range_header.removeprefix("bytes=").split("-", maxsplit=1)
        start = int(start_text)
        end = int(end_text)
        return httpx.Response(
            206,
            content=source[start : end + 1],
            headers={"Content-Range": f"bytes {start}-{end}/{len(source)}"},
        )

    async def run_test() -> bytes:
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            return await fetch_and_stitch(
                resource_id="movie",
                url="https://example.invalid/movie.mkv",
                offset=60,
                size=16,
                chunks=chunks,
                cache=cache,
                http_client=client,
            )

    assert asyncio.run(run_test()) == source[60:64]
