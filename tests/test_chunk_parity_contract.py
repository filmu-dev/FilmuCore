"""HTTP chunk-engine parity contract tests.

These tests define the Python-side contract that must remain equivalent to the
mounted runtime chunk behavior validated in Rust tests.
"""

from __future__ import annotations

from dataclasses import dataclass

from filmu_py.core.chunk_engine import (
    ChunkCache,
    ReadType,
    calculate_file_chunks,
    detect_read_type,
    resolve_chunks_for_read,
)


@dataclass(frozen=True, slots=True)
class ChunkParityCase:
    name: str
    offset: int
    size: int
    expected_regions: tuple[str, ...]


def _chunk_region(
    *,
    start: int,
    end: int,
    header_start: int,
    header_end: int,
    footer_start: int,
    footer_end: int,
) -> str:
    if start == header_start and end == header_end:
        return "header"
    if start == footer_start and end == footer_end:
        return "footer"
    return "body"


def _assert_chunks_cover_request(
    *,
    chunks: list[object],
    offset: int,
    size: int,
    file_size: int,
) -> None:
    assert chunks
    request_end = min(offset + size, file_size) - 1
    first = chunks[0]
    last = chunks[-1]
    first_start = int(first.start)
    last_end = int(last.end)
    assert first_start <= offset
    assert last_end >= request_end


def test_mount_http_chunk_parity_contract_ranges() -> None:
    file_size = 1_073_741_824
    file_chunks = calculate_file_chunks("parity", file_size)
    cases = (
        ChunkParityCase(
            name="header_only",
            offset=0,
            size=4096,
            expected_regions=("header",),
        ),
        ChunkParityCase(
            name="footer_only",
            offset=file_chunks.footer.start,
            size=4096,
            expected_regions=("footer",),
        ),
        ChunkParityCase(
            name="body_only",
            offset=file_chunks.header.size + 8_192,
            size=131_072,
            expected_regions=("body",),
        ),
        ChunkParityCase(
            name="header_body_boundary",
            offset=file_chunks.header.end - 1_024,
            size=4_096,
            expected_regions=("header", "body"),
        ),
        ChunkParityCase(
            name="body_footer_boundary",
            offset=file_chunks.footer.start - 1_024,
            size=4_096,
            expected_regions=("body", "footer"),
        ),
    )

    for case in cases:
        chunks = resolve_chunks_for_read(case.offset, case.size, file_chunks)
        _assert_chunks_cover_request(
            chunks=chunks,
            offset=case.offset,
            size=case.size,
            file_size=file_size,
        )
        regions = tuple(
            _chunk_region(
                start=chunk.start,
                end=chunk.end,
                header_start=file_chunks.header.start,
                header_end=file_chunks.header.end,
                footer_start=file_chunks.footer.start,
                footer_end=file_chunks.footer.end,
            )
            for chunk in chunks
        )
        assert regions == case.expected_regions, case.name


def test_mount_http_chunk_parity_contract_cache_and_read_classification() -> None:
    file_chunks = calculate_file_chunks("parity-cache", 1_073_741_824)
    cache = ChunkCache(max_bytes=4 * 1024 * 1024)

    assert detect_read_type(0, 4096, file_chunks, cache) is ReadType.HEADER_SCAN
    assert detect_read_type(file_chunks.footer.start, 4096, file_chunks, cache) is ReadType.FOOTER_READ

    body_offset = file_chunks.header.size + (16 * 1024 * 1024)
    assert (
        detect_read_type(
            body_offset,
            1024,
            file_chunks,
            cache,
            previous_offset=0,
        )
        is ReadType.GENERAL_SCAN
    )

    sequential_offset = file_chunks.header.size + 8_192
    assert (
        detect_read_type(
            sequential_offset,
            16 * 1024,
            file_chunks,
            cache,
            previous_offset=file_chunks.header.size,
        )
        is ReadType.BODY_READ
    )

    cached_offset = file_chunks.header.size + 32 * 1024
    cached_size = 16 * 1024
    cached_chunks = resolve_chunks_for_read(cached_offset, cached_size, file_chunks)
    for chunk in cached_chunks:
        cache.put(chunk.cache_key, b"x" * chunk.size)

    assert detect_read_type(cached_offset, cached_size, file_chunks, cache) is ReadType.CACHE_HIT
