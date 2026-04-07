"""Torrent-name parsing for the standalone RTN compatibility package."""

from __future__ import annotations

from guessit import guessit  # type: ignore[import-untyped]

from .schemas import ParsedData


def _json_safe_value(value: object) -> object:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    return str(value)


def parse_torrent_name(raw_title: str) -> ParsedData:
    """Parse one torrent title into structured RTN-compatible data."""

    normalized = raw_title.strip()
    if not normalized:
        raise ValueError("raw_title must not be empty")
    parsed = guessit(normalized)
    parsed_payload = {str(key): _json_safe_value(value) for key, value in parsed.items()}
    resolution = parsed_payload.get("screen_size")
    return ParsedData(
        raw_title=normalized,
        parsed_title=parsed_payload,
        resolution=resolution if isinstance(resolution, str) else None,
    )
