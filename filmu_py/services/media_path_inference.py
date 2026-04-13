"""Path-based season/episode inference helpers for media completion logic."""

from __future__ import annotations

import re

_SEASON_NUMBER_RE: tuple[re.Pattern[str], ...] = (
    re.compile(r"[Ss]eason\s*(\d+)", re.IGNORECASE),  # "Season 1", "season 01"
    re.compile(r"[Ss](\d{1,2})[Ee]\d{1,2}"),  # S01E02
    re.compile(r"(\d{1,2})x\d{1,2}"),  # 1x02
)
_EPISODE_NUMBER_RE: tuple[re.Pattern[str], ...] = (
    re.compile(r"[Ss]\d{1,2}[Ee](\d{1,3})", re.IGNORECASE),
    re.compile(r"\b\d{1,2}x(\d{1,3})\b", re.IGNORECASE),
    re.compile(r"\b[Ee]p?(?:isode)?\s*(\d{1,3})\b", re.IGNORECASE),
)
_SEASON_RANGE_RE: tuple[re.Pattern[str], ...] = (
    re.compile(r"[Ss](?:eason)?s?\s*(\d{1,2})[-\u2013](\d{1,2})\b", re.IGNORECASE),
    re.compile(r"[Ss](\d{1,2})[-\u2013][Ss](\d{1,2})\b"),
)
_MAX_SEASON_RANGE = 20


def infer_season_number_from_path(path: str | None) -> int | None:
    """Return a season number inferred from common file-naming patterns, or None."""

    if not path:
        return None
    for pattern in _SEASON_NUMBER_RE:
        match = pattern.search(path)
        if match:
            try:
                return int(match.group(1))
            except (ValueError, IndexError):
                pass
    return None


def infer_season_range_from_path(path: str | None) -> list[int]:
    """Return all season numbers inferred from a path, including pack ranges."""

    if not path:
        return []
    for pattern in _SEASON_RANGE_RE:
        match = pattern.search(path)
        if match:
            try:
                start, end = int(match.group(1)), int(match.group(2))
                if start <= end and (end - start) < _MAX_SEASON_RANGE:
                    return list(range(start, end + 1))
            except (ValueError, IndexError):
                pass
    single = infer_season_number_from_path(path)
    if single is not None:
        return [single]
    return []


def infer_episode_number_from_path(path: str | None) -> int | None:
    """Return one episode number inferred from common file-naming patterns, or None."""

    if not path:
        return None
    for pattern in _EPISODE_NUMBER_RE:
        match = pattern.search(path)
        if match:
            try:
                return int(match.group(1))
            except (ValueError, IndexError):
                pass
    return None
