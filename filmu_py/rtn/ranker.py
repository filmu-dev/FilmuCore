"""RTN ranking and fetch-check logic compatible with the audited riven-ts behavior."""

from __future__ import annotations

import re
import unicodedata
from collections import Counter
from collections.abc import Sequence

import Levenshtein

from .defaults import RankingModel, default_ranking_model
from .schemas import ParsedData, RankedTorrent, RankingProfile

_RESOLUTION_RANKS: dict[str, int] = {
    "2160p": 7,
    "1440p": 6,
    "1080p": 5,
    "720p": 4,
    "480p": 3,
    "360p": 2,
    "unknown": 1,
}
_TITLE_BOUNDARY_HINTS = {
    "bluray",
    "blu",
    "ray",
    "web",
    "webdl",
    "webrip",
    "remux",
    "proper",
    "repack",
    "complete",
    "season",
    "episode",
}


def _normalize_title(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold().replace("_", " ")
    normalized = re.sub(r"[^\w\s]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _flatten_strings(value: object) -> list[str]:
    if isinstance(value, str):
        return [value.casefold()]
    if isinstance(value, list):
        flattened: list[str] = []
        for item in value:
            flattened.extend(_flatten_strings(item))
        return flattened
    if isinstance(value, dict):
        flattened_dict: list[str] = []
        for item in value.values():
            flattened_dict.extend(_flatten_strings(item))
        return flattened_dict
    return []


def _parsed_strings(parsed_title: dict[str, object], *keys: str) -> list[str]:
    strings: list[str] = []
    for key in keys:
        strings.extend(_flatten_strings(parsed_title.get(key)))
    return strings


def _resolution_key(value: str | None) -> str:
    if value is None:
        return "unknown"
    normalized = value.strip().casefold()
    if normalized == "4k":
        return "2160p"
    return normalized if normalized in _RESOLUTION_RANKS else "unknown"


def _candidate_title(data: ParsedData) -> str:
    parsed_title = data.parsed_title.get("title")
    if isinstance(parsed_title, str) and parsed_title.strip():
        return parsed_title
    return data.raw_title


def title_similarity(
    data: ParsedData,
    correct_title: str,
    aliases: Sequence[str] | None = None,
) -> float:
    """Return the TS-compatible Levenshtein ratio against the correct title plus aliases."""

    candidate = _normalize_title(_candidate_title(data))
    raw_candidate = _normalize_title(data.raw_title)
    options = [correct_title, *(aliases or ())]
    best = 0.0
    for option in options:
        normalized_option = _normalize_title(option)
        # Compare against parsed title candidate
        total_length = len(candidate) + len(normalized_option)
        if total_length == 0:
            ratio = 1.0
        else:
            ratio = (
                total_length - Levenshtein.distance(candidate, normalized_option)
            ) / total_length
        if candidate.startswith(f"{normalized_option} "):
            next_token = candidate[slice(len(normalized_option) + 1, None)].split(" ", 1)[0]
            if any(char.isdigit() for char in next_token) or next_token in _TITLE_BOUNDARY_HINTS:
                ratio = max(ratio, 1.0)
        # Also compare against raw_title when the parser may have misidentified the title
        # (e.g. PTT moves 'xxx' to 'other' and sets release group as 'title')
        if raw_candidate != candidate:
            raw_total = len(raw_candidate) + len(normalized_option)
            if raw_total > 0:
                raw_ratio = (
                    raw_total - Levenshtein.distance(raw_candidate, normalized_option)
                ) / raw_total
                if raw_candidate.startswith(f"{normalized_option} "):
                    next_raw_token = raw_candidate[slice(len(normalized_option) + 1, None)].split(" ", 1)[0]
                    if (
                        any(char.isdigit() for char in next_raw_token)
                        or next_raw_token in _TITLE_BOUNDARY_HINTS
                    ):
                        raw_ratio = max(raw_ratio, 1.0)
                ratio = max(ratio, raw_ratio)
        if ratio > best:
            best = ratio
    return best


def resolve_rank(default_rank: int, use_custom_rank: bool, custom_rank: int) -> int:
    """Resolve one rank respecting the `use_custom_rank` toggle from settings.json."""

    return custom_rank if use_custom_rank else default_rank


def _quality_keys(data: ParsedData) -> set[str]:
    tokens = _parsed_strings(data.parsed_title, "source", "other", "video_codec")
    keys: set[str] = set()
    if any("av1" in token for token in tokens):
        keys.add("av1")
    if any(token in {"avc", "h264", "h.264", "x264"} or "264" in token for token in tokens):
        keys.add("avc")
    if any(token in {"hevc", "h265", "h.265", "x265"} or "265" in token for token in tokens):
        keys.add("hevc")
    if any("xvid" in token for token in tokens):
        keys.add("xvid")
    if any("mpeg" in token for token in tokens):
        keys.add("mpeg")
    if any("remux" in token for token in tokens):
        keys.add("remux")
    elif any("bluray" in token or "blu-ray" in token for token in tokens):
        keys.add("bluray")
    if any(token == "dvd" or "dvd" in token for token in tokens):
        keys.add("dvd")
    if any("hdtv" in token for token in tokens):
        keys.add("hdtv")
    if any(token == "vhs" for token in tokens):
        keys.add("vhs")
    if any("webmux" in token for token in tokens):
        keys.add("webmux")
    if any("webdl" in token or "web-dl" in token for token in tokens):
        keys.add("webdl")
    elif any(token == "web" or token.startswith("web ") for token in tokens):
        keys.add("web")
    return keys


def _rip_keys(data: ParsedData) -> set[str]:
    tokens = _parsed_strings(data.parsed_title, "source", "other")
    keys: set[str] = set()
    for key in (
        "bdrip",
        "brrip",
        "dvdrip",
        "hdrip",
        "ppvrip",
        "satrip",
        "tvrip",
        "uhdrip",
        "vhsrip",
        "webdlrip",
        "webrip",
    ):
        if any(key in token for token in tokens):
            keys.add(key)
    return keys


def _hdr_keys(data: ParsedData) -> set[str]:
    tokens = _parsed_strings(data.parsed_title, "hdr", "other")
    keys: set[str] = set()
    if any("10bit" in token or "10 bit" in token for token in tokens):
        keys.add("bit10")
    bit_depth = data.parsed_title.get("bit_depth")
    if isinstance(bit_depth, int) and bit_depth >= 10:
        keys.add("bit10")
    if any("dolby vision" in token or token == "dv" for token in tokens):
        keys.add("dolby_vision")
    if any("hdr10+" in token or "hdr10plus" in token for token in tokens):
        keys.add("hdr10plus")
    elif any("hdr" in token for token in tokens):
        keys.add("hdr")
    if any(token == "sdr" for token in tokens):
        keys.add("sdr")
    return keys


def _audio_keys(data: ParsedData) -> set[str]:
    tokens = _parsed_strings(data.parsed_title, "audio_codec", "audio_profile", "other")
    keys: set[str] = set()
    if any("truehd" in token or "true hd" in token for token in tokens):
        keys.add("truehd")
    if any(
        "dts-hd" in token or "dts hd" in token or "dts ma" in token or "master audio" in token
        for token in tokens
    ):
        keys.add("dts_lossless")
    elif any(token == "dts" for token in tokens):
        keys.add("dts_lossy")
    if any("atmos" in token for token in tokens):
        keys.add("atmos")
    if any("dolby digital plus" in token or token == "ddp" for token in tokens):
        keys.add("dolby_digital_plus")
    elif any("dolby digital" in token or token == "dd" for token in tokens):
        keys.add("dolby_digital")
    if any(token == "aac" for token in tokens):
        keys.add("aac")
    if any(token == "flac" for token in tokens):
        keys.add("flac")
    if any(token == "mono" for token in tokens):
        keys.add("mono")
    if any(token == "mp3" for token in tokens):
        keys.add("mp3")
    if any(token == "stereo" or token == "2.0" for token in tokens):
        keys.add("stereo")
    if any("surround" in token or token in {"5.1", "7.1"} for token in tokens):
        keys.add("surround")
    return keys


def _extra_keys(data: ParsedData) -> set[str]:
    tokens = _parsed_strings(data.parsed_title, "other", "edition", "site")
    keys: set[str] = set()
    token_map = {
        "three_d": ("3d",),
        "converted": ("converted",),
        "documentary": ("documentary",),
        "dubbed": ("dubbed",),
        "edition": ("edition",),
        "hardcoded": ("hardcoded",),
        "network": ("netflix", "hbo", "amzn", "network"),
        "proper": ("proper",),
        "repack": ("repack",),
        "retail": ("retail",),
        "scene": ("scene",),
        "site": ("rarbg", "torrentleech", "piratebay", "site"),
        "subbed": ("subbed",),
        "uncensored": ("uncensored",),
        "upscaled": ("upscaled",),
    }
    for key, patterns in token_map.items():
        if any(any(pattern in token for pattern in patterns) for token in tokens):
            keys.add(key)
    return keys


def _trash_keys(data: ParsedData) -> set[str]:
    tokens = _parsed_strings(data.parsed_title, "source", "other", "audio_profile")
    keys: set[str] = set()
    if any("cam" in token for token in tokens):
        keys.add("cam")
    if any("clean audio" in token for token in tokens):
        keys.add("clean_audio")
    if any("pdtv" in token for token in tokens):
        keys.add("pdtv")
    if any(token == "r5" for token in tokens):
        keys.add("r5")
    if any("screener" in token for token in tokens):
        keys.add("screener")
    if any(token == "telecine" or token == "tc" for token in tokens):
        keys.add("telecine")
    if any(token == "telesync" or token == "ts" for token in tokens):
        keys.add("telesync")
    size = data.parsed_title.get("size")
    if isinstance(size, (int, float)) and size <= 0:
        keys.add("size")
    return keys


def _language_tokens(data: ParsedData) -> set[str]:
    return set(
        _parsed_strings(
            data.parsed_title, "language", "languages", "audio_language", "subtitle_language"
        )
    )


def _matches_patterns(raw_title: str, patterns: Sequence[str]) -> bool:
    for pattern in patterns:
        try:
            if re.search(pattern, raw_title, flags=re.IGNORECASE):
                return True
        except re.error:
            continue
    return False


def _is_fetch_speed_hard_failure(failure: str) -> bool:
    """Return whether one failed check must still block candidates in speed mode."""

    return failure.startswith("trash:") or failure in {
        "adult",
        "excluded_pattern",
        "missing_required_language",
        "excluded_language",
    }


def check_fetch(
    data: ParsedData,
    profile: RankingProfile,
) -> tuple[bool, tuple[str, ...]]:
    """Run the RTN fetch-check pipeline and return `(fetch, failed_checks)`.

    When `enable_fetch_speed_mode` is enabled, non-safety fetch failures remain observable in
    `failed_checks` but do not block candidate fetch eligibility.
    """

    failed_checks: set[str] = set()

    trash_keys = _trash_keys(data)
    if trash_keys:
        failed_checks.update(f"trash:{key}" for key in trash_keys)

    if profile.options.remove_adult_content and data.parsed_title.get("adult") is True:
        failed_checks.add("adult")

    if _matches_patterns(data.raw_title, profile.exclude):
        failed_checks.add("excluded_pattern")

    languages = _language_tokens(data)
    english_present = any(token in {"eng", "english"} for token in languages)
    if profile.languages.required and not (
        set(profile.languages.required).intersection(languages)
        or (profile.options.allow_english_in_languages and english_present)
    ):
        failed_checks.add("missing_required_language")
    if profile.languages.exclude and set(profile.languages.exclude).intersection(languages):
        failed_checks.add("excluded_language")
    if profile.options.remove_unknown_languages and not languages:
        failed_checks.add("unknown_language")

    resolution_key = _resolution_key(data.resolution)
    resolution_enabled = {
        "2160p": profile.resolutions.r2160p,
        "1080p": profile.resolutions.r1080p,
        "720p": profile.resolutions.r720p,
        "480p": profile.resolutions.r480p,
        "360p": profile.resolutions.r360p,
        "unknown": profile.resolutions.unknown,
    }.get(resolution_key, profile.resolutions.unknown)
    if not resolution_enabled:
        failed_checks.add(f"resolution:{resolution_key}")

    category_matches = {
        "quality": _quality_keys(data),
        "rips": _rip_keys(data),
        "hdr": _hdr_keys(data),
        "audio": _audio_keys(data),
        "extras": _extra_keys(data),
        "trash": trash_keys,
    }
    for category_name, keys in category_matches.items():
        category = getattr(profile.custom_ranks, category_name)
        for key in keys:
            if not category.get(key).fetch:
                failed_checks.add(f"fetch_disabled:{category_name}:{key}")

    if _matches_patterns(data.raw_title, profile.require):
        return True, ()
    if profile.options.enable_fetch_speed_mode:
        hard_failures = {check for check in failed_checks if _is_fetch_speed_hard_failure(check)}
        return (len(hard_failures) == 0), tuple(sorted(failed_checks))
    return (len(failed_checks) == 0), tuple(sorted(failed_checks))


def rank(
    data: ParsedData,
    profile: RankingProfile,
    ranking_model: RankingModel | None = None,
) -> tuple[int, dict[str, int]]:
    """Return the RTN additive score and score-part breakdown for one parsed torrent."""

    model = ranking_model or default_ranking_model()
    score_parts: Counter[str] = Counter()
    resolution_key = _resolution_key(data.resolution)
    resolution_score = {
        "2160p": 500,
        "1080p": 300,
        "720p": 100,
        "480p": 0,
        "360p": 0,
        "unknown": 0,
    }.get(resolution_key, 0)
    score_parts["resolution"] += resolution_score

    for category_name, keys, default_map in (
        ("quality", _quality_keys(data), model.quality),
        ("rips", _rip_keys(data), model.rips),
        ("hdr", _hdr_keys(data), model.hdr),
        ("audio", _audio_keys(data), model.audio),
        ("extras", _extra_keys(data), model.extras),
        ("trash", _trash_keys(data), model.trash),
    ):
        category = getattr(profile.custom_ranks, category_name)
        for key in sorted(keys):
            entry = category.get(key)
            score_parts[category_name] += resolve_rank(
                default_map.get(key, 0), entry.use_custom_rank, entry.rank
            )

    if _matches_patterns(data.raw_title, profile.preferred):
        score_parts["preferred"] += 10000
    if set(profile.languages.preferred).intersection(_language_tokens(data)):
        score_parts["preferred_languages"] += 10000

    return sum(score_parts.values()), dict(score_parts)


def sort_torrents(
    results: Sequence[RankedTorrent], *, bucket_limit: int | None = None
) -> list[RankedTorrent]:
    """Sort ranked torrents by score and optionally cap results per resolution bucket."""

    ordered = sorted(
        results,
        key=lambda result: (
            -result.rank,
            -_RESOLUTION_RANKS.get(_resolution_key(result.data.resolution), 0),
            result.data.raw_title,
        ),
    )
    if bucket_limit is None or bucket_limit <= 0:
        return ordered

    assert bucket_limit is not None  # narrowed by the guard above
    limit: int = bucket_limit
    limited: list[RankedTorrent] = []
    per_bucket: Counter[str] = Counter()
    for result in ordered:
        bucket = _resolution_key(result.data.resolution)
        if per_bucket[bucket] >= limit:
            continue
        per_bucket[bucket] += 1
        limited.append(result)
    return limited
