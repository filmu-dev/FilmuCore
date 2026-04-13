"""Stream-candidate parsing and ranking helpers extracted from media service."""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass, field

import Levenshtein

from filmu_py.db.models import MediaItemORM, StreamORM
from filmu_py.rtn import parse_torrent_name

_TITLE_ALIASES_ATTRIBUTE_KEY = "aliases"
_SIMILARITY_THRESHOLD_DEFAULT = 0.85
_RESOLUTION_RANKS: dict[str, int] = {
    "2160p": 7,
    "1080p": 6,
    "720p": 5,
    "480p": 4,
    "360p": 3,
    "unknown": 1,
}


@dataclass(frozen=True)
class ParsedStreamCandidateRecord:
    """Parsed stream-candidate payload persisted before later ranking stages."""

    raw_title: str
    infohash: str
    parsed_title: dict[str, object]
    resolution: str | None


@dataclass(frozen=True)
class ParsedStreamCandidateValidation:
    """Content-level validation result for one parsed stream candidate."""

    ok: bool
    reason: str | None = None


@dataclass(frozen=True)
class RankedStreamCandidateRecord:
    """Persisted candidate ranking outcome used by selection and diagnostics."""

    item_id: str
    stream_id: str
    rank_score: int
    lev_ratio: float
    fetch: bool
    passed: bool
    rejection_reason: str | None = None
    stream: StreamORM | None = field(default=None, compare=False, repr=False)


@dataclass(frozen=True)
class SelectedStreamCandidateRecord:
    """Detached winner snapshot safe to use after session boundaries."""

    id: str
    infohash: str
    raw_title: str
    resolution: str | None
    provider: str | None = None


@dataclass(frozen=True)
class RankingRule:
    """One additive scoring/fetch rule for a normalized ranking axis value."""

    rank: int
    fetch: bool = True


def _default_quality_source_scores() -> dict[str, RankingRule]:
    return {
        "bluray_remux": RankingRule(rank=1200),
        "bluray": RankingRule(rank=1100),
        "bdrip": RankingRule(rank=1000),
        "webdl": RankingRule(rank=900),
        "web-dl": RankingRule(rank=900),
        "webrip": RankingRule(rank=750),
        "dvdrip": RankingRule(rank=500),
        "hdrip": RankingRule(rank=400),
        "hdtv": RankingRule(rank=200),
        "cam": RankingRule(rank=-10000, fetch=False),
        "telesync": RankingRule(rank=-10000, fetch=False),
        "telecine": RankingRule(rank=-10000, fetch=False),
        "screener": RankingRule(rank=-10000, fetch=False),
        "ts": RankingRule(rank=-10000, fetch=False),
        "hdcam": RankingRule(rank=-10000, fetch=False),
    }


def _default_resolution_scores() -> dict[str, RankingRule]:
    return {
        "2160p": RankingRule(rank=500),
        "1080p": RankingRule(rank=300),
        "720p": RankingRule(rank=100),
        "480p": RankingRule(rank=0),
        "unknown": RankingRule(rank=0),
    }


def _default_codec_scores() -> dict[str, RankingRule]:
    return {
        "hevc": RankingRule(rank=150),
        "h265": RankingRule(rank=150),
        "avc": RankingRule(rank=50),
        "x264": RankingRule(rank=50),
        "xvid": RankingRule(rank=-500, fetch=False),
    }


def _default_hdr_scores() -> dict[str, RankingRule]:
    return {
        "dolby_vision": RankingRule(rank=120),
        "hdr10+": RankingRule(rank=100),
        "hdr10": RankingRule(rank=80),
    }


def _default_audio_scores() -> dict[str, RankingRule]:
    return {
        "truehd": RankingRule(rank=200),
        "dts-hd": RankingRule(rank=180),
        "atmos": RankingRule(rank=150),
        "dts": RankingRule(rank=80),
        "aac": RankingRule(rank=20),
        "mp3": RankingRule(rank=-50),
    }


@dataclass(frozen=True)
class RankingModel:
    """First-pass RTN-style additive scoring model with overridable defaults."""

    quality_source_scores: dict[str, RankingRule] = field(
        default_factory=_default_quality_source_scores
    )
    resolution_scores: dict[str, RankingRule] = field(default_factory=_default_resolution_scores)
    codec_scores: dict[str, RankingRule] = field(default_factory=_default_codec_scores)
    hdr_scores: dict[str, RankingRule] = field(default_factory=_default_hdr_scores)
    audio_scores: dict[str, RankingRule] = field(default_factory=_default_audio_scores)
    remove_ranks_under: int = -10000
    require: list[str] = field(default_factory=list)


def _extract_string(attributes: dict[str, object], key: str) -> str | None:
    value = attributes.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def extract_int(attributes: dict[str, object], key: str) -> int | None:
    value = attributes.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
    return None


def infer_request_media_type(*, external_ref: str, attributes: dict[str, object]) -> str:
    """Return the best current media-type label for one request-intent record."""

    item_type = _extract_string(attributes, "item_type")
    if item_type is not None and item_type in {"movie", "show", "season", "episode"}:
        return item_type
    if external_ref.startswith("tmdb:"):
        return "movie"
    if external_ref.startswith("tvdb:"):
        return "show"
    return "unknown"


def extract_int_value(attributes: dict[str, object], key: str, *aliases: str) -> int | None:
    """Return one integer-like metadata field when present."""

    for candidate_key in (key, *aliases):
        value = attributes.get(candidate_key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def candidate_matches_partial_scope(
    parsed_seasons: list[int] | None,
    requested_seasons: list[int],
    *,
    allow_unknown: bool = False,
) -> bool:
    """Return whether one parsed candidate should be kept for a partial-season request."""

    if not parsed_seasons:
        return allow_unknown
    requested = set(requested_seasons)
    return any(season in requested for season in parsed_seasons)


def candidate_parsed_seasons(parsed_title: dict[str, object]) -> list[int] | None:
    """Return normalized parsed season numbers from one torrent-title payload when present."""

    raw_value = parsed_title.get("season")
    if raw_value is None:
        return None
    if isinstance(raw_value, int):
        return [raw_value]
    if isinstance(raw_value, list):
        seasons = [value for value in raw_value if isinstance(value, int)]
        return seasons or None
    return None


def _fallback_infohash_for_raw_title(raw_title: str) -> str:
    return hashlib.sha1(raw_title.encode("utf-8")).hexdigest()


def _normalize_title_for_similarity(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold().replace("_", " ")
    normalized = re.sub(r"[^\w\s]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _dedupe_title_aliases(values: object, *, canonical_title: str | None = None) -> list[str]:
    aliases: list[str] = []
    seen: set[str] = set()
    if canonical_title is not None:
        normalized_canonical = _normalize_title_for_similarity(canonical_title)
        if normalized_canonical:
            seen.add(normalized_canonical)

    if not isinstance(values, list):
        return aliases

    for value in values:
        if not isinstance(value, str):
            continue
        alias = value.strip()
        if not alias:
            continue
        normalized_alias = _normalize_title_for_similarity(alias)
        if not normalized_alias or normalized_alias in seen:
            continue
        seen.add(normalized_alias)
        aliases.append(alias)
    return aliases


def extract_title_aliases(
    attributes: dict[str, object], *, canonical_title: str | None = None
) -> list[str]:
    return _dedupe_title_aliases(
        attributes.get(_TITLE_ALIASES_ATTRIBUTE_KEY),
        canonical_title=canonical_title,
    )


def _candidate_title_for_similarity(stream: StreamORM) -> str:
    parsed_title = stream.parsed_title.get("title")
    if isinstance(parsed_title, str) and parsed_title.strip():
        return parsed_title
    return stream.raw_title


def _levenshtein_ratio(left: str, right: str) -> float:
    normalized_left = _normalize_title_for_similarity(left)
    normalized_right = _normalize_title_for_similarity(right)
    total_length = len(normalized_left) + len(normalized_right)
    if total_length == 0:
        return 1.0
    distance = Levenshtein.distance(normalized_left, normalized_right)
    return (total_length - distance) / total_length


def _max_title_similarity(left: str, right: str, aliases: list[str] | None = None) -> float:
    ratios = [_levenshtein_ratio(left, right)]
    ratios.extend(_levenshtein_ratio(alias, right) for alias in aliases or [])
    return max(ratios, default=0.0)


def _normalize_resolution_for_ranking(resolution: str | None) -> str:
    if resolution is None:
        return "unknown"
    normalized = resolution.strip().casefold()
    if normalized == "4k":
        return "2160p"
    if normalized in _RESOLUTION_RANKS:
        return normalized
    return "unknown"


def _flatten_parsed_strings(value: object) -> list[str]:
    if isinstance(value, str):
        return [value.casefold()]
    if isinstance(value, list):
        flattened: list[str] = []
        for item in value:
            flattened.extend(_flatten_parsed_strings(item))
        return flattened
    if isinstance(value, dict):
        flattened = []
        for item in value.values():
            flattened.extend(_flatten_parsed_strings(item))
        return flattened
    return []


def _parsed_strings(parsed_title: dict[str, object], *keys: str) -> list[str]:
    strings: list[str] = []
    for key in keys:
        strings.extend(_flatten_parsed_strings(parsed_title.get(key)))
    return strings


def _quality_source_key(parsed_title: dict[str, object]) -> str | None:
    tokens = _parsed_strings(parsed_title, "source", "other")
    if any("hdcam" in token for token in tokens):
        return "hdcam"
    if any("cam" in token or "camera" in token for token in tokens):
        return "cam"
    if any("telesync" in token or token == "ts" for token in tokens):
        return "telesync"
    if any("telecine" in token or token == "tc" for token in tokens):
        return "telecine"
    if any("screener" in token for token in tokens):
        return "screener"
    if any("hdtv" in token for token in tokens):
        return "hdtv"
    if any(("blu-ray" in token or "bluray" in token) for token in tokens) and any(
        "remux" in token for token in tokens
    ):
        return "bluray_remux"
    if any(("blu-ray" in token or "bluray" in token) for token in tokens) and any(
        token == "rip" or token.endswith("rip") for token in tokens
    ):
        return "bdrip"
    if any("blu-ray" in token or "bluray" in token for token in tokens):
        return "bluray"
    if any("dvd" in token for token in tokens) and any(
        token == "rip" or token.endswith("rip") for token in tokens
    ):
        return "dvdrip"
    if any("hdrip" in token for token in tokens):
        return "hdrip"
    if any("webrip" in token for token in tokens):
        return "webrip"
    if any("web" in token for token in tokens):
        if any(token == "rip" or token.endswith("rip") for token in tokens):
            return "webrip"
        if any("dl" in token for token in tokens):
            return "web-dl"
        return "webdl"
    return None


def _codec_key(parsed_title: dict[str, object]) -> str | None:
    tokens = _parsed_strings(parsed_title, "video_codec", "other")
    if any("xvid" in token for token in tokens):
        return "xvid"
    if any(token in {"h.265", "h265", "hevc"} or "265" in token for token in tokens):
        return "h265"
    if any(token in {"h.264", "h264", "avc"} or "264" in token for token in tokens):
        return "x264"
    if any("av1" in token for token in tokens):
        return "av1"
    return None


def _hdr_key(parsed_title: dict[str, object]) -> str | None:
    tokens = _parsed_strings(parsed_title, "hdr", "other")
    if any("dolby vision" in token or token == "dv" for token in tokens):
        return "dolby_vision"
    if any("hdr10+" in token or "hdr10plus" in token for token in tokens):
        return "hdr10+"
    if any(token == "hdr10" for token in tokens):
        return "hdr10"
    if any(token == "hdr" or token.startswith("hdr ") for token in tokens):
        return "hdr10"
    return None


def _audio_keys(parsed_title: dict[str, object]) -> set[str]:
    tokens = _parsed_strings(parsed_title, "audio_codec", "audio_profile", "other")
    keys: set[str] = set()
    if any("truehd" in token or "true hd" in token for token in tokens):
        keys.add("truehd")
    if any(
        "dts-hd" in token or "dts hd" in token or "dts ma" in token or "master audio" in token
        for token in tokens
    ):
        keys.add("dts-hd")
    if any("atmos" in token for token in tokens):
        keys.add("atmos")
    if any(token == "dts" for token in tokens):
        keys.add("dts")
    if any(token == "aac" for token in tokens):
        keys.add("aac")
    if any(token == "mp3" for token in tokens):
        keys.add("mp3")
    return keys


def _matches_require_override(raw_title: str, require_patterns: list[str]) -> bool:
    for pattern in require_patterns:
        try:
            if re.search(pattern, raw_title, flags=re.IGNORECASE):
                return True
        except re.error:
            continue
    return False


def rank_persisted_streams_for_item(
    item: MediaItemORM,
    streams: list[StreamORM],
    *,
    similarity_threshold: float = _SIMILARITY_THRESHOLD_DEFAULT,
    ranking_model: RankingModel | None = None,
) -> list[RankedStreamCandidateRecord]:
    """Rank persisted parsed stream candidates without reparsing raw titles."""

    canonical_title = item.title or item.external_ref
    title_aliases = extract_title_aliases(
        dict(item.attributes or {}),
        canonical_title=canonical_title,
    )
    model = ranking_model or RankingModel()
    ranked: list[RankedStreamCandidateRecord] = []
    for stream in streams:
        similarity_title = _candidate_title_for_similarity(stream)
        lev_ratio = _max_title_similarity(canonical_title, similarity_title, title_aliases)
        normalized_resolution = _normalize_resolution_for_ranking(stream.resolution)
        stream.lev_ratio = lev_ratio
        stream.resolution = normalized_resolution

        if lev_ratio < similarity_threshold:
            stream.rank = 0
            ranked.append(
                RankedStreamCandidateRecord(
                    item_id=item.id,
                    stream_id=stream.id,
                    rank_score=0,
                    lev_ratio=lev_ratio,
                    fetch=False,
                    passed=False,
                    rejection_reason="similarity_below_threshold",
                    stream=stream,
                )
            )
            continue

        resolution_rule = model.resolution_scores.get(normalized_resolution, RankingRule(rank=0))
        rank_score = int(resolution_rule.rank)
        fetch_allowed = True
        rejection_reason: str | None = None

        quality_source_key = _quality_source_key(stream.parsed_title)
        if quality_source_key is not None:
            rule = model.quality_source_scores.get(quality_source_key)
            if rule is not None:
                rank_score += rule.rank
                if not rule.fetch and rejection_reason is None:
                    fetch_allowed = False
                    rejection_reason = f"quality_source_fetch_disabled:{quality_source_key}"

        codec_key = _codec_key(stream.parsed_title)
        if codec_key is not None:
            rule = model.codec_scores.get(codec_key)
            if rule is not None:
                rank_score += rule.rank
                if not rule.fetch and rejection_reason is None:
                    fetch_allowed = False
                    rejection_reason = f"codec_fetch_disabled:{codec_key}"

        hdr_key = _hdr_key(stream.parsed_title)
        if hdr_key is not None:
            rule = model.hdr_scores.get(hdr_key)
            if rule is not None:
                rank_score += rule.rank
                if not rule.fetch and rejection_reason is None:
                    fetch_allowed = False
                    rejection_reason = f"hdr_fetch_disabled:{hdr_key}"

        for audio_key in sorted(_audio_keys(stream.parsed_title)):
            rule = model.audio_scores.get(audio_key)
            if rule is not None:
                rank_score += rule.rank
                if not rule.fetch and rejection_reason is None:
                    fetch_allowed = False
                    rejection_reason = f"audio_fetch_disabled:{audio_key}"

        if rank_score < model.remove_ranks_under and rejection_reason is None:
            fetch_allowed = False
            rejection_reason = "rank_below_threshold"

        if _matches_require_override(stream.raw_title, model.require):
            fetch_allowed = True
            rejection_reason = None

        stream.rank = rank_score
        ranked.append(
            RankedStreamCandidateRecord(
                item_id=item.id,
                stream_id=stream.id,
                rank_score=rank_score,
                lev_ratio=lev_ratio,
                fetch=fetch_allowed,
                passed=fetch_allowed,
                rejection_reason=rejection_reason,
                stream=stream,
            )
        )

    return sorted(
        ranked,
        key=lambda record: (
            not record.passed,
            -record.rank_score,
            -record.lev_ratio,
            _part_tiebreaker(record.stream.parsed_title) if record.stream is not None else 0,
            record.stream_id,
        ),
    )


def select_stream_candidate(
    ranked_results: list[RankedStreamCandidateRecord],
) -> SelectedStreamCandidateRecord | None:
    """Select the best passing candidate by score, then similarity, then stable id order."""

    passing_candidates = [record for record in ranked_results if record.passed]
    if not passing_candidates:
        return None

    selected_record = min(
        passing_candidates,
        key=lambda record: (
            -record.rank_score,
            -record.lev_ratio,
            _part_tiebreaker(record.stream.parsed_title) if record.stream is not None else 0,
            record.stream_id,
        ),
    )
    stream = selected_record.stream
    if stream is None:
        return SelectedStreamCandidateRecord(
            id=selected_record.stream_id,
            infohash="",
            raw_title="",
            resolution=None,
        )
    return SelectedStreamCandidateRecord(
        id=stream.id,
        infohash=stream.infohash,
        raw_title=stream.raw_title,
        resolution=stream.resolution,
        provider=None,
    )


def parse_stream_candidate_title(
    raw_title: str,
    *,
    infohash: str | None = None,
) -> ParsedStreamCandidateRecord:
    """Parse one raw stream candidate title into a persisted pre-ranking record."""

    parsed = parse_torrent_name(raw_title)
    return ParsedStreamCandidateRecord(
        raw_title=parsed.raw_title,
        infohash=infohash or _fallback_infohash_for_raw_title(parsed.raw_title),
        parsed_title=parsed.parsed_title,
        resolution=parsed.resolution,
    )


def validate_parsed_stream_candidate(
    item: MediaItemORM,
    candidate: ParsedStreamCandidateRecord,
) -> ParsedStreamCandidateValidation:
    """Perform the first content-level validation pass on one parsed candidate."""

    attributes = dict(item.attributes or {})
    expected_type = infer_request_media_type(external_ref=item.external_ref, attributes=attributes)
    parsed_type = candidate.parsed_title.get("type")
    parsed_kind = parsed_type if isinstance(parsed_type, str) else None

    if expected_type == "movie" and parsed_kind == "episode":
        return ParsedStreamCandidateValidation(
            ok=False, reason="movie_request_got_episode_candidate"
        )
    if expected_type in {"show", "season", "episode"} and parsed_kind == "movie":
        has_season_marker = extract_int(candidate.parsed_title, "season") is not None
        has_episode_marker = extract_int(candidate.parsed_title, "episode") is not None
        if has_season_marker or has_episode_marker:
            return ParsedStreamCandidateValidation(ok=False, reason="show_request_got_movie_candidate")

    expected_season = extract_int_value(attributes, "season_number", "season", "parent_season_number")
    parsed_season = extract_int(candidate.parsed_title, "season")
    if (
        expected_type in {"season", "episode"}
        and expected_season is not None
        and parsed_season is not None
        and expected_season != parsed_season
    ):
        return ParsedStreamCandidateValidation(ok=False, reason="season_mismatch")

    expected_episode = extract_int_value(attributes, "episode_number", "episode")
    parsed_episode = extract_int(candidate.parsed_title, "episode")
    if (
        expected_type == "episode"
        and expected_episode is not None
        and parsed_episode is not None
        and expected_episode != parsed_episode
    ):
        return ParsedStreamCandidateValidation(ok=False, reason="episode_mismatch")

    expected_year = extract_int(attributes, "year")
    parsed_year = extract_int(candidate.parsed_title, "year")
    if expected_year is not None and parsed_year is not None and abs(expected_year - parsed_year) > 1:
        return ParsedStreamCandidateValidation(ok=False, reason="year_mismatch")

    return ParsedStreamCandidateValidation(ok=True)


def _coerce_part_value(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
    return None


def _part_tiebreaker(parsed_title: dict[str, object]) -> int:
    part_value = _coerce_part_value(parsed_title.get("part"))
    if part_value is not None:
        return part_value
    release_group = parsed_title.get("release_group")
    if isinstance(release_group, str):
        match = re.search(r"(?:part|pt)[\s._-]*(\d+)", release_group, flags=re.IGNORECASE)
        if match is not None:
            return int(match.group(1))
    return 0
