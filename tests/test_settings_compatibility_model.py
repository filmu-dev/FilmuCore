"""Compatibility-schema modeling tests for the dual-surface settings runtime."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from filmu_py.config import (
    DownloadersSettings,
    LibraryFilterRules,
    ScrapingSettings,
    Settings,
)
from filmu_py.rtn.schemas import RankingProfile

_DEBRID_ENV_KEYS = (
    "FILMU_PY_API_KEY",
    "FILMU_PY_REALDEBRID_API_TOKEN",
    "FILMU_PY_ALLDEBRID_API_TOKEN",
    "FILMU_PY_DEBRIDLINK_API_TOKEN",
    "REAL_DEBRID_API_KEY",
    "REALDEBRID_API_KEY",
    "ALL_DEBRID_API_KEY",
    "ALLDEBRID_API_KEY",
    "DEBRID_LINK_API_KEY",
    "DEBRIDLINK_API_KEY",
)


def _clear_debrid_env(monkeypatch: Any) -> None:
    for key in _DEBRID_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def _compatibility_fixture() -> dict[str, Any]:
    fixture_path = (
        Path(__file__).resolve().parents[1] / "docs" / "original riven settings.json schema.MD"
    )
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def _expected_compatibility_payload() -> dict[str, Any]:
    payload = _compatibility_fixture()
    payload.setdefault("api_key_id", "primary")
    payload.setdefault("tmdb_api_key", "")
    return payload


def _collect_key_paths(value: Any, prefix: str = "") -> set[str]:
    if isinstance(value, dict):
        paths: set[str] = set()
        for key, nested in value.items():
            current = f"{prefix}.{key}" if prefix else key
            paths.add(current)
            paths.update(_collect_key_paths(nested, current))
        return paths
    if isinstance(value, list):
        paths = {prefix}
        for index, nested in enumerate(value):
            current = f"{prefix}[{index}]"
            paths.add(current)
            paths.update(_collect_key_paths(nested, current))
        return paths
    return {prefix} if prefix else set()


def test_settings_from_compatibility_dict_round_trips_full_payload() -> None:
    """The full compatibility payload should round-trip without shape or value drift."""

    payload = _expected_compatibility_payload()

    settings = Settings.from_compatibility_dict(payload)

    assert settings.to_compatibility_dict() == payload


def test_to_compatibility_dict_matches_original_shape_key_for_key() -> None:
    """Compatibility serialization should preserve the exact original key paths."""

    payload = _expected_compatibility_payload()
    settings = Settings.from_compatibility_dict(payload)

    assert _collect_key_paths(settings.to_compatibility_dict()) == _collect_key_paths(payload)


def test_scraper_configs_deserialize_provider_specific_fields() -> None:
    """Per-scraper optional fields should hydrate into the typed internal model."""

    settings = Settings.from_compatibility_dict(_compatibility_fixture())

    assert isinstance(settings.scraping, ScrapingSettings)
    assert settings.scraping.torrentio.filter == "sort=qualitysize%7Cqualityfilter=480p,scr,cam"
    assert settings.scraping.jackett.infohash_fetch_timeout == 30
    assert settings.scraping.prowlarr.limiter_seconds == 60
    assert settings.scraping.orionoid.parameters.videoquality == "sd_hd8k"
    assert settings.scraping.aiostreams.proxy_url == ""
    assert settings.scraping.aiostreams.uuid == ""
    assert settings.scraping.aiostreams.password == ""


def test_downloaders_and_ranking_models_still_hydrate_after_refactor() -> None:
    """Existing downloader and RTN ranking primitives should remain available and typed."""

    payload = _compatibility_fixture()

    settings = Settings.from_compatibility_dict(payload)

    assert isinstance(settings.downloaders, DownloadersSettings)
    assert (
        settings.downloaders.real_debrid.api_key == payload["downloaders"]["real_debrid"]["api_key"]
    )
    assert settings.realdebrid_api_token is not None
    assert (
        settings.realdebrid_api_token.get_secret_value()
        == payload["downloaders"]["real_debrid"]["api_key"]
    )
    assert isinstance(settings.ranking, RankingProfile)
    assert settings.ranking.custom_ranks.hdr.root["dolby_vision"].fetch is False


def test_library_filter_rules_allow_optional_fields_without_null_shape_noise() -> None:
    """Optional library-profile filter rules should accept partial payloads cleanly."""

    empty_rules = LibraryFilterRules.model_validate({})
    settings = Settings.from_compatibility_dict(_compatibility_fixture())
    anime_rules = settings.filesystem.library_profiles["anime"].filter_rules

    assert empty_rules.model_dump(exclude_none=True) == {}
    assert anime_rules.is_anime is True
    assert anime_rules.content_types is None
    assert anime_rules.genres is None
    assert anime_rules.max_rating is None
    assert anime_rules.content_ratings is None


def test_settings_reads_legacy_debrid_env_names_from_dotenv(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    """Legacy frontend-style debrid env keys should still hydrate runtime downloader settings."""

    _clear_debrid_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                f"FILMU_PY_API_KEY={'a' * 32}",
                "REAL_DEBRID_API_KEY=legacy-rd-token",
                "ALL_DEBRID_API_KEY=legacy-ad-token",
                "DEBRID_LINK_API_KEY=legacy-dl-token",
            ]
        ),
        encoding="utf-8",
    )

    settings = Settings()

    assert settings.realdebrid_api_token is not None
    assert settings.realdebrid_api_token.get_secret_value() == "legacy-rd-token"
    assert settings.downloaders.real_debrid.api_key == "legacy-rd-token"
    assert settings.downloaders.real_debrid.enabled is True

    assert settings.alldebrid_api_token is not None
    assert settings.alldebrid_api_token.get_secret_value() == "legacy-ad-token"
    assert settings.downloaders.all_debrid.api_key == "legacy-ad-token"
    assert settings.downloaders.all_debrid.enabled is True

    assert settings.debridlink_api_token is not None
    assert settings.debridlink_api_token.get_secret_value() == "legacy-dl-token"
    assert settings.downloaders.debrid_link.api_key == "legacy-dl-token"
    assert settings.downloaders.debrid_link.enabled is True


def test_settings_reads_legacy_debrid_env_names_from_process_environment(monkeypatch: Any) -> None:
    """Legacy env names must hydrate when set directly in the process environment."""

    _clear_debrid_env(monkeypatch)
    monkeypatch.setenv("FILMU_PY_API_KEY", "a" * 32)
    monkeypatch.setenv("REAL_DEBRID_API_KEY", "legacy-rd-token")
    monkeypatch.setenv("ALL_DEBRID_API_KEY", "legacy-ad-token")
    monkeypatch.setenv("DEBRID_LINK_API_KEY", "legacy-dl-token")

    settings = Settings()

    assert settings.realdebrid_api_token is not None
    assert settings.realdebrid_api_token.get_secret_value() == "legacy-rd-token"
    assert settings.downloaders.real_debrid.api_key == "legacy-rd-token"

    assert settings.alldebrid_api_token is not None
    assert settings.alldebrid_api_token.get_secret_value() == "legacy-ad-token"
    assert settings.downloaders.all_debrid.api_key == "legacy-ad-token"

    assert settings.debridlink_api_token is not None
    assert settings.debridlink_api_token.get_secret_value() == "legacy-dl-token"
    assert settings.downloaders.debrid_link.api_key == "legacy-dl-token"


def test_settings_prefers_native_debrid_env_names_when_both_layers_are_defined(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    """Native backend env keys should win when both legacy and native debrid aliases exist."""

    _clear_debrid_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                f"FILMU_PY_API_KEY={'a' * 32}",
                "FILMU_PY_REALDEBRID_API_TOKEN=native-rd-token",
                "FILMU_PY_ALLDEBRID_API_TOKEN=native-ad-token",
                "FILMU_PY_DEBRIDLINK_API_TOKEN=native-dl-token",
                "REAL_DEBRID_API_KEY=legacy-rd-token",
                "ALL_DEBRID_API_KEY=legacy-ad-token",
                "DEBRID_LINK_API_KEY=legacy-dl-token",
            ]
        ),
        encoding="utf-8",
    )

    settings = Settings()

    assert settings.realdebrid_api_token is not None
    assert settings.realdebrid_api_token.get_secret_value() == "native-rd-token"
    assert settings.downloaders.real_debrid.api_key == "native-rd-token"

    assert settings.alldebrid_api_token is not None
    assert settings.alldebrid_api_token.get_secret_value() == "native-ad-token"
    assert settings.downloaders.all_debrid.api_key == "native-ad-token"

    assert settings.debridlink_api_token is not None
    assert settings.debridlink_api_token.get_secret_value() == "native-dl-token"
    assert settings.downloaders.debrid_link.api_key == "native-dl-token"


def test_settings_reads_no_underscore_legacy_debrid_env_names(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    """Compatibility should also tolerate older no-underscore debrid env variants."""

    _clear_debrid_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                f"FILMU_PY_API_KEY={'a' * 32}",
                "REALDEBRID_API_KEY=legacy-rd-token-no-underscore",
                "ALLDEBRID_API_KEY=legacy-ad-token-no-underscore",
                "DEBRIDLINK_API_KEY=legacy-dl-token-no-underscore",
            ]
        ),
        encoding="utf-8",
    )

    settings = Settings()

    assert settings.realdebrid_api_token is not None
    assert settings.realdebrid_api_token.get_secret_value() == "legacy-rd-token-no-underscore"
    assert settings.downloaders.real_debrid.api_key == "legacy-rd-token-no-underscore"

    assert settings.alldebrid_api_token is not None
    assert settings.alldebrid_api_token.get_secret_value() == "legacy-ad-token-no-underscore"
    assert settings.downloaders.all_debrid.api_key == "legacy-ad-token-no-underscore"

    assert settings.debridlink_api_token is not None
    assert settings.debridlink_api_token.get_secret_value() == "legacy-dl-token-no-underscore"
    assert settings.downloaders.debrid_link.api_key == "legacy-dl-token-no-underscore"
