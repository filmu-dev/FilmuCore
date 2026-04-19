from __future__ import annotations

from collections.abc import Generator

import pytest

from filmu_py.config import reset_runtime_settings
from tests.db_seed import DbModelFactory


@pytest.fixture(autouse=True)
def _reset_runtime_settings_state(monkeypatch: pytest.MonkeyPatch) -> Generator[None]:
    monkeypatch.setenv(
        "FILMU_PY_OIDC",
        '{"enabled": false, "allow_api_key_fallback": true}',
    )
    monkeypatch.setenv("FILMU_PY_PLUGIN_STRICT_SIGNATURES", "false")
    monkeypatch.setenv(
        "FILMU_PY_PLUGIN_RUNTIME",
        '{"enforcement_mode": "report_only", "require_strict_signatures": false}',
    )
    reset_runtime_settings()
    yield
    reset_runtime_settings()


@pytest.fixture
def db_model_factory() -> DbModelFactory:
    return DbModelFactory()
