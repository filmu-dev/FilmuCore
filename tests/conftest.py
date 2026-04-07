from __future__ import annotations

from collections.abc import Generator

import pytest

from filmu_py.config import reset_runtime_settings


@pytest.fixture(autouse=True)
def _reset_runtime_settings_state() -> Generator[None]:
    reset_runtime_settings()
    yield
    reset_runtime_settings()
