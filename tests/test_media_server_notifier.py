from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from filmu_py.config import UpdatersSettings
from filmu_py.services.media_server import MediaServerNotifier


class _AsyncClientStub:
    def __init__(self, *, handlers: dict[tuple[str, str], object], calls: list[tuple[str, str]]) -> None:
        self._handlers = handlers
        self._calls = calls

    async def __aenter__(self) -> _AsyncClientStub:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
        _ = (exc_type, exc, tb)
        return False

    async def get(self, url: str, **kwargs: object) -> httpx.Response:
        return self._respond("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: object) -> httpx.Response:
        return self._respond("POST", url, **kwargs)

    def _respond(self, method: str, url: str, **kwargs: object) -> httpx.Response:
        self._calls.append((method, url))
        handler = self._handlers[(method, url)]
        if isinstance(handler, Exception):
            raise handler
        if callable(handler):
            result = handler(method=method, url=url, kwargs=kwargs)
            if isinstance(result, Exception):
                raise result
            return result
        assert isinstance(handler, httpx.Response)
        return handler


def _make_response(method: str, url: str, *, status_code: int = 200, text: str = "", json: Any = None) -> httpx.Response:
    request = httpx.Request(method, url)
    if json is not None:
        return httpx.Response(status_code, json=json, request=request)
    return httpx.Response(status_code, text=text, request=request)


def _settings() -> UpdatersSettings:
    return UpdatersSettings(
        library_path="/mnt/filmuvfs",
        plex={"enabled": False, "token": "", "url": "http://localhost:32400"},
        jellyfin={"enabled": False, "api_key": "", "url": "http://localhost:8096"},
        emby={"enabled": False, "api_key": "", "url": "http://localhost:8096"},
    )


def test_notify_plex_logs_success(monkeypatch: Any, caplog: Any) -> None:
    settings = _settings()
    settings.plex.enabled = True
    settings.plex.token = "plex-token"
    calls: list[tuple[str, str]] = []
    handlers = {
        ("GET", "http://localhost:32400/library/sections"): _make_response(
            "GET",
            "http://localhost:32400/library/sections",
            text='<MediaContainer><Directory key="1" /></MediaContainer>',
        ),
        ("GET", "http://localhost:32400/library/sections/1/refresh"): _make_response(
            "GET",
            "http://localhost:32400/library/sections/1/refresh",
        ),
    }
    monkeypatch.setattr(
        "filmu_py.services.media_server.httpx.AsyncClient",
        lambda **_: _AsyncClientStub(handlers=handlers, calls=calls),
    )
    caplog.set_level(logging.INFO)

    asyncio.run(MediaServerNotifier(settings).notify_plex("item-1"))

    assert calls == [
        ("GET", "http://localhost:32400/library/sections"),
        ("GET", "http://localhost:32400/library/sections/1/refresh"),
    ]
    triggered = [record for record in caplog.records if record.msg == "media_server.scan_triggered"]
    assert triggered
    assert triggered[-1].provider == "plex"


def test_notify_plex_logs_failure_without_raising(monkeypatch: Any, caplog: Any) -> None:
    settings = _settings()
    settings.plex.enabled = True
    settings.plex.token = "plex-token"
    calls: list[tuple[str, str]] = []
    error = httpx.ConnectError(
        "boom",
        request=httpx.Request("GET", "http://localhost:32400/library/sections"),
    )
    handlers = {("GET", "http://localhost:32400/library/sections"): error}
    monkeypatch.setattr(
        "filmu_py.services.media_server.httpx.AsyncClient",
        lambda **_: _AsyncClientStub(handlers=handlers, calls=calls),
    )
    caplog.set_level(logging.WARNING)

    asyncio.run(MediaServerNotifier(settings).notify_plex("item-1"))

    assert calls == [("GET", "http://localhost:32400/library/sections")]
    failed = [record for record in caplog.records if record.msg == "media_server.scan_failed"]
    assert failed
    assert failed[-1].provider == "plex"


def test_notify_plex_disabled_does_nothing(monkeypatch: Any, caplog: Any) -> None:
    settings = _settings()
    monkeypatch.setattr(
        "filmu_py.services.media_server.httpx.AsyncClient",
        lambda **_: (_ for _ in ()).throw(AssertionError("AsyncClient should not be created")),
    )

    asyncio.run(MediaServerNotifier(settings).notify_plex("item-1"))

    assert not caplog.records


def test_notify_jellyfin_logs_success(monkeypatch: Any, caplog: Any) -> None:
    settings = _settings()
    settings.jellyfin.enabled = True
    settings.jellyfin.api_key = "jf-token"
    calls: list[tuple[str, str]] = []
    handlers = {
        ("POST", "http://localhost:8096/Library/Refresh"): _make_response(
            "POST",
            "http://localhost:8096/Library/Refresh",
        )
    }
    monkeypatch.setattr(
        "filmu_py.services.media_server.httpx.AsyncClient",
        lambda **_: _AsyncClientStub(handlers=handlers, calls=calls),
    )
    caplog.set_level(logging.INFO)

    asyncio.run(MediaServerNotifier(settings).notify_jellyfin("item-1"))

    assert calls == [("POST", "http://localhost:8096/Library/Refresh")]
    triggered = [record for record in caplog.records if record.msg == "media_server.scan_triggered"]
    assert triggered
    assert triggered[-1].provider == "jellyfin"


def test_notify_jellyfin_logs_failure_without_raising(monkeypatch: Any, caplog: Any) -> None:
    settings = _settings()
    settings.jellyfin.enabled = True
    settings.jellyfin.api_key = "jf-token"
    calls: list[tuple[str, str]] = []
    error = httpx.ConnectError(
        "boom",
        request=httpx.Request("POST", "http://localhost:8096/Library/Refresh"),
    )
    handlers = {("POST", "http://localhost:8096/Library/Refresh"): error}
    monkeypatch.setattr(
        "filmu_py.services.media_server.httpx.AsyncClient",
        lambda **_: _AsyncClientStub(handlers=handlers, calls=calls),
    )
    caplog.set_level(logging.WARNING)

    asyncio.run(MediaServerNotifier(settings).notify_jellyfin("item-1"))

    assert calls == [("POST", "http://localhost:8096/Library/Refresh")]
    failed = [record for record in caplog.records if record.msg == "media_server.scan_failed"]
    assert failed
    assert failed[-1].provider == "jellyfin"


def test_notify_jellyfin_disabled_does_nothing(monkeypatch: Any, caplog: Any) -> None:
    settings = _settings()
    monkeypatch.setattr(
        "filmu_py.services.media_server.httpx.AsyncClient",
        lambda **_: (_ for _ in ()).throw(AssertionError("AsyncClient should not be created")),
    )

    asyncio.run(MediaServerNotifier(settings).notify_jellyfin("item-1"))

    assert not caplog.records


def test_notify_emby_logs_success(monkeypatch: Any, caplog: Any) -> None:
    settings = _settings()
    settings.emby.enabled = True
    settings.emby.api_key = "emby-token"
    calls: list[tuple[str, str]] = []
    handlers = {
        ("POST", "http://localhost:8096/Library/Refresh"): _make_response(
            "POST",
            "http://localhost:8096/Library/Refresh",
        )
    }
    monkeypatch.setattr(
        "filmu_py.services.media_server.httpx.AsyncClient",
        lambda **_: _AsyncClientStub(handlers=handlers, calls=calls),
    )
    caplog.set_level(logging.INFO)

    asyncio.run(MediaServerNotifier(settings).notify_emby("item-1"))

    assert calls == [("POST", "http://localhost:8096/Library/Refresh")]
    triggered = [record for record in caplog.records if record.msg == "media_server.scan_triggered"]
    assert triggered
    assert triggered[-1].provider == "emby"


def test_notify_emby_logs_failure_without_raising(monkeypatch: Any, caplog: Any) -> None:
    settings = _settings()
    settings.emby.enabled = True
    settings.emby.api_key = "emby-token"
    calls: list[tuple[str, str]] = []
    error = httpx.ConnectError(
        "boom",
        request=httpx.Request("POST", "http://localhost:8096/Library/Refresh"),
    )
    handlers = {("POST", "http://localhost:8096/Library/Refresh"): error}
    monkeypatch.setattr(
        "filmu_py.services.media_server.httpx.AsyncClient",
        lambda **_: _AsyncClientStub(handlers=handlers, calls=calls),
    )
    caplog.set_level(logging.WARNING)

    asyncio.run(MediaServerNotifier(settings).notify_emby("item-1"))

    assert calls == [("POST", "http://localhost:8096/Library/Refresh")]
    failed = [record for record in caplog.records if record.msg == "media_server.scan_failed"]
    assert failed
    assert failed[-1].provider == "emby"


def test_notify_emby_disabled_does_nothing(monkeypatch: Any, caplog: Any) -> None:
    settings = _settings()
    monkeypatch.setattr(
        "filmu_py.services.media_server.httpx.AsyncClient",
        lambda **_: (_ for _ in ()).throw(AssertionError("AsyncClient should not be created")),
    )

    asyncio.run(MediaServerNotifier(settings).notify_emby("item-1"))

    assert not caplog.records


def test_notify_all_runs_all_providers_and_completes_when_one_fails(monkeypatch: Any, caplog: Any) -> None:
    notifier = MediaServerNotifier(_settings())
    started: set[str] = set()
    ready = asyncio.Event()
    release = asyncio.Event()

    async def make_handler(provider: str, *, fail: bool = False) -> None:
        started.add(provider)
        if len(started) == 3:
            ready.set()
        await release.wait()
        if fail:
            raise RuntimeError(f"{provider}-boom")

    monkeypatch.setattr(notifier, "notify_plex", lambda item_id: make_handler("plex"))
    monkeypatch.setattr(notifier, "notify_jellyfin", lambda item_id: make_handler("jellyfin", fail=True))
    monkeypatch.setattr(notifier, "notify_emby", lambda item_id: make_handler("emby"))
    caplog.set_level(logging.WARNING)

    async def run_test() -> None:
        task = asyncio.create_task(notifier.notify_all("item-1"))
        await asyncio.wait_for(ready.wait(), timeout=1.0)
        release.set()
        await task

    asyncio.run(run_test())

    assert started == {"plex", "jellyfin", "emby"}
    failed = [record for record in caplog.records if record.msg == "media_server.scan_failed"]
    assert failed
    assert failed[-1].provider == "jellyfin"
