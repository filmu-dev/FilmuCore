from __future__ import annotations

import asyncio

import httpx

from filmu_py.plugins import TestPluginContext
from filmu_py.plugins.builtin.plex import PlexLibraryRefreshPlugin


def test_plex_event_hook_refreshes_sections_on_completed_event() -> None:
    calls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(200, json={"ok": True})

    plugin = PlexLibraryRefreshPlugin(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={
            "notifications": {
                "plex": {
                    "enabled": True,
                    "url": "https://plex.example",
                    "token": "plex-token",
                    "section_ids": ["5", "8"],
                    "notify_on": ["item.completed"],
                }
            },
            "plex": {
                "enabled": True,
                "url": "https://plex.example",
                "token": "plex-token",
                "section_ids": ["5", "8"],
                "notify_on": ["item.completed"],
            },
        }
    )

    asyncio.run(plugin.initialize(harness.build("plex")))
    asyncio.run(
        plugin.handle(
            "item.state.changed",
            {"title": "Completed", "to_state": "completed"},
        )
    )

    assert calls == [
        "https://plex.example/library/sections/5/refresh?X-Plex-Token=plex-token",
        "https://plex.example/library/sections/8/refresh?X-Plex-Token=plex-token",
    ]
    assert harness.rate_limiter.requests[0][0] == "plex:library_refresh"
