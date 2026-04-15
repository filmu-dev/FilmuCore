"""Built-in stream-control plugin layered on the host serving/status gateway."""

from __future__ import annotations

from collections.abc import Awaitable
from typing import cast

from filmu_py.plugins.context import PluginContext
from filmu_py.plugins.interfaces import StreamControlInput, StreamControlPlugin, StreamControlResult

STREAM_CONTROL_PLUGIN_NAME = "stream-control"


class HostStreamControlPlugin(StreamControlPlugin):
    """Built-in stream-control plugin for safe operator-driven playback controls."""

    plugin_name: str = STREAM_CONTROL_PLUGIN_NAME

    def __init__(self) -> None:
        self.ctx: PluginContext | None = None
        self.enabled = True

    async def initialize(self, ctx: PluginContext) -> None:
        self.ctx = ctx
        configured_enabled = ctx.settings.get("enabled")
        self.enabled = bool(configured_enabled) if configured_enabled is not None else True

    async def control(self, request: StreamControlInput) -> StreamControlResult:
        if self.ctx is None:
            raise RuntimeError("HostStreamControlPlugin must be initialized before control")
        if not self.enabled:
            return StreamControlResult(
                action=request.action,
                item_identifier=request.item_identifier,
                accepted=False,
                outcome="disabled",
                detail="stream-control plugin is disabled",
            )

        datasource = self.ctx.datasource
        stream_control_gateway = (
            getattr(datasource, "stream_control", None) if datasource is not None else None
        )
        if stream_control_gateway is None or not hasattr(stream_control_gateway, "control"):
            return StreamControlResult(
                action=request.action,
                item_identifier=request.item_identifier,
                accepted=False,
                outcome="controller_unavailable",
                detail="host stream-control gateway is unavailable",
                controller_attached=False,
            )

        typed_gateway = stream_control_gateway.control
        result = typed_gateway(request)
        if hasattr(result, "__await__"):
            return await cast(Awaitable[StreamControlResult], result)
        raise TypeError("stream_control gateway control(request) must return an awaitable")
