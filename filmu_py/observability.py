"""Observability wiring (Prometheus, OpenTelemetry, Sentry)."""

from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .config import Settings

logger = logging.getLogger(__name__)


def setup_observability(app: FastAPI, settings: Settings) -> None:
    """Attach observability integrations based on settings."""

    if settings.prometheus_enabled:
        _register_metrics_endpoint(app)

    if settings.sentry_dsn:
        try:
            import sentry_sdk
            from sentry_sdk.integrations.fastapi import FastApiIntegration

            sentry_sdk.init(
                dsn=settings.sentry_dsn.get_secret_value(),
                traces_sample_rate=0.1,
                integrations=[FastApiIntegration()],
                environment=settings.env,
            )
        except Exception:
            # Keep boot resilient when optional observability dependencies fail.
            logger.warning("Sentry initialization failed", exc_info=True)

    if settings.otel_enabled and settings.otel_exporter_otlp_endpoint:
        try:
            from opentelemetry import trace
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                OTLPSpanExporter,
            )
            from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
            from opentelemetry.sdk.resources import Resource
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor

            provider = TracerProvider(
                resource=Resource.create({"service.name": settings.service_name})
            )
            provider.add_span_processor(
                BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_endpoint))
            )
            trace.set_tracer_provider(provider)
            FastAPIInstrumentor.instrument_app(app, tracer_provider=provider)
        except Exception:
            logger.warning("OpenTelemetry initialization failed", exc_info=True)


def _register_metrics_endpoint(app: FastAPI) -> None:
    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> PlainTextResponse:
        payload = generate_latest()
        return PlainTextResponse(
            content=payload.decode("utf-8"),
            media_type=CONTENT_TYPE_LATEST,
        )
