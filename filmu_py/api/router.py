"""Root API router for compatibility endpoints."""

from __future__ import annotations

from time import perf_counter

from fastapi import APIRouter, Depends
from prometheus_client import Counter, Histogram
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from .deps import verify_api_key
from .routes.default import router as default_router
from .routes.items import router as items_router
from .routes.scrape import router as scrape_router
from .routes.settings import router as settings_router
from .routes.stream import router as stream_router
from .routes.triven import router as triven_router
from .routes.webhooks import router as webhooks_router

ROUTE_REQUESTS_TOTAL = Counter(
    "filmu_py_route_requests_total",
    "Total HTTP requests per route",
    ["route", "method", "status_code"],
)
ROUTE_LATENCY_SECONDS = Histogram(
    "filmu_py_route_latency_seconds",
    "HTTP request latency per route",
    ["route", "method"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)
ROUTE_ERRORS_TOTAL = Counter(
    "filmu_py_route_errors_total",
    "HTTP 4xx/5xx responses per route by error class",
    ["route", "method", "error_class"],
)


def _route_template(request: Request) -> str:
    route = request.scope.get("route")
    path = getattr(route, "path", None)
    if isinstance(path, str) and path:
        return path
    return request.url.path


class RouteMetricsMiddleware(BaseHTTPMiddleware):
    """Record route-level Prometheus counters using template-based route labels."""

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        method = request.method
        started_at = perf_counter()

        try:
            response = await call_next(request)
        except Exception:
            route = _route_template(request)
            elapsed = perf_counter() - started_at
            ROUTE_REQUESTS_TOTAL.labels(route=route, method=method, status_code="500").inc()
            ROUTE_LATENCY_SECONDS.labels(route=route, method=method).observe(elapsed)
            ROUTE_ERRORS_TOTAL.labels(
                route=route,
                method=method,
                error_class="server_error",
            ).inc()
            raise

        route = _route_template(request)
        elapsed = perf_counter() - started_at
        status_code = response.status_code

        ROUTE_REQUESTS_TOTAL.labels(
            route=route,
            method=method,
            status_code=str(status_code),
        ).inc()
        ROUTE_LATENCY_SECONDS.labels(route=route, method=method).observe(elapsed)

        if 400 <= status_code < 500:
            ROUTE_ERRORS_TOTAL.labels(
                route=route,
                method=method,
                error_class="client_error",
            ).inc()
        elif status_code >= 500:
            ROUTE_ERRORS_TOTAL.labels(
                route=route,
                method=method,
                error_class="server_error",
            ).inc()

        return response


def create_api_router() -> APIRouter:
    """Create API router mounted under /api/v1."""

    api = APIRouter(prefix="/api/v1", dependencies=[Depends(verify_api_key)])
    api.include_router(default_router)
    api.include_router(items_router)
    api.include_router(scrape_router)
    api.include_router(settings_router)
    api.include_router(stream_router)
    api.include_router(triven_router)
    api.include_router(webhooks_router)
    return api
