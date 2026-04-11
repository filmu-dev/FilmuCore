"""OIDC JWT validation helpers."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, cast

import httpx
from authlib.jose import JoseError, JsonWebKey, jwt  # type: ignore[import-untyped]

from filmu_py.config import OidcSettings
from filmu_py.core.cache import CacheManager


class OidcValidationError(ValueError):
    """Raised when a bearer token cannot be trusted as an OIDC token."""


@dataclass(frozen=True, slots=True)
class OidcValidationResult:
    """Validated OIDC claims projected for request identity resolution."""

    issuer: str
    subject: str
    audience: tuple[str, ...]
    claims: dict[str, Any]


def _decode_json_segment(segment: str) -> dict[str, Any]:
    padded = segment + "=" * (-len(segment) % 4)
    try:
        decoded = base64.urlsafe_b64decode(padded.encode("ascii"))
        payload = json.loads(decoded.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise OidcValidationError("Malformed JWT header") from exc
    if not isinstance(payload, dict):
        raise OidcValidationError("Malformed JWT header")
    return cast(dict[str, Any], payload)


def _token_header(token: str) -> dict[str, Any]:
    header_segment, separator, _rest = token.partition(".")
    if not separator:
        raise OidcValidationError("Malformed bearer token")
    return _decode_json_segment(header_segment)


def _normalize_audience(value: object) -> tuple[str, ...]:
    if isinstance(value, str) and value:
        return (value,)
    if isinstance(value, list):
        return tuple(item for item in value if isinstance(item, str) and item)
    return ()


def _jwks_discovery_url(settings: OidcSettings) -> str:
    if settings.jwks_url:
        return settings.jwks_url
    if not settings.issuer:
        raise OidcValidationError("OIDC issuer is not configured")
    return f"{settings.issuer.rstrip('/')}/.well-known/jwks.json"


async def _load_jwks(settings: OidcSettings, cache: CacheManager | None) -> dict[str, Any]:
    if settings.jwks_json is not None:
        return settings.jwks_json

    jwks_url = _jwks_discovery_url(settings)
    cache_key = f"oidc:jwks:{jwks_url}"
    if cache is not None:
        cached = await cache.get(cache_key)
        if cached:
            try:
                decoded = json.loads(cached.decode("utf-8"))
            except (ValueError, UnicodeDecodeError) as exc:
                raise OidcValidationError("Cached JWKS is malformed") from exc
            if isinstance(decoded, dict):
                return cast(dict[str, Any], decoded)

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(jwks_url)
            response.raise_for_status()
            jwks = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise OidcValidationError("Unable to load OIDC JWKS") from exc

    if not isinstance(jwks, dict) or not isinstance(jwks.get("keys"), list):
        raise OidcValidationError("OIDC JWKS is malformed")
    if cache is not None:
        await cache.set(
            cache_key,
            json.dumps(jwks, separators=(",", ":")).encode("utf-8"),
            ttl_seconds=settings.cache_ttl_seconds,
        )
    return cast(dict[str, Any], jwks)


async def validate_oidc_bearer_token(
    token: str,
    *,
    settings: OidcSettings,
    cache: CacheManager | None = None,
) -> OidcValidationResult:
    """Validate one bearer token against configured issuer, audience, and JWKS."""

    if not settings.enabled:
        raise OidcValidationError("OIDC validation is disabled")
    if not settings.issuer or not settings.audience:
        raise OidcValidationError("OIDC issuer and audience must be configured")

    header = _token_header(token)
    algorithm = header.get("alg")
    if not isinstance(algorithm, str) or algorithm not in set(settings.allowed_algorithms):
        raise OidcValidationError("OIDC token algorithm is not allowed")

    jwks = await _load_jwks(settings, cache)
    key_set = JsonWebKey.import_key_set(jwks)
    try:
        claims = jwt.decode(
            token,
            key_set,
            claims_options={
                "iss": {"essential": True, "value": settings.issuer},
                "aud": {"essential": True, "value": settings.audience},
                "sub": {"essential": True},
                "exp": {"essential": True},
            },
        )
        claims.validate(leeway=settings.clock_skew_seconds)
    except JoseError as exc:
        raise OidcValidationError("OIDC token validation failed") from exc

    payload = dict(claims)
    subject = payload.get("sub")
    issuer = payload.get("iss")
    if not isinstance(subject, str) or not subject:
        raise OidcValidationError("OIDC subject is missing")
    if issuer != settings.issuer:
        raise OidcValidationError("OIDC issuer mismatch")

    return OidcValidationResult(
        issuer=settings.issuer,
        subject=subject,
        audience=_normalize_audience(payload.get("aud")),
        claims=payload,
    )
