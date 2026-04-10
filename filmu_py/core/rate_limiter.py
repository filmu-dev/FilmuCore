"""Distributed rate limiter with Redis Lua token-bucket."""

from __future__ import annotations

import time
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, cast

from prometheus_client import Counter, Histogram
from redis.asyncio import Redis
from redis.exceptions import ResponseError

TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local refill_rate = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local ttl_seconds = tonumber(ARGV[5])

local data = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(data[1])
local last_refill = tonumber(data[2])

if tokens == nil then
  tokens = capacity
  last_refill = now
end

local elapsed = math.max(0, now - last_refill)
local refill = elapsed * refill_rate
tokens = math.min(capacity, tokens + refill)

local allowed = 0
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
end

redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
redis.call('EXPIRE', key, ttl_seconds)

return {allowed, tokens}
"""

RATE_LIMIT_ACQUIRE_TOTAL = Counter(
    "filmu_py_rate_limit_acquire_total",
    "Rate limiter acquire outcomes by bucket class",
    ["bucket_class", "outcome"],
)
RATE_LIMIT_REMAINING_TOKENS = Histogram(
    "filmu_py_rate_limit_remaining_tokens",
    "Remaining token count observed after rate limiter decisions",
    ["bucket_class", "outcome"],
    buckets=[0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0],
)
RATE_LIMIT_RETRY_AFTER_SECONDS = Histogram(
    "filmu_py_rate_limit_retry_after_seconds",
    "Retry-after guidance emitted by rate limiter denials",
    ["bucket_class"],
    buckets=[0.0, 0.1, 0.5, 1.0, 2.5, 5.0, 15.0, 30.0, 60.0, 300.0, 900.0],
)


def _bucket_class(bucket_key: str) -> str:
    """Reduce raw bucket keys into a bounded-cardinality operator label."""

    normalized = bucket_key.strip().lower()
    if not normalized:
        return "unknown"
    parts = [part for part in normalized.split(":") if part]
    if not parts:
        return "unknown"
    return ":".join(parts[: min(3, len(parts))])


@dataclass(frozen=True)
class RateLimitDecision:
    """Represents a rate-limit decision for a request."""

    allowed: bool
    remaining_tokens: float
    retry_after_seconds: float


class DistributedRateLimiter:
    """Redis-backed token bucket limiter."""

    def __init__(self, redis: Redis):
        self.redis = redis
        self._script_sha: str | None = None

    async def _await_maybe(self, value: object) -> object:
        """Await Redis client values only when coroutine-based client is in use."""

        if isinstance(value, Awaitable):
            return await value
        return value

    async def _ensure_script(self) -> str:
        if self._script_sha is None:
            loaded = await self._await_maybe(self.redis.script_load(TOKEN_BUCKET_LUA))
            self._script_sha = cast(str, loaded)
        return self._script_sha

    async def acquire(
        self,
        bucket_key: str,
        capacity: float,
        refill_rate_per_second: float,
        requested_tokens: float = 1.0,
        now_seconds: float | None = None,
        expiry_seconds: int | None = None,
    ) -> RateLimitDecision:
        now_seconds = now_seconds if now_seconds is not None else time.time()
        ttl = (
            expiry_seconds
            if expiry_seconds is not None
            else max(1, int(capacity / refill_rate_per_second))
        )
        sha = await self._ensure_script()

        try:
            eval_result = await self._await_maybe(
                self.redis.evalsha(
                    sha,
                    1,
                    bucket_key,
                    str(now_seconds),
                    str(capacity),
                    str(refill_rate_per_second),
                    str(requested_tokens),
                    str(ttl),
                )
            )
            result = cast(list[Any], eval_result)
        except ResponseError as exc:
            if "NOSCRIPT" not in str(exc):
                raise

            self._script_sha = None
            sha = await self._ensure_script()
            eval_result = await self._await_maybe(
                self.redis.evalsha(
                    sha,
                    1,
                    bucket_key,
                    str(now_seconds),
                    str(capacity),
                    str(refill_rate_per_second),
                    str(requested_tokens),
                    str(ttl),
                )
            )
            result = cast(list[Any], eval_result)

        allowed = bool(result[0])
        remaining = float(result[1])
        deficit = max(0.0, requested_tokens - remaining)
        retry_after = 0.0 if allowed else (deficit / refill_rate_per_second)
        bucket_class = _bucket_class(bucket_key)
        outcome = "allowed" if allowed else "denied"
        RATE_LIMIT_ACQUIRE_TOTAL.labels(bucket_class=bucket_class, outcome=outcome).inc()
        RATE_LIMIT_REMAINING_TOKENS.labels(bucket_class=bucket_class, outcome=outcome).observe(
            max(0.0, remaining)
        )
        if not allowed:
            RATE_LIMIT_RETRY_AFTER_SECONDS.labels(bucket_class=bucket_class).observe(
                max(0.0, retry_after)
            )
        return RateLimitDecision(
            allowed=allowed,
            remaining_tokens=remaining,
            retry_after_seconds=retry_after,
        )

    async def reset(self, bucket_key: str) -> None:
        """Delete token-bucket state for a given key."""

        await self.redis.delete(bucket_key)
