"""Caching primitives for in-process and Redis-backed caches."""

from __future__ import annotations

import asyncio
import time

from cachetools import LRUCache
from prometheus_client import Counter
from redis.asyncio import Redis

CACHE_HITS_TOTAL = Counter(
    "filmu_py_cache_hits_total",
    "Cache hits",
    ["layer", "namespace"],
)
CACHE_MISSES_TOTAL = Counter(
    "filmu_py_cache_misses_total",
    "Cache misses",
    ["layer", "namespace"],
)
CACHE_INVALIDATIONS_TOTAL = Counter(
    "filmu_py_cache_invalidations_total",
    "Cache invalidations",
    ["namespace", "reason"],
)
CACHE_STALE_SERVES_TOTAL = Counter(
    "filmu_py_cache_stale_serves_total",
    "Stale cache serves (stale-while-revalidate)",
    ["namespace"],
)


class CacheManager:
    """Two-layer cache manager with local TTL and Redis backing."""

    def __init__(
        self,
        redis: Redis,
        maxsize: int = 1_000,
        default_ttl_seconds: int = 60,
        namespace: str | None = None,
    ):
        self.redis = redis
        self.local: LRUCache[str, bytes] = LRUCache(maxsize=maxsize)
        self._local_expiry: dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._default_ttl_seconds = default_ttl_seconds
        self._namespace = namespace.strip() if namespace else None

    def _prefixed_key(self, key: str) -> str:
        if self._namespace:
            return f"{self._namespace}:{key}"
        return key

    def _namespace_label(self) -> str:
        return self._namespace or "default"

    def _is_expired(self, key: str, now: float) -> bool:
        expiry = self._local_expiry.get(key)
        return expiry is None or expiry <= now

    async def get(self, key: str) -> bytes | None:
        cache_key = self._prefixed_key(key)
        now = time.time()

        async with self._lock:
            local_value = self.local.get(cache_key)
            if isinstance(local_value, bytes) and not self._is_expired(cache_key, now):
                CACHE_HITS_TOTAL.labels(layer="local", namespace=self._namespace_label()).inc()
                return local_value

            CACHE_MISSES_TOTAL.labels(layer="local", namespace=self._namespace_label()).inc()
            self.local.pop(cache_key, None)
            self._local_expiry.pop(cache_key, None)

        redis_value = await self.redis.get(cache_key)
        if isinstance(redis_value, bytes):
            async with self._lock:
                self.local[cache_key] = redis_value
                self._local_expiry[cache_key] = now + self._default_ttl_seconds
            CACHE_HITS_TOTAL.labels(layer="redis", namespace=self._namespace_label()).inc()
            return redis_value

        CACHE_MISSES_TOTAL.labels(layer="redis", namespace=self._namespace_label()).inc()
        return None

    async def set(self, key: str, value: bytes, ttl_seconds: int | None = None) -> None:
        cache_key = self._prefixed_key(key)
        ttl = ttl_seconds if ttl_seconds is not None else self._default_ttl_seconds
        expires_at = time.time() + ttl

        async with self._lock:
            self.local[cache_key] = value
            self._local_expiry[cache_key] = expires_at

        await self.redis.set(cache_key, value, ex=ttl)

    async def invalidate(self, key: str, *, reason: str = "explicit") -> None:
        """Delete a cache entry from local and Redis layers with a reason label."""

        cache_key = self._prefixed_key(key)
        async with self._lock:
            self.local.pop(cache_key, None)
            self._local_expiry.pop(cache_key, None)

        await self.redis.delete(cache_key)
        CACHE_INVALIDATIONS_TOTAL.labels(namespace=self._namespace_label(), reason=reason).inc()

    async def delete(self, key: str, *, reason: str = "explicit") -> None:
        """Backward-compatible alias for explicit cache invalidation."""

        await self.invalidate(key, reason=reason)
