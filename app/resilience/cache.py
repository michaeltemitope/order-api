"""
Two-tier read cache: L1 is in-process (cachetools TTLCache, nanoseconds,
but private to this replica and lost on restart), L2 is Redis (a network
hop, but shared across every replica). A read checks L1 first, falls back
to L2, and only hits the database on a full miss, populating both tiers on
the way back up.

This is the same shape as the p95-latency fix described in the author's CV
(two-tier cache cutting payment-gateway p95 from 180ms to 35ms), L1 makes
hot keys read from an in-process dict most of the time, L2 keeps a cold
replica from ever hitting the database for data another replica already
warmed.
"""
import json
from collections.abc import Awaitable, Callable
from typing import Any

from cachetools import TTLCache
from redis.asyncio import Redis


class TwoTierCache:
    def __init__(
        self,
        redis: Redis,
        namespace: str,
        l1_max_size: int,
        l1_ttl_seconds: float,
        l2_ttl_seconds: float,
    ):
        self._redis = redis
        self._namespace = namespace
        self._l1: TTLCache = TTLCache(maxsize=l1_max_size, ttl=l1_ttl_seconds)
        self._l2_ttl = l2_ttl_seconds

    def _redis_key(self, key: str) -> str:
        return f"{self._namespace}:{key}"

    async def get(self, key: str) -> Any | None:
        if key in self._l1:
            return self._l1[key]

        raw = await self._redis.get(self._redis_key(key))
        if raw is None:
            return None

        value = json.loads(raw)
        self._l1[key] = value  # backfill L1 from L2
        return value

    async def set(self, key: str, value: Any) -> None:
        self._l1[key] = value
        await self._redis.set(self._redis_key(key), json.dumps(value), ex=int(self._l2_ttl))

    async def invalidate(self, key: str) -> None:
        self._l1.pop(key, None)
        await self._redis.delete(self._redis_key(key))

    async def get_or_set(self, key: str, loader: Callable[[], Awaitable[Any]]) -> Any:
        cached = await self.get(key)
        if cached is not None:
            return cached
        value = await loader()
        if value is not None:
            await self.set(key, value)
        return value
