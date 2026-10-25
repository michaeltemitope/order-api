"""
Redis-backed rate limiter, shared across every replica.

hsk-tutor's rate limiter (a sibling project) is an in-memory dict, correct
for a single instance, wrong the moment you run two replicas behind a load
balancer, since each replica would enforce its own separate limit. This
version keeps the counter in Redis instead, so all replicas agree.

Fixed-window counter (INCR + EXPIRE on a key namespaced by the current
window), not a sliding-window log. INCR is a single atomic Redis command, so
there's no separate check-then-increment round trip to race on, a sliding
window (ZSET-based) would be more precise at the window boundary, but needs
a Lua script (or WATCH/MULTI) for the same atomicity, which the project's
Redis-compatible test double doesn't support (`fakeredis`, no EVAL). Fixed
windows trade a known, bounded imprecision, up to ~2x the limit for
requests that land right at a window boundary, for a simpler, still fully
atomic implementation. That trade-off is standard practice for this kind of
limiter, not a shortcut specific to this repo.
"""
import time
from collections.abc import Callable

from fastapi import HTTPException, Request, status
from redis.asyncio import Redis


def _client_key(request: Request) -> str:
    api_key = request.headers.get("X-API-Key")
    if api_key:
        return f"key:{api_key}"
    return f"ip:{request.client.host}" if request.client else "ip:unknown"


class RedisRateLimiter:
    """Usage: Depends(RedisRateLimiter(redis, times=20, seconds=60))"""

    def __init__(self, redis: Redis, times: int, seconds: int, key_fn: Callable[[Request], str] = _client_key):
        self._redis = redis
        self.times = times
        self.seconds = seconds
        self._key_fn = key_fn

    async def __call__(self, request: Request) -> None:
        window = int(time.time() // self.seconds)
        redis_key = f"ratelimit:{request.url.path}:{self._key_fn(request)}:{window}"

        count = await self._redis.incr(redis_key)
        if count == 1:
            # Only the request that created the key sets its TTL, avoids
            # extending the window's lifetime on every subsequent hit.
            await self._redis.expire(redis_key, self.seconds)

        if count > self.times:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many requests, please try again later.",
                headers={"Retry-After": str(self.seconds)},
            )
