import pytest
from fastapi import HTTPException

from app.middleware.rate_limit import RedisRateLimiter

pytestmark = pytest.mark.asyncio


class _FakeClient:
    def __init__(self, host: str):
        self.host = host


class _FakeRequest:
    def __init__(self, path: str, host: str, headers: dict | None = None):
        self.url = type("U", (), {"path": path})()
        self.client = _FakeClient(host)
        self.headers = headers or {}


async def test_allows_requests_under_the_limit(fake_redis):
    limiter = RedisRateLimiter(fake_redis, times=3, seconds=60)
    request = _FakeRequest("/orders", "1.2.3.4")
    for _ in range(3):
        await limiter(request)  # should not raise


async def test_blocks_requests_over_the_limit(fake_redis):
    limiter = RedisRateLimiter(fake_redis, times=3, seconds=60)
    request = _FakeRequest("/orders", "1.2.3.4")
    for _ in range(3):
        await limiter(request)

    with pytest.raises(HTTPException) as exc_info:
        await limiter(request)
    assert exc_info.value.status_code == 429
    assert "Retry-After" in exc_info.value.headers


async def test_limits_are_scoped_per_client(fake_redis):
    limiter = RedisRateLimiter(fake_redis, times=1, seconds=60)
    request_a = _FakeRequest("/orders", "1.1.1.1")
    request_b = _FakeRequest("/orders", "2.2.2.2")

    await limiter(request_a)
    await limiter(request_b)  # different client, should not raise

    with pytest.raises(HTTPException):
        await limiter(request_a)


async def test_api_key_takes_priority_over_ip(fake_redis):
    limiter = RedisRateLimiter(fake_redis, times=1, seconds=60)
    # Same IP, different API keys, should be tracked independently.
    request_a = _FakeRequest("/orders", "9.9.9.9", headers={"X-API-Key": "alice"})
    request_b = _FakeRequest("/orders", "9.9.9.9", headers={"X-API-Key": "bob"})

    await limiter(request_a)
    await limiter(request_b)  # should not raise despite sharing an IP


async def test_limits_are_scoped_per_route(fake_redis):
    limiter = RedisRateLimiter(fake_redis, times=1, seconds=60)
    orders_req = _FakeRequest("/orders", "5.5.5.5")
    pay_req = _FakeRequest("/orders/abc/pay", "5.5.5.5")

    await limiter(orders_req)
    await limiter(pay_req)  # different route key, should not raise
