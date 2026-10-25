"""
FastAPI dependency accessors. Shared, stateful objects (Redis client, cache,
circuit breaker, rate limiter, event publisher) are constructed once in
main.py's lifespan and stored on `app.state`, these functions just fetch
them per-request. Routers depend on these functions rather than importing
concrete instances directly, so tests can override any of them via
`app.dependency_overrides` without touching route code.
"""
from fastapi import Request

from .database import get_db  # re-exported for convenience
from .middleware.rate_limit import RedisRateLimiter
from .resilience.cache import TwoTierCache
from .resilience.circuit_breaker import CircuitBreaker
from .services.payment_gateway import PaymentGatewayClient

__all__ = [
    "get_db",
    "get_cache",
    "get_breaker",
    "get_gateway",
    "rate_limit_orders",
]


def get_cache(request: Request) -> TwoTierCache:
    return request.app.state.cache


def get_breaker(request: Request) -> CircuitBreaker:
    return request.app.state.payment_breaker


def get_gateway(request: Request) -> PaymentGatewayClient:
    return request.app.state.payment_gateway


async def rate_limit_orders(request: Request) -> None:
    limiter: RedisRateLimiter = request.app.state.order_rate_limiter
    await limiter(request)
