from fastapi import APIRouter, Depends, Request

from ..dependencies import get_breaker
from ..resilience.circuit_breaker import CircuitBreaker

router = APIRouter(tags=["health"])


@router.get("/health")
async def health():
    return {"ok": True}


@router.get("/health/dependencies")
async def dependencies_health(request: Request, breaker: CircuitBreaker = Depends(get_breaker)):
    """Not a liveness probe, a quick look at the state of the resilience
    primitives, useful when debugging a degraded deployment."""
    return {
        "payment_gateway_circuit": breaker.state.value,
        "kafka_fake_mode": request.app.state.settings.KAFKA_FAKE,
    }
