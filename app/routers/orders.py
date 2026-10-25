from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from ..dependencies import get_breaker, get_cache, get_db, get_gateway, rate_limit_orders
from ..resilience.cache import TwoTierCache
from ..resilience.circuit_breaker import CircuitBreaker
from ..schemas import CreateOrderRequest, OrderOut
from ..services import idempotency, order_service
from ..services.order_service import OrderNotFoundError, PaymentGatewayUnavailableError
from ..services.payment_gateway import PaymentGatewayClient

router = APIRouter(prefix="/orders", tags=["orders"])

_CREATE_ENDPOINT = "POST /orders"


@router.post("", response_model=OrderOut, status_code=status.HTTP_201_CREATED, dependencies=[Depends(rate_limit_orders)])
async def create_order(
    body: CreateOrderRequest,
    db: AsyncSession = Depends(get_db),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
    if not idempotency_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Idempotency-Key header is required for order creation.",
        )

    request_hash = idempotency.hash_request(body.model_dump(mode="json"))
    reservation = await idempotency.reserve(db, idempotency_key, _CREATE_ENDPOINT, request_hash)

    if reservation.outcome == idempotency.Outcome.REPLAY:
        return reservation.response_body

    if reservation.outcome == idempotency.Outcome.IN_PROGRESS:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A request with this Idempotency-Key is already being processed.",
        )

    if reservation.outcome == idempotency.Outcome.CONFLICT:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="This Idempotency-Key was already used with a different request body.",
        )

    # OWNED: we're the request that gets to actually run the handler.
    try:
        order = await order_service.create_order(
            db,
            customer_email=body.customer_email,
            items=[item.model_dump() for item in body.items],
        )
    except Exception:
        await idempotency.release(db, idempotency_key, _CREATE_ENDPOINT)
        raise

    response_body = OrderOut.model_validate(order).model_dump(mode="json")
    await idempotency.finalize(db, idempotency_key, _CREATE_ENDPOINT, status.HTTP_201_CREATED, response_body)
    return response_body


@router.get("/{order_id}", response_model=OrderOut)
async def get_order(
    order_id: str,
    db: AsyncSession = Depends(get_db),
    cache: TwoTierCache = Depends(get_cache),
):
    order = await order_service.get_order(db, cache, order_id)
    if order is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")
    return order


@router.post("/{order_id}/pay", response_model=OrderOut, dependencies=[Depends(rate_limit_orders)])
async def pay_order(
    order_id: str,
    db: AsyncSession = Depends(get_db),
    cache: TwoTierCache = Depends(get_cache),
    breaker: CircuitBreaker = Depends(get_breaker),
    gateway: PaymentGatewayClient = Depends(get_gateway),
):
    try:
        return await order_service.pay_order(db, cache, breaker, gateway, order_id)
    except OrderNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found") from None
    except PaymentGatewayUnavailableError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Payment gateway is currently unavailable, please retry shortly.",
            headers={"Retry-After": "30"},
        ) from exc
