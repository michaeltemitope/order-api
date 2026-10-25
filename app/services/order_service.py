"""Order business logic, the only layer that touches models.Order directly.
Routers depend on this; this depends on models + the resilience primitives.
"""
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Order, OrderStatus, OutboxEvent
from ..resilience.cache import TwoTierCache
from ..resilience.circuit_breaker import CircuitBreaker, CircuitOpenError
from .payment_gateway import PaymentGatewayClient, PaymentGatewayError


class OrderNotFoundError(Exception):
    pass


class PaymentGatewayUnavailableError(Exception):
    """Raised when the circuit breaker around the payment gateway is open.
    The router maps this to 503 + Retry-After, not 500, the request is
    valid, the dependency is just known-down right now."""


async def create_order(db: AsyncSession, customer_email: str, items: list[dict]) -> Order:
    total_cents = sum(item["qty"] * item["unit_price_cents"] for item in items)
    order = Order(customer_email=customer_email, items=items, total_cents=total_cents)
    db.add(order)
    await db.flush()  # assigns order.id, still inside the open transaction

    # Same transaction, same commit as the Order insert above, this is the
    # whole point of the outbox pattern: the event can never be written
    # without the order, or the order without the event.
    db.add(
        OutboxEvent(
            aggregate_type="order",
            aggregate_id=order.id,
            event_type="order.created",
            payload={
                "order_id": order.id,
                "customer_email": order.customer_email,
                "total_cents": order.total_cents,
            },
        )
    )
    await db.commit()
    await db.refresh(order)
    return order


async def get_order(db: AsyncSession, cache: TwoTierCache, order_id: str) -> Order | None:
    async def load_from_db() -> dict | None:
        order = await db.get(Order, order_id)
        if order is None:
            return None
        return {
            "id": order.id,
            "customer_email": order.customer_email,
            "items": order.items,
            "total_cents": order.total_cents,
            "status": order.status.value,
        }

    cached = await cache.get_or_set(order_id, load_from_db)
    if cached is None:
        return None
    return Order(**{**cached, "status": OrderStatus(cached["status"])})


async def pay_order(
    db: AsyncSession,
    cache: TwoTierCache,
    breaker: CircuitBreaker,
    gateway: PaymentGatewayClient,
    order_id: str,
) -> Order:
    order = await db.get(Order, order_id)
    if order is None:
        raise OrderNotFoundError(order_id)
    if order.status == OrderStatus.PAID:
        return order  # already paid, idempotent no-op

    try:
        await breaker.call(lambda: gateway.charge(order.id, order.total_cents))
    except CircuitOpenError as exc:
        raise PaymentGatewayUnavailableError(str(exc)) from exc
    except PaymentGatewayError:
        order.status = OrderStatus.PAYMENT_FAILED
        db.add(
            OutboxEvent(
                aggregate_type="order",
                aggregate_id=order.id,
                event_type="order.payment_failed",
                payload={"order_id": order.id},
            )
        )
        await db.commit()
        await cache.invalidate(order_id)
        await db.refresh(order)
        return order

    order.status = OrderStatus.PAID
    db.add(
        OutboxEvent(
            aggregate_type="order",
            aggregate_id=order.id,
            event_type="order.paid",
            payload={"order_id": order.id, "total_cents": order.total_cents},
        )
    )
    await db.commit()
    await cache.invalidate(order_id)
    await db.refresh(order)
    return order


async def list_orders(db: AsyncSession, limit: int = 50) -> list[Order]:
    result = await db.execute(select(Order).order_by(Order.created_at.desc()).limit(limit))
    return list(result.scalars().all())
