import pytest
from sqlalchemy import select

from app.models import OrderStatus, OutboxEvent
from app.resilience.cache import TwoTierCache
from app.resilience.circuit_breaker import CircuitBreaker
from app.services import order_service
from app.services.payment_gateway import PaymentGatewayClient

pytestmark = pytest.mark.asyncio

ITEMS = [{"sku": "book-1", "qty": 2, "unit_price_cents": 1500}]


@pytest.fixture
def cache(fake_redis):
    return TwoTierCache(fake_redis, namespace="order", l1_max_size=10, l1_ttl_seconds=5, l2_ttl_seconds=30)


async def test_create_order_computes_total(db_session):
    order = await order_service.create_order(db_session, "buyer@example.com", ITEMS)
    assert order.total_cents == 3000
    assert order.status == OrderStatus.PENDING


async def test_create_order_writes_outbox_event_in_same_transaction(db_session):
    order = await order_service.create_order(db_session, "buyer@example.com", ITEMS)

    result = await db_session.execute(select(OutboxEvent).where(OutboxEvent.aggregate_id == order.id))
    events = result.scalars().all()

    assert len(events) == 1
    assert events[0].event_type == "order.created"
    assert events[0].payload["total_cents"] == 3000


async def test_get_order_returns_none_when_missing(db_session, cache):
    assert await order_service.get_order(db_session, cache, "does-not-exist") is None


async def test_get_order_returns_created_order(db_session, cache):
    created = await order_service.create_order(db_session, "buyer@example.com", ITEMS)
    fetched = await order_service.get_order(db_session, cache, created.id)
    assert fetched.id == created.id
    assert fetched.total_cents == 3000


async def test_get_order_second_call_is_served_from_cache(db_session, cache, fake_redis):
    created = await order_service.create_order(db_session, "buyer@example.com", ITEMS)
    await order_service.get_order(db_session, cache, created.id)  # populates cache

    # Delete the row from the DB directly, if the second get() still
    # succeeds, it proves the cache (not the DB) served it.
    await db_session.delete(await db_session.get(type(created), created.id))
    await db_session.commit()

    fetched = await order_service.get_order(db_session, cache, created.id)
    assert fetched is not None
    assert fetched.id == created.id


async def test_pay_order_success_marks_paid_and_emits_event(db_session, cache):
    order = await order_service.create_order(db_session, "buyer@example.com", ITEMS)
    breaker = CircuitBreaker(failure_threshold=5, recovery_seconds=30)
    gateway = PaymentGatewayClient(failure_rate=0.0, latency_ms=0)

    paid = await order_service.pay_order(db_session, cache, breaker, gateway, order.id)

    assert paid.status == OrderStatus.PAID
    result = await db_session.execute(
        select(OutboxEvent).where(OutboxEvent.aggregate_id == order.id, OutboxEvent.event_type == "order.paid")
    )
    assert result.scalar_one_or_none() is not None


async def test_pay_order_already_paid_is_a_noop(db_session, cache):
    order = await order_service.create_order(db_session, "buyer@example.com", ITEMS)
    breaker = CircuitBreaker(failure_threshold=5, recovery_seconds=30)
    gateway = PaymentGatewayClient(failure_rate=0.0, latency_ms=0)

    await order_service.pay_order(db_session, cache, breaker, gateway, order.id)
    second = await order_service.pay_order(db_session, cache, breaker, gateway, order.id)

    assert second.status == OrderStatus.PAID
    # No second "order.paid" event, the second call never touched the gateway.
    result = await db_session.execute(
        select(OutboxEvent).where(OutboxEvent.aggregate_id == order.id, OutboxEvent.event_type == "order.paid")
    )
    assert len(result.scalars().all()) == 1


async def test_pay_order_gateway_failure_marks_payment_failed(db_session, cache):
    order = await order_service.create_order(db_session, "buyer@example.com", ITEMS)
    breaker = CircuitBreaker(failure_threshold=5, recovery_seconds=30)
    gateway = PaymentGatewayClient(failure_rate=1.0, latency_ms=0)  # always fails

    result = await order_service.pay_order(db_session, cache, breaker, gateway, order.id)

    assert result.status == OrderStatus.PAYMENT_FAILED
    events = await db_session.execute(
        select(OutboxEvent).where(OutboxEvent.aggregate_id == order.id, OutboxEvent.event_type == "order.payment_failed")
    )
    assert events.scalar_one_or_none() is not None


async def test_pay_order_open_circuit_raises_without_calling_gateway(db_session, cache):
    order = await order_service.create_order(db_session, "buyer@example.com", ITEMS)
    breaker = CircuitBreaker(failure_threshold=1, recovery_seconds=30)
    gateway = PaymentGatewayClient(failure_rate=0.0, latency_ms=0)

    calls = {"n": 0}
    real_charge = gateway.charge

    async def counting_charge(*args, **kwargs):
        calls["n"] += 1
        return await real_charge(*args, **kwargs)

    gateway.charge = counting_charge

    # First: trip the breaker with a gateway that always fails.
    failing_gateway = PaymentGatewayClient(failure_rate=1.0, latency_ms=0)
    order_2 = await order_service.create_order(db_session, "buyer2@example.com", ITEMS)
    await order_service.pay_order(db_session, cache, breaker, failing_gateway, order_2.id)
    assert breaker.state.value == "open"

    with pytest.raises(order_service.PaymentGatewayUnavailableError):
        await order_service.pay_order(db_session, cache, breaker, gateway, order.id)

    assert calls["n"] == 0  # breaker rejected before touching the (working) gateway


async def test_pay_order_not_found_raises(db_session, cache):
    breaker = CircuitBreaker(failure_threshold=5, recovery_seconds=30)
    gateway = PaymentGatewayClient(failure_rate=0.0, latency_ms=0)

    with pytest.raises(order_service.OrderNotFoundError):
        await order_service.pay_order(db_session, cache, breaker, gateway, "does-not-exist")
