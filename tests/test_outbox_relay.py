import pytest
import pytest_asyncio
from sqlalchemy import select

from app.models import OutboxEvent
from app.services.kafka_publisher import FakeEventPublisher
from app.workers.outbox_relay import relay_once

pytestmark = pytest.mark.asyncio

TOPIC = "order-events"


@pytest_asyncio.fixture
async def publisher():
    p = FakeEventPublisher()
    await p.start()
    yield p
    await p.stop()


async def test_relay_publishes_unpublished_events(db_session, db_session_factory, publisher):
    db_session.add(
        OutboxEvent(aggregate_type="order", aggregate_id="order-1", event_type="order.created", payload={"a": 1})
    )
    await db_session.commit()

    processed = await relay_once(db_session_factory, publisher, TOPIC, batch_size=50, supports_skip_locked=False)

    assert processed == 1
    assert len(publisher.published) == 1
    topic, key, value = publisher.published[0]
    assert topic == TOPIC
    assert key == "order-1"
    assert value["event_type"] == "order.created"
    assert value["a"] == 1


async def test_relay_marks_events_published(db_session, db_session_factory, publisher):
    db_session.add(
        OutboxEvent(aggregate_type="order", aggregate_id="order-1", event_type="order.created", payload={})
    )
    await db_session.commit()

    await relay_once(db_session_factory, publisher, TOPIC, batch_size=50, supports_skip_locked=False)

    result = await db_session.execute(select(OutboxEvent).where(OutboxEvent.aggregate_id == "order-1"))
    event = result.scalar_one()
    assert event.published_at is not None


async def test_relay_does_not_republish_already_published_events(db_session, db_session_factory, publisher):
    db_session.add(
        OutboxEvent(aggregate_type="order", aggregate_id="order-1", event_type="order.created", payload={})
    )
    await db_session.commit()

    await relay_once(db_session_factory, publisher, TOPIC, batch_size=50, supports_skip_locked=False)
    second_pass = await relay_once(db_session_factory, publisher, TOPIC, batch_size=50, supports_skip_locked=False)

    assert second_pass == 0
    assert len(publisher.published) == 1


async def test_relay_returns_zero_when_nothing_pending(db_session_factory, publisher):
    processed = await relay_once(db_session_factory, publisher, TOPIC, batch_size=50, supports_skip_locked=False)
    assert processed == 0


async def test_relay_respects_batch_size(db_session, db_session_factory, publisher):
    for i in range(5):
        db_session.add(
            OutboxEvent(aggregate_type="order", aggregate_id=f"order-{i}", event_type="order.created", payload={})
        )
    await db_session.commit()

    first_batch = await relay_once(db_session_factory, publisher, TOPIC, batch_size=2, supports_skip_locked=False)
    second_batch = await relay_once(db_session_factory, publisher, TOPIC, batch_size=2, supports_skip_locked=False)
    third_batch = await relay_once(db_session_factory, publisher, TOPIC, batch_size=2, supports_skip_locked=False)

    assert (first_batch, second_batch, third_batch) == (2, 2, 1)
