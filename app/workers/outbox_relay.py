"""
Outbox relay: the second half of the transactional outbox pattern.
`order_service.create_order` writes the Order and its OutboxEvent in one
transaction (see that module). This process is what actually gets the
event to Kafka: poll unpublished rows, publish, mark published.

Delivery guarantee is *at-least-once*, not exactly-once: if the process
crashes after `publisher.publish()` succeeds but before the commit that
marks the row published, the same event gets published again on the next
poll. Consumers of `order-events` must be idempotent (e.g. dedupe on
`(aggregate_id, event_type)` or a Kafka key + compacted topic), that's a
standard, well-understood trade-off for outbox relays, not an oversight.

`with_for_update(skip_locked=True)` is what makes it safe to run this relay
as more than one process for throughput: each replica locks a different
batch of rows instead of the same one. SQLite doesn't support
SKIP LOCKED, so it's only applied against Postgres, tests run this against
SQLite with a single relay "replica", where the lock isn't needed anyway.

Runs two ways (see docker-compose.yml):
  - in-process: FastAPI's lifespan starts this as a background asyncio task
    (OUTBOX_RELAY_IN_PROCESS=true, the default, simplest for local dev).
  - standalone: `python -m app.workers.outbox_relay` as its own container,
    scaled independently of the API (OUTBOX_RELAY_IN_PROCESS=false).
"""
import asyncio
import logging
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from .. import config
from ..models import OutboxEvent
from ..services.kafka_publisher import EventPublisher

logger = logging.getLogger(__name__)


async def relay_once(
    session_factory: async_sessionmaker,
    publisher: EventPublisher,
    topic: str,
    batch_size: int,
    supports_skip_locked: bool,
) -> int:
    async with session_factory() as db:
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.id)
            .limit(batch_size)
        )
        if supports_skip_locked:
            stmt = stmt.with_for_update(skip_locked=True)

        result = await db.execute(stmt)
        events = list(result.scalars().all())
        if not events:
            return 0

        for event in events:
            await publisher.publish(
                topic,
                key=event.aggregate_id,
                value={"event_type": event.event_type, **event.payload},
            )
            event.published_at = datetime.now(UTC)

        await db.commit()
        logger.info("relayed %d outbox event(s)", len(events))
        return len(events)


async def run_forever(
    session_factory: async_sessionmaker,
    publisher: EventPublisher,
    topic: str,
    poll_interval: float,
    batch_size: int,
    supports_skip_locked: bool,
) -> None:
    while True:
        try:
            processed = await relay_once(session_factory, publisher, topic, batch_size, supports_skip_locked)
        except Exception:
            logger.exception("outbox relay iteration failed")
            processed = 0
        if processed == 0:
            await asyncio.sleep(poll_interval)


async def _main() -> None:
    """Standalone entrypoint: python -m app.workers.outbox_relay"""
    logging.basicConfig(level=logging.INFO)
    settings = config.settings

    from sqlalchemy.ext.asyncio import create_async_engine

    engine: AsyncEngine = create_async_engine(settings.DATABASE_URL, pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    if settings.KAFKA_FAKE:
        from ..services.kafka_publisher import FakeEventPublisher

        publisher: EventPublisher = FakeEventPublisher()
    else:
        from ..services.kafka_publisher import KafkaEventPublisher

        publisher = KafkaEventPublisher(settings.KAFKA_BOOTSTRAP_SERVERS)

    await publisher.start()
    try:
        await run_forever(
            session_factory,
            publisher,
            topic=settings.KAFKA_ORDER_EVENTS_TOPIC,
            poll_interval=settings.OUTBOX_POLL_INTERVAL_SECONDS,
            batch_size=settings.OUTBOX_BATCH_SIZE,
            supports_skip_locked="postgresql" in settings.DATABASE_URL,
        )
    finally:
        await publisher.stop()


if __name__ == "__main__":
    asyncio.run(_main())
