"""
Publisher abstraction so the outbox relay (app/workers/outbox_relay.py) and
its tests don't depend on a running Kafka broker. Two implementations:

  KafkaEventPublisher   real aiokafka producer, used in dev/prod.
  FakeEventPublisher     in-memory list, used in tests and in
                          KAFKA_FAKE=true local runs.
"""
import json
import logging
from typing import Protocol

from aiokafka import AIOKafkaProducer

logger = logging.getLogger(__name__)


class EventPublisher(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def publish(self, topic: str, key: str, value: dict) -> None: ...


class KafkaEventPublisher:
    def __init__(self, bootstrap_servers: str):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            enable_idempotence=True,  # dedup at the Kafka-producer level too
            acks="all",
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def publish(self, topic: str, key: str, value: dict) -> None:
        await self._producer.send_and_wait(topic, key=key, value=value)


class FakeEventPublisher:
    """Records published messages instead of sending them anywhere.
    Used by tests and by KAFKA_FAKE=true so the API can run standalone."""

    def __init__(self):
        self.published: list[tuple[str, str, dict]] = []
        self._started = False

    async def start(self) -> None:
        self._started = True

    async def stop(self) -> None:
        self._started = False

    async def publish(self, topic: str, key: str, value: dict) -> None:
        if not self._started:
            raise RuntimeError("publisher not started")
        logger.info("fake-publish topic=%s key=%s", topic, key)
        self.published.append((topic, key, value))
