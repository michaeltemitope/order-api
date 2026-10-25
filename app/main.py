import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import async_sessionmaker

from .config import settings
from .database import engine
from .middleware.rate_limit import RedisRateLimiter
from .resilience.cache import TwoTierCache
from .resilience.circuit_breaker import CircuitBreaker
from .routers import health, orders
from .services.kafka_publisher import EventPublisher, FakeEventPublisher, KafkaEventPublisher
from .services.payment_gateway import PaymentGatewayClient
from .workers.outbox_relay import run_forever as run_outbox_relay

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.settings = settings

    redis_client = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    app.state.redis = redis_client

    app.state.cache = TwoTierCache(
        redis_client,
        namespace="order",
        l1_max_size=settings.CACHE_L1_MAX_SIZE,
        l1_ttl_seconds=settings.CACHE_L1_TTL_SECONDS,
        l2_ttl_seconds=settings.CACHE_L2_TTL_SECONDS,
    )
    app.state.payment_breaker = CircuitBreaker(
        failure_threshold=settings.BREAKER_FAILURE_THRESHOLD,
        recovery_seconds=settings.BREAKER_RECOVERY_SECONDS,
    )
    app.state.payment_gateway = PaymentGatewayClient(
        failure_rate=settings.PAYMENT_GATEWAY_FAILURE_RATE,
        latency_ms=settings.PAYMENT_GATEWAY_LATENCY_MS,
    )
    app.state.order_rate_limiter = RedisRateLimiter(
        redis_client, times=settings.RATE_LIMIT_TIMES, seconds=settings.RATE_LIMIT_SECONDS
    )

    publisher: EventPublisher
    publisher = FakeEventPublisher() if settings.KAFKA_FAKE else KafkaEventPublisher(settings.KAFKA_BOOTSTRAP_SERVERS)
    await publisher.start()
    app.state.event_publisher = publisher

    relay_task: asyncio.Task | None = None
    if settings.OUTBOX_RELAY_IN_PROCESS:
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        relay_task = asyncio.create_task(
            run_outbox_relay(
                session_factory,
                publisher,
                topic=settings.KAFKA_ORDER_EVENTS_TOPIC,
                poll_interval=settings.OUTBOX_POLL_INTERVAL_SECONDS,
                batch_size=settings.OUTBOX_BATCH_SIZE,
                supports_skip_locked="postgresql" in settings.DATABASE_URL,
            )
        )
        logger.info("outbox relay running in-process")

    yield

    if relay_task is not None:
        relay_task.cancel()
    await publisher.stop()
    await redis_client.aclose()
    await engine.dispose()


app = FastAPI(title="Resilient Order API", version="1.0.0", lifespan=lifespan)

app.include_router(health.router)
app.include_router(orders.router)
