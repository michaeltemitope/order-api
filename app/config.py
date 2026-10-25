from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Every value has a working default so the app boots with zero config
    for local dev / tests. Only DATABASE_URL points at a real service by
    default (see .env.example), everything else degrades or fakes cleanly.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Database ---
    DATABASE_URL: str = "postgresql+asyncpg://order_user:order_pass@localhost:5432/orders"

    # --- Redis (two-tier cache L2 + distributed rate limiter) ---
    REDIS_URL: str = "redis://localhost:6379/0"

    # --- Kafka (outbox relay target) ---
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_ORDER_EVENTS_TOPIC: str = "order-events"
    # When true, the app publishes to an in-memory fake instead of a real
    # broker, lets the API run standalone without Kafka for local hacking.
    KAFKA_FAKE: bool = False

    # --- Outbox relay ---
    OUTBOX_POLL_INTERVAL_SECONDS: float = 1.0
    OUTBOX_BATCH_SIZE: int = 50
    # Runs the relay as a background asyncio task inside the API process.
    # Set to false in production and run `python -m app.workers.outbox_relay`
    # as its own deployment instead, see docker-compose.yml.
    OUTBOX_RELAY_IN_PROCESS: bool = True

    # --- Two-tier cache ---
    CACHE_L1_MAX_SIZE: int = 1000
    CACHE_L1_TTL_SECONDS: float = 5.0
    CACHE_L2_TTL_SECONDS: float = 60.0

    # --- Rate limiting (Redis sliding window) ---
    RATE_LIMIT_TIMES: int = 20
    RATE_LIMIT_SECONDS: int = 60

    # --- Circuit breaker (payment gateway) ---
    BREAKER_FAILURE_THRESHOLD: int = 5
    BREAKER_RECOVERY_SECONDS: float = 30.0

    # --- Simulated payment gateway (for demo purposes only) ---
    PAYMENT_GATEWAY_FAILURE_RATE: float = 0.0
    PAYMENT_GATEWAY_LATENCY_MS: int = 50


settings = Settings()
