import enum
import uuid
from datetime import datetime

from sqlalchemy import JSON, DateTime, Enum, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


def _uuid() -> str:
    return str(uuid.uuid4())


class OrderStatus(str, enum.Enum):
    PENDING = "pending"
    PAID = "paid"
    PAYMENT_FAILED = "payment_failed"
    CANCELLED = "cancelled"


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    customer_email: Mapped[str] = mapped_column(String(320), index=True)
    # Money is stored as integer cents, never float, to avoid rounding
    # errors accumulating across currency arithmetic.
    items: Mapped[list] = mapped_column(JSON)  # [{sku, qty, unit_price_cents}]
    total_cents: Mapped[int] = mapped_column(Integer)
    status: Mapped[OrderStatus] = mapped_column(
        Enum(OrderStatus, native_enum=False, length=20), default=OrderStatus.PENDING
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class OutboxEvent(Base):
    """Transactional outbox: written in the same DB transaction as the
    business-entity change it describes, so 'the order was created' and
    'an event says the order was created' can never disagree, no dual
    write to Postgres + Kafka, no distributed transaction.

    A separate relay (app/workers/outbox_relay.py) polls the unpublished
    rows and forwards them to Kafka. See that module's docstring for the
    at-least-once delivery guarantee this buys, and what it costs.
    """

    __tablename__ = "outbox_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    aggregate_type: Mapped[str] = mapped_column(String(50))
    aggregate_id: Mapped[str] = mapped_column(String(36), index=True)
    event_type: Mapped[str] = mapped_column(String(100))
    payload: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)


class IdempotencyKey(Base):
    """Stripe-style idempotency: the *first* request with a given key runs
    the handler and its response is stored here; every retry with the same
    key + same request body replays the stored response without re-running
    the handler. A retry with the same key but a *different* body is a
    client bug (key reuse) and gets rejected, see
    app/services/idempotency.py.
    """

    __tablename__ = "idempotency_keys"

    # Composite key: the same Idempotency-Key value is valid on different
    # endpoints without colliding.
    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    endpoint: Mapped[str] = mapped_column(String(100), primary_key=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    status_code: Mapped[int] = mapped_column(Integer)
    response_body: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
