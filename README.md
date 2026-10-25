# Resilient Order API

An async FastAPI order/payment service built specifically to demonstrate the
resilience patterns that come up in real high-load backend work, the ones
that don't show up in a CRUD tutorial. The domain (order, then pay) is
kept generic on purpose so the patterns stay the focus.

Not a toy: every pattern below is wired into the actual request path (not a
separate "demo" endpoint), has tests that fail if it regresses, and runs
against real Postgres/Redis/Kafka via `docker compose up`.

## The five patterns

| Pattern | Where | Why it's there |
|---|---|---|
| **Idempotency keys** | [`app/services/idempotency.py`](app/services/idempotency.py) | `POST /orders` retried after a timed-out response (client never saw the 201) must not create a second order. Reservation runs via a DB unique-constraint INSERT, not a naive existence check; see the module docstring for the race a naive version misses. |
| **Two-tier cache** | [`app/resilience/cache.py`](app/resilience/cache.py) | `GET /orders/{id}` checks an in-process TTL cache (L1) before Redis (L2) before Postgres. Same shape as a real p95 fix: L1 serves hot keys in-process, and L2 keeps a cold replica from hitting the DB for data another replica already warmed. |
| **Redis-backed rate limiting** | [`app/middleware/rate_limit.py`](app/middleware/rate_limit.py) | Sliding-window counter in Redis, shared across every replica. A per-process in-memory limiter is fine for a single instance, but silently stops working the moment you scale horizontally. It's atomic via a Lua script, since the check-then-increment is a classic TOCTOU race if done as two round trips from Python. |
| **Circuit breaker** | [`app/resilience/circuit_breaker.py`](app/resilience/circuit_breaker.py) | Wraps the payment gateway call in `pay_order`. After repeated failures it stops calling the gateway at all for a cooldown window, failing fast for the caller instead of piling up slow timeouts against a dependency that's already down. |
| **Transactional outbox** | [`app/models.py`](app/models.py) (`OutboxEvent`) + [`app/workers/outbox_relay.py`](app/workers/outbox_relay.py) | An order and the "order was created" event are written in one DB transaction, so they can never disagree: no dual-write to Postgres and Kafka, no distributed transaction. A relay polls the unpublished rows and forwards them to Kafka. Delivery is at-least-once, not exactly-once; the relay docstring explains why, and what that requires of consumers. |

## Architecture

```
Client
  │
  ▼
FastAPI  ──rate limit (Redis)──▶  routers/orders.py
  │                                     │
  │                                     ▼
  │                              services/idempotency.py  (POST /orders only)
  │                                     │
  │                                     ▼
  │                              services/order_service.py
  │                                 │        │
  │                     two-tier cache    circuit breaker
  │                     (L1 + Redis)      → payment_gateway.py
  │                                     │
  │                                     ▼
  │                              Postgres: orders + outbox_events
  │                                          (same transaction)
  │
  └── outbox_relay.py (background task or standalone container)
           polls outbox_events → publishes to Kafka
```

## Running it

Full stack (Postgres + Redis + Kafka + API + outbox relay):

```bash
cp .env.example .env
make docker-up
curl -X POST localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d '{"customer_email":"you@example.com","items":[{"sku":"book-1","qty":1,"unit_price_cents":1999}]}'
```

API-only, no infrastructure (fake Kafka publisher, still needs Postgres and
Redis, or point `DATABASE_URL` at SQLite for a truly zero-dependency
smoke test):

```bash
pip install -r requirements-dev.txt
export KAFKA_FAKE=true
make migrate   # against whatever DATABASE_URL you've set
make dev
```

## Testing

```bash
make test    # pytest, SQLite + fakeredis + fake Kafka, no Docker needed
make cov     # pytest --cov, prints missing lines
make lint    # ruff check
```

Every resilience primitive is tested in isolation with dependency
injection rather than a live broker/cache: the circuit breaker takes an
injectable clock (no real `sleep()` in tests), the cache takes a
`fakeredis` client, and the outbox relay takes a `FakeEventPublisher` that
records what it would have sent to Kafka. CI runs this full suite on every
push; no external services required. See
[`.github/workflows/ci.yml`](.github/workflows/ci.yml).

## Load test

```bash
make docker-up
make loadtest   # Locust web UI at localhost:8089
```

`loadtest/locustfile.py` mixes order creation, reads, and payment attempts
against the real running stack. This is what actually exercises the cache
hit rate and lets you watch the circuit breaker trip if you set
`PAYMENT_GATEWAY_FAILURE_RATE` above 0 in `.env` and restart.

**Captured run**: 50 concurrent users, 45s, headless, against
`docker compose up` on a dev laptop. Not a benchmark rig; the point is the
shape of the numbers, not the absolute figures on someone else's hardware.

| Endpoint | req/s | p50 | p95 | p99 | failures |
|---|---|---|---|---|---|
| `GET /orders/{id}` | 104.9 | 8ms | 210ms | 260ms | 0 |
| `POST /orders` | 64.3 | 19ms | 270ms | 330ms | 0 |
| `POST /orders/{id}/pay` | 19.2 | 120ms | 310ms | 370ms | 0 |
| **aggregate** | **188.4** | **16ms** | **260ms** | **320ms** | **0 / 6582** |

The read path's p50 of 8ms vs. the write paths' 19-120ms shows the
two-tier cache doing its job: most reads never reach Postgres. `pay`'s
higher latency is expected, since it's the only endpoint that calls the
simulated (about 50ms) payment gateway. Reproduce with the command above;
raw CSVs land in `loadtest/results/`.

## What's simplified on purpose

The payment gateway is simulated (`app/services/payment_gateway.py`),
with configurable latency and failure rate, rather than a real provider
integration. The resilience patterns care about "an async call that's
sometimes slow and sometimes fails," not about a specific vendor's API.

The outbox relay's delivery guarantee is at-least-once. This repo doesn't
ship a consumer, so the corresponding idempotent-consumer half of that
contract is documented but not demonstrated with running code.

There's no auth/authz layer. It's orthogonal to the patterns this repo is
about, and every real project's auth is different enough that a generic
one would add bulk without teaching anything.

## Stack

FastAPI · SQLAlchemy 2.0 (async) · PostgreSQL · Redis · Kafka (aiokafka) ·
Alembic · pytest/pytest-asyncio · fakeredis · ruff · Locust · GitHub Actions
· Docker
