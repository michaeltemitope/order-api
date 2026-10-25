import pytest

from app.services import idempotency

ENDPOINT = "POST /orders"


@pytest.mark.asyncio
async def test_first_reservation_is_owned(db_session):
    result = await idempotency.reserve(db_session, "key-1", ENDPOINT, "hash-a")
    assert result.outcome == idempotency.Outcome.OWNED


@pytest.mark.asyncio
async def test_finalize_then_replay_returns_stored_response(db_session):
    await idempotency.reserve(db_session, "key-1", ENDPOINT, "hash-a")
    await idempotency.finalize(db_session, "key-1", ENDPOINT, 201, {"id": "order-1"})

    result = await idempotency.reserve(db_session, "key-1", ENDPOINT, "hash-a")

    assert result.outcome == idempotency.Outcome.REPLAY
    assert result.status_code == 201
    assert result.response_body == {"id": "order-1"}


@pytest.mark.asyncio
async def test_same_key_different_body_is_a_conflict(db_session):
    await idempotency.reserve(db_session, "key-1", ENDPOINT, "hash-a")
    await idempotency.finalize(db_session, "key-1", ENDPOINT, 201, {"id": "order-1"})

    result = await idempotency.reserve(db_session, "key-1", ENDPOINT, "hash-DIFFERENT")

    assert result.outcome == idempotency.Outcome.CONFLICT


@pytest.mark.asyncio
async def test_unfinalized_reservation_reports_in_progress(db_session):
    await idempotency.reserve(db_session, "key-1", ENDPOINT, "hash-a")
    # No finalize() call, simulates a concurrent request still running the handler.

    result = await idempotency.reserve(db_session, "key-1", ENDPOINT, "hash-a")

    assert result.outcome == idempotency.Outcome.IN_PROGRESS


@pytest.mark.asyncio
async def test_release_frees_the_key_for_retry(db_session):
    await idempotency.reserve(db_session, "key-1", ENDPOINT, "hash-a")
    await idempotency.release(db_session, "key-1", ENDPOINT)

    result = await idempotency.reserve(db_session, "key-1", ENDPOINT, "hash-a")

    assert result.outcome == idempotency.Outcome.OWNED


@pytest.mark.asyncio
async def test_same_key_different_endpoint_does_not_collide(db_session):
    result_a = await idempotency.reserve(db_session, "key-1", "POST /orders", "hash-a")
    result_b = await idempotency.reserve(db_session, "key-1", "POST /orders/x/pay", "hash-a")

    assert result_a.outcome == idempotency.Outcome.OWNED
    assert result_b.outcome == idempotency.Outcome.OWNED


def test_hash_request_is_stable_regardless_of_key_order():
    a = idempotency.hash_request({"email": "x@y.com", "items": [1, 2]})
    b = idempotency.hash_request({"items": [1, 2], "email": "x@y.com"})
    assert a == b


def test_hash_request_differs_for_different_bodies():
    a = idempotency.hash_request({"email": "x@y.com"})
    b = idempotency.hash_request({"email": "z@y.com"})
    assert a != b
