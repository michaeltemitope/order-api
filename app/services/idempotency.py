"""
Idempotency-Key handling, Stripe-style, with a real reservation step,
this is the part naive implementations skip and get wrong under concurrency.

A naive version checks "does this key exist?", and if not, runs the handler.
Two requests with the same *new* key arriving concurrently both pass that
check before either has written anything, and both run the handler,
exactly the double-charge/double-order bug idempotency keys exist to
prevent.

The fix: reserve the key with an INSERT before doing anything else. The
table's primary key (key, endpoint) makes the second concurrent INSERT fail
with an IntegrityError instead of silently succeeding, the database
resolves the race, not application logic. The loser is told the request is
already in flight rather than being allowed to run it a second time.

Lifecycle for a given key:
  1. reserve()   INSERT placeholder row. Three outcomes:
                   - OWNED      no prior row, caller may proceed and must
                                call finalize() or release() when done.
                   - REPLAY     a prior row has the *same* request hash and
                                already finished, return its stored
                                response, do not re-run the handler.
                   - IN_PROGRESS / CONFLICT  a prior row exists that the
                                caller does not own (still running, or a
                                different request body under the same key).
  2. finalize()  caller ran the handler; store the real response.
  3. release()   caller's handler raised; delete the reservation so a
                  retry with the same key can attempt it again.
"""
import enum
import hashlib
import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy import delete, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import IdempotencyKey

_PLACEHOLDER_STATUS = 0  # sentinel: reservation exists but not finalized yet


class Outcome(str, enum.Enum):
    OWNED = "owned"
    REPLAY = "replay"
    IN_PROGRESS = "in_progress"
    CONFLICT = "conflict"


@dataclass
class ReserveResult:
    outcome: Outcome
    status_code: int | None = None
    response_body: dict | None = None


def hash_request(body: Any) -> str:
    canonical = json.dumps(body, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def reserve(db: AsyncSession, key: str, endpoint: str, request_hash: str) -> ReserveResult:
    row = IdempotencyKey(
        key=key,
        endpoint=endpoint,
        request_hash=request_hash,
        status_code=_PLACEHOLDER_STATUS,
        response_body={},
    )
    db.add(row)
    try:
        await db.commit()
        return ReserveResult(outcome=Outcome.OWNED)
    except IntegrityError:
        await db.rollback()

    existing = await db.get(IdempotencyKey, (key, endpoint))
    if existing is None:
        # Extremely unlikely race: the conflicting row was deleted (via
        # release()) between our failed insert and this read. Tell the
        # caller to retry rather than looping here.
        return ReserveResult(outcome=Outcome.IN_PROGRESS)

    if existing.status_code == _PLACEHOLDER_STATUS:
        return ReserveResult(outcome=Outcome.IN_PROGRESS)

    if existing.request_hash != request_hash:
        return ReserveResult(outcome=Outcome.CONFLICT)

    return ReserveResult(
        outcome=Outcome.REPLAY,
        status_code=existing.status_code,
        response_body=existing.response_body,
    )


async def finalize(db: AsyncSession, key: str, endpoint: str, status_code: int, response_body: dict) -> None:
    await db.execute(
        update(IdempotencyKey)
        .where(IdempotencyKey.key == key, IdempotencyKey.endpoint == endpoint)
        .values(status_code=status_code, response_body=response_body)
    )
    await db.commit()


async def release(db: AsyncSession, key: str, endpoint: str) -> None:
    await db.execute(delete(IdempotencyKey).where(IdempotencyKey.key == key, IdempotencyKey.endpoint == endpoint))
    await db.commit()
