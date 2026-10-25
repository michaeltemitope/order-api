"""
A minimal async circuit breaker: CLOSED -> OPEN -> HALF_OPEN -> CLOSED.

  CLOSED      calls go through normally; consecutive failures are counted.
  OPEN        calls are rejected immediately (CircuitOpenError), no attempt
              is made against the downstream, this is the whole point: stop
              hammering a service that's already struggling, and fail fast
              for the caller instead of piling up slow timeouts.
  HALF_OPEN   after `recovery_seconds`, the next call is allowed through as
              a probe. Success -> CLOSED (reset). Failure -> OPEN again
              (reset the recovery timer).

Deliberately hand-rolled instead of pulling in pybreaker: the point of this
repo is to demonstrate understanding the pattern, and the whole thing is
~60 lines. A real production system with many breakers would reach for a
library instead.
"""
import asyncio
import enum
import time
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


class CircuitState(str, enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised instead of calling the wrapped function while the breaker is
    OPEN. Callers should treat this the same as "downstream unavailable"
    and fail fast (e.g. return 503) rather than retry immediately."""


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int,
        recovery_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_seconds = recovery_seconds
        self._clock = clock

        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._opened_at: float | None = None
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        return self._state

    async def _transition_if_recovered(self) -> None:
        if self._state is CircuitState.OPEN and self._opened_at is not None:
            if self._clock() - self._opened_at >= self.recovery_seconds:
                self._state = CircuitState.HALF_OPEN

    async def call(self, fn: Callable[[], Awaitable[T]]) -> T:
        async with self._lock:
            await self._transition_if_recovered()
            if self._state is CircuitState.OPEN:
                raise CircuitOpenError(
                    f"circuit is open, retry after "
                    f"{self.recovery_seconds - (self._clock() - self._opened_at):.1f}s"
                )
            # CLOSED and HALF_OPEN both allow exactly this one call through;
            # the lock ensures only one HALF_OPEN probe runs at a time.

        try:
            result = await fn()
        except Exception:
            await self._on_failure()
            raise
        else:
            await self._on_success()
            return result

    async def _on_success(self) -> None:
        async with self._lock:
            self._consecutive_failures = 0
            self._state = CircuitState.CLOSED
            self._opened_at = None

    async def _on_failure(self) -> None:
        async with self._lock:
            self._consecutive_failures += 1
            if self._state is CircuitState.HALF_OPEN or self._consecutive_failures >= self.failure_threshold:
                self._state = CircuitState.OPEN
                self._opened_at = self._clock()
