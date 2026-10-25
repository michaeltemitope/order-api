import pytest

from app.resilience.circuit_breaker import CircuitBreaker, CircuitOpenError, CircuitState

pytestmark = pytest.mark.asyncio


async def _ok():
    return "ok"


async def _fail():
    raise RuntimeError("downstream failed")


async def test_starts_closed(manual_clock):
    breaker = CircuitBreaker(failure_threshold=3, recovery_seconds=10, clock=manual_clock)
    assert breaker.state == CircuitState.CLOSED
    assert await breaker.call(_ok) == "ok"
    assert breaker.state == CircuitState.CLOSED


async def test_opens_after_threshold_consecutive_failures(manual_clock):
    breaker = CircuitBreaker(failure_threshold=3, recovery_seconds=10, clock=manual_clock)

    for _ in range(3):
        with pytest.raises(RuntimeError):
            await breaker.call(_fail)

    assert breaker.state == CircuitState.OPEN


async def test_open_circuit_rejects_without_calling_downstream(manual_clock):
    breaker = CircuitBreaker(failure_threshold=1, recovery_seconds=10, clock=manual_clock)
    calls = {"n": 0}

    async def counting_fail():
        calls["n"] += 1
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        await breaker.call(counting_fail)
    assert breaker.state == CircuitState.OPEN

    with pytest.raises(CircuitOpenError):
        await breaker.call(counting_fail)

    assert calls["n"] == 1  # second call was rejected before touching counting_fail


async def test_transitions_to_half_open_after_recovery_window(manual_clock):
    breaker = CircuitBreaker(failure_threshold=1, recovery_seconds=10, clock=manual_clock)

    with pytest.raises(RuntimeError):
        await breaker.call(_fail)
    assert breaker.state == CircuitState.OPEN

    manual_clock.advance(5)
    with pytest.raises(CircuitOpenError):
        await breaker.call(_ok)  # still within recovery window

    manual_clock.advance(6)  # total 11s > 10s recovery window
    assert await breaker.call(_ok) == "ok"  # half-open probe succeeds
    assert breaker.state == CircuitState.CLOSED


async def test_half_open_probe_failure_reopens_circuit(manual_clock):
    breaker = CircuitBreaker(failure_threshold=1, recovery_seconds=10, clock=manual_clock)

    with pytest.raises(RuntimeError):
        await breaker.call(_fail)
    manual_clock.advance(11)

    with pytest.raises(RuntimeError):
        await breaker.call(_fail)  # half-open probe fails

    assert breaker.state == CircuitState.OPEN

    manual_clock.advance(5)
    with pytest.raises(CircuitOpenError):
        await breaker.call(_ok)  # recovery timer was reset by the failed probe


async def test_success_resets_failure_counter(manual_clock):
    breaker = CircuitBreaker(failure_threshold=3, recovery_seconds=10, clock=manual_clock)

    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(_fail)
    assert breaker.state == CircuitState.CLOSED  # under threshold

    await breaker.call(_ok)  # resets consecutive-failure count

    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(_fail)
    assert breaker.state == CircuitState.CLOSED  # would have opened without the reset
