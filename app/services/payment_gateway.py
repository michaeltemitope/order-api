"""
Stand-in for a real external payment provider (Stripe, Adyen, a bank's
PromptPay-style QR gateway, ...). It's simulated on purpose, this repo is
about the resilience patterns around an unreliable dependency, not about
integrating a specific real provider, but the shape (async call, network
latency, occasional failure) is what a real client looks like from the
caller's side, which is all `order_service` and the circuit breaker around
it actually care about.
"""
import asyncio
import random
import uuid
from dataclasses import dataclass


class PaymentGatewayError(Exception):
    pass


@dataclass
class PaymentResult:
    transaction_id: str
    order_id: str
    amount_cents: int


class PaymentGatewayClient:
    def __init__(self, failure_rate: float = 0.0, latency_ms: int = 50):
        self.failure_rate = failure_rate
        self.latency_ms = latency_ms

    async def charge(self, order_id: str, amount_cents: int) -> PaymentResult:
        await asyncio.sleep(self.latency_ms / 1000)
        if random.random() < self.failure_rate:
            raise PaymentGatewayError(f"gateway declined charge for order {order_id}")
        return PaymentResult(
            transaction_id=str(uuid.uuid4()),
            order_id=order_id,
            amount_cents=amount_cents,
        )
