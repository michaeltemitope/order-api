"""
Load test against the real stack (docker-compose up), not against mocks,
the point is to see the two-tier cache and circuit breaker behave under
actual concurrency, not just pass unit tests.

Usage:
    docker compose up --build -d
    make loadtest              # opens the Locust web UI on :8089
    # or headless:
    locust -f loadtest/locustfile.py --host http://localhost:8000 \
        --headless -u 50 -r 10 -t 60s --csv=results/run1

See README.md's "Load test" section for a captured run and what to read
from the results.
"""
import random
import uuid

from locust import HttpUser, between, task


class OrderApiUser(HttpUser):
    wait_time = between(0.05, 0.3)

    def on_start(self):
        self.known_order_ids: list[str] = []

    @task(3)
    def create_order(self):
        payload = {
            "customer_email": f"loadtest-{uuid.uuid4().hex[:8]}@example.com",
            "items": [{"sku": "book-1", "qty": random.randint(1, 3), "unit_price_cents": 1999}],
        }
        with self.client.post(
            "/orders",
            json=payload,
            headers={"Idempotency-Key": str(uuid.uuid4())},
            catch_response=True,
        ) as resp:
            if resp.status_code == 201:
                self.known_order_ids.append(resp.json()["id"])
                if len(self.known_order_ids) > 200:
                    self.known_order_ids.pop(0)
            elif resp.status_code == 429:
                resp.success()  # rate limiting working as designed, not a failure

    @task(5)
    def read_order(self):
        if not self.known_order_ids:
            return
        order_id = random.choice(self.known_order_ids)
        self.client.get(f"/orders/{order_id}", name="/orders/[id]")

    @task(1)
    def pay_order(self):
        if not self.known_order_ids:
            return
        order_id = random.choice(self.known_order_ids)
        with self.client.post(f"/orders/{order_id}/pay", name="/orders/[id]/pay", catch_response=True) as resp:
            if resp.status_code in (200, 409, 429, 503):
                resp.success()  # all expected outcomes under load, not failures
