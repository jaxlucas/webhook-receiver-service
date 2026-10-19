"""Phase 4 load test: a constant 2,000 events/min with 10 % duplicates and 1 % invalid requests.

    locust -f loadtest/locustfile.py --headless -H http://localhost:8080 -u 40 -r 40 -t 5m

Each user sends one request per ``40 users / (2000/60 rps)`` ≈ 1.2 s (``constant_throughput``), so
``-u 40`` gives ~33 req/s. ``scripts/load.sh`` runs the two acceptance scenarios and reads the results
from Prometheus.
"""

from __future__ import annotations

import itertools
import os
import random
import uuid

from locust import HttpUser, constant_throughput, task

TARGET_RPS = float(os.environ.get("TARGET_RPS", 2000 / 60))
USERS = int(os.environ.get("LOCUST_USERS", 40))
RUN = uuid.uuid4().hex[:8]
counter = itertools.count()
sent: list[tuple[str, dict]] = []


class Sender(HttpUser):
    wait_time = constant_throughput(TARGET_RPS / USERS)

    @task
    def send(self) -> None:
        roll = random.random()
        if roll < 0.01:
            # Invalid: event_type with a space → 422 is the expected answer.
            with self.client.post(
                "/events",
                json={"idempotency_key": "bad", "event_type": "bad type", "payload": None},
                name="POST /events (invalid)",
                catch_response=True,
            ) as response:
                if response.status_code == 422:
                    response.success()
                else:
                    response.failure(f"expected 422, got {response.status_code}")
            return
        if roll < 0.11 and sent:
            key, payload = random.choice(sent[-500:])
            name, expected = "POST /events (duplicate)", 200
        else:
            key = f"{RUN}-{next(counter)}"
            payload = {"order": key, "amount": random.randint(1, 10_000), "items": [1, 2, 3]}
            name, expected = "POST /events", 202
        with self.client.post(
            "/events",
            json={"idempotency_key": key, "event_type": "order.paid", "payload": payload},
            name=name,
            catch_response=True,
        ) as response:
            # A retried POST (connection error during a replica kill) legitimately turns into a 200.
            if response.status_code == expected or (expected == 202 and response.status_code == 200):
                response.success()
                if expected == 202:
                    # Only answered keys become duplicate candidates, so a duplicate never races its original.
                    sent.append((key, payload))
            else:
                response.failure(f"expected {expected}, got {response.status_code}")
