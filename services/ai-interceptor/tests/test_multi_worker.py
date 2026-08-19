"""The shared state, exercised through HTTP.

Two ``TestClient`` instances over two independently built applications stand in
for two uvicorn workers: separate application objects, separate limiters,
separate plan stores, one Redis. Both assertions below fail against the
process-local implementations, which is the point of having them.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.security import digest

from .test_intercept import CRITICAL_PAYLOAD, ENTERPRISE_KEY
from .test_shared_state import DEAD_REDIS_URL, make_plan_store


def redis_settings(url: str, **overrides) -> Settings:
    values: dict = {
        "redis_url": url,
        "require_shared_state": True,
        "environment": "test",
        "llm_simulated_latency_ms": 0,
    }
    values.update(overrides)
    return Settings(**values)


def write_registry(path: Path, *, quota_per_minute: int) -> None:
    """A registry with a small quota, so the proof needs nine requests, not 601."""
    path.write_text(
        json.dumps(
            [
                {
                    "key_id": "lic_probe",
                    "key_digest": digest(ENTERPRISE_KEY),
                    "tenant": "northwind-aerospace",
                    "tier": "ENTERPRISE",
                    "quota_per_minute": quota_per_minute,
                    "expires_at": (datetime.now(tz=timezone.utc) + timedelta(days=1)).isoformat(),
                    "features": ["ai.intercept", "ai.autopilot"],
                }
            ]
        ),
        encoding="utf-8",
    )


def test_health_and_readiness_report_the_redis_backend(clean_redis: str) -> None:
    with TestClient(create_app(redis_settings(clean_redis))) as client:
        assert client.get("/healthz").json()["state_backend"] == "redis"

        ready = client.get("/readyz")
        assert ready.status_code == 200

        body = ready.json()
        assert body["status"] == "ready"
        assert body["redis"]["reachable"] is True
        assert body["rate_limiter"]["backend"] == "redis"
        assert body["plan_store"]["backend"] == "redis"


def test_a_second_worker_replays_the_first_workers_plan(clean_redis: str) -> None:
    """A duplicate mutation must not be re-planned, or re-billed, by a peer."""
    settings = redis_settings(clean_redis)

    with TestClient(create_app(settings)) as worker_a:
        first = worker_a.post(
            "/v1/intercept", json=CRITICAL_PAYLOAD, headers={"X-License-Key": ENTERPRISE_KEY}
        )
        assert first.status_code == 200
        assert first.headers["X-Idempotent-Replay"] == "false"

    with TestClient(create_app(settings)) as worker_b:
        second = worker_b.post(
            "/v1/intercept", json=CRITICAL_PAYLOAD, headers={"X-License-Key": ENTERPRISE_KEY}
        )

    assert second.status_code == 200
    assert second.headers["X-Idempotent-Replay"] == "true"
    assert second.json()["plan_id"] == first.json()["plan_id"]


def test_quota_is_a_total_not_a_per_worker_allowance(clean_redis: str, tmp_path: Path) -> None:
    registry = tmp_path / "licenses.json"
    write_registry(registry, quota_per_minute=5)
    settings = redis_settings(clean_redis, license_registry_path=registry)

    statuses: list[int] = []
    with TestClient(create_app(settings)) as worker_a, TestClient(create_app(settings)) as worker_b:
        for index in range(9):
            worker = worker_a if index % 2 == 0 else worker_b
            statuses.append(worker.get("/v1/license", headers={"X-License-Key": ENTERPRISE_KEY}).status_code)

    assert statuses.count(200) == 5
    assert statuses.count(429) == 4


def test_intercept_fails_closed_when_the_store_is_unreachable(clean_redis: str) -> None:
    with TestClient(create_app(redis_settings(clean_redis))) as client:
        # Break the store after startup so the lifespan ping still succeeds and
        # only the request path sees the outage.
        client.app.state.plan_cache = make_plan_store(DEAD_REDIS_URL)

        response = client.post(
            "/v1/intercept", json=CRITICAL_PAYLOAD, headers={"X-License-Key": ENTERPRISE_KEY}
        )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "idempotency_unavailable"
    assert response.headers["Retry-After"] == "5"


def test_startup_refuses_to_run_without_the_shared_state_it_was_promised() -> None:
    settings = redis_settings(DEAD_REDIS_URL)

    with pytest.raises(RuntimeError, match="unreachable"):
        with TestClient(create_app(settings)):
            pass


def test_requiring_shared_state_without_a_url_is_a_configuration_error() -> None:
    with pytest.raises(ValueError, match="OO_REQUIRE_SHARED_STATE"):
        Settings(require_shared_state=True, redis_url=None)
