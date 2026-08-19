"""Redis-backed quota and idempotency, at the object level.

Every test here that says "two workers" builds two independent objects against
one Redis, because that is exactly what four uvicorn workers are: separate
processes, separate Python objects, one shared server. A test that reused a
single limiter would pass against the process-local implementation too, and so
would prove nothing.

``test_multi_worker.py`` makes the same argument at the HTTP layer.
"""

from __future__ import annotations

import asyncio

import pytest
import redis.asyncio as aioredis

from app.idempotency import PlanStoreUnavailable, RedisPlanStore
from app.redis_state import RedisSlidingWindowLimiter

#: Somewhere nothing is listening, for the failure-policy tests.
DEAD_REDIS_URL = "redis://127.0.0.1:6390/0"

pytestmark = pytest.mark.anyio


def make_limiter(url: str, *, limit_window: int = 60, fail_open: bool = True) -> RedisSlidingWindowLimiter:
    client = aioredis.Redis.from_url(url, socket_timeout=2, socket_connect_timeout=2, decode_responses=True)
    return RedisSlidingWindowLimiter(
        client,
        window_seconds=limit_window,
        key_prefix="quota:",
        op_timeout=2.0,
        fail_open=fail_open,
    )


def make_plan_store(url: str, *, ttl_seconds: int = 3600, fail_open: bool = False) -> RedisPlanStore:
    client = aioredis.Redis.from_url(url, socket_timeout=2, socket_connect_timeout=2, decode_responses=True)
    return RedisPlanStore(
        client,
        key_prefix="plan:",
        ttl_seconds=ttl_seconds,
        op_timeout=2.0,
        fail_open=fail_open,
    )


# ---------------------------------------------------------------------------
# Quota
# ---------------------------------------------------------------------------


async def test_quota_is_shared_between_two_limiters(clean_redis: str) -> None:
    worker_a = make_limiter(clean_redis)
    worker_b = make_limiter(clean_redis)

    verdicts = []
    for index in range(10):
        limiter = worker_a if index % 2 == 0 else worker_b
        allowed, _, _ = await limiter.check("lic_shared", limit=6)
        verdicts.append(allowed)

    # Six admitted in total, not six per limiter: this is the assertion that
    # fails against the process-local implementation.
    assert verdicts.count(True) == 6
    assert verdicts.count(False) == 4
    assert verdicts[:6] == [True] * 6


async def test_concurrent_checks_never_exceed_the_limit(clean_redis: str) -> None:
    """The check-and-claim must be atomic, not read-then-write.

    Twenty requests land at once across four limiters. A GET-then-SET limiter
    lets every in-flight request read the same count and admit itself; the Lua
    script cannot.
    """
    limiters = [make_limiter(clean_redis) for _ in range(4)]
    limit = 7

    results = await asyncio.gather(
        *(limiters[i % len(limiters)].check("lic_race", limit=limit) for i in range(20))
    )

    assert sum(1 for allowed, _, _ in results if allowed) == limit


async def test_remaining_counts_down_and_retry_after_is_reported(clean_redis: str) -> None:
    limiter = make_limiter(clean_redis, limit_window=60)

    first = await limiter.check("lic_headers", limit=3)
    assert first == (True, 2, 0.0)

    await limiter.check("lic_headers", limit=3)
    await limiter.check("lic_headers", limit=3)

    allowed, remaining, retry_after = await limiter.check("lic_headers", limit=3)
    assert allowed is False
    assert remaining == 0
    # The oldest hit was moments ago, so the window reopens in just under 60s.
    assert 55 < retry_after <= 60


async def test_window_expires(clean_redis: str) -> None:
    """A one second window really does reopen after one second."""
    limiter = make_limiter(clean_redis, limit_window=1)

    assert (await limiter.check("lic_expiry", limit=1))[0] is True
    assert (await limiter.check("lic_expiry", limit=1))[0] is False

    await asyncio.sleep(1.1)
    assert (await limiter.check("lic_expiry", limit=1))[0] is True


async def test_quota_is_scoped_per_licence(clean_redis: str) -> None:
    limiter = make_limiter(clean_redis)

    assert (await limiter.check("lic_one", limit=1))[0] is True
    assert (await limiter.check("lic_one", limit=1))[0] is False
    assert (await limiter.check("lic_two", limit=1))[0] is True


async def test_quota_keys_are_namespaced_away_from_engine_state(clean_redis: str) -> None:
    limiter = make_limiter(clean_redis)
    await limiter.check("lic_keyspace", limit=5)

    client = aioredis.Redis.from_url(clean_redis, decode_responses=True)
    try:
        keys = await client.keys("*")
    finally:
        await client.aclose()

    assert keys == ["quota:lic_keyspace"]
    # Never inside the Go engine's twin:/twinindex:/twinalarm:/dedupe: space.
    assert not any(key.startswith(("twin", "dedupe:")) for key in keys)


async def test_quota_keys_carry_a_ttl(clean_redis: str) -> None:
    limiter = make_limiter(clean_redis, limit_window=60)
    await limiter.check("lic_ttl", limit=5)

    client = aioredis.Redis.from_url(clean_redis, decode_responses=True)
    try:
        ttl = await client.pttl("quota:lic_ttl")
    finally:
        await client.aclose()

    assert 0 < ttl <= 60_000


async def test_quota_fails_open_when_redis_is_unreachable() -> None:
    limiter = make_limiter(DEAD_REDIS_URL)

    allowed, remaining, retry_after = await limiter.check("lic_outage", limit=5)

    assert allowed is True
    assert retry_after == 0.0
    assert remaining == 5
    assert limiter.stats.failed_open == 1
    assert limiter.stats.errors == 1


async def test_quota_can_be_configured_to_fail_closed() -> None:
    limiter = make_limiter(DEAD_REDIS_URL, fail_open=False)

    allowed, remaining, retry_after = await limiter.check("lic_outage", limit=5)

    assert allowed is False
    assert remaining == 0
    assert retry_after > 0
    assert limiter.stats.failed_open == 0


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


async def test_plan_survives_between_two_stores(clean_redis: str) -> None:
    worker_a = make_plan_store(clean_redis)
    worker_b = make_plan_store(clean_redis)

    key = ("northwind-aerospace", "evt_shared_0001")
    await worker_a.put(key, {"plan_id": "plan_abc", "commands": []})

    replayed = await worker_b.get(key)

    assert replayed == {"plan_id": "plan_abc", "commands": []}
    assert worker_b.stats.hits == 1


async def test_plan_keys_are_scoped_by_tenant_and_encoded(clean_redis: str) -> None:
    store = make_plan_store(clean_redis)

    assert store.key_for(("acme", "evt_1")) == "plan:v1:acme:evt_1"
    # A caller cannot smuggle a separator into the event id to alias onto
    # another key.
    assert store.key_for(("acme", "a:b")) == "plan:v1:acme:a%3Ab"
    assert store.key_for(("acme", "evt_1")) != store.key_for(("other", "evt_1"))


async def test_first_writer_wins(clean_redis: str) -> None:
    worker_a = make_plan_store(clean_redis)
    worker_b = make_plan_store(clean_redis)
    key = ("northwind-aerospace", "evt_race_0002")

    await worker_a.put(key, {"plan_id": "plan_first"})
    await worker_b.put(key, {"plan_id": "plan_second"})

    assert (await worker_a.get(key))["plan_id"] == "plan_first"
    assert worker_b.stats.conflicts == 1


async def test_plan_records_expire(clean_redis: str) -> None:
    store = make_plan_store(clean_redis, ttl_seconds=120)
    key = ("northwind-aerospace", "evt_ttl_0003")
    await store.put(key, {"plan_id": "plan_ttl"})

    client = aioredis.Redis.from_url(clean_redis, decode_responses=True)
    try:
        ttl = await client.pttl(store.key_for(key))
    finally:
        await client.aclose()

    assert 0 < ttl <= 120_000


async def test_unparseable_record_is_treated_as_a_miss(clean_redis: str) -> None:
    store = make_plan_store(clean_redis)
    key = ("northwind-aerospace", "evt_poison_0004")

    client = aioredis.Redis.from_url(clean_redis, decode_responses=True)
    try:
        await client.set(store.key_for(key), "{not json")
    finally:
        await client.aclose()

    assert await store.get(key) is None
    assert store.stats.read_errors == 1


async def test_idempotency_fails_closed_when_redis_is_unreachable() -> None:
    store = make_plan_store(DEAD_REDIS_URL)

    with pytest.raises(PlanStoreUnavailable):
        await store.get(("northwind-aerospace", "evt_outage_0005"))

    assert store.stats.failed_closed == 1


async def test_idempotency_can_be_configured_to_fail_open() -> None:
    store = make_plan_store(DEAD_REDIS_URL, fail_open=True)

    assert await store.get(("northwind-aerospace", "evt_outage_0006")) is None
    assert store.stats.failed_open == 1


async def test_failed_write_never_discards_a_paid_for_plan() -> None:
    """put() is best effort: the inference is already spent."""
    store = make_plan_store(DEAD_REDIS_URL)

    await store.put(("northwind-aerospace", "evt_outage_0007"), {"plan_id": "plan_x"})

    assert store.stats.write_errors == 1
    assert store.stats.stores == 0
