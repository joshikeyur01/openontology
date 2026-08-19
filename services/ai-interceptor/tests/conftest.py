"""Shared fixtures.

The Redis-backed tests need a real server: the whole point of the sliding
window is that it is atomic *inside Redis*, and a fake that reimplements the
Lua in Python would test the fake. ``OO_TEST_REDIS_URL`` points at one.

Locally the suite skips those tests when no server is configured, so ``pytest
tests -q`` still works on a laptop with nothing running. CI sets
``OO_TEST_REQUIRE_REDIS=1``, which turns the skip into a failure — otherwise a
misconfigured workflow would report green while silently exercising none of the
shared-state code.
"""

from __future__ import annotations

import asyncio
import os

import pytest

TEST_REDIS_URL_VAR = "OO_TEST_REDIS_URL"
REQUIRE_REDIS_VAR = "OO_TEST_REQUIRE_REDIS"


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """anyio's pytest plugin runs the async tests; asyncio is the only target."""
    return "asyncio"


@pytest.fixture(scope="session")
def redis_url() -> str:
    url = os.environ.get(TEST_REDIS_URL_VAR, "").strip()
    required = os.environ.get(REQUIRE_REDIS_VAR, "").strip().lower() in {"1", "true", "yes"}

    if not url:
        if required:
            pytest.fail(
                f"{REQUIRE_REDIS_VAR} is set but {TEST_REDIS_URL_VAR} is empty: "
                "the shared-state tests would be skipped silently"
            )
        pytest.skip(f"{TEST_REDIS_URL_VAR} is not set")

    try:
        asyncio.run(_ping(url))
    except Exception as exc:  # pragma: no cover - environment dependent
        if required:
            pytest.fail(f"{TEST_REDIS_URL_VAR}={url} is unreachable: {exc}")
        pytest.skip(f"{TEST_REDIS_URL_VAR}={url} is unreachable: {exc}")

    return url


async def _ping(url: str) -> None:
    import redis.asyncio as aioredis

    client = aioredis.Redis.from_url(url, socket_connect_timeout=2, socket_timeout=2)
    try:
        await client.ping()
    finally:
        await client.aclose()


@pytest.fixture()
def clean_redis(redis_url: str):
    """Give every test an empty keyspace so counters and quotas start at zero."""
    asyncio.run(_flush(redis_url))
    return redis_url


async def _flush(url: str) -> None:
    import redis.asyncio as aioredis

    client = aioredis.Redis.from_url(url)
    try:
        await client.flushdb()
    finally:
        await client.aclose()
