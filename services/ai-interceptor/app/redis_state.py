"""Shared quota state.

The licence quota is the commercial layer's meter: it decides how much billable
work a subscription may cause. Held in a per-process ``deque`` it is correct for
exactly one worker and silently wrong for two — four uvicorn workers each admit
the full 600 requests/minute, so an ENTERPRISE tenant gets 2400 and the meter
that the price list is built on is off by the replica count. Nothing fails
loudly; the bill is simply wrong.

This module moves the window into Redis, where every replica meters against one
authority. The design mirrors ``idempotency_filter.go`` in the Go engine:

* a flat, colon-suffixed key prefix that cannot shadow the engine's ``twin:``,
  ``twinindex:``, ``twinalarm:`` or ``dedupe:`` keyspace;
* a TTL on every key, so the working set is bounded by traffic in the last
  window rather than by the number of licences ever seen, and every key stays
  eligible for eviction under the compose topology's ``volatile-lru`` policy;
* an explicit, configurable failure policy with lock-free counters behind it.

**Failure policy: fail open.** When Redis is unreachable the limiter admits the
request. A quota exists to protect revenue and capacity, and the cost of getting
it wrong for the duration of an outage is bounded — some requests go unmetered
and the tenant is under-billed. The alternative converts a metering outage into
a total outage of the paid API for every tenant at once, which is a far larger
incident than the one it prevents. This is the same availability-first reasoning
the Go filter uses for telemetry ingestion, and the opposite of the choice made
for idempotency in ``app/idempotency.py``, where a wrong answer costs an
inference and can actuate an asset twice.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from .config import Settings
from .security import RateLimiter, SlidingWindowLimiter

logger = logging.getLogger(__name__)

#: Errors that mean "Redis did not answer", as opposed to "Redis answered no".
#: Only these trigger the failure policy; a malformed reply is a bug and is
#: allowed to surface.
TRANSPORT_ERRORS = (RedisError, OSError, asyncio.TimeoutError)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


def create_redis_client(settings: Settings) -> aioredis.Redis | None:
    """Build the shared connection pool, or ``None`` when Redis is not configured.

    Construction is lazy — redis-py does not dial until the first command — so
    this is safe to call at import time, before uvicorn has forked its workers
    and before an event loop exists. Each worker process therefore ends up with
    its own pool against the same server, which is exactly what is wanted.
    """
    if not settings.redis_url:
        return None

    timeout = settings.redis_op_timeout_seconds
    return aioredis.Redis.from_url(
        settings.redis_url,
        max_connections=settings.redis_pool_size,
        socket_timeout=timeout,
        socket_connect_timeout=timeout,
        # A quota check that hangs must not hold a request open for longer than
        # the check is worth; retrying a timed-out command would double that.
        retry_on_timeout=False,
        decode_responses=True,
        health_check_interval=30,
    )


async def ping_redis(client: aioredis.Redis | None, timeout: float) -> None:
    """Raise if the shared state store cannot be reached."""
    if client is None:
        raise RuntimeError("no redis client configured")
    await asyncio.wait_for(client.ping(), timeout=timeout)


async def close_redis(client: aioredis.Redis | None) -> None:
    """Release the pool on shutdown, never raising into the lifespan."""
    if client is None:
        return
    try:
        await client.aclose()
    except Exception:  # pragma: no cover - shutdown must not fail
        logger.warning("failed to close redis connection pool", exc_info=True)


# ---------------------------------------------------------------------------
# Sliding window
# ---------------------------------------------------------------------------

#: Sliding-window admission, evaluated entirely inside Redis.
#:
#: The whole point is that this is one round trip and one atomic step. A
#: GET-then-SET limiter is not a limiter at all under concurrency: every replica
#: reads the same count before any of them writes, and the quota is exceeded by
#: as many requests as happen to be in flight. Here the trim, the count and the
#: admission all happen inside a single script invocation, so at most ``limit``
#: members can ever exist in the window no matter how many replicas race.
#:
#: The clock is Redis's own ``TIME``, not the caller's. Window boundaries are
#: therefore immune to clock skew between replicas, which would otherwise let a
#: tenant widen its window by hitting whichever host is running fast.
#:
#:      KEYS[1] quota:<key_id>
#:      ARGV    window_ms, limit, unique member id
#:
#: Returns ``{allowed, remaining, retry_after_ms}``.
SLIDING_WINDOW_SCRIPT = """
local key    = KEYS[1]
local window = tonumber(ARGV[1])
local limit  = tonumber(ARGV[2])
local member = ARGV[3]

local clock = redis.call('TIME')
local now   = (tonumber(clock[1]) * 1000) + math.floor(tonumber(clock[2]) / 1000)

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local used = redis.call('ZCARD', key)

if used >= limit then
  local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  local retry = 0
  if oldest[2] then
    retry = (tonumber(oldest[2]) + window) - now
    if retry < 0 then retry = 0 end
  end
  redis.call('PEXPIRE', key, window)
  return {0, 0, retry}
end

redis.call('ZADD', key, now, member)
redis.call('PEXPIRE', key, window)
return {1, limit - used - 1, 0}
"""


@dataclass
class LimiterStats:
    """Counters for the readiness endpoint, mirroring IdempotencyStats in Go."""

    backend: str
    fail_open: bool
    checked: int = 0
    allowed: int = 0
    throttled: int = 0
    failed_open: int = 0
    errors: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "backend": self.backend,
            "fail_open": self.fail_open,
            "checked": self.checked,
            "allowed": self.allowed,
            "throttled": self.throttled,
            "failed_open": self.failed_open,
            "errors": self.errors,
        }


class RedisSlidingWindowLimiter:
    """Per-licence request quota over a window shared by every replica.

    Interface-compatible with :class:`~app.security.SlidingWindowLimiter`; the
    middleware call site is unchanged.
    """

    def __init__(
        self,
        client: aioredis.Redis,
        *,
        window_seconds: int = 60,
        key_prefix: str = "quota:",
        op_timeout: float = 2.0,
        fail_open: bool = True,
    ) -> None:
        self._client = client
        self._window_ms = int(window_seconds * 1000)
        self._prefix = key_prefix
        self._timeout = op_timeout
        self._fail_open = fail_open
        self._script = client.register_script(SLIDING_WINDOW_SCRIPT)
        self._stats = LimiterStats(backend="redis", fail_open=fail_open)

    def key_for(self, key: str) -> str:
        return f"{self._prefix}{key}"

    @property
    def stats(self) -> LimiterStats:
        return self._stats

    async def check(self, key: str, limit: int) -> tuple[bool, int, float]:
        """Record a hit and report ``(allowed, remaining, retry_after)``."""
        self._stats.checked += 1
        # ZADD is a set insert: two hits landing on the same millisecond would
        # collapse into one member and silently refund a request, so each hit
        # carries its own identity.
        member = uuid.uuid4().hex

        try:
            allowed, remaining, retry_ms = await asyncio.wait_for(
                self._script(
                    keys=[self.key_for(key)],
                    args=[self._window_ms, limit, member],
                ),
                timeout=self._timeout,
            )
        except TRANSPORT_ERRORS as exc:
            self._stats.errors += 1
            if not self._fail_open:
                self._stats.throttled += 1
                logger.error("quota store unreachable; rejecting request", extra={"error": str(exc)})
                return False, 0, float(self._window_ms) / 1000.0
            self._stats.failed_open += 1
            self._stats.allowed += 1
            logger.warning(
                "quota store unreachable; admitting request unmetered",
                extra={"license_key_id": key, "error": str(exc)},
            )
            # Remaining is unknowable while the store is down; reporting the
            # full limit keeps the header well-formed and is advisory only.
            return True, limit, 0.0

        if int(allowed) == 1:
            self._stats.allowed += 1
            return True, int(remaining), 0.0

        self._stats.throttled += 1
        return False, 0, float(retry_ms) / 1000.0


def build_limiter(settings: Settings, client: aioredis.Redis | None) -> RateLimiter:
    """Pick the quota implementation that matches the deployment."""
    if client is None:
        logger.warning(
            "quota state is process-local; correct for a single worker only "
            "(set OO_REDIS_URL before scaling out)"
        )
        return SlidingWindowLimiter(window_seconds=settings.rate_limit_window_seconds)

    return RedisSlidingWindowLimiter(
        client,
        window_seconds=settings.rate_limit_window_seconds,
        key_prefix=settings.quota_key_prefix,
        op_timeout=settings.redis_op_timeout_seconds,
        fail_open=settings.rate_limit_fail_open,
    )


def limiter_stats(limiter: RateLimiter) -> dict[str, object]:
    """Best-effort counters for whichever limiter is installed."""
    stats = getattr(limiter, "stats", None)
    if isinstance(stats, LimiterStats):
        return stats.as_dict()
    return {"backend": "memory", "fail_open": True}
