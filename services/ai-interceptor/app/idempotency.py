"""Plan idempotency, keyed by ``(tenant, event_id)``.

This is the Python counterpart of ``idempotency_filter.go`` in the resolution
engine, one layer further down the pipeline and with the opposite failure
policy.

Kafka delivers at least once and the command worker retries, so the same
mutation reaches ``POST /v1/intercept`` more than once as a matter of routine.
Every duplicate that gets past this store costs a model inference — billed to
the operator, charged to the tenant — and produces a second command sequence
carrying ISOLATE and SHUTDOWN actions against a live asset. Holding the record
in a per-process ``OrderedDict`` means replica B has never heard of the plan
replica A just issued, so with four workers roughly three quarters of
redeliveries are re-planned.

**Failure policy: fail closed.** When Redis is unreachable, ``get`` raises and
the request is rejected with a retryable 503 rather than being planned blind.
The Go filter chooses the opposite for telemetry ingestion, and correctly so:
there, dropping a sample loses data that no retry recovers, while a duplicate is
absorbed by a monotonic cache write and a state machine that suppresses repeated
transitions. Here nothing is lost by refusing — the caller is a Kafka consumer
whose offset is uncommitted, so it retries — and a duplicate is not absorbed by
anything downstream. When the cheap failure is "try again in a moment" and the
expensive one is "bill twice and issue a second shutdown order", the policy
follows.

The write side is deliberately best effort. Once a plan has been paid for,
discarding it helps nobody, and a failed write cannot cause a duplicate on its
own: the next delivery is gated by the read above, which is still failing
closed. ``OO_IDEMPOTENCY_FAIL_OPEN=true`` inverts the read policy for operators
who would rather over-bill than shed load.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import quote

import redis.asyncio as aioredis

from .config import Settings
from .redis_state import TRANSPORT_ERRORS

logger = logging.getLogger(__name__)

#: Marks a stored plan's schema. A future change to the CommandSequence model
#: must not resurrect plans written by an older build, so the version is part of
#: the key rather than something the reader has to tolerate.
PLAN_RECORD_VERSION = "v1"


class PlanStoreUnavailable(RuntimeError):
    """Raised when idempotency cannot be established and the policy is closed.

    ``main`` maps it to a 503 with ``Retry-After``; the caller is expected to
    redeliver, which is what a Kafka consumer does anyway.
    """


class PlanStore(Protocol):
    """The contract the intercept endpoint depends on."""

    async def get(self, key: tuple[str, str]) -> dict[str, Any] | None: ...

    async def put(self, key: tuple[str, str], value: dict[str, Any]) -> None: ...


@dataclass
class PlanStoreStats:
    """Counters for the readiness endpoint."""

    backend: str
    fail_open: bool
    hits: int = 0
    misses: int = 0
    stores: int = 0
    conflicts: int = 0
    write_errors: int = 0
    read_errors: int = 0
    failed_open: int = 0
    failed_closed: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "backend": self.backend,
            "fail_open": self.fail_open,
            "hits": self.hits,
            "misses": self.misses,
            "stores": self.stores,
            "conflicts": self.conflicts,
            "write_errors": self.write_errors,
            "read_errors": self.read_errors,
            "failed_open": self.failed_open,
            "failed_closed": self.failed_closed,
        }


class PlanCache:
    """Bounded idempotency cache keyed by ``(tenant, event_id)``.

    Process-local, and therefore correct only for a single worker with no
    replicas. It remains the default so tests and a laptop run need no
    infrastructure; :func:`build_plan_store` selects the Redis implementation as
    soon as ``OO_REDIS_URL`` is set.
    """

    def __init__(self, max_entries: int) -> None:
        self._max = max_entries
        self._data: OrderedDict[tuple[str, str], dict[str, Any]] = OrderedDict()
        self._lock = asyncio.Lock()
        self._stats = PlanStoreStats(backend="memory", fail_open=True)

    @property
    def stats(self) -> PlanStoreStats:
        return self._stats

    async def get(self, key: tuple[str, str]) -> dict[str, Any] | None:
        if self._max == 0:
            return None
        async with self._lock:
            value = self._data.get(key)
            if value is not None:
                self._data.move_to_end(key)
                self._stats.hits += 1
            else:
                self._stats.misses += 1
            return value

    async def put(self, key: tuple[str, str], value: dict[str, Any]) -> None:
        if self._max == 0:
            return
        async with self._lock:
            self._data[key] = value
            self._data.move_to_end(key)
            self._stats.stores += 1
            while len(self._data) > self._max:
                self._data.popitem(last=False)


class RedisPlanStore:
    """Idempotency records shared by every replica.

    Interface-compatible with :class:`PlanCache`; the endpoint's ``await
    cache.get(...)`` / ``await cache.put(...)`` call sites are unchanged.

    Bounding is by TTL rather than by entry count. An LRU bound is the wrong
    shape for shared state — replicas cannot agree on which entry is least
    recently used without another round trip — and a TTL expresses the actual
    invariant: a mutation is a duplicate for as long as its redelivery window
    is open, and a distinct decision afterwards.
    """

    def __init__(
        self,
        client: aioredis.Redis,
        *,
        key_prefix: str = "plan:",
        ttl_seconds: int = 86_400,
        op_timeout: float = 2.0,
        fail_open: bool = False,
    ) -> None:
        self._client = client
        self._prefix = key_prefix
        self._ttl_ms = int(ttl_seconds * 1000)
        self._timeout = op_timeout
        self._fail_open = fail_open
        self._stats = PlanStoreStats(backend="redis", fail_open=fail_open)

    @property
    def stats(self) -> PlanStoreStats:
        return self._stats

    def key_for(self, key: tuple[str, str]) -> str:
        """Render ``plan:<version>:<tenant>:<event_id>``.

        Both variable segments are percent-encoded. The tenant is server
        authoritative — it comes from the licence registry, never from the
        request body — so encoding the event id is what stops a caller
        smuggling a separator into it and aliasing onto one of its own other
        events. Cross-tenant aliasing is impossible by construction.
        """
        tenant, event_id = key
        return f"{self._prefix}{PLAN_RECORD_VERSION}:{quote(tenant, safe='')}:{quote(event_id, safe='')}"

    async def get(self, key: tuple[str, str]) -> dict[str, Any] | None:
        try:
            raw = await asyncio.wait_for(self._client.get(self.key_for(key)), timeout=self._timeout)
        except TRANSPORT_ERRORS as exc:
            self._stats.read_errors += 1
            if self._fail_open:
                self._stats.failed_open += 1
                logger.warning(
                    "idempotency store unreachable; planning without a duplicate check",
                    extra={"tenant": key[0], "event_id": key[1], "error": str(exc)},
                )
                return None
            self._stats.failed_closed += 1
            logger.error(
                "idempotency store unreachable; refusing to plan",
                extra={"tenant": key[0], "event_id": key[1], "error": str(exc)},
            )
            raise PlanStoreUnavailable(
                "Idempotency store is unreachable; the request cannot be served without "
                "risking a duplicate plan."
            ) from exc

        if raw is None:
            self._stats.misses += 1
            return None

        try:
            record = json.loads(raw)
        except (TypeError, ValueError):
            # A key we cannot parse is a bug or a collision, not an outage.
            # Treat it as a miss and let the fresh plan overwrite nothing —
            # the NX on write leaves the poisoned value in place until its TTL
            # expires, which is visible in the counters rather than silent.
            self._stats.read_errors += 1
            logger.error("discarding unparseable plan record", extra={"tenant": key[0], "event_id": key[1]})
            return None

        if not isinstance(record, dict):
            self._stats.read_errors += 1
            logger.error("discarding plan record of unexpected type", extra={"tenant": key[0], "event_id": key[1]})
            return None

        self._stats.hits += 1
        return record

    async def put(self, key: tuple[str, str], value: dict[str, Any]) -> None:
        """Store the plan, first writer wins.

        ``NX`` makes a stored plan immutable for its lifetime. Two replicas that
        both miss the read and both plan — the residual race this design does
        not close, see the README — then agree on which plan every later replay
        returns, instead of the answer changing depending on who wrote last.
        """
        payload = json.dumps(value, separators=(",", ":"))
        try:
            stored = await asyncio.wait_for(
                self._client.set(self.key_for(key), payload, px=self._ttl_ms, nx=True),
                timeout=self._timeout,
            )
        except TRANSPORT_ERRORS as exc:
            # Never raise: the inference is already paid for, and refusing to
            # return it would waste it and trigger a retry that pays again.
            self._stats.write_errors += 1
            logger.warning(
                "failed to persist plan for replay",
                extra={"tenant": key[0], "event_id": key[1], "error": str(exc)},
            )
            return

        if stored:
            self._stats.stores += 1
        else:
            self._stats.conflicts += 1
            logger.info(
                "plan already stored by another replica; keeping the first writer's plan",
                extra={"tenant": key[0], "event_id": key[1]},
            )


def build_plan_store(settings: Settings, client: aioredis.Redis | None) -> PlanStore:
    """Pick the idempotency implementation that matches the deployment."""
    if client is None:
        logger.warning(
            "idempotency state is process-local; correct for a single worker only "
            "(set OO_REDIS_URL before scaling out)"
        )
        return PlanCache(settings.idempotency_cache_size)

    return RedisPlanStore(
        client,
        key_prefix=settings.plan_key_prefix,
        ttl_seconds=settings.idempotency_ttl_seconds,
        op_timeout=settings.redis_op_timeout_seconds,
        fail_open=settings.idempotency_fail_open,
    )


def plan_store_stats(store: PlanStore) -> dict[str, object]:
    """Best-effort counters for whichever store is installed."""
    stats = getattr(store, "stats", None)
    if isinstance(stats, PlanStoreStats):
        return stats.as_dict()
    return {"backend": "memory", "fail_open": True}
