"""OpenOntology command worker — Module 2, Part B: the Kafka closure loop.

The Go resolution engine turns raw telemetry into Enriched Context Payloads and
publishes them to ``ontology.mutations``. Until now that was the end of the
line: something had to read those payloads, decide what to do, and get an
instruction back to the plant. This service is that missing half of the circuit.

    telemetry.raw ──▶ resolution-engine ──▶ ontology.mutations
                                                   │
                                                   ▼
                                         command-worker (this module)
                                            1. licence gatekeeper
                                            2. AI interceptor routing
                                            3. strict command modelling
                                                   │
                                                   ▼
                                            ontology.commands ──▶ actuators

Four properties matter more than anything else in this file:

*   **Nothing billable happens before authorisation.** Every mutation is passed
    through :func:`validate_enterprise_license` before it reaches the AI layer.
*   **Commands are deterministic under replay.** Kafka is at-least-once, so the
    same mutation can be processed twice. ``command_id`` is a UUIDv5 derived
    from the source event, which lets an actuator discard the second copy
    instead of isolating a compressor twice.
*   **A bad message never wedges a partition.** Anything unprocessable is
    dead-lettered with a structured reason and the offset is committed.
*   **A broken dependency never loses a message.** The dead-letter queue is for
    faults in the *message*. A fault in the environment — an expired key, a
    misrouted endpoint, an interceptor at its rate limit — leaves the offset
    uncommitted and the record where it is, however many times it recurs. The
    two are not distinguished by HTTP status but by the question "would this
    record succeed unchanged once someone fixed the deployment?"

``OO_INTERCEPTOR_MODE`` selects who does the planning. ``http`` is the default
and the product: the commercial interceptor, over the network, under a real
subscription. ``stub`` swaps in the in-process simulator so the whole loop can
be exercised offline, in tests, and on an aeroplane.

Run it with ``uvicorn command_worker:app`` or ``python command_worker.py``.
The FastAPI surface is for operators and for synchronous, on-demand closure;
the consumer loop underneath it is what actually drains the topic.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import math
import random
import sys
import threading
import time
import uuid
from collections import OrderedDict
from contextlib import asynccontextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from enum import Enum
from pathlib import Path
from typing import Any, AsyncIterator, Final, Literal
from uuid import UUID

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.errors import KafkaError, KafkaTimeoutError
from fastapi import Depends, FastAPI, Header, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    ValidationError,
    field_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    REGISTRY,
    Counter,
    Histogram,
    generate_latest,
)
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

__version__ = "1.0.0"

SERVICE: Final[str] = "openontology-command-worker"

#: Schema pinned on every message published to ``ontology.commands``.
COMMAND_SCHEMA_VERSION: Final[str] = "openontology.command.v1"

#: Major version of the inbound mutation contract this worker understands.
MUTATION_SCHEMA_PREFIX: Final[str] = "openontology.mutation.v2"

# Both versions are accepted for the length of a rollout. A fleet is not
# upgraded atomically — the engine and this worker restart independently — and
# refusing v1 the moment v2 ships would dead-letter every mutation still in
# flight from an engine that has not been restarted yet. Those are real alarms
# about real assets, discarded over a version string.
SUPPORTED_MUTATION_SCHEMAS: Final[tuple[str, ...]] = (
    "openontology.mutation.v1",
    "openontology.mutation.v2",
)

#: Namespace for deterministic command identifiers. Derived rather than
#: hard-coded so it is reproducible from the URL alone.
COMMAND_NAMESPACE: Final[UUID] = uuid.uuid5(
    uuid.NAMESPACE_URL, "https://openontology.io/ns/commands/v1"
)


# ---------------------------------------------------------------------------
# Structured logging
# ---------------------------------------------------------------------------

_request_id: ContextVar[str] = ContextVar("request_id", default="-")
_tenant: ContextVar[str] = ContextVar("tenant", default="-")
_event_id: ContextVar[str] = ContextVar("event_id", default="-")

_RESERVED: Final[frozenset[str]] = frozenset(
    logging.LogRecord("", 0, "", 0, "", None, None).__dict__
) | {"asctime", "message", "taskName"}

logger = logging.getLogger("openontology.command_worker")


def bind_log_context(
    request_id: str = "-", tenant: str = "-", event_id: str = "-"
) -> tuple[Token[str], Token[str], Token[str]]:
    """Bind correlation identifiers for the duration of one unit of work."""
    return _request_id.set(request_id), _tenant.set(tenant), _event_id.set(event_id)


def reset_log_context(tokens: tuple[Token[str], Token[str], Token[str]]) -> None:
    """Restore the previously bound correlation identifiers."""
    request_token, tenant_token, event_token = tokens
    _request_id.reset(request_token)
    _tenant.reset(tenant_token)
    _event_id.reset(event_token)


def current_request_id() -> str:
    return _request_id.get()


class JsonFormatter(logging.Formatter):
    """Render a LogRecord as a single JSON object.

    Industrial operators aggregate logs centrally; free text is unusable at that
    scale. Anything passed via ``logger.info(..., extra={...})`` becomes a
    first-class field.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "service": SERVICE,
            "message": record.getMessage(),
            "request_id": _request_id.get(),
            "tenant": _tenant.get(),
            "event_id": _event_id.get(),
        }

        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str, separators=(",", ":"))


def configure_logging(level: str) -> None:
    """Install the JSON formatter on the root logger, once."""
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "aiokafka"):
        named = logging.getLogger(name)
        named.handlers.clear()
        named.propagate = True

    # aiokafka logs every heartbeat and fetch at DEBUG; keep it at WARNING
    # unless the operator explicitly asked for DEBUG on the whole process.
    if level.upper() != "DEBUG":
        logging.getLogger("aiokafka").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class Settings(BaseSettings):
    """Immutable process configuration, entirely environment driven."""

    model_config = SettingsConfigDict(
        env_prefix="OO_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    # --- service identity -------------------------------------------------
    service_name: str = SERVICE
    environment: str = "local"
    log_level: str = "INFO"
    docs_enabled: bool = True
    http_host: str = "0.0.0.0"
    http_port: int = Field(default=8082, ge=1, le=65535)

    # --- kafka ------------------------------------------------------------
    kafka_bootstrap_servers: str = "localhost:9092"
    mutations_topic: str = "ontology.mutations"
    commands_topic: str = "ontology.commands"
    commands_dlq_topic: str = "ontology.commands.dlq"
    consumer_group: str = "ontology-command-worker"
    consumer_workers: int = Field(default=2, ge=0, le=32)
    auto_offset_reset: Literal["earliest", "latest"] = "earliest"
    kafka_session_timeout_ms: int = Field(default=30_000, ge=1_000, le=300_000)
    kafka_request_timeout_ms: int = Field(default=40_000, ge=1_000, le=300_000)
    kafka_max_poll_records: int = Field(default=50, ge=1, le=500)
    producer_linger_ms: int = Field(default=10, ge=0, le=1_000)
    consumer_restart_backoff_seconds: float = Field(default=3.0, gt=0, le=60)

    # --- commercial gatekeeper -------------------------------------------
    license_header: str = "X-License-Key"
    #: Licence presented for mutations that carry none of their own. Kafka
    #: records emitted by the open-core engine are unauthenticated by design,
    #: so the operator of this worker supplies the subscription under which the
    #: closure loop runs.
    license_key: SecretStr | None = None
    license_registry_path: Path | None = None
    license_cache_ttl_seconds: int = Field(default=300, ge=1, le=86_400)
    license_cache_max_entries: int = Field(default=1024, ge=1, le=100_000)
    #: Subscription plans entitled to the actuation loop, in the vocabulary the
    #: commercial interceptor uses (see :data:`CANONICAL_PLANS`). ``STANDARD`` is
    #: the entry point; ``ENTERPRISE`` inherits it. Legacy spellings such as
    #: ``SMALL_BUSINESS`` are folded in by :func:`normalise_plan`.
    allowed_plans: str = "STANDARD,ENTERPRISE"

    # --- AI interceptor routing ------------------------------------------
    #: ``http`` calls the real commercial interceptor and is the default: it is
    #: the only mode that exercises the product. ``stub`` swaps in the
    #: in-process simulator (and the ``/v1/interceptor-stub`` endpoint), which
    #: keeps the whole topology offline for development and tests.
    interceptor_mode: Literal["stub", "http"] = "http"
    interceptor_url: str = "http://ai-interceptor:8000"
    interceptor_path: str = "/v1/intercept"

    # --- tier routing -----------------------------------------------------
    # The sharpest edge of the open-core boundary. A tier in
    # enterprise_interceptor_tiers is planned by the GraphRAG two-agent layer
    # instead of the standard interceptor; every other tier keeps the standard
    # one. Both speak the same contract, so this is a URL swap rather than a
    # second code path — which is exactly the substitutability docs/OPEN-CORE.md
    # promises anyone reimplementing the paid layer.
    #
    # Unset enterprise_interceptor_url and the routing disappears entirely: one
    # interceptor for every tier, which is the single-vendor deployment.
    enterprise_interceptor_url: str = ""
    enterprise_interceptor_tiers: str = "ENTERPRISE"
    enterprise_license_key: str = ""

    #: Licence introspection endpoint, used by the boot-time preflight.
    interceptor_license_path: str = "/v1/license"
    #: Read timeout. Deliberately longer than the interceptor's own
    #: ``OO_LLM_TIMEOUT_SECONDS`` so a slow model surfaces as the interceptor's
    #: classifiable 502 rather than as an opaque client-side timeout.
    interceptor_timeout_seconds: float = Field(default=35.0, gt=0, le=300)
    interceptor_connect_timeout_seconds: float = Field(default=5.0, gt=0, le=60)
    interceptor_simulated_latency_ms: int = Field(default=15, ge=0, le=5_000)

    # --- interceptor backpressure ----------------------------------------
    #: Ceiling on in-flight interceptor calls across every consumer worker.
    interceptor_max_concurrency: int = Field(default=8, ge=1, le=256)
    #: Steady-state call ceiling. ``0`` means "learn it": the preflight reads
    #: ``quota_per_minute`` off ``GET /v1/license`` and every response's
    #: ``X-RateLimit-Limit`` header keeps it current, so the worker paces itself
    #: to whatever the presented subscription actually bought.
    interceptor_max_rpm: int = Field(default=0, ge=0, le=600_000)
    #: Fraction of the discovered quota to actually use. The headroom leaves
    #: room for the operator's own ``make plan`` calls on the same key.
    interceptor_rate_margin: float = Field(default=0.9, gt=0.0, le=1.0)
    #: Upper bound on an honoured ``Retry-After``. A hostile or buggy proxy must
    #: not be able to park a consumer for an hour.
    interceptor_retry_after_cap_seconds: float = Field(default=30.0, gt=0, le=300)

    # --- interceptor preflight -------------------------------------------
    #: Prove at boot that both halves resolve the configured key to the same
    #: tenant and tier, and that it carries ``ai.intercept``.
    interceptor_preflight: bool = True
    #: Refuse to consume when the preflight fails, rather than logging and
    #: draining anyway. Off by default so a slow interceptor start-up cannot
    #: wedge the topology; on for estates that would rather stop than
    #: mis-attribute a command.
    interceptor_preflight_strict: bool = False
    interceptor_preflight_attempts: int = Field(default=10, ge=1, le=100)

    # --- resilience -------------------------------------------------------
    max_attempts: int = Field(default=4, ge=1, le=10)
    retry_base_seconds: float = Field(default=0.25, gt=0, le=30)
    retry_max_seconds: float = Field(default=5.0, gt=0, le=120)
    #: Ceiling on the in-place pause a consumer takes after a transient failure
    #: before it rewinds and re-reads the same offset.
    transient_backoff_max_seconds: float = Field(default=30.0, gt=0, le=300)

    # --- command shaping --------------------------------------------------
    max_commands: int = Field(default=6, ge=1, le=20)
    default_deadline_seconds: int = Field(default=900, ge=1, le=604_800)

    @field_validator("log_level")
    @classmethod
    def _normalise_log_level(cls, value: str) -> str:
        level = value.strip().upper()
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if level not in allowed:
            raise ValueError(f"log_level must be one of {sorted(allowed)}")
        return level

    @field_validator("mutations_topic", "commands_topic", "commands_dlq_topic")
    @classmethod
    def _non_empty_topic(cls, value: str) -> str:
        topic = value.strip()
        if not topic:
            raise ValueError("kafka topic names must not be empty")
        return topic

    @field_validator("interceptor_url")
    @classmethod
    def _trim_url(cls, value: str) -> str:
        return value.strip().rstrip("/")

    @field_validator("interceptor_path", "interceptor_license_path")
    @classmethod
    def _absolute_path(cls, value: str) -> str:
        path = value.strip()
        if not path.startswith("/"):
            raise ValueError(f"interceptor paths must start with '/': {value!r}")
        return path

    @property
    def bootstrap_servers(self) -> list[str]:
        return [item.strip() for item in self.kafka_bootstrap_servers.split(",") if item.strip()]

    @property
    def entitled_plans(self) -> frozenset[str]:
        return frozenset(
            normalise_plan(item) for item in self.allowed_plans.split(",") if item.strip()
        )

    @property
    def interceptor_endpoint(self) -> str:
        return f"{self.interceptor_url}{self.interceptor_path}"

    @property
    def enterprise_endpoint(self) -> str:
        if not self.enterprise_interceptor_url:
            return ""
        return f"{self.enterprise_interceptor_url.rstrip('/')}{self.interceptor_path}"

    @property
    def enterprise_tiers(self) -> frozenset[str]:
        return frozenset(
            tier.strip().upper()
            for tier in self.enterprise_interceptor_tiers.split(",")
            if tier.strip()
        )

    def routes_to_enterprise(self, tier: str) -> bool:
        """Whether a subscription of this tier is planned by the GraphRAG layer."""
        return bool(self.enterprise_endpoint) and tier.strip().upper() in self.enterprise_tiers

    @property
    def interceptor_license_endpoint(self) -> str:
        return f"{self.interceptor_url}{self.interceptor_license_path}"

    def model_post_init(self, __context: Any) -> None:
        if self.commands_topic == self.mutations_topic:
            raise ValueError(
                "commands_topic must differ from mutations_topic or the worker will consume its own output"
            )
        if self.commands_dlq_topic == self.commands_topic:
            raise ValueError("commands_dlq_topic must differ from commands_topic")
        if not self.bootstrap_servers:
            raise ValueError("kafka_bootstrap_servers must list at least one broker")
        if not self.entitled_plans:
            raise ValueError("allowed_plans must list at least one subscription plan")
        if self.interceptor_mode == "http" and self.license_key is None:
            # Caught here rather than on the first mutation: without a key every
            # record would fail authentication identically, and a configuration
            # error that presents as a stream of per-message failures is a
            # configuration error that gets diagnosed as a data problem.
            raise ValueError(
                "OO_LICENSE_KEY must be set when OO_INTERCEPTOR_MODE=http: the interceptor "
                "authenticates every call, so without a key every mutation would stall. "
                "Set OO_INTERCEPTOR_MODE=stub to run the closure loop offline instead."
            )


_settings_lock = threading.Lock()
_settings: Settings | None = None


def get_settings() -> Settings:
    """Process-wide settings singleton, built once under a lock."""
    global _settings
    if _settings is None:
        with _settings_lock:
            if _settings is None:
                _settings = Settings()
    return _settings


# ---------------------------------------------------------------------------
# Commercial gatekeeper: subscriptions, atomic cache, validation
# ---------------------------------------------------------------------------


def key_digest(plaintext: str) -> str:
    """SHA-256 hex digest of a plaintext licence key.

    Keys are compared as digests so the plaintext subscription key never sits in
    process memory beyond the call that presented it.
    """
    return hashlib.sha256(plaintext.strip().encode("utf-8")).hexdigest()


#: The one subscription vocabulary the whole topology speaks.
#:
#: These are the *interceptor's* tier names, not this worker's original ones.
#: The commercial interceptor is the billing-facing system of record: it is what
#: ``GET /v1/license`` introspects, what ``X-License-Tier`` reports on every
#: response, and what its per-tier quotas are keyed on. A worker that calls it
#: over HTTP has to agree with it, so the worker moves rather than the seam.
CANONICAL_PLANS: Final[frozenset[str]] = frozenset({"ENTERPRISE", "STANDARD", "COMMUNITY"})

#: Legacy plan names accepted on input and folded into the canonical set.
#:
#: ``SMALL_BUSINESS`` was this worker's name for the tier the interceptor calls
#: ``STANDARD``; they were always the same subscription (``lic_standard_demo``,
#: tenant ``rotterdam-polymers``) described twice. Deployments configured with
#: the old spelling keep working — ``OO_ALLOWED_PLANS=SMALL_BUSINESS,ENTERPRISE``
#: still entitles exactly the subscriptions it always did.
PLAN_ALIASES: Final[dict[str, str]] = {
    "SMALL_BUSINESS": "STANDARD",
    "SMALLBUSINESS": "STANDARD",
    "SMB": "STANDARD",
    "BUSINESS": "STANDARD",
    "FREE": "COMMUNITY",
    "OSS": "COMMUNITY",
}

#: Plans permitted to issue an irreversible command without a human ack. Mirrors
#: the interceptor's ``ai.autopilot`` feature, which only its ENTERPRISE tier
#: carries; see ``_policy_notes`` in the interceptor's ``main.py``.
AUTONOMOUS_PLANS: Final[frozenset[str]] = frozenset({"ENTERPRISE"})


def normalise_plan(value: str) -> str:
    """Fold a plan or tier name into the canonical vocabulary.

    Unknown names pass through upper-cased rather than raising: a billing export
    is allowed to invent a tier this build has never heard of, and the right
    response is to fail the *entitlement* check against ``OO_ALLOWED_PLANS``, not
    to refuse to parse the registry.
    """
    plan = value.strip().upper().replace("-", "_").replace(" ", "_")
    return PLAN_ALIASES.get(plan, plan)


@dataclass(frozen=True)
class Subscription:
    """One customer's entitlement to the closure loop."""

    key_id: str
    key_digest: str
    tenant: str
    plan: str
    seats: int
    active: bool
    expires_at: datetime

    def is_expired(self, now: datetime | None = None) -> bool:
        return (now or datetime.now(tz=timezone.utc)) >= self.expires_at

    def autonomous_execution_allowed(self) -> bool:
        """Only the enterprise plan may issue commands without a human ack."""
        return self.plan in AUTONOMOUS_PLANS


class LicenseRejected(Exception):
    """Structured 401-equivalent raised whenever authorisation fails.

    Carrying the HTTP status on the exception lets one type serve both the
    FastAPI surface (rendered as a JSON 401) and the Kafka consumer (logged and
    dead-lettered), without the consumer having to know anything about HTTP.
    """

    status_code: int = status.HTTP_401_UNAUTHORIZED

    def __init__(self, code: str, message: str, *, hint: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.hint = hint

    def as_dict(self) -> dict[str, Any]:
        body: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.hint:
            body["hint"] = self.hint
        return body


class SubscriptionRegistry:
    """Subscription system of record.

    The built-in fixtures mirror the demo keys served by the commercial
    interceptor so one key works across the whole topology. A production
    deployment points ``OO_LICENSE_REGISTRY_PATH`` at the billing system's
    export; the lookup contract below is all the gatekeeper depends on.
    """

    def __init__(self, subscriptions: list[Subscription]) -> None:
        self._by_digest: dict[str, Subscription] = {sub.key_digest: sub for sub in subscriptions}

    def __len__(self) -> int:
        return len(self._by_digest)

    def lookup(self, digest: str) -> Subscription | None:
        record = self._by_digest.get(digest)
        if record is None:
            return None
        # Constant-time confirmation: the dict hit already leaked nothing, but
        # comparing explicitly keeps the code honest if the store changes.
        if not hmac.compare_digest(record.key_digest, digest):
            return None
        return record

    @classmethod
    def from_file(cls, path: Path) -> SubscriptionRegistry:
        """Load subscriptions from JSON.

        Each entry accepts either ``key`` (plaintext, hashed on load) or
        ``key_digest`` (pre-hashed, preferred for production), and names the
        subscription level as either ``plan`` or ``tier`` — the two services
        spell the same field differently, and one billing export has to be able
        to feed both. Values are folded through :func:`normalise_plan`.
        """
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except OSError as exc:
            raise RuntimeError(f"cannot read licence registry {path}: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"licence registry {path} is not valid JSON: {exc}") from exc

        if not isinstance(raw, list):
            raise RuntimeError(f"licence registry {path} must contain a JSON array")

        subscriptions: list[Subscription] = []
        for index, entry in enumerate(raw):
            try:
                digest = entry.get("key_digest") or key_digest(entry["key"])
                level = entry.get("plan") or entry["tier"]
                subscriptions.append(
                    Subscription(
                        key_id=str(entry["key_id"]),
                        key_digest=str(digest),
                        tenant=str(entry["tenant"]),
                        plan=normalise_plan(str(level)),
                        seats=int(entry.get("seats", 1)),
                        active=bool(entry.get("active", True)),
                        expires_at=datetime.fromisoformat(entry["expires_at"]),
                    )
                )
            except (KeyError, TypeError, ValueError, AttributeError) as exc:
                raise RuntimeError(f"licence registry {path}, entry {index}: {exc}") from exc
        return cls(subscriptions)

    @classmethod
    def fixtures(cls) -> SubscriptionRegistry:
        """Built-in subscriptions for local development and the compose stack.

        Key-for-key identical to ``LicenseRegistry.demo()`` in the interceptor's
        ``app/security.py``: same key ids, same tenants, same tiers, same expiry
        posture. When ``OO_INTERCEPTOR_MODE=http`` the worker presents its key to
        that registry, so any drift between the two tables is a live
        misconfiguration — :class:`InterceptorPreflight` checks the agreement at
        boot rather than leaving it to be discovered one dead letter at a time.

        ``lic_suspended_demo`` has no interceptor counterpart. The interceptor
        models expiry but not arrears, so a suspended subscription is stopped
        here, before anything billable is requested — which is where a
        non-payment stop belongs anyway.
        """
        far_future = datetime.now(tz=timezone.utc) + timedelta(days=365)
        yesterday = datetime.now(tz=timezone.utc) - timedelta(days=1)
        return cls(
            [
                Subscription(
                    key_id="lic_enterprise_demo",
                    key_digest=key_digest("oo-live-enterprise-demo-key"),
                    tenant="northwind-aerospace",
                    plan="ENTERPRISE",
                    seats=500,
                    active=True,
                    expires_at=far_future,
                ),
                Subscription(
                    key_id="lic_standard_demo",
                    key_digest=key_digest("oo-live-standard-demo-key"),
                    tenant="rotterdam-polymers",
                    plan="STANDARD",
                    seats=25,
                    active=True,
                    expires_at=far_future,
                ),
                Subscription(
                    key_id="lic_community_demo",
                    key_digest=key_digest("oo-live-community-demo-key"),
                    tenant="community-user",
                    plan="COMMUNITY",
                    seats=1,
                    active=True,
                    expires_at=far_future,
                ),
                Subscription(
                    key_id="lic_expired_demo",
                    key_digest=key_digest("oo-live-expired-demo-key"),
                    tenant="lapsed-customer",
                    plan="STANDARD",
                    seats=25,
                    active=True,
                    expires_at=yesterday,
                ),
                Subscription(
                    key_id="lic_suspended_demo",
                    key_digest=key_digest("oo-live-suspended-demo-key"),
                    tenant="arrears-industrial",
                    plan="STANDARD",
                    seats=25,
                    active=False,
                    expires_at=far_future,
                ),
            ]
        )


@dataclass
class _CacheEntry:
    subscription: Subscription | None
    expires_at: float


class LicenseCache:
    """Atomic, TTL-bounded, LRU-capped subscription cache.

    ``resolve`` performs check-and-load as one indivisible operation under a
    single lock, so N concurrent requests presenting the same key produce
    exactly one registry lookup and can never observe a half-populated entry.
    Misses are cached too: without negative caching, a stream of bad keys turns
    into a stream of registry lookups, which is precisely the shape of an
    enumeration attack.
    """

    def __init__(self, registry: SubscriptionRegistry, ttl_seconds: int, max_entries: int) -> None:
        self._registry = registry
        self._ttl = float(ttl_seconds)
        self._max_entries = max_entries
        self._entries: OrderedDict[str, _CacheEntry] = OrderedDict()
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def resolve(self, digest: str) -> tuple[Subscription | None, bool]:
        """Return ``(subscription_or_None, served_from_cache)``."""
        now = time.monotonic()
        with self._lock:
            entry = self._entries.get(digest)
            if entry is not None and entry.expires_at > now:
                self._entries.move_to_end(digest)
                self._hits += 1
                return entry.subscription, True

            self._misses += 1
            subscription = self._registry.lookup(digest)
            self._entries[digest] = _CacheEntry(subscription, now + self._ttl)
            self._entries.move_to_end(digest)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)
                self._evictions += 1
            return subscription, False

    def invalidate(self, digest: str) -> None:
        with self._lock:
            self._entries.pop(digest, None)

    def stats(self) -> dict[str, Any]:
        with self._lock:
            total = self._hits + self._misses
            return {
                "entries": len(self._entries),
                "max_entries": self._max_entries,
                "ttl_seconds": self._ttl,
                "hits": self._hits,
                "misses": self._misses,
                "evictions": self._evictions,
                "hit_ratio": round(self._hits / total, 6) if total else 0.0,
                "registry_size": len(self._registry),
            }


class LicenseGatekeeper:
    """The enterprise API gatekeeper layer.

    Four checks run on every payload, in the order that fails cheapest first:
    presence, authenticity, liveness (active and unexpired) and entitlement.
    """

    def __init__(self, settings: Settings, cache: LicenseCache) -> None:
        self._settings = settings
        self._cache = cache
        self._default_key = (
            settings.license_key.get_secret_value() if settings.license_key is not None else None
        )

    @property
    def cache(self) -> LicenseCache:
        return self._cache

    def extract_key(self, payload: dict[str, Any], *, allow_fallback: bool = True) -> str | None:
        """Pull the subscription key out of a payload, header map or config.

        Accepts ``license_key`` at the top level, nested under ``license`` or
        ``headers``, and finally falls back to the worker's configured key so
        that engine-produced Kafka records — which carry no credential of their
        own — run under the operator's subscription.

        ``allow_fallback=False`` disables that last step. HTTP callers must
        always present their own key: letting an anonymous request inherit the
        worker's subscription would turn the gatekeeper into a formality.
        """
        candidates: list[Any] = [payload.get("license_key"), payload.get("licence_key")]

        for container_name in ("license", "licence", "headers", "meta"):
            container = payload.get(container_name)
            if isinstance(container, dict):
                for field_name in ("license_key", "licence_key", "key", self._settings.license_header):
                    candidates.append(container.get(field_name))
                    candidates.append(container.get(field_name.lower()))

        for candidate in candidates:
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()

        if allow_fallback and self._default_key and self._default_key.strip():
            return self._default_key.strip()
        return None

    def authorize(self, payload: dict[str, Any], *, allow_fallback: bool = True) -> Subscription:
        """Resolve and validate the subscription behind a payload.

        Raises :class:`LicenseRejected` — a structured 401 — on every failure
        path. Returns the entitled :class:`Subscription` otherwise.
        """
        if not isinstance(payload, dict):
            raise LicenseRejected(
                "payload_not_an_object",
                "The licence gatekeeper expects a JSON object.",
            )

        presented = self.extract_key(payload, allow_fallback=allow_fallback)
        if not presented:
            raise LicenseRejected(
                "license_key_missing",
                f"No subscription key on the payload and no {self._settings.license_header} configured.",
                hint=f"Set OO_LICENSE_KEY, or send the {self._settings.license_header} header.",
            )

        digest = key_digest(presented)
        subscription, cached = self._cache.resolve(digest)

        if subscription is None:
            logger.warning(
                "rejected unknown subscription key",
                extra={"reason": "license_key_invalid", "cache_hit": cached},
            )
            raise LicenseRejected(
                "license_key_invalid",
                "The supplied subscription key is not recognised.",
                hint="Contact sales@openontology.io for a small-business or enterprise key.",
            )

        if not subscription.active:
            raise LicenseRejected(
                "license_suspended",
                f"Subscription {subscription.key_id} for tenant {subscription.tenant} is suspended.",
                hint="Settle the outstanding balance to reactivate the subscription.",
            )

        if subscription.is_expired():
            raise LicenseRejected(
                "license_expired",
                f"Subscription for tenant {subscription.tenant} expired at "
                f"{subscription.expires_at.isoformat()}.",
                hint="Renew the subscription to restore the actuation loop.",
            )

        entitled = self._settings.entitled_plans
        if subscription.plan not in entitled:
            raise LicenseRejected(
                "plan_not_entitled",
                f"Plan {subscription.plan} does not include the command closure loop.",
                hint=f"Upgrade to one of: {', '.join(sorted(entitled))}.",
            )

        logger.debug(
            "subscription authorised",
            extra={
                "license_key_id": subscription.key_id,
                "plan": subscription.plan,
                "cache_hit": cached,
            },
        )
        return subscription


_gatekeeper_lock = threading.Lock()
_gatekeeper: LicenseGatekeeper | None = None


def build_gatekeeper(settings: Settings) -> LicenseGatekeeper:
    """Construct a gatekeeper and its atomic cache from settings."""
    registry = (
        SubscriptionRegistry.from_file(settings.license_registry_path)
        if settings.license_registry_path
        else SubscriptionRegistry.fixtures()
    )
    cache = LicenseCache(
        registry,
        ttl_seconds=settings.license_cache_ttl_seconds,
        max_entries=settings.license_cache_max_entries,
    )
    return LicenseGatekeeper(settings, cache)


def get_gatekeeper() -> LicenseGatekeeper:
    """Process-wide gatekeeper singleton."""
    global _gatekeeper
    if _gatekeeper is None:
        with _gatekeeper_lock:
            if _gatekeeper is None:
                _gatekeeper = build_gatekeeper(get_settings())
    return _gatekeeper


def validate_enterprise_license(payload: dict) -> bool:
    """Strict gate in front of every billable operation.

    Reads the subscription key from the payload (or the configured fallback),
    resolves it through the atomic cache, and confirms the subscription is
    active, unexpired and on an entitled plan — the small-business model and
    above.

    Returns ``True`` when the payload may proceed. Raises
    :class:`LicenseRejected`, a structured 401/Unauthorized equivalent, when the
    key is missing, unrecognised, suspended, expired or under-entitled. It never
    returns ``False``: a silent falsy result is far too easy to ignore at a call
    site that is the only thing standing between an unpaid caller and a
    command written to a live actuation topic.
    """
    get_gatekeeper().authorize(payload)
    return True


# ---------------------------------------------------------------------------
# Inbound contract: the Enriched Context Payload
# ---------------------------------------------------------------------------


class _Inbound(BaseModel):
    """Base for engine-produced models.

    ``extra="ignore"`` is deliberate: the open-core engine may add fields in a
    minor release and this worker must not start dead-lettering traffic when it
    does.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True, str_strip_whitespace=True)


def null_collection_is_empty(value: Any) -> Any:
    """Read a JSON ``null`` collection as an absent one.

    Go marshals a nil slice as ``null``, not ``[]``, and the resolution engine
    leaves ``parent_systems``, ``components`` and ``assigned_operators`` nil
    whenever it could not reach the ontology graph. Those payloads are
    *degraded*, which is a documented, expected state: the engine sets
    ``degraded: true`` precisely so the closure loop can act cautiously — the
    planner drops irreversible actions and asks for an inspection instead.

    Refusing them turns the one representational difference between Go and JSON
    into a dead letter, and does it to exactly the mutations that most warrant a
    human walking to the asset. ``[]`` and ``null`` mean the same thing here:
    nothing was resolved.
    """
    return [] if value is None else value


class Severity(str, Enum):
    INFO = "INFO"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class TransitionKind(str, Enum):
    RAISED = "RAISED"
    ESCALATED = "ESCALATED"
    SUSTAINED = "SUSTAINED"
    CLEARED = "CLEARED"


class SensorReading(_Inbound):
    sensor_id: str
    value: float
    unit: str | None = None
    observed_at: datetime | None = None
    age_seconds: float = 0.0


class TelemetrySnapshot(_Inbound):
    trigger: SensorReading | None = None
    readings: list[SensorReading] = Field(default_factory=list)
    captured_at: datetime | None = None
    complete: bool = False

    _readings_tolerant = field_validator("readings", mode="before")(null_collection_is_empty)


class Operator(_Inbound):
    operator_id: str
    name: str = ""
    role: str = ""
    shift: str | None = None
    contact: str | None = None
    escalation_order: int = 0


class SystemNode(_Inbound):
    node_id: str
    name: str = ""
    type: str = ""
    depth: int = 0


class OntologyContext(_Inbound):
    asset_id: str = ""
    asset_name: str = ""
    asset_class: str = ""
    site: str = ""
    criticality: str = "UNKNOWN"
    parent_systems: list[SystemNode] = Field(default_factory=list)
    components: list[str] = Field(default_factory=list)
    assigned_operators: list[Operator] = Field(default_factory=list)
    maintenance_window: str | None = None
    resolved_at: datetime | None = None
    source: str = "unknown"
    cache_hit: bool = False

    # A graph the engine could not reach yields three nil slices; see
    # :func:`null_collection_is_empty` for why that must not be a dead letter.
    _collections_tolerant = field_validator(
        "parent_systems", "components", "assigned_operators", mode="before"
    )(null_collection_is_empty)

    @property
    def primary_operator(self) -> Operator | None:
        if not self.assigned_operators:
            return None
        return min(self.assigned_operators, key=lambda op: op.escalation_order)

    @property
    def escalation_chain(self) -> list[Operator]:
        return sorted(self.assigned_operators, key=lambda op: op.escalation_order)

    @property
    def immediate_parent(self) -> SystemNode | None:
        if not self.parent_systems:
            return None
        return min(self.parent_systems, key=lambda node: node.depth)


class RuleTrigger(_Inbound):
    rule_id: str = ""
    sensor_id: str
    operator: str = ">"
    threshold: float = 0.0
    unit: str | None = None
    observed_value: float = 0.0
    exceeded_by: float = 0.0
    exceeded_pct: float = 0.0
    description: str | None = None


class MutationEnvelope(_Inbound):
    """One Enriched Context Payload drained from ``ontology.mutations``."""

    event_id: str = Field(min_length=1)
    schema_version: str
    producer: str = "ontology-resolution-engine"
    emitted_at: datetime | None = None
    asset_id: str = Field(min_length=1, max_length=128)
    transition: TransitionKind
    severity: Severity
    anomaly_active_since: datetime | None = None
    breach_count: int = 0
    rule: RuleTrigger
    telemetry_snapshot: TelemetrySnapshot = Field(default_factory=TelemetrySnapshot)
    ontology_context: OntologyContext = Field(default_factory=OntologyContext)
    degraded: bool = False
    degraded_reason: str | None = None
    source_partition: int | None = None
    source_offset: int | None = None

    @field_validator("schema_version")
    @classmethod
    def _known_schema(cls, value: str) -> str:
        # Accept every version this worker understands. The raw record is
        # forwarded to the interceptor untouched (see _body), so a v2 payload
        # reaches the planner with its flow topology intact even though the
        # fields below ignore it.
        if not value.startswith(SUPPORTED_MUTATION_SCHEMAS):
            supported = " or ".join(SUPPORTED_MUTATION_SCHEMAS)
            raise ValueError(f"unsupported schema_version {value!r}; expected {supported}")
        return value


# ---------------------------------------------------------------------------
# The AI action plan, and the command contract it becomes
# ---------------------------------------------------------------------------


class ActionType(str, Enum):
    """The actuation vocabulary understood by plant-floor executors."""

    ISOLATE = "ISOLATE"
    SHUTDOWN = "SHUTDOWN"
    SHIFT_SPEED = "SHIFT_SPEED"
    INSPECT = "INSPECT"
    SCHEDULE_MAINTENANCE = "SCHEDULE_MAINTENANCE"
    NOTIFY = "NOTIFY"
    ACKNOWLEDGE = "ACKNOWLEDGE"

    @property
    def is_irreversible(self) -> bool:
        """Actions that take load off a live asset and cannot be undone cheaply."""
        return self in {ActionType.ISOLATE, ActionType.SHUTDOWN}


class ExecutionPriority(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    LOW = "LOW"


#: The interceptor speaks a slightly richer planning vocabulary than the
#: actuators accept. Translation happens here, once, rather than in every
#: downstream executor.
ACTION_ALIASES: Final[dict[str, ActionType]] = {
    "ISOLATE": ActionType.ISOLATE,
    "SHUTDOWN": ActionType.SHUTDOWN,
    "STOP": ActionType.SHUTDOWN,
    "SHIFT_SPEED": ActionType.SHIFT_SPEED,
    "THROTTLE": ActionType.SHIFT_SPEED,
    "DERATE": ActionType.SHIFT_SPEED,
    "REDUCE_LOAD": ActionType.SHIFT_SPEED,
    "INSPECT": ActionType.INSPECT,
    "SCHEDULE_MAINTENANCE": ActionType.SCHEDULE_MAINTENANCE,
    "MAINTENANCE": ActionType.SCHEDULE_MAINTENANCE,
    "NOTIFY": ActionType.NOTIFY,
    "ALERT": ActionType.NOTIFY,
    "ACKNOWLEDGE": ActionType.ACKNOWLEDGE,
    "ACK": ActionType.ACKNOWLEDGE,
}

#: MEDIUM collapses to LOW: the actuation contract has three bands, and
#: rounding an ambiguous plan *down* keeps a mis-parsed priority from jumping
#: an instruction ahead of genuinely urgent work.
PRIORITY_ALIASES: Final[dict[str, ExecutionPriority]] = {
    "CRITICAL": ExecutionPriority.CRITICAL,
    "EMERGENCY": ExecutionPriority.CRITICAL,
    "P0": ExecutionPriority.CRITICAL,
    "HIGH": ExecutionPriority.HIGH,
    "URGENT": ExecutionPriority.HIGH,
    "P1": ExecutionPriority.HIGH,
    "MEDIUM": ExecutionPriority.LOW,
    "NORMAL": ExecutionPriority.LOW,
    "LOW": ExecutionPriority.LOW,
    "INFO": ExecutionPriority.LOW,
    "P2": ExecutionPriority.LOW,
    "P3": ExecutionPriority.LOW,
}


class PlannedAction(BaseModel):
    """One step of the plan returned by the AI interceptor."""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    sequence: int = Field(default=1, ge=1)
    target_component: str = ""
    action: str = Field(min_length=1)
    priority: str = Field(default="LOW", min_length=1)
    assigned_to: str = ""
    assigned_operator_id: str | None = None
    parameters: dict[str, Any] = Field(default_factory=dict)
    expected_effect: str = ""
    rollback: str | None = None
    deadline_seconds: int = Field(default=900, ge=0, le=604_800)


class Escalation(BaseModel):
    model_config = ConfigDict(extra="ignore")

    required: bool = False
    notify: list[str] = Field(default_factory=list)
    reason: str = ""
    sla_seconds: int = Field(default=0, ge=0)


class ActionPlan(BaseModel):
    """The interceptor's response envelope, as this worker consumes it.

    ``extra="ignore"`` lets the real commercial interceptor's richer
    ``CommandSequence`` parse unchanged alongside the in-process stub.
    """

    model_config = ConfigDict(extra="ignore")

    plan_id: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    asset_id: str = Field(min_length=1)
    tenant: str = ""
    model: str = ""
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    reasoning_summary: str = ""
    commands: list[PlannedAction] = Field(min_length=1)
    escalation: Escalation = Field(default_factory=Escalation)
    evidence: list[str] = Field(default_factory=list)
    latency_ms: float = 0.0


class CommandPayload(BaseModel):
    """The strict contract published to ``ontology.commands``.

    The first four fields are the command itself; everything after them is the
    provenance an executor needs to trust, audit and deduplicate it. ``frozen``
    and ``extra="forbid"`` together mean a command cannot be mutated after
    construction and cannot smuggle an unmodelled field past validation.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    command_id: UUID
    target_asset_id: str = Field(min_length=1, max_length=128)
    action_type: ActionType
    execution_priority: ExecutionPriority

    # --- provenance -------------------------------------------------------
    schema_version: Literal["openontology.command.v1"] = COMMAND_SCHEMA_VERSION
    issued_at: datetime
    issued_by: str = SERVICE
    tenant: str = Field(min_length=1)
    license_key_id: str = Field(min_length=1)
    source_event_id: str = Field(min_length=1)
    plan_id: str = Field(min_length=1)
    correlation_id: str = Field(min_length=1)

    # --- execution envelope ----------------------------------------------
    sequence: int = Field(ge=1)
    target_component: str | None = None
    parameters: dict[str, Any] = Field(default_factory=dict)
    expected_effect: str = ""
    rollback: str | None = None
    assigned_to: str = ""
    assigned_operator_id: str | None = None
    deadline_seconds: int = Field(ge=0, le=604_800)
    expires_at: datetime
    requires_human_approval: bool = True
    escalation_required: bool = False
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    trigger_sensor_id: str = ""
    trigger_severity: Severity = Severity.INFO
    trigger_transition: TransitionKind = TransitionKind.RAISED
    context_degraded: bool = False

    @field_validator("target_asset_id")
    @classmethod
    def _identifier_shape(cls, value: str) -> str:
        # Same constraint the Go engine enforces on asset ids: executors key
        # their equipment registries on this string.
        if not value or not value[0].isalnum():
            raise ValueError(f"target_asset_id {value!r} must start with an alphanumeric character")
        if any(char in value for char in (" ", "\t", "\n", ":", "|", "/")):
            raise ValueError(f"target_asset_id {value!r} must not contain separators or whitespace")
        return value

    def kafka_key(self) -> bytes:
        """Partition key: every command for one asset stays ordered."""
        return self.target_asset_id.encode("utf-8")

    def kafka_headers(self) -> list[tuple[str, bytes]]:
        """Headers a router can filter on without deserialising the body."""
        return [
            ("schema_version", self.schema_version.encode("utf-8")),
            ("command_id", str(self.command_id).encode("utf-8")),
            ("target_asset_id", self.target_asset_id.encode("utf-8")),
            ("action_type", self.action_type.value.encode("utf-8")),
            ("execution_priority", self.execution_priority.value.encode("utf-8")),
            ("tenant", self.tenant.encode("utf-8")),
            ("source_event_id", self.source_event_id.encode("utf-8")),
            ("correlation_id", self.correlation_id.encode("utf-8")),
            ("requires_human_approval", str(self.requires_human_approval).lower().encode("utf-8")),
            ("producer", SERVICE.encode("utf-8")),
        ]

    def to_json_bytes(self) -> bytes:
        return json.dumps(self.model_dump(mode="json"), separators=(",", ":")).encode("utf-8")


class CommandBatch(BaseModel):
    """What one mutation closed into: the commands and where they landed."""

    model_config = ConfigDict(extra="forbid")

    plan_id: str
    event_id: str
    asset_id: str
    tenant: str
    topic: str
    issued: int
    commands: list[CommandPayload]


class ErrorDetail(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str
    message: str
    hint: str | None = None


class ErrorResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    error: ErrorDetail
    request_id: str


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ok"]
    service: str
    version: str
    environment: str
    mutations_topic: str
    commands_topic: str
    interceptor_mode: str
    uptime_seconds: float


def error_response(
    status_code: int,
    code: str,
    message: str,
    request_id: str,
    hint: str | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    """Uniform error envelope for every rejection path."""
    detail: dict[str, Any] = {"code": code, "message": message}
    if hint:
        detail["hint"] = hint
    response = JSONResponse(
        status_code=status_code,
        content={"error": detail, "request_id": request_id},
        headers=headers,
    )
    response.headers["X-Request-ID"] = request_id
    return response


# ---------------------------------------------------------------------------
# Errors raised inside the closure pipeline
# ---------------------------------------------------------------------------


class PermanentMessageError(Exception):
    """The message can never succeed; dead-letter it and commit the offset.

    A poison pill must not be allowed to block a partition forever.
    """

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


class TransientFailure(Exception):
    """A dependency failed; leave the offset uncommitted so Kafka redelivers."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


class InterceptorError(Exception):
    """The AI interceptor call failed.

    Two independent axes, because they answer two different questions.

    ``permanent`` asks *is this the message's fault?* Only a permanent error is
    dead-lettered. A payload the interceptor rejects as unparseable is
    permanent. A credential the interceptor rejects is not: the same record
    succeeds untouched the moment the operator fixes the key, and dead-lettering
    it would quietly convert one misconfiguration into a DLQ full of commands
    that were never issued.

    ``retryable`` asks *is retrying here, within seconds, worth anything?* A 429
    or a 503 is. A 404 on the configured path is not — it will still be a 404
    four backoffs later, and spending the attempts only delays the operator's
    error by the length of the backoff ladder.

    A non-permanent, non-retryable failure still leaves the offset uncommitted;
    the mutation is redelivered until the environment is repaired.
    """

    def __init__(
        self,
        message: str,
        *,
        permanent: bool,
        retryable: bool = True,
        status_code: int | None = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.permanent = permanent
        self.retryable = retryable
        self.status_code = status_code
        #: Server-advertised wait, in seconds, when one was supplied.
        self.retry_after = retry_after

    @property
    def should_retry(self) -> bool:
        """Whether :func:`retry_async` should spend another attempt on this."""
        return self.retryable and not self.permanent


# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------


async def retry_async(
    operation: Any,
    *,
    attempts: int,
    base_seconds: float,
    max_seconds: float,
    retry_on: tuple[type[BaseException], ...],
    description: str,
    retry_after_cap: float | None = None,
) -> Any:
    """Run ``operation`` with exponential backoff and full jitter.

    An exception may steer its own retry: ``should_retry`` (default ``True``)
    decides whether another attempt is spent at all, and ``retry_after`` (in
    seconds) replaces the computed backoff when the server has said how long it
    wants to be left alone. Everything without those attributes — a
    ``KafkaError``, an ``OSError`` — retries exactly as it always did.

    Cancellation is never retried: a shutdown must not be delayed by a backoff
    sleep. The last failure is re-raised once the attempts are exhausted.
    """
    last_error: BaseException | None = None

    for attempt in range(1, attempts + 1):
        try:
            return await operation()
        except asyncio.CancelledError:
            raise
        except retry_on as exc:
            last_error = exc
            if not getattr(exc, "should_retry", True) or attempt == attempts:
                raise

            hinted = getattr(exc, "retry_after", None)
            if hinted is not None:
                # The server named its own recovery time; a locally computed
                # backoff can only be wrong, and usually in the impatient
                # direction. A little jitter on top keeps N workers that were
                # all throttled at once from returning in lockstep.
                delay = float(hinted) if retry_after_cap is None else min(float(hinted), retry_after_cap)
                delay += random.random() * min(1.0, max(delay, 0.0) * 0.1)
            else:
                ceiling = min(max_seconds, base_seconds * (2 ** (attempt - 1)))
                delay = ceiling / 2 + random.random() * (ceiling / 2)
            logger.warning(
                "retrying after failure",
                extra={
                    "operation": description,
                    "attempt": attempt,
                    "attempts": attempts,
                    "delay_seconds": round(delay, 3),
                    "retry_after_hint": hinted,
                    "status_code": getattr(exc, "status_code", None),
                    "error": str(exc),
                },
            )
            await asyncio.sleep(delay)

    # Unreachable: the loop either returns or raises. Kept so the function has
    # no implicit ``None`` return path.
    raise RuntimeError(f"{description} exhausted its retries") from last_error


# ---------------------------------------------------------------------------
# The Enterprise AI Interceptor: stub simulator and HTTP client
# ---------------------------------------------------------------------------


class StubInterceptor:
    """Deterministic in-process simulation of the Enterprise AI Interceptor.

    It reproduces the shape of the commercial planner's response — the same
    envelope, the same command vocabulary — from fixed rules rather than an
    inference call, so the whole closure loop can be exercised offline and in
    tests without spending tokens or requiring network access.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def plan(self, mutation: MutationEnvelope, subscription: Subscription) -> ActionPlan:
        started = time.perf_counter()

        if self._settings.interceptor_simulated_latency_ms:
            await asyncio.sleep(self._settings.interceptor_simulated_latency_ms / 1000)

        context = mutation.ontology_context
        component = self._component_for(mutation)
        operator = context.primary_operator
        assignee = operator.name if operator and operator.name else "duty-engineer"
        operator_id = operator.operator_id if operator else None
        sensor = mutation.rule.sensor_id
        observed = mutation.rule.observed_value
        threshold = mutation.rule.threshold
        unit = mutation.rule.unit or ""

        actions: list[PlannedAction] = []

        if mutation.transition is TransitionKind.CLEARED:
            actions.append(
                PlannedAction(
                    sequence=1,
                    target_component=component,
                    action="ACKNOWLEDGE",
                    priority="LOW",
                    assigned_to=assignee,
                    assigned_operator_id=operator_id,
                    parameters={"sensor_id": sensor, "observed_value": observed},
                    expected_effect=f"Close the {sensor} alarm on {mutation.asset_id}; the reading is back inside limits.",
                    deadline_seconds=self._settings.default_deadline_seconds,
                )
            )
        elif mutation.severity is Severity.CRITICAL:
            if sensor == "temperature_celsius":
                actions.append(
                    PlannedAction(
                        sequence=1,
                        target_component=component,
                        action="SHUTDOWN",
                        priority="CRITICAL",
                        assigned_to=assignee,
                        assigned_operator_id=operator_id,
                        parameters={
                            "sensor_id": sensor,
                            "observed_value": observed,
                            "threshold": threshold,
                            "unit": unit,
                            "ramp_down_seconds": 60,
                        },
                        expected_effect=(
                            f"Remove thermal load from {component}; {sensor} is {observed:g}{unit} "
                            f"against a {threshold:g}{unit} ceiling."
                        ),
                        rollback="Restart only after the bearing temperature is below 80% of the ceiling for 10 minutes.",
                        deadline_seconds=300,
                    )
                )
            else:
                actions.append(
                    PlannedAction(
                        sequence=1,
                        target_component=component,
                        action="ISOLATE",
                        priority="CRITICAL",
                        assigned_to=assignee,
                        assigned_operator_id=operator_id,
                        parameters={
                            "sensor_id": sensor,
                            "observed_value": observed,
                            "threshold": threshold,
                            "unit": unit,
                            "isolation_mode": "electrical_and_process",
                        },
                        expected_effect=(
                            f"Isolate {component} before secondary damage; {sensor} is {observed:g}{unit} "
                            f"against a {threshold:g}{unit} limit."
                        ),
                        rollback="Restore only under a permit to work, after a vibration survey.",
                        deadline_seconds=300,
                    )
                )
            actions.append(
                PlannedAction(
                    sequence=2,
                    target_component=component,
                    action="SCHEDULE_MAINTENANCE",
                    priority="HIGH",
                    assigned_to=assignee,
                    assigned_operator_id=operator_id,
                    parameters={
                        "window": context.maintenance_window or "next_available",
                        "work_type": "root_cause_inspection",
                    },
                    expected_effect=f"Book intrusive inspection of {component} in the next maintenance window.",
                    deadline_seconds=86_400,
                )
            )
        elif mutation.severity is Severity.HIGH:
            actions.append(
                PlannedAction(
                    sequence=1,
                    target_component=component,
                    action="SHIFT_SPEED",
                    priority="HIGH",
                    assigned_to=assignee,
                    assigned_operator_id=operator_id,
                    parameters={
                        "sensor_id": sensor,
                        "observed_value": observed,
                        "threshold": threshold,
                        "unit": unit,
                        "target_setpoint_pct": 80,
                        "ramp_seconds": 120,
                    },
                    expected_effect=(
                        f"Derate {component} to 80% to pull {sensor} back under {threshold:g}{unit}."
                    ),
                    rollback="Return to the original setpoint once the reading holds below the limit for 15 minutes.",
                    deadline_seconds=600,
                )
            )
            actions.append(
                PlannedAction(
                    sequence=2,
                    target_component=component,
                    action="INSPECT",
                    priority="HIGH",
                    assigned_to=assignee,
                    assigned_operator_id=operator_id,
                    parameters={"method": "walkdown", "focus": sensor},
                    expected_effect=f"Confirm whether the {sensor} excursion on {component} is real or instrumentation drift.",
                    deadline_seconds=3_600,
                )
            )
        else:
            actions.append(
                PlannedAction(
                    sequence=1,
                    target_component=component,
                    action="NOTIFY",
                    priority="LOW",
                    assigned_to=assignee,
                    assigned_operator_id=operator_id,
                    parameters={"sensor_id": sensor, "observed_value": observed},
                    expected_effect=f"Raise {mutation.asset_id} {sensor} on the shift handover log.",
                    deadline_seconds=self._settings.default_deadline_seconds,
                )
            )

        if mutation.degraded:
            # Thin context is a reason to look before acting, never a reason to
            # act harder.
            actions = [action for action in actions if action.action not in {"SHUTDOWN", "ISOLATE"}]
            if not actions:
                actions = [
                    PlannedAction(
                        sequence=1,
                        target_component=component,
                        action="INSPECT",
                        priority="HIGH",
                        assigned_to=assignee,
                        assigned_operator_id=operator_id,
                        parameters={"reason": "degraded_context", "sensor_id": sensor},
                        expected_effect=(
                            f"Graph context for {mutation.asset_id} is incomplete; verify on site before actuating."
                        ),
                        deadline_seconds=1_800,
                    )
                ]

        actions = actions[: self._settings.max_commands]
        for index, action in enumerate(actions, start=1):
            action.sequence = index

        escalate = mutation.severity is Severity.CRITICAL or context.criticality == "SAFETY_CRITICAL"
        return ActionPlan(
            plan_id=f"plan_stub_{uuid.uuid4().hex[:16]}",
            event_id=mutation.event_id,
            asset_id=mutation.asset_id,
            tenant=subscription.tenant,
            model="stub-interceptor",
            confidence=0.55 if mutation.degraded else 0.86,
            reasoning_summary=(
                f"{sensor} on {mutation.asset_id} reported {observed:g}{unit} against a "
                f"{threshold:g}{unit} limit ({mutation.transition.value}, {mutation.severity.value}); "
                f"{len(actions)} action(s) issued against {component}."
            ),
            commands=actions,
            escalation=Escalation(
                required=escalate,
                notify=[op.operator_id for op in context.escalation_chain[:3]],
                reason="Critical severity on a governed asset." if escalate else "",
                sla_seconds=900 if escalate else 0,
            ),
            evidence=[
                f"rule={mutation.rule.rule_id or 'n/a'}",
                f"breach_count={mutation.breach_count}",
                f"snapshot_complete={mutation.telemetry_snapshot.complete}",
            ],
            latency_ms=round((time.perf_counter() - started) * 1000, 3),
        )

    @staticmethod
    def _component_for(mutation: MutationEnvelope) -> str:
        context = mutation.ontology_context
        if context.components:
            return context.components[0]
        parent = context.immediate_parent
        if parent is not None and parent.name:
            return parent.name
        return context.asset_name or mutation.asset_id


def parse_retry_after(value: str | None, *, cap: float) -> float | None:
    """Parse a ``Retry-After`` header into a bounded number of seconds.

    Both wire forms are accepted: delta-seconds, and the HTTP-date that proxies
    and CDNs prefer. Anything unparseable returns ``None`` so the caller falls
    back to its own backoff, and every result is clamped — a header is a hint
    from a remote service, not permission to park a consumer indefinitely.
    """
    if not value:
        return None

    raw = value.strip()
    try:
        seconds = float(raw)
    except ValueError:
        try:
            when = parsedate_to_datetime(raw)
        except (TypeError, ValueError):
            return None
        if when is None:
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        seconds = (when - datetime.now(tz=timezone.utc)).total_seconds()

    if not math.isfinite(seconds):
        return None
    return max(0.0, min(seconds, cap))


class RateGate:
    """Paces interceptor calls to whatever the presented subscription bought.

    Two mechanisms live in one object because they solve one problem:

    *   a **pace** — a virtual-time bucket that spaces calls ``60/rpm`` apart, so
        a burst of mutations drains at the licence's rate instead of arriving
        all at once and being converted into a wall of 429s;
    *   a **cooldown** — when the interceptor *does* answer 429, every worker
        stalls, not only the one unlucky enough to have received it. Throttling
        one caller while its siblings keep hammering is how a rate limit turns
        into a retry storm.

    Waiting here is the design, not a cost. The consumer keeps its partition,
    offsets stay uncommitted, and the backlog accumulates in Kafka — the one
    component in this topology built to hold a backlog.
    """

    def __init__(self, *, rpm: int, max_concurrency: int, margin: float = 1.0) -> None:
        self._margin = margin
        self._max_concurrency = max_concurrency
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._lock = asyncio.Lock()
        self._rpm = 0
        self._interval = 0.0
        self._next_at = 0.0
        self._source = "unlimited"
        self._waits = 0
        self._wait_seconds = 0.0
        self._cooldowns = 0
        self._cooldown_seconds = 0.0
        if rpm > 0:
            self.retune(rpm, source="config")

    @property
    def rpm(self) -> int:
        return self._rpm

    def retune(self, quota_per_minute: int, *, source: str) -> None:
        """Adopt a new ceiling, derived from the subscription's own quota.

        Called once by the preflight and then from every successful response's
        ``X-RateLimit-Limit``, so an entitlement change upstream reaches the
        pacer without a redeploy.
        """
        if quota_per_minute <= 0:
            return
        effective = max(1, int(quota_per_minute * self._margin))
        if effective == self._rpm:
            return

        self._rpm = effective
        self._interval = 60.0 / effective
        self._source = source
        logger.info(
            "interceptor rate gate tuned",
            extra={
                "quota_per_minute": quota_per_minute,
                "effective_rpm": effective,
                "margin": self._margin,
                "source": source,
                "max_concurrency": self._max_concurrency,
            },
        )

    def apply_cooldown(self, seconds: float) -> None:
        """Stall every caller for ``seconds``, however the pace is configured."""
        if seconds <= 0:
            return
        self._cooldowns += 1
        self._cooldown_seconds += seconds
        # Unlocked on purpose: this is a single guarded assignment between
        # awaits, and a cooldown that loses a race is re-applied by the very
        # next 429 — which, by construction, is about to arrive.
        self._next_at = max(self._next_at, time.monotonic() + seconds)

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        """Hold one call's worth of concurrency and pace budget."""
        async with self._semaphore:
            await self._pace()
            yield

    async def _pace(self) -> None:
        async with self._lock:
            now = time.monotonic()
            start = max(now, self._next_at)
            self._next_at = start + self._interval if self._interval > 0.0 else max(self._next_at, now)
            delay = start - now

        if delay <= 0.0:
            return
        self._waits += 1
        self._wait_seconds += delay
        await asyncio.sleep(delay)

    def stats(self) -> dict[str, Any]:
        return {
            "effective_rpm": self._rpm or None,
            "source": self._source,
            "max_concurrency": self._max_concurrency,
            "paced_waits": self._waits,
            "paced_seconds": round(self._wait_seconds, 3),
            "cooldowns": self._cooldowns,
            "cooldown_seconds": round(self._cooldown_seconds, 3),
        }


class HttpInterceptorClient:
    """Calls the real commercial AI interceptor over HTTP."""

    #: Stripped from the outbound body. None of these are ``mutation.v1``
    #: fields; all of them are places :meth:`LicenseGatekeeper.extract_key` will
    #: find a credential, and a credential belongs in a header.
    _CREDENTIAL_KEYS: Final[frozenset[str]] = frozenset(
        {"license_key", "licence_key", "license", "licence", "headers", "meta"}
    )

    def __init__(
        self,
        settings: Settings,
        client: httpx.AsyncClient,
        gate: RateGate,
        metrics: Metrics,
    ) -> None:
        self._settings = settings
        self._client = client
        self._gate = gate
        self._metrics = metrics

    async def plan(
        self,
        mutation: MutationEnvelope,
        subscription: Subscription,
        raw: dict[str, Any] | None = None,
    ) -> ActionPlan:
        started = time.perf_counter()
        body = self._body(mutation, raw)

        # Tier routing. The GraphRAG layer is a separate subscription with its
        # own key, so the credential travels with the endpoint — presenting the
        # standard key to the enterprise planner would be a 401 on every
        # ENTERPRISE mutation.
        endpoint, presented = self._destination(subscription)

        headers = {
            self._settings.license_header: presented,
            "X-Request-ID": current_request_id(),
            "Content-Type": "application/json",
        }

        async with self._gate.slot():
            self._metrics.interceptor_calls += 1
            try:
                response = await self._client.post(endpoint, json=body, headers=headers)
            except httpx.TimeoutException as exc:
                self._metrics.interceptor_timeouts += 1
                raise InterceptorError(
                    f"interceptor at {endpoint} timed out after "
                    f"{self._settings.interceptor_timeout_seconds}s: {exc}",
                    permanent=False,
                    retryable=True,
                ) from exc
            except httpx.HTTPError as exc:
                self._metrics.interceptor_transport_errors += 1
                raise InterceptorError(
                    f"interceptor transport error: {exc}", permanent=False, retryable=True
                ) from exc

        self._learn_quota(response)
        if response.status_code != 200:
            raise self._failure(response)

        try:
            plan = ActionPlan.model_validate(response.json())
        except (json.JSONDecodeError, ValueError) as exc:
            # A truncated or proxy-mangled body is a transport symptom, and the
            # next attempt gets a fresh one.
            raise InterceptorError(
                f"interceptor returned a body that is not JSON: {exc}",
                permanent=False,
                retryable=True,
            ) from exc
        except ValidationError as exc:
            # Permanent, and deliberately so: the interceptor caches its plan per
            # (tenant, event_id), so a redelivery of this mutation is answered
            # from that cache with the identical non-conforming plan. Retrying is
            # guaranteed to fail the same way, which is the definition of a
            # poison pill.
            self._metrics.interceptor_rejected += 1
            raise InterceptorError(
                f"interceptor response did not match the plan contract: {exc}",
                permanent=True,
                status_code=response.status_code,
            ) from exc

        elapsed = time.perf_counter() - started
        INTERCEPTOR_LATENCY.labels(mode="http", outcome="ok").observe(elapsed)

        if not plan.latency_ms:
            plan = plan.model_copy(update={"latency_ms": round(elapsed * 1000, 3)})
        return plan

    # -- helpers ----------------------------------------------------------

    def _body(self, mutation: MutationEnvelope, raw: dict[str, Any] | None) -> dict[str, Any]:
        """Forward the engine's own record, not this worker's copy of it.

        :class:`MutationEnvelope` is deliberately lenient — every field it does
        not require defaults to ``None``, and ``extra="ignore"`` drops anything
        a newer engine added. Round-tripping through it therefore rewrites
        "absent" as ``null`` and silently deletes unmodelled fields. The
        interceptor's contract is the stricter of the two, and a ``null`` where
        it requires a timestamp is a 422 — a dead letter caused entirely by this
        worker's re-serialisation. Sending what the engine published keeps the
        interceptor judging the engine's output.
        """
        if raw is None:
            return mutation.model_dump(mode="json")
        return {key: value for key, value in raw.items() if key not in self._CREDENTIAL_KEYS}

    def _learn_quota(self, response: httpx.Response) -> None:
        """Track the licence's advertised quota from the response headers."""
        if self._settings.interceptor_max_rpm:
            return  # an explicit ceiling is the operator's call, not the server's
        raw = response.headers.get("X-RateLimit-Limit")
        if not raw:
            return
        try:
            self._gate.retune(int(raw), source="x-ratelimit-limit")
        except ValueError:
            logger.debug("ignoring unparseable X-RateLimit-Limit", extra={"value": raw[:64]})

    def _failure(self, response: httpx.Response) -> InterceptorError:
        """Turn a non-200 into the right kind of failure.

        The governing question is never "how bad is this status" but "whose
        fault is it" — only a fault in *this message* may be dead-lettered.
        """
        code = response.status_code
        detail = response.text[:400]
        retry_after = parse_retry_after(
            response.headers.get("Retry-After"),
            cap=self._settings.interceptor_retry_after_cap_seconds,
        )

        if code in (400, 413, 422):
            # The payload itself is unacceptable. This one record is poison; the
            # next one may be fine.
            self._metrics.interceptor_rejected += 1
            return InterceptorError(
                f"interceptor rejected the payload ({code}): {detail}",
                permanent=True,
                status_code=code,
            )

        if code in (401, 402, 403):
            # Nothing to do with this record. The key is missing, unrecognised,
            # lapsed or under-entitled — every mutation would fail identically,
            # and dead-lettering them would convert a billing problem into a
            # topic full of commands that were never issued. Redeliver instead,
            # and make the operator's error loud.
            self._metrics.interceptor_auth_failures += 1
            logger.error(
                "interceptor refused the worker's subscription; the closure loop is stalled, "
                "not losing messages",
                extra={
                    "status_code": code,
                    "detail": detail,
                    "license_header": self._settings.license_header,
                    "endpoint": self._settings.interceptor_endpoint,
                },
            )
            return InterceptorError(
                f"interceptor refused the worker's subscription ({code}): {detail}",
                permanent=False,
                retryable=False,
                status_code=code,
            )

        if code in (404, 405, 501) or 300 <= code < 400:
            # The request never reached the planner: wrong URL, wrong path,
            # wrong service. A configuration fault, not a data fault.
            logger.error(
                "interceptor endpoint is misconfigured; the closure loop is stalled",
                extra={
                    "status_code": code,
                    "endpoint": self._settings.interceptor_endpoint,
                    "detail": detail,
                },
            )
            return InterceptorError(
                f"interceptor endpoint {self._settings.interceptor_endpoint} answered {code}: {detail}",
                permanent=False,
                retryable=False,
                status_code=code,
            )

        if code in (408, 409, 425):
            # Timed out, raced, or replayed too early. The request is fine; the
            # moment was not.
            return InterceptorError(
                f"interceptor could not serve the request now ({code}): {detail}",
                permanent=False,
                retryable=True,
                status_code=code,
                retry_after=retry_after,
            )

        if code == 429:
            # The interceptor's own rate limiter. Stall every worker, not just
            # this one, for as long as it asked for.
            self._metrics.interceptor_throttled += 1
            wait = retry_after if retry_after is not None else self._settings.retry_max_seconds
            self._gate.apply_cooldown(wait)
            return InterceptorError(
                f"interceptor rate limit reached ({code}): {detail}",
                permanent=False,
                retryable=True,
                status_code=code,
                retry_after=retry_after,
            )

        if code >= 500:
            self._metrics.interceptor_server_errors += 1
            if retry_after is not None:
                self._gate.apply_cooldown(retry_after)
            return InterceptorError(
                f"interceptor unavailable ({code}): {detail}",
                permanent=False,
                retryable=True,
                status_code=code,
                retry_after=retry_after,
            )

        # Anything else — a status this worker has never been taught. Treat it
        # as environmental. Being wrong in this direction stalls a partition an
        # operator can see and fix; being wrong in the other direction discards
        # a command that a plant floor was waiting for.
        return InterceptorError(
            f"unexpected interceptor status {code}: {detail}",
            permanent=False,
            retryable=False,
            status_code=code,
        )

    def _destination(self, subscription: Subscription) -> tuple[str, str]:
        """Pick the endpoint and the credential to present to it.

        An enterprise-tier subscription is planned by the GraphRAG layer when
        one is configured. Falling back to the standard interceptor when no
        enterprise key is set is deliberate: a missing credential should cost
        the richer plan, not the alarm.
        """
        tier = getattr(subscription, "plan", "") or getattr(subscription, "tier", "")
        if not self._settings.routes_to_enterprise(str(tier)):
            return self._settings.interceptor_endpoint, self._presented_key()

        enterprise_key = self._settings.enterprise_license_key.strip()
        if not enterprise_key:
            logger.warning(
                "enterprise tier configured for GraphRAG routing but no key is set; "
                "falling back to the standard interceptor",
                extra={"tier": str(tier)},
            )
            return self._settings.interceptor_endpoint, self._presented_key()

        return self._settings.enterprise_endpoint, enterprise_key

    def _presented_key(self) -> str:
        key = self._settings.license_key
        if key is None:  # pragma: no cover - Settings.model_post_init forbids it
            raise InterceptorError(
                "OO_LICENSE_KEY must be set when OO_INTERCEPTOR_MODE=http",
                permanent=False,
                retryable=False,
            )
        return key.get_secret_value()


class ActionRouter:
    """Routes a validated mutation to the Enterprise AI Interceptor."""

    def __init__(
        self,
        settings: Settings,
        client: httpx.AsyncClient | None,
        gate: RateGate | None = None,
        metrics: Metrics | None = None,
    ) -> None:
        self._settings = settings
        self._stub = StubInterceptor(settings)
        self._gate = gate
        self._http = (
            HttpInterceptorClient(settings, client, gate, metrics)
            if client is not None and gate is not None and metrics is not None
            else None
        )

    @property
    def mode(self) -> str:
        return self._settings.interceptor_mode

    @property
    def stub(self) -> StubInterceptor:
        return self._stub

    @property
    def gate(self) -> RateGate | None:
        return self._gate

    async def route(
        self,
        mutation: MutationEnvelope,
        subscription: Subscription,
        raw: dict[str, Any] | None = None,
    ) -> ActionPlan:
        """Obtain an action plan, retrying transient interceptor failures."""
        if self._settings.interceptor_mode == "http":
            if self._http is None:
                raise InterceptorError(
                    "HTTP interceptor client is not initialised",
                    permanent=False,
                    retryable=False,
                )

            async def call() -> ActionPlan:
                assert self._http is not None
                return await self._http.plan(mutation, subscription, raw)
        else:

            async def call() -> ActionPlan:
                return await self._stub.plan(mutation, subscription)

        return await retry_async(
            call,
            attempts=self._settings.max_attempts,
            base_seconds=self._settings.retry_base_seconds,
            max_seconds=self._settings.retry_max_seconds,
            retry_on=(InterceptorError,),
            description="ai-interceptor",
            retry_after_cap=self._settings.interceptor_retry_after_cap_seconds,
        )


# ---------------------------------------------------------------------------
# Boot-time agreement check between the two halves of the product
# ---------------------------------------------------------------------------


@dataclass
class PreflightStatus:
    """What the last agreement check concluded, for ``/readyz`` and ``/stats``."""

    state: Literal["pending", "skipped", "passed", "failed"] = "pending"
    detail: str = ""
    attempts: int = 0
    #: Whether the last failure is worth another attempt. An interceptor that
    #: has not finished booting is; a tenant disagreement never will be.
    retryable: bool = True
    checked_at: datetime | None = None
    worker_tenant: str | None = None
    worker_plan: str | None = None
    interceptor_tenant: str | None = None
    interceptor_tier: str | None = None
    quota_per_minute: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "detail": self.detail,
            "attempts": self.attempts,
            "checked_at": self.checked_at.isoformat() if self.checked_at else None,
            "worker_tenant": self.worker_tenant,
            "worker_plan": self.worker_plan,
            "interceptor_tenant": self.interceptor_tenant,
            "interceptor_tier": self.interceptor_tier,
            "quota_per_minute": self.quota_per_minute,
        }


class InterceptorPreflight:
    """Proves at boot that both halves of the product agree about the licence.

    The worker and the interceptor each keep their own subscription table, and
    for most of this codebase's life nothing ever compared them: the worker ran
    a stub, so its idea of a plan was never checked against anyone's. The moment
    the HTTP path is the default, that private opinion becomes a shared
    contract, and the three ways it can be wrong are all silent:

    *   the worker's ``OO_ALLOWED_PLANS`` doesn't contain the plan its own key
        resolves to, so it rejects every mutation before the interceptor is
        ever called;
    *   the two tables disagree about the *tenant* behind a key, so commands are
        published stamped with the wrong customer;
    *   the tier is entitled here and unentitled there, so every call 403s.

    None of that needs traffic to detect. One request at start-up settles it,
    and the same response carries ``quota_per_minute``, which is exactly the
    number the rate gate needs to pace itself to.
    """

    def __init__(
        self,
        settings: Settings,
        gatekeeper: LicenseGatekeeper,
        client: httpx.AsyncClient,
        gate: RateGate,
        status: PreflightStatus,
    ) -> None:
        self._settings = settings
        self._gatekeeper = gatekeeper
        self._client = client
        self._gate = gate
        self._status = status

    async def run(self) -> bool:
        """Return whether the two services agree. Never raises."""
        self._status.attempts += 1
        self._status.checked_at = datetime.now(tz=timezone.utc)

        subscription = self._resolve_local()
        if subscription is None:
            return False

        introspection = await self._introspect()
        if introspection is None:
            return False

        return self._compare(subscription, introspection)

    # -- steps ------------------------------------------------------------

    def _resolve_local(self) -> Subscription | None:
        """Run the worker's own gate against the worker's own key.

        This is the check that answers "does the worker reject its own licence?"
        directly, at boot, instead of leaving it to be inferred from a DLQ full
        of ``license_plan_not_entitled``.
        """
        try:
            subscription = self._gatekeeper.authorize({})
        except LicenseRejected as exc:
            self._fail(
                f"the worker's own licence fails its own gate: {exc.code} — {exc.message}",
                retryable=False,
                hint=(
                    f"OO_ALLOWED_PLANS={sorted(self._settings.entitled_plans)} does not admit the "
                    "plan behind OO_LICENSE_KEY"
                ),
            )
            return None

        self._status.worker_tenant = subscription.tenant
        self._status.worker_plan = subscription.plan
        return subscription

    async def _introspect(self) -> dict[str, Any] | None:
        """Ask the interceptor what it thinks the configured key is."""
        key = self._settings.license_key
        if key is None:  # pragma: no cover - Settings.model_post_init forbids it
            self._fail("OO_LICENSE_KEY is unset", retryable=False)
            return None

        headers = {
            self._settings.license_header: key.get_secret_value(),
            "X-Request-ID": f"preflight-{uuid.uuid4().hex[:12]}",
        }
        try:
            response = await self._client.get(
                self._settings.interceptor_license_endpoint, headers=headers
            )
        except httpx.HTTPError as exc:
            # Almost always the interceptor still starting. Worth another look.
            self._fail(
                f"interceptor unreachable at {self._settings.interceptor_url}: {exc}",
                retryable=True,
                level=logging.INFO,
            )
            return None

        if response.status_code != 200:
            # 4xx is a verdict on the key and will not change on its own; 5xx and
            # 429 are the service having a moment.
            transient = response.status_code >= 500 or response.status_code == 429
            self._fail(
                f"licence introspection returned {response.status_code}: {response.text[:300]}",
                retryable=transient,
                level=logging.INFO if transient else logging.ERROR,
            )
            return None

        try:
            body = response.json()
        except ValueError as exc:
            self._fail(f"licence introspection returned a non-JSON body: {exc}", retryable=True)
            return None
        if not isinstance(body, dict):
            self._fail("licence introspection did not return an object", retryable=True)
            return None
        return body

    def _compare(self, subscription: Subscription, introspection: dict[str, Any]) -> bool:
        tenant = str(introspection.get("tenant", ""))
        tier = normalise_plan(str(introspection.get("tier", "")))
        features = {str(item) for item in introspection.get("features", [])}
        quota = introspection.get("quota_per_minute")

        self._status.interceptor_tenant = tenant
        self._status.interceptor_tier = tier
        self._status.quota_per_minute = int(quota) if isinstance(quota, int) else None

        if not introspection.get("valid", False):
            self._fail(
                f"the interceptor considers the subscription for {tenant} invalid", retryable=False
            )
            return False
        if tenant != subscription.tenant:
            # The worst of the three: nothing errors, commands simply get
            # published under a tenant that did not ask for them.
            self._fail(
                f"tenant disagreement — the worker resolves this key to {subscription.tenant!r}, "
                f"the interceptor to {tenant!r}; commands would be mis-attributed",
                retryable=False,
            )
            return False
        if tier != subscription.plan:
            self._fail(
                f"plan disagreement — the worker calls this subscription {subscription.plan!r}, "
                f"the interceptor calls it {tier!r}",
                retryable=False,
            )
            return False
        if "ai.intercept" not in features:
            self._fail(
                f"tier {tier} does not carry 'ai.intercept'; every /v1/intercept call would 403",
                retryable=False,
            )
            return False

        if self._status.quota_per_minute and not self._settings.interceptor_max_rpm:
            # Only when the operator asked us to learn it. A configured ceiling
            # is a deliberate decision — often the reason someone is running a
            # deployment at all — and discovering the quota must not silently
            # overrule it.
            self._gate.retune(self._status.quota_per_minute, source="preflight")

        self._status.state = "passed"
        self._status.detail = ""
        self._status.retryable = True
        logger.info(
            "interceptor preflight passed: both services agree on the subscription",
            extra={
                "tenant": tenant,
                "plan": subscription.plan,
                "license_key_id": subscription.key_id,
                "quota_per_minute": self._status.quota_per_minute,
                "endpoint": self._settings.interceptor_endpoint,
                "attempts": self._status.attempts,
            },
        )
        return True

    def _fail(
        self,
        detail: str,
        *,
        retryable: bool = True,
        hint: str | None = None,
        level: int = logging.ERROR,
    ) -> None:
        self._status.state = "failed"
        self._status.detail = detail
        self._status.retryable = retryable
        # A cold interceptor logs at INFO — it is the normal shape of a start-up,
        # not an incident. A disagreement logs at ERROR, because someone has to
        # go and change something.
        logger.log(
            level,
            "interceptor preflight failed",
            extra={
                "detail": detail,
                "hint": hint,
                "retryable": retryable,
                "endpoint": self._settings.interceptor_license_endpoint,
                "attempts": self._status.attempts,
                "strict": self._settings.interceptor_preflight_strict,
            },
        )


async def run_preflight_until_ready(
    settings: Settings,
    preflight: InterceptorPreflight,
    status: PreflightStatus,
    proceed: asyncio.Event,
) -> None:
    """Drive the preflight and decide when the consumers may start draining.

    Retries are for one situation only: an interceptor that has not finished
    booting. Compose starts both containers at once, so a first attempt landing
    on a closed port is the normal case, not a fault — it retries quietly. A
    disagreement about tenant or tier is not retried at all; another nine
    attempts produce the same answer nine times and bury the message operators
    need to read.

    Once the answer is settled, ``strict`` decides what it means. The default is
    to log loudly and drain anyway, because a topology that refuses to start is
    harder to diagnose than one that starts and complains. Strict holds the
    consumers instead: no partitions taken, no commands issued, lag visible.
    """
    delay = settings.retry_base_seconds
    for attempt in range(1, settings.interceptor_preflight_attempts + 1):
        if await preflight.run():
            proceed.set()
            return

        if not status.retryable:
            break
        if attempt < settings.interceptor_preflight_attempts:
            await asyncio.sleep(delay + random.random() * delay)
            delay = min(delay * 2, settings.transient_backoff_max_seconds)

    if settings.interceptor_preflight_strict:
        logger.error(
            "preflight did not pass; consumers stay parked and ontology.mutations will lag",
            extra={"detail": status.detail, "attempts": status.attempts},
        )
        return

    logger.error(
        "starting the closure loop despite a failed preflight",
        extra={
            "detail": status.detail,
            "attempts": status.attempts,
            "hint": "set OO_INTERCEPTOR_PREFLIGHT_STRICT=true to hold the consumers instead",
        },
    )
    proceed.set()


# ---------------------------------------------------------------------------
# Plan -> CommandPayload translation
# ---------------------------------------------------------------------------


def deterministic_command_id(event_id: str, sequence: int, action: ActionType) -> UUID:
    """Derive a stable command id from the source event.

    Kafka is at-least-once in both directions, so the same mutation can be
    planned twice. Deriving the id from ``(event_id, sequence, action)`` means a
    replay produces byte-identical commands, and a downstream executor that
    keys on ``command_id`` isolates the compressor once rather than twice.
    """
    return uuid.uuid5(COMMAND_NAMESPACE, f"{event_id}|{sequence}|{action.value}")


def translate_plan(
    mutation: MutationEnvelope,
    plan: ActionPlan,
    subscription: Subscription,
    settings: Settings,
    correlation_id: str,
) -> list[CommandPayload]:
    """Convert an AI action plan into strictly typed command payloads.

    Any action or priority the actuation contract does not recognise is a
    contract violation, not something to guess at: it raises
    :class:`PermanentMessageError` so the message is dead-lettered with the
    offending value rather than actuated on a best guess.
    """
    issued_at = datetime.now(tz=timezone.utc)
    commands: list[CommandPayload] = []

    for index, planned in enumerate(plan.commands[: settings.max_commands], start=1):
        raw_action = planned.action.strip().upper()
        action = ACTION_ALIASES.get(raw_action)
        if action is None:
            raise PermanentMessageError(
                "unknown_action_type",
                f"Plan {plan.plan_id} requested unsupported action {planned.action!r}; "
                f"known actions: {', '.join(sorted(ACTION_ALIASES))}.",
            )

        raw_priority = planned.priority.strip().upper()
        priority = PRIORITY_ALIASES.get(raw_priority)
        if priority is None:
            raise PermanentMessageError(
                "unknown_execution_priority",
                f"Plan {plan.plan_id} requested unsupported priority {planned.priority!r}; "
                f"known priorities: {', '.join(sorted(PRIORITY_ALIASES))}.",
            )

        deadline = planned.deadline_seconds or settings.default_deadline_seconds
        requires_approval = not (
            subscription.autonomous_execution_allowed() and not action.is_irreversible
        )

        try:
            command = CommandPayload(
                command_id=deterministic_command_id(mutation.event_id, index, action),
                target_asset_id=mutation.asset_id,
                action_type=action,
                execution_priority=priority,
                issued_at=issued_at,
                tenant=subscription.tenant,
                license_key_id=subscription.key_id,
                source_event_id=mutation.event_id,
                plan_id=plan.plan_id,
                correlation_id=correlation_id,
                sequence=index,
                target_component=planned.target_component or None,
                parameters=planned.parameters,
                expected_effect=planned.expected_effect,
                rollback=planned.rollback,
                assigned_to=planned.assigned_to,
                assigned_operator_id=planned.assigned_operator_id,
                deadline_seconds=deadline,
                expires_at=issued_at + timedelta(seconds=deadline),
                requires_human_approval=requires_approval,
                escalation_required=plan.escalation.required,
                confidence=plan.confidence,
                trigger_sensor_id=mutation.rule.sensor_id,
                trigger_severity=mutation.severity,
                trigger_transition=mutation.transition,
                context_degraded=mutation.degraded,
            )
        except ValidationError as exc:
            raise PermanentMessageError(
                "command_contract_violation",
                f"Plan {plan.plan_id} step {index} does not satisfy the command contract: {exc}",
            ) from exc

        commands.append(command)

    if not commands:
        raise PermanentMessageError(
            "empty_action_plan",
            f"Plan {plan.plan_id} contained no executable commands.",
        )
    return commands


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


class Metrics:
    """Counters for the closure loop.

    Every mutation runs inside one event loop, and ``+=`` on an attribute is not
    interrupted by another coroutine without an intervening ``await``, so these
    need no lock.
    """

    def __init__(self) -> None:
        self.started_at = time.time()
        self.consumed = 0
        self.authorised = 0
        self.license_rejected = 0
        self.plans_requested = 0
        self.plans_failed = 0
        self.commands_issued = 0
        self.batches_published = 0
        self.publish_failures = 0
        self.dead_lettered = 0
        self.dlq_failures = 0
        self.transient_failures = 0
        self.consumer_restarts = 0
        self.messages_rewound = 0

        # --- interceptor call outcomes, split by whose fault they are -------
        self.interceptor_calls = 0
        self.interceptor_throttled = 0
        self.interceptor_server_errors = 0
        self.interceptor_timeouts = 0
        self.interceptor_transport_errors = 0
        self.interceptor_auth_failures = 0
        self.interceptor_rejected = 0

    def snapshot(self) -> dict[str, Any]:
        return {
            "uptime_seconds": round(time.time() - self.started_at, 3),
            "mutations_consumed": self.consumed,
            "mutations_authorised": self.authorised,
            "license_rejections": self.license_rejected,
            "plans_requested": self.plans_requested,
            "plans_failed": self.plans_failed,
            "commands_issued": self.commands_issued,
            "batches_published": self.batches_published,
            "publish_failures": self.publish_failures,
            "dead_lettered": self.dead_lettered,
            "dlq_failures": self.dlq_failures,
            "transient_failures": self.transient_failures,
            "consumer_restarts": self.consumer_restarts,
            "messages_rewound": self.messages_rewound,
            "interceptor": {
                "calls": self.interceptor_calls,
                "throttled_429": self.interceptor_throttled,
                "server_errors_5xx": self.interceptor_server_errors,
                "timeouts": self.interceptor_timeouts,
                "transport_errors": self.interceptor_transport_errors,
                "auth_failures": self.interceptor_auth_failures,
                "payload_rejections": self.interceptor_rejected,
            },
        }


# ---------------------------------------------------------------------------
# Prometheus exposition
# ---------------------------------------------------------------------------
#
# Unlike the interceptor, this service runs one process — two consumer workers
# are asyncio tasks inside it, not forks — so a plain in-process registry is
# already correct and no multiprocess directory is needed.
#
# The counters above are the source of truth and are not duplicated into
# prometheus_client objects; a bridging collector reads them at scrape time
# instead. Keeping one set of counters is the point: two would drift, and the
# ones the code increments are the ones /stats already reports.

# Client-side latency, which is deliberately not the same measurement as the
# interceptor's own. This one includes connection acquisition, the rate gate's
# pacing delay, retries and the network; the difference between the two is how
# you tell "the model is slow" from "we are throttling ourselves".
INTERCEPTOR_LATENCY = Histogram(
    "openontology_worker_interceptor_latency_seconds",
    "Round-trip time for one interceptor call as the client observed it.",
    ["mode", "outcome"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

PLAN_CONFIDENCE = Histogram(
    "openontology_worker_plan_confidence",
    "Confidence on plans the closure loop acted on.",
    ["mode"],
    buckets=(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0),
)

# Deliberately not named ..._commands_issued_total: the bridging collector below
# already exports that name from Metrics.commands_issued, and two collectors
# claiming one family is a registration error, not a merge.
COMMANDS_BY_ACTION = Counter(
    "openontology_worker_commands_by_action_total",
    "Commands published to the commands topic, by action and approval requirement.",
    ["action", "requires_human_approval"],
)


class _WorkerMetricsCollector:
    """Bridges the plain-attribute Metrics counters into the registry.

    Registered once against the default registry; collect() is called on every
    scrape, so the values are read fresh rather than mirrored.
    """

    _COUNTERS = {
        "mutations_consumed": "Mutations read from the mutations topic.",
        "mutations_authorised": "Mutations that passed the subscription gate.",
        "license_rejections": "Mutations refused by the subscription gate.",
        "plans_requested": "Plan requests sent to the interceptor.",
        "plans_failed": "Plan requests that returned an error.",
        "commands_issued": "Commands published to the commands topic.",
        "batches_published": "Command batches published.",
        "publish_failures": "Publishes that failed.",
        "dead_lettered": "Messages routed to the commands dead-letter topic.",
        "dlq_failures": "Dead-letter publishes that themselves failed.",
        "transient_failures": "Transient failures retried with backoff.",
        "consumer_restarts": "Consumer restarts.",
        "messages_rewound": "Messages redelivered after an uncommitted offset.",
    }

    # Interceptor outcomes are one metric family split by cause, rather than
    # seven unrelated counters: "who is at fault" is the question, and a single
    # family with a `cause` label is what lets a dashboard stack the answer.
    _INTERCEPTOR_CAUSES = {
        "calls": "ok",
        "throttled_429": "throttled",
        "server_errors_5xx": "server_error",
        "timeouts": "timeout",
        "transport_errors": "transport_error",
        "auth_failures": "auth_failure",
        "payload_rejections": "payload_rejected",
    }

    def __init__(self, metrics: "Metrics") -> None:
        self._metrics = metrics

    def rebind(self, metrics: "Metrics") -> None:
        """Follow a new app instance's counters.

        A process only ever serves one app at a time, so the previous instance's
        counters are dead; continuing to export them would report a restarted
        worker's totals as though they were current.
        """
        self._metrics = metrics

    def collect(self):  # noqa: ANN201 - prometheus_client's collector protocol
        snapshot = self._metrics.snapshot()

        yield GaugeMetricFamily(
            "openontology_worker_uptime_seconds",
            "Process uptime in seconds.",
            value=snapshot["uptime_seconds"],
        )

        for field, help_text in self._COUNTERS.items():
            yield CounterMetricFamily(
                f"openontology_worker_{field}",
                help_text,
                value=snapshot[field],
            )

        interceptor = CounterMetricFamily(
            "openontology_worker_interceptor_calls",
            "Interceptor call outcomes, split by whose fault the outcome was.",
            labels=["cause"],
        )
        for field, cause in self._INTERCEPTOR_CAUSES.items():
            interceptor.add_metric([cause], snapshot["interceptor"][field])
        yield interceptor


_metrics_collector: "_WorkerMetricsCollector | None" = None


def register_worker_metrics(metrics: "Metrics") -> None:
    """Point the bridging collector at this app instance's counters.

    create_app() runs once at import time and again per instance under test, so
    this has to be safe to call repeatedly. It rebinds the existing collector
    rather than swallowing the duplicate-registration error, because that error
    also fires when a metric family is genuinely declared twice — catching it
    broadly is how a name collision goes unnoticed until nothing is exported.
    """
    global _metrics_collector

    if _metrics_collector is not None:
        _metrics_collector.rebind(metrics)
        return

    collector = _WorkerMetricsCollector(metrics)
    REGISTRY.register(collector)
    _metrics_collector = collector


# ---------------------------------------------------------------------------
# Kafka publisher
# ---------------------------------------------------------------------------


class CommandPublisher:
    """Owns the single idempotent producer shared by every worker."""

    def __init__(self, settings: Settings, metrics: Metrics) -> None:
        self._settings = settings
        self._metrics = metrics
        self._producer: AIOKafkaProducer | None = None

    @property
    def started(self) -> bool:
        return self._producer is not None

    async def start(self) -> None:
        producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.bootstrap_servers,
            client_id=f"{SERVICE}-producer",
            # Idempotent production plus acks=all: a broker-side retry can no
            # longer duplicate or reorder a command record.
            enable_idempotence=True,
            acks="all",
            compression_type="gzip",
            linger_ms=self._settings.producer_linger_ms,
            request_timeout_ms=self._settings.kafka_request_timeout_ms,
        )
        try:
            await producer.start()
        except (KafkaError, OSError) as exc:
            raise RuntimeError(f"cannot start Kafka producer: {exc}") from exc

        self._producer = producer
        logger.info(
            "command producer started",
            extra={
                "brokers": self._settings.bootstrap_servers,
                "commands_topic": self._settings.commands_topic,
                "dlq_topic": self._settings.commands_dlq_topic,
            },
        )

    async def stop(self) -> None:
        if self._producer is None:
            return
        try:
            # stop() flushes buffered records before closing the connections.
            await self._producer.stop()
        except (KafkaError, OSError) as exc:
            logger.error("command producer did not shut down cleanly", extra={"error": str(exc)})
        finally:
            self._producer = None
            logger.info("command producer stopped")

    def _require(self) -> AIOKafkaProducer:
        if self._producer is None:
            raise TransientFailure("producer_unavailable", "The Kafka producer is not running.")
        return self._producer

    async def publish(self, command: CommandPayload) -> None:
        """Publish one command onto ``ontology.commands``."""
        producer = self._require()

        async def send() -> None:
            await producer.send_and_wait(
                self._settings.commands_topic,
                value=command.to_json_bytes(),
                key=command.kafka_key(),
                headers=command.kafka_headers(),
            )

        try:
            await retry_async(
                send,
                attempts=self._settings.max_attempts,
                base_seconds=self._settings.retry_base_seconds,
                max_seconds=self._settings.retry_max_seconds,
                retry_on=(KafkaError, KafkaTimeoutError, OSError),
                description="publish-command",
            )
        except (KafkaError, OSError) as exc:
            self._metrics.publish_failures += 1
            raise TransientFailure(
                "command_publish_failed",
                f"Could not publish command {command.command_id} to "
                f"{self._settings.commands_topic}: {exc}",
            ) from exc

    async def dead_letter(
        self,
        raw_value: bytes | None,
        key: bytes | None,
        reason: str,
        detail: str,
        source: dict[str, Any],
    ) -> None:
        """Route an unprocessable record to the DLQ.

        A DLQ failure is logged and swallowed: the alternative is replaying a
        poison pill forever, which costs the partition and fixes nothing.
        """
        producer = self._producer
        if producer is None:
            self._metrics.dlq_failures += 1
            logger.error(
                "cannot dead-letter record: producer unavailable",
                extra={"dlq_reason": reason, "detail": detail[:500]},
            )
            return

        headers = [
            ("dlq_reason", reason.encode("utf-8")),
            ("dlq_error", detail[:900].encode("utf-8")),
            ("dlq_source_topic", str(source.get("topic", "")).encode("utf-8")),
            ("dlq_source_partition", str(source.get("partition", "")).encode("utf-8")),
            ("dlq_source_offset", str(source.get("offset", "")).encode("utf-8")),
            ("dlq_at", datetime.now(tz=timezone.utc).isoformat().encode("utf-8")),
            ("producer", SERVICE.encode("utf-8")),
        ]

        try:
            await producer.send_and_wait(
                self._settings.commands_dlq_topic,
                value=raw_value if raw_value is not None else b"",
                key=key,
                headers=headers,
            )
            self._metrics.dead_lettered += 1
            logger.warning(
                "record dead-lettered",
                extra={
                    "dlq_reason": reason,
                    "dlq_topic": self._settings.commands_dlq_topic,
                    "detail": detail[:500],
                    **source,
                },
            )
        except (KafkaError, OSError) as exc:
            self._metrics.dlq_failures += 1
            logger.error(
                "dead-letter publish failed; the record is dropped",
                extra={"dlq_reason": reason, "error": str(exc), **source},
            )


# ---------------------------------------------------------------------------
# The closure pipeline
# ---------------------------------------------------------------------------


class ClosurePipeline:
    """Gatekeeper, AI routing, command modelling and publication, in order.

    The consumer loop and the synchronous HTTP endpoint both call
    :meth:`close_loop`, so there is exactly one implementation of the pipeline
    and no way for the two entry points to drift apart.
    """

    def __init__(
        self,
        settings: Settings,
        gatekeeper: LicenseGatekeeper,
        router: ActionRouter,
        publisher: CommandPublisher,
        metrics: Metrics,
    ) -> None:
        self._settings = settings
        self._gatekeeper = gatekeeper
        self._router = router
        self._publisher = publisher
        self._metrics = metrics

    async def close_loop(self, payload: dict[str, Any], correlation_id: str) -> CommandBatch:
        """Take one raw mutation all the way to published commands."""
        # 1. Gatekeeper. Nothing billable happens before this returns.
        subscription = self._gatekeeper.authorize(payload)
        self._metrics.authorised += 1

        # 2. Contract validation.
        try:
            mutation = MutationEnvelope.model_validate(payload)
        except ValidationError as exc:
            raise PermanentMessageError(
                "mutation_invalid",
                f"Payload did not match {MUTATION_SCHEMA_PREFIX}: {exc}",
            ) from exc

        # 3. Route to the Enterprise AI Interceptor. The raw record travels
        #    alongside the parsed one so the HTTP path can forward exactly what
        #    the engine published rather than this worker's lossy re-dump.
        self._metrics.plans_requested += 1
        try:
            plan = await self._router.route(mutation, subscription, payload)
        except InterceptorError as exc:
            self._metrics.plans_failed += 1
            if exc.permanent:
                raise PermanentMessageError(
                    "interceptor_rejected", f"AI interceptor rejected the mutation: {exc}"
                ) from exc
            raise TransientFailure(
                "interceptor_unavailable", f"AI interceptor unavailable: {exc}"
            ) from exc

        # 4. Strict command modelling.
        commands = translate_plan(mutation, plan, subscription, self._settings, correlation_id)

        # 5. Publication — the closure.
        for command in commands:
            await self._publisher.publish(command)
            self._metrics.commands_issued += 1
            logger.info(
                "command issued",
                extra={
                    "command_id": str(command.command_id),
                    "target_asset_id": command.target_asset_id,
                    "action_type": command.action_type.value,
                    "execution_priority": command.execution_priority.value,
                    "sequence": command.sequence,
                    "requires_human_approval": command.requires_human_approval,
                    "commands_topic": self._settings.commands_topic,
                    "plan_id": plan.plan_id,
                    "confidence": plan.confidence,
                },
            )

        PLAN_CONFIDENCE.labels(mode=self._settings.interceptor_mode).observe(plan.confidence)
        for command in commands:
            COMMANDS_BY_ACTION.labels(
                action=command.action_type.value,
                requires_human_approval=str(command.requires_human_approval).lower(),
            ).inc()

        self._metrics.batches_published += 1
        return CommandBatch(
            plan_id=plan.plan_id,
            event_id=mutation.event_id,
            asset_id=mutation.asset_id,
            tenant=subscription.tenant,
            topic=self._settings.commands_topic,
            issued=len(commands),
            commands=commands,
        )


# ---------------------------------------------------------------------------
# Kafka consumer worker
# ---------------------------------------------------------------------------


class CommandWorker:
    """Drains ``ontology.mutations`` and closes each payload into a command.

    Offsets are committed manually and only after a message is fully handled,
    which gives at-least-once delivery. Combined with deterministic command ids
    the effective downstream behaviour is exactly-once.
    """

    def __init__(
        self,
        settings: Settings,
        pipeline: ClosurePipeline,
        publisher: CommandPublisher,
        metrics: Metrics,
        proceed: asyncio.Event | None = None,
    ) -> None:
        self._settings = settings
        self._pipeline = pipeline
        self._publisher = publisher
        self._metrics = metrics
        self._proceed = proceed
        self._stopping = False
        self._running: dict[int, str] = {}
        self._streaks: dict[int, int] = {}

    @property
    def worker_states(self) -> dict[int, str]:
        return dict(self._running)

    def request_stop(self) -> None:
        self._stopping = True

    async def run(self, worker_id: int) -> None:
        """One consumer-group member, restarted across transient failures."""
        log = logger.getChild(f"worker.{worker_id}")

        if self._proceed is not None and not self._proceed.is_set():
            # Joining the group before the licence agreement is settled would
            # only take partitions this worker cannot yet process.
            self._running[worker_id] = "awaiting-preflight"
            log.info("holding for the interceptor preflight", extra={"worker": worker_id})
            await self._proceed.wait()

        while not self._stopping:
            self._running[worker_id] = "connecting"
            consumer = AIOKafkaConsumer(
                self._settings.mutations_topic,
                bootstrap_servers=self._settings.bootstrap_servers,
                group_id=self._settings.consumer_group,
                client_id=f"{SERVICE}-{worker_id}",
                enable_auto_commit=False,
                auto_offset_reset=self._settings.auto_offset_reset,
                session_timeout_ms=self._settings.kafka_session_timeout_ms,
                request_timeout_ms=self._settings.kafka_request_timeout_ms,
                max_poll_records=self._settings.kafka_max_poll_records,
            )

            try:
                await consumer.start()
            except asyncio.CancelledError:
                self._running[worker_id] = "cancelled"
                raise
            except (KafkaError, OSError) as exc:
                self._running[worker_id] = "reconnecting"
                self._metrics.consumer_restarts += 1
                log.error(
                    "consumer failed to start; retrying",
                    extra={"error": str(exc), "worker": worker_id},
                )
                await self._backoff()
                continue

            self._running[worker_id] = "consuming"
            log.info(
                "consumer worker started",
                extra={
                    "worker": worker_id,
                    "topic": self._settings.mutations_topic,
                    "group": self._settings.consumer_group,
                },
            )

            try:
                async for message in consumer:
                    if self._stopping:
                        break
                    try:
                        await self._handle(message)
                    except TransientFailure as exc:
                        await self._rewind(consumer, message, exc, log, worker_id)
                        continue
                    self._streaks[worker_id] = 0
                    await consumer.commit(
                        {TopicPartition(message.topic, message.partition): message.offset + 1}
                    )
            except asyncio.CancelledError:
                self._running[worker_id] = "cancelled"
                await self._close_consumer(consumer, log, worker_id)
                raise
            except TransientFailure as exc:
                # Defensive: everything raised from _handle is caught above, so
                # reaching here means a dependency failed outside the per-message
                # path. Rebuild the consumer rather than lose the loop.
                self._metrics.transient_failures += 1
                self._metrics.consumer_restarts += 1
                log.error(
                    "transient failure outside message handling; restarting the consumer",
                    extra={"worker": worker_id, "reason": exc.reason, "error": exc.message},
                )
                await self._close_consumer(consumer, log, worker_id)
                await self._backoff()
                continue
            except (KafkaError, OSError) as exc:
                self._metrics.consumer_restarts += 1
                log.error(
                    "consumer loop failed; restarting",
                    extra={"worker": worker_id, "error": str(exc)},
                )
                await self._close_consumer(consumer, log, worker_id)
                await self._backoff()
                continue

            await self._close_consumer(consumer, log, worker_id)
            if self._stopping:
                break

        self._running[worker_id] = "stopped"
        log.info("consumer worker stopped", extra={"worker": worker_id})

    async def _handle(self, message: Any) -> None:
        """Process exactly one mutation record.

        Permanent problems are dead-lettered here and the caller commits.
        Transient problems propagate so the offset stays uncommitted.
        """
        self._metrics.consumed += 1
        source = {
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
        }
        correlation_id = self._correlation_id(message)
        tokens = bind_log_context(request_id=correlation_id)

        try:
            payload = self._decode(message)
            if payload is None:
                await self._publisher.dead_letter(
                    message.value,
                    message.key,
                    "payload_not_json",
                    "Record body is not a JSON object.",
                    source,
                )
                return

            self._inject_license_key(payload, message)
            reset_log_context(tokens)
            tokens = bind_log_context(
                request_id=correlation_id, event_id=str(payload.get("event_id", "-"))
            )

            try:
                batch = await self._pipeline.close_loop(payload, correlation_id)
            except LicenseRejected as exc:
                self._metrics.license_rejected += 1
                logger.warning(
                    "mutation rejected by the enterprise gatekeeper",
                    extra={"status": exc.status_code, "reason": exc.code, "detail": exc.message, **source},
                )
                await self._publisher.dead_letter(
                    message.value, message.key, f"license_{exc.code}", exc.message, source
                )
                return
            except PermanentMessageError as exc:
                await self._publisher.dead_letter(
                    message.value, message.key, exc.reason, exc.message, source
                )
                return

            reset_log_context(tokens)
            tokens = bind_log_context(
                request_id=correlation_id, tenant=batch.tenant, event_id=batch.event_id
            )
            logger.info(
                "closure complete",
                extra={
                    "plan_id": batch.plan_id,
                    "asset_id": batch.asset_id,
                    "commands_issued": batch.issued,
                    "commands_topic": batch.topic,
                    **source,
                },
            )
        finally:
            reset_log_context(tokens)

    @staticmethod
    def _decode(message: Any) -> dict[str, Any] | None:
        if not message.value:
            return None
        try:
            decoded = json.loads(message.value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        if not isinstance(decoded, dict):
            return None
        return decoded

    def _inject_license_key(self, payload: dict[str, Any], message: Any) -> None:
        """Lift a licence key off the record headers onto the payload.

        The open-core engine publishes unauthenticated records, but a gateway or
        multi-tenant broker in front of this topic may stamp the originating
        subscription into a header. Honouring it lets one worker serve several
        tenants; when it is absent the gatekeeper falls back to the worker's own
        configured key.
        """
        if payload.get("license_key"):
            return
        for name, value in message.headers or ():
            if name.lower() in {"license_key", "licence_key", self._settings.license_header.lower()}:
                try:
                    payload["license_key"] = value.decode("utf-8")
                except (UnicodeDecodeError, AttributeError):
                    logger.warning("ignoring undecodable licence header", extra={"header": name})
                return

    @staticmethod
    def _correlation_id(message: Any) -> str:
        for name, value in message.headers or ():
            if name.lower() in {"x-request-id", "correlation_id", "event_id"}:
                try:
                    decoded = value.decode("utf-8").strip()
                except (UnicodeDecodeError, AttributeError):
                    continue
                if decoded:
                    return decoded
        return uuid.uuid4().hex

    async def _rewind(
        self,
        consumer: AIOKafkaConsumer,
        message: Any,
        exc: TransientFailure,
        log: Any,
        worker_id: int,
    ) -> None:
        """Re-read the failed offset without leaving the consumer group.

        The previous behaviour was to tear the consumer down and rebuild it.
        That does preserve the message — the offset was never committed — but
        every rebuild triggers a group rebalance, and a rebalance stops the
        *other* workers too. A dependency that is merely slow, which is exactly
        what the interceptor's rate limiter produces, therefore presented as a
        topology-wide stall that looked like a Kafka fault.

        Seeking back to the failed offset discards the prefetch buffer and
        re-reads the same record, with group membership untouched. Consumer lag
        grows while the pause lasts, which is the correct and visible shape for
        backpressure: the backlog waits in the log, and nothing is dead-lettered
        for a failure the message did not cause.
        """
        self._metrics.transient_failures += 1
        self._metrics.messages_rewound += 1
        streak = self._streaks.get(worker_id, 0) + 1
        self._streaks[worker_id] = streak

        ceiling = min(
            self._settings.transient_backoff_max_seconds,
            self._settings.retry_base_seconds * (2 ** min(streak - 1, 10)),
        )
        delay = ceiling / 2 + random.random() * (ceiling / 2)

        partition = TopicPartition(message.topic, message.partition)
        try:
            consumer.seek(partition, message.offset)
        except (KafkaError, ValueError, AssertionError) as seek_error:
            # The partition was revoked mid-flight. It belongs to another member
            # now, which will read it from the last committed offset — still
            # this record. Nothing is lost; drop back to the outer loop.
            log.warning(
                "could not rewind; partition no longer assigned",
                extra={
                    "worker": worker_id,
                    "reason": exc.reason,
                    "error": str(seek_error),
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                },
            )
            return

        self._running[worker_id] = "backpressure"
        log.warning(
            "transient failure; rewound to the failed offset and paused",
            extra={
                "worker": worker_id,
                "reason": exc.reason,
                "error": exc.message,
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "consecutive_failures": streak,
                "pause_seconds": round(delay, 3),
            },
        )
        await asyncio.sleep(delay)
        self._running[worker_id] = "consuming"

    async def _close_consumer(self, consumer: AIOKafkaConsumer, log: Any, worker_id: int) -> None:
        try:
            await consumer.stop()
        except (KafkaError, OSError) as exc:
            log.warning(
                "consumer did not shut down cleanly",
                extra={"worker": worker_id, "error": str(exc)},
            )

    async def _backoff(self) -> None:
        delay = self._settings.consumer_restart_backoff_seconds
        await asyncio.sleep(delay / 2 + random.random() * (delay / 2))


# ---------------------------------------------------------------------------
# FastAPI surface
# ---------------------------------------------------------------------------


def get_pipeline(request: Request) -> ClosurePipeline:
    return request.app.state.pipeline


def get_router(request: Request) -> ActionRouter:
    return request.app.state.router


async def require_subscription(
    request: Request,
    x_license_key: str | None = Header(
        default=None, alias="X-License-Key", description="Subscription key."
    ),
) -> Subscription:
    """Gatekeeper dependency for every commercial endpoint.

    ``x_license_key`` is declared so the header shows up in the OpenAPI schema;
    the value itself is read through :func:`_presented_key`, which also honours
    a renamed header and ``Authorization: Bearer``.
    """
    presented = _presented_key(request) or (x_license_key.strip() if x_license_key else None)
    gatekeeper: LicenseGatekeeper = request.app.state.gatekeeper

    # allow_fallback=False: an anonymous HTTP caller must never inherit the
    # worker's own subscription.
    return gatekeeper.authorize({"license_key": presented or ""}, allow_fallback=False)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        metrics = Metrics()
        register_worker_metrics(metrics)
        gatekeeper = get_gatekeeper()
        publisher = CommandPublisher(settings, metrics)

        preflight_status = PreflightStatus()
        # Consumers wait on this. In stub mode, and whenever the preflight is
        # disabled, it is set before anything can await it.
        proceed = asyncio.Event()
        gate: RateGate | None = None
        http_client: httpx.AsyncClient | None = None
        preflight_task: asyncio.Task[None] | None = None

        if settings.interceptor_mode == "http":
            gate = RateGate(
                rpm=settings.interceptor_max_rpm,
                max_concurrency=settings.interceptor_max_concurrency,
                margin=settings.interceptor_rate_margin,
            )
            http_client = httpx.AsyncClient(
                # Separate connect and read budgets: a dead interceptor should
                # fail in seconds, while a live one thinking about a hard plan
                # gets the whole read timeout.
                timeout=httpx.Timeout(
                    settings.interceptor_timeout_seconds,
                    connect=settings.interceptor_connect_timeout_seconds,
                ),
                limits=httpx.Limits(
                    max_connections=max(32, settings.interceptor_max_concurrency),
                    max_keepalive_connections=16,
                ),
            )
        else:
            preflight_status.state = "skipped"
            preflight_status.detail = "interceptor_mode=stub"
            proceed.set()

        router = ActionRouter(settings, http_client, gate, metrics)
        await publisher.start()

        pipeline = ClosurePipeline(settings, gatekeeper, router, publisher, metrics)
        worker = CommandWorker(settings, pipeline, publisher, metrics, proceed)

        application.state.settings = settings
        application.state.metrics = metrics
        application.state.gatekeeper = gatekeeper
        application.state.publisher = publisher
        application.state.router = router
        application.state.pipeline = pipeline
        application.state.worker = worker
        application.state.http_client = http_client
        application.state.rate_gate = gate
        application.state.preflight = preflight_status

        if http_client is not None and gate is not None:
            if settings.interceptor_preflight:
                preflight_task = asyncio.create_task(
                    run_preflight_until_ready(
                        settings,
                        InterceptorPreflight(
                            settings, gatekeeper, http_client, gate, preflight_status
                        ),
                        preflight_status,
                        proceed,
                    ),
                    name="interceptor-preflight",
                )
            else:
                preflight_status.state = "skipped"
                preflight_status.detail = "OO_INTERCEPTOR_PREFLIGHT=false"
                proceed.set()

        tasks = [
            asyncio.create_task(worker.run(index), name=f"command-worker-{index}")
            for index in range(settings.consumer_workers)
        ]
        application.state.tasks = tasks

        logger.info(
            "command worker ready",
            extra={
                "environment": settings.environment,
                "brokers": settings.bootstrap_servers,
                "mutations_topic": settings.mutations_topic,
                "commands_topic": settings.commands_topic,
                "dlq_topic": settings.commands_dlq_topic,
                "consumer_workers": settings.consumer_workers,
                "interceptor_mode": settings.interceptor_mode,
                "enterprise_interceptor_endpoint": settings.enterprise_endpoint or None,
                "enterprise_interceptor_tiers": sorted(settings.enterprise_tiers) if settings.enterprise_endpoint else [],
                "interceptor_endpoint": (
                    settings.interceptor_endpoint if settings.interceptor_mode == "http" else None
                ),
                "entitled_plans": sorted(settings.entitled_plans),
            },
        )

        try:
            yield
        finally:
            logger.info("command worker shutting down")
            worker.request_stop()
            proceed.set()  # release anything still parked on the preflight
            if preflight_task is not None:
                preflight_task.cancel()
            for task in tasks:
                task.cancel()
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for index, result in enumerate(results):
                    if isinstance(result, BaseException) and not isinstance(
                        result, asyncio.CancelledError
                    ):
                        logger.error(
                            "consumer worker exited with an error",
                            extra={"worker": index, "error": str(result)},
                        )
            if preflight_task is not None:
                await asyncio.gather(preflight_task, return_exceptions=True)
            await publisher.stop()
            if http_client is not None:
                await http_client.aclose()
            logger.info("command worker stopped", extra=metrics.snapshot())

    app = FastAPI(
        title="OpenOntology Command Worker",
        version=__version__,
        summary="Closes the telemetry-to-actuation loop by turning ontology mutations into commands.",
        docs_url="/docs" if settings.docs_enabled else None,
        redoc_url="/redoc" if settings.docs_enabled else None,
        openapi_url="/openapi.json" if settings.docs_enabled else None,
        lifespan=lifespan,
    )

    _register_error_handlers(app)
    _register_routes(app, settings)
    return app


def _register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(LicenseRejected)
    async def _license_rejected(request: Request, exc: LicenseRejected) -> JSONResponse:
        logger.warning(
            "request rejected by the enterprise gatekeeper",
            extra={"status": exc.status_code, "reason": exc.code, "path": request.url.path},
        )
        return error_response(
            exc.status_code,
            exc.code,
            exc.message,
            current_request_id(),
            hint=exc.hint,
            headers={"WWW-Authenticate": "Bearer"},
        )

    @app.exception_handler(PermanentMessageError)
    async def _permanent(request: Request, exc: PermanentMessageError) -> JSONResponse:
        logger.warning("payload rejected", extra={"reason": exc.reason})
        return error_response(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            exc.reason,
            exc.message,
            current_request_id(),
        )

    @app.exception_handler(TransientFailure)
    async def _transient(request: Request, exc: TransientFailure) -> JSONResponse:
        logger.error("dependency unavailable", extra={"reason": exc.reason, "detail": exc.message})
        return error_response(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            exc.reason,
            exc.message,
            current_request_id(),
            hint="Retry once the dependency recovers; no command was published.",
            headers={"Retry-After": "5"},
        )

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError) -> JSONResponse:
        logger.warning("request payload invalid", extra={"errors": exc.errors()[:5]})
        return error_response(
            422,
            "payload_invalid",
            "The request body did not match the expected schema.",
            current_request_id(),
            hint=f"Verify the payload against {MUTATION_SCHEMA_PREFIX}.",
        )

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("unhandled error", extra={"path": request.url.path})
        return error_response(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "internal_error",
            "An unexpected error occurred.",
            current_request_id(),
        )


def _register_routes(app: FastAPI, settings: Settings) -> None:
    @app.get("/healthz", response_model=HealthResponse, tags=["ops"], summary="Liveness probe")
    async def healthz(request: Request) -> HealthResponse:
        metrics: Metrics = request.app.state.metrics
        return HealthResponse(
            status="ok",
            service=settings.service_name,
            version=__version__,
            environment=settings.environment,
            mutations_topic=settings.mutations_topic,
            commands_topic=settings.commands_topic,
            interceptor_mode=settings.interceptor_mode,
            uptime_seconds=round(time.time() - metrics.started_at, 3),
        )

    @app.get("/readyz", tags=["ops"], summary="Readiness probe")
    async def readyz(request: Request) -> JSONResponse:
        publisher: CommandPublisher = request.app.state.publisher
        worker: CommandWorker = request.app.state.worker
        preflight: PreflightStatus = request.app.state.preflight
        states = worker.worker_states
        # "backpressure" is a healthy state: the worker is paced, not broken.
        consuming = sum(1 for state in states.values() if state in {"consuming", "backpressure"})

        ready = publisher.started and (settings.consumer_workers == 0 or consuming > 0)
        body = {
            "status": "ready" if ready else "degraded",
            "producer": "ok" if publisher.started else "unavailable",
            "consumers": {str(worker_id): state for worker_id, state in states.items()},
            "consuming": consuming,
            "expected": settings.consumer_workers,
            "interceptor_mode": settings.interceptor_mode,
            "preflight": preflight.as_dict(),
        }
        return JSONResponse(
            status_code=status.HTTP_200_OK if ready else status.HTTP_503_SERVICE_UNAVAILABLE,
            content=body,
        )

    @app.get("/metrics", tags=["ops"], summary="Prometheus exposition", include_in_schema=False)
    async def metrics_endpoint() -> Response:
        return Response(content=generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)

    @app.get("/stats", tags=["ops"], summary="Closure-loop counters")
    async def stats(request: Request) -> dict[str, Any]:
        metrics: Metrics = request.app.state.metrics
        gatekeeper: LicenseGatekeeper = request.app.state.gatekeeper
        worker: CommandWorker = request.app.state.worker
        gate: RateGate | None = request.app.state.rate_gate
        preflight: PreflightStatus = request.app.state.preflight
        return {
            "service": settings.service_name,
            "version": __version__,
            "config": {
                "mutations_topic": settings.mutations_topic,
                "commands_topic": settings.commands_topic,
                "dlq_topic": settings.commands_dlq_topic,
                "consumer_group": settings.consumer_group,
                "consumer_workers": settings.consumer_workers,
                "interceptor_mode": settings.interceptor_mode,
                "interceptor_endpoint": (
                    settings.interceptor_endpoint if settings.interceptor_mode == "http" else None
                ),
                # Tier routing, reported so an operator can see which planner a
                # given subscription actually reaches without reading the env.
                "enterprise_interceptor_endpoint": settings.enterprise_endpoint or None,
                "enterprise_interceptor_tiers": (
                    sorted(settings.enterprise_tiers) if settings.enterprise_endpoint else []
                ),
                "entitled_plans": sorted(settings.entitled_plans),
                "max_commands": settings.max_commands,
            },
            "metrics": metrics.snapshot(),
            "license_cache": gatekeeper.cache.stats(),
            "rate_gate": gate.stats() if gate is not None else None,
            "preflight": preflight.as_dict(),
            "consumers": {str(k): v for k, v in worker.worker_states.items()},
        }

    @app.post(
        "/v1/commands/issue",
        response_model=CommandBatch,
        status_code=status.HTTP_200_OK,
        tags=["closure"],
        summary="Close one mutation into commands synchronously",
        responses={
            401: {"model": ErrorResponse},
            422: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
    )
    async def issue_commands(
        payload: dict[str, Any],
        request: Request,
        subscription: Subscription = Depends(require_subscription),
        pipeline: ClosurePipeline = Depends(get_pipeline),
    ) -> CommandBatch:
        """Run the same pipeline the consumer runs, on demand.

        Useful for replaying a single mutation, for smoke tests, and for callers
        that would rather push a payload than publish to Kafka. The commands it
        produces land on ``ontology.commands`` exactly as the consumer's would.
        """
        correlation_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
        tokens = bind_log_context(
            request_id=correlation_id,
            tenant=subscription.tenant,
            event_id=str(payload.get("event_id", "-")),
        )
        try:
            # The dependency validated the header; the pipeline validates again
            # from the payload so both entry points run the identical gate.
            # Forwarding the presented key keeps that second check resolving to
            # the caller's subscription rather than the worker's fallback key.
            return await pipeline.close_loop(
                _with_presented_key(payload, request), correlation_id
            )
        finally:
            reset_log_context(tokens)

    @app.post(
        "/v1/interceptor-stub/plan",
        response_model=ActionPlan,
        tags=["closure"],
        summary="Stub endpoint simulating the Enterprise AI Interceptor",
        responses={401: {"model": ErrorResponse}, 422: {"model": ErrorResponse}},
    )
    async def interceptor_stub(
        payload: dict[str, Any],
        subscription: Subscription = Depends(require_subscription),
        router: ActionRouter = Depends(get_router),
    ) -> ActionPlan:
        """Expose the in-process planner over HTTP.

        Point ``OO_INTERCEPTOR_URL`` at this service and
        ``OO_INTERCEPTOR_PATH=/v1/interceptor-stub/plan`` to exercise the full
        HTTP routing path without the commercial service running.
        """
        try:
            mutation = MutationEnvelope.model_validate(payload)
        except ValidationError as exc:
            raise PermanentMessageError(
                "mutation_invalid", f"Payload did not match {MUTATION_SCHEMA_PREFIX}: {exc}"
            ) from exc
        return await router.stub.plan(mutation, subscription)


def _presented_key(request: Request) -> str | None:
    """Read the subscription key off a request's headers."""
    settings: Settings = request.app.state.settings
    presented = request.headers.get(settings.license_header) or request.headers.get("X-License-Key")
    if presented and presented.strip():
        return presented.strip()

    authorization = request.headers.get("Authorization", "")
    scheme, _, credentials = authorization.partition(" ")
    if scheme.lower() == "bearer" and credentials.strip():
        return credentials.strip()
    return None


def _with_presented_key(payload: dict[str, Any], request: Request) -> dict[str, Any]:
    """Copy the caller's key onto a payload for the pipeline's own gate.

    The payload is copied rather than mutated so a caller's body is never
    altered in place, and the key is dropped again by the time anything is
    published: only :class:`MutationEnvelope` fields reach a command.
    """
    forwarded = dict(payload)
    presented = _presented_key(request)
    if presented:
        forwarded["license_key"] = presented
    return forwarded


app = create_app()


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    _settings = get_settings()
    uvicorn.run(
        "command_worker:app",
        host=_settings.http_host,
        port=_settings.http_port,
        log_config=None,
        reload=_settings.environment == "local",
    )
