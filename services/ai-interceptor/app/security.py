"""Commercial licensing layer.

This is the seam that separates the open-core engine from the paid extension:
the Go engine emits mutations to Kafka for anyone, but reaching the AI planner
requires a valid subscription. The middleware demonstrates the four checks a
real commercial layer performs on every request — authentication, expiry,
feature entitlement and quota — before any billable work happens.

Keys are stored and compared as SHA-256 digests so the plaintext subscription
key never sits in process memory beyond the request that presented it.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Protocol

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from .logging_config import bind_request_context, reset_request_context
from .metrics import LICENSE_REJECTIONS, QUOTA_DECISIONS

logger = logging.getLogger(__name__)

# Feature flags gating individual commercial capabilities.
FEATURE_INTERCEPT = "ai.intercept"
FEATURE_AUTOPILOT = "ai.autopilot"
FEATURE_FLEET_ANALYTICS = "fleet.analytics"

#: The subscription vocabulary this service is the system of record for. It is
#: what ``GET /v1/license`` reports and what ``X-License-Tier`` stamps on every
#: response, so it is the vocabulary the rest of the topology conforms to. The
#: Kafka command worker mirrors these names in ``CANONICAL_PLANS``.
CANONICAL_TIERS = frozenset({"ENTERPRISE", "STANDARD", "COMMUNITY"})

#: Legacy names folded into the canonical set on load. The command worker
#: originally called the STANDARD tier ``SMALL_BUSINESS``; a billing export
#: written in either spelling must resolve to the same subscription in both
#: services.
TIER_ALIASES = {
    "SMALL_BUSINESS": "STANDARD",
    "SMALLBUSINESS": "STANDARD",
    "SMB": "STANDARD",
    "BUSINESS": "STANDARD",
    "FREE": "COMMUNITY",
    "OSS": "COMMUNITY",
}


def digest(key: str) -> str:
    """SHA-256 hex digest of a plaintext licence key."""
    return hashlib.sha256(key.strip().encode("utf-8")).hexdigest()


def normalise_tier(value: str) -> str:
    """Fold a tier or plan name into the canonical vocabulary.

    Unknown names pass through upper-cased: an unrecognised tier should fail the
    *entitlement* check rather than make the registry unreadable.
    """
    tier = value.strip().upper().replace("-", "_").replace(" ", "_")
    return TIER_ALIASES.get(tier, tier)


@dataclass(frozen=True)
class License:
    key_id: str
    key_digest: str
    tenant: str
    tier: str
    quota_per_minute: int
    expires_at: datetime
    features: frozenset[str] = field(default_factory=frozenset)

    def is_expired(self, now: datetime | None = None) -> bool:
        return (now or datetime.now(tz=timezone.utc)) >= self.expires_at

    def has(self, feature: str) -> bool:
        return feature in self.features


class LicenseRegistry:
    """In-memory subscription registry.

    A production deployment swaps this for the billing system of record; the
    lookup contract below is all the middleware depends on.
    """

    def __init__(self, licenses: list[License]) -> None:
        self._by_digest: dict[str, License] = {lic.key_digest: lic for lic in licenses}

    def __len__(self) -> int:
        return len(self._by_digest)

    def lookup(self, presented_key: str) -> License | None:
        """Resolve a plaintext key, comparing digests in constant time."""
        presented = digest(presented_key)
        record = self._by_digest.get(presented)
        if record is None:
            return None
        if not hmac.compare_digest(record.key_digest, presented):
            return None
        return record

    @classmethod
    def from_file(cls, path: Path) -> LicenseRegistry:
        """Load licences from JSON.

        Each entry accepts either ``key`` (plaintext, hashed on load) or
        ``key_digest`` (pre-hashed, preferred for production), and names the
        subscription level as either ``tier`` or ``plan`` — the command worker
        spells the same field differently, and one billing export has to be able
        to feed both. Values are folded through :func:`normalise_tier`.
        """
        raw = json.loads(path.read_text(encoding="utf-8"))
        licenses: list[License] = []
        for entry in raw:
            key_digest = entry.get("key_digest") or digest(entry["key"])
            licenses.append(
                License(
                    key_id=entry["key_id"],
                    key_digest=key_digest,
                    tenant=entry["tenant"],
                    tier=normalise_tier(entry.get("tier") or entry["plan"]),
                    quota_per_minute=int(entry["quota_per_minute"]),
                    expires_at=datetime.fromisoformat(entry["expires_at"]),
                    features=frozenset(entry.get("features", [])),
                )
            )
        return cls(licenses)

    @classmethod
    def demo(cls) -> LicenseRegistry:
        """Built-in licences for local development and the compose topology.

        Mirrored key-for-key by ``SubscriptionRegistry.fixtures()`` in the Kafka
        command worker, which presents one of these keys on every ``/v1/intercept``
        call once it runs in HTTP mode. The worker checks the agreement at boot
        against ``GET /v1/license``, so edits here must be made there too.
        """
        far_future = datetime.now(tz=timezone.utc) + timedelta(days=365)
        yesterday = datetime.now(tz=timezone.utc) - timedelta(days=1)
        return cls(
            [
                License(
                    key_id="lic_enterprise_demo",
                    key_digest=digest("oo-live-enterprise-demo-key"),
                    tenant="northwind-aerospace",
                    tier="ENTERPRISE",
                    quota_per_minute=600,
                    expires_at=far_future,
                    features=frozenset(
                        {FEATURE_INTERCEPT, FEATURE_AUTOPILOT, FEATURE_FLEET_ANALYTICS}
                    ),
                ),
                License(
                    key_id="lic_standard_demo",
                    key_digest=digest("oo-live-standard-demo-key"),
                    tenant="rotterdam-polymers",
                    tier="STANDARD",
                    quota_per_minute=60,
                    expires_at=far_future,
                    features=frozenset({FEATURE_INTERCEPT}),
                ),
                License(
                    key_id="lic_community_demo",
                    key_digest=digest("oo-live-community-demo-key"),
                    tenant="community-user",
                    tier="COMMUNITY",
                    quota_per_minute=10,
                    expires_at=far_future,
                    # No ai.intercept: the open-core tier proves the paywall.
                    features=frozenset({FEATURE_FLEET_ANALYTICS}),
                ),
                License(
                    key_id="lic_expired_demo",
                    key_digest=digest("oo-live-expired-demo-key"),
                    tenant="lapsed-customer",
                    tier="STANDARD",
                    quota_per_minute=60,
                    expires_at=yesterday,
                    features=frozenset({FEATURE_INTERCEPT}),
                ),
            ]
        )


class RateLimiter(Protocol):
    """The quota contract :class:`LicenseKeyMiddleware` depends on.

    Implementations record the hit and report
    ``(allowed, remaining, retry_after_seconds)``. Two exist:
    :class:`SlidingWindowLimiter` below, and
    :class:`~app.redis_state.RedisSlidingWindowLimiter` for deployments that
    run more than one worker.
    """

    async def check(self, key: str, limit: int) -> tuple[bool, int, float]: ...


class SlidingWindowLimiter:
    """Per-licence request quota over a sliding window.

    State is process-local: correct for exactly one worker with no replicas,
    and wrong by a factor of the replica count for anything else. Setting
    ``OO_REDIS_URL`` selects the shared implementation in ``app/redis_state.py``
    instead, which honours this same interface.
    """

    def __init__(self, window_seconds: int = 60) -> None:
        self._window = float(window_seconds)
        self._hits: dict[str, deque[float]] = {}
        self._lock = asyncio.Lock()

    async def check(self, key: str, limit: int) -> tuple[bool, int, float]:
        """Record a hit and report ``(allowed, remaining, retry_after)``."""
        now = time.monotonic()
        cutoff = now - self._window

        async with self._lock:
            bucket = self._hits.setdefault(key, deque())
            while bucket and bucket[0] <= cutoff:
                bucket.popleft()

            if len(bucket) >= limit:
                retry_after = max(0.0, bucket[0] + self._window - now)
                return False, 0, retry_after

            bucket.append(now)
            return True, max(0, limit - len(bucket)), 0.0


def error_response(
    status_code: int,
    code: str,
    message: str,
    request_id: str,
    hint: str | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    """Uniform error envelope for every rejection path."""
    body: dict[str, object] = {"error": {"code": code, "message": message}, "request_id": request_id}
    if hint:
        body["error"]["hint"] = hint  # type: ignore[index]
    response = JSONResponse(status_code=status_code, content=body, headers=headers)
    response.headers["X-Request-ID"] = request_id
    return response


class LicenseKeyMiddleware(BaseHTTPMiddleware):
    """Authenticate, entitle and meter every commercial request."""

    def __init__(
        self,
        app,
        *,
        registry: LicenseRegistry,
        limiter: RateLimiter,
        header_name: str,
        exempt_paths: frozenset[str],
        feature_by_prefix: dict[str, str],
    ) -> None:
        super().__init__(app)
        self._registry = registry
        self._limiter = limiter
        self._header = header_name
        self._exempt = exempt_paths
        self._feature_by_prefix = feature_by_prefix

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
        tokens = bind_request_context(request_id)
        started = time.perf_counter()

        try:
            if self._is_exempt(request.url.path):
                response = await call_next(request)
                return self._finalise(response, request_id, started)

            presented = self._extract_key(request)
            if not presented:
                return self._reject(
                    401,
                    "license_key_missing",
                    f"Provide a subscription key via the {self._header} header.",
                    request_id,
                    started,
                    hint="Contact sales@openontology.io for an enterprise key.",
                )

            license_record = self._registry.lookup(presented)
            if license_record is None:
                logger.warning("rejected request with unknown licence key", extra={"path": request.url.path})
                return self._reject(
                    401, "license_key_invalid", "The supplied subscription key is not recognised.", request_id, started
                )

            tokens_with_tenant = bind_request_context(request_id, license_record.tenant)
            reset_request_context(tokens)
            tokens = tokens_with_tenant

            if license_record.is_expired():
                return self._reject(
                    402,
                    "license_expired",
                    f"Subscription for tenant {license_record.tenant} expired at "
                    f"{license_record.expires_at.isoformat()}.",
                    request_id,
                    started,
                    hint="Renew the subscription to restore access.",
                )

            required_feature = self._required_feature(request.url.path)
            if required_feature and not license_record.has(required_feature):
                return self._reject(
                    403,
                    "feature_not_licensed",
                    f"Tier {license_record.tier} does not include '{required_feature}'.",
                    request_id,
                    started,
                    hint=f"Upgrade to a tier that includes '{required_feature}'.",
                )

            allowed, remaining, retry_after = await self._limiter.check(
                license_record.key_id, license_record.quota_per_minute
            )
            QUOTA_DECISIONS.labels(
                tier=license_record.tier,
                outcome="admitted" if allowed else "throttled",
            ).inc()
            if not allowed:
                return self._reject(
                    429,
                    "quota_exceeded",
                    f"Rate limit of {license_record.quota_per_minute} requests/minute exceeded.",
                    request_id,
                    started,
                    headers={
                        "Retry-After": str(max(1, int(retry_after) + 1)),
                        "X-RateLimit-Limit": str(license_record.quota_per_minute),
                        "X-RateLimit-Remaining": "0",
                    },
                )

            request.state.license = license_record
            request.state.request_id = request_id

            response = await call_next(request)
            response.headers["X-RateLimit-Limit"] = str(license_record.quota_per_minute)
            response.headers["X-RateLimit-Remaining"] = str(remaining)
            response.headers["X-License-Tier"] = license_record.tier

            elapsed_ms = (time.perf_counter() - started) * 1000
            logger.info(
                "request completed",
                extra={
                    "path": request.url.path,
                    "method": request.method,
                    "status": response.status_code,
                    "duration_ms": round(elapsed_ms, 2),
                    "license_key_id": license_record.key_id,
                },
            )
            return self._finalise(response, request_id, started)
        finally:
            reset_request_context(tokens)

    # -- helpers ----------------------------------------------------------

    def _is_exempt(self, path: str) -> bool:
        return path in self._exempt

    def _required_feature(self, path: str) -> str | None:
        for prefix, feature in self._feature_by_prefix.items():
            if path.startswith(prefix):
                return feature
        return None

    def _extract_key(self, request: Request) -> str | None:
        header_value = request.headers.get(self._header)
        if header_value and header_value.strip():
            return header_value.strip()

        authorization = request.headers.get("Authorization", "")
        scheme, _, credentials = authorization.partition(" ")
        if scheme.lower() == "bearer" and credentials.strip():
            return credentials.strip()
        return None

    def _reject(
        self,
        status_code: int,
        code: str,
        message: str,
        request_id: str,
        started: float,
        hint: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> JSONResponse:
        logger.warning(
            "request rejected by licensing layer",
            extra={"status": status_code, "reason": code},
        )
        # Counted here rather than at each call site: every refusal in this
        # middleware funnels through _reject, so one increment cannot drift out
        # of step with a new rejection branch someone adds later. `code` is a
        # closed set of error codes, so the label cardinality is bounded.
        LICENSE_REJECTIONS.labels(reason=code).inc()
        response = error_response(status_code, code, message, request_id, hint=hint, headers=headers)
        return self._finalise(response, request_id, started)

    @staticmethod
    def _finalise(response: Response, request_id: str, started: float) -> Response:
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Response-Time-ms"] = f"{(time.perf_counter() - started) * 1000:.2f}"
        return response
