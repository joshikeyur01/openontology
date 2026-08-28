"""Prometheus instrumentation for the commercial interceptor.

The image runs four uvicorn workers, and a scrape reaches exactly one of them.
An in-process registry would therefore report a quarter of the traffic, varying
by which worker answered — the same class of bug the quota and idempotency
stores already exist to avoid, in a different disguise.

``prometheus_client``'s multiprocess mode is the fix: every worker writes its
samples into shared mmap files under ``PROMETHEUS_MULTIPROC_DIR`` and the
scrape aggregates across all of them. That directory must exist, be writable,
and be **emptied at container start** — stale files from a previous boot are
indistinguishable from a live worker's and would be summed into the totals.
The Dockerfile's entrypoint does the emptying; :func:`build_registry` verifies
the mode is actually engaged rather than assuming it.

Without the environment variable set (``pytest``, ``uvicorn --reload``) this
degrades to a normal single-process registry, which is correct for one worker
and is what the tests want.

Metric naming follows the engine's ``openontology_`` prefix so both halves of
the system land in one namespace on the dashboard.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Awaitable, Callable

from fastapi import Request, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    multiprocess,
)

logger = logging.getLogger("openontology.metrics")

MULTIPROC_ENV = "PROMETHEUS_MULTIPROC_DIR"

# Buckets are chosen for what this endpoint actually does rather than from the
# library default, which tops out at 10s and is built for sub-second web
# traffic. A plan involves a model call: the mock provider answers in tens of
# milliseconds, a real one takes seconds, and the planner's own deadline is 30s.
# The 30 and 60 buckets are what make a timeout visible as a shape rather than
# as a flat line at +Inf.
LATENCY_BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)

# Confidence is bounded 0-1 and the interesting question is "how much of today's
# planning was low-confidence", so the buckets are evenly spread rather than
# exponential.
CONFIDENCE_BUCKETS = (0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0)


# ---------------------------------------------------------------------------
# Collectors
# ---------------------------------------------------------------------------
#
# Labels are deliberately low cardinality. `tenant` is bounded by the licence
# registry, which is a billing artifact and stays small. `asset_id` is NOT a
# label anywhere here: a fleet has unbounded assets, and one series per asset is
# how a Prometheus instance falls over. Per-asset detail belongs in the operator
# console, which reads live state directly.

REQUEST_DURATION = Histogram(
    "openontology_interceptor_request_duration_seconds",
    "Time to serve a request, including the model call.",
    ["endpoint", "status"],
    buckets=LATENCY_BUCKETS,
)

PLAN_CONFIDENCE = Histogram(
    "openontology_interceptor_plan_confidence",
    "Confidence reported on issued command sequences.",
    ["tier", "severity"],
    buckets=CONFIDENCE_BUCKETS,
)

PLANS_ISSUED = Counter(
    "openontology_interceptor_plans_issued_total",
    "Command sequences produced by a model call.",
    ["tenant", "tier", "severity", "degraded"],
)

PLAN_REPLAYS = Counter(
    "openontology_interceptor_plan_replays_total",
    "Requests served from the idempotency store instead of a second inference.",
    ["tenant"],
)

COMMANDS_PLANNED = Counter(
    "openontology_interceptor_commands_planned_total",
    "Individual commands emitted across all plans, by action.",
    ["action"],
)

LICENSE_REJECTIONS = Counter(
    "openontology_interceptor_license_rejections_total",
    "Requests refused by the licensing middleware, by reason.",
    ["reason"],
)

QUOTA_DECISIONS = Counter(
    "openontology_interceptor_quota_decisions_total",
    "Sliding-window quota outcomes.",
    ["tier", "outcome"],
)

STATE_STORE_FAILURES = Counter(
    "openontology_interceptor_state_failures_total",
    "Shared-state operations that could not reach Redis, by store and policy.",
    ["store", "policy"],
)

BUILD_INFO = Gauge(
    "openontology_interceptor_build_info",
    "Always 1; the labels carry the build and configuration.",
    ["version", "llm_provider", "llm_model", "state_backend"],
)

# Every rejection reason the licensing middleware can produce. Declaring them
# here is what the engine's pre-populated transition series does, for the same
# reason: a labelled counter has no series at all until it first fires, so a
# dashboard panel reads "No data" instead of zero, and rate() over the window
# before the first rejection is undefined. Bounded and closed, so pre-creating
# them costs five series.
LICENSE_REJECTION_REASONS = (
    "license_key_missing",
    "license_key_invalid",
    "license_expired",
    "feature_not_licensed",
    "quota_exceeded",
)

for _reason in LICENSE_REJECTION_REASONS:
    LICENSE_REJECTIONS.labels(reason=_reason)


def build_registry() -> CollectorRegistry:
    """Return the registry a scrape should render.

    In multiprocess mode this is a fresh registry with the aggregating
    collector attached — not the default one, which holds only this worker's
    samples.
    """
    multiproc_dir = os.environ.get(MULTIPROC_ENV)
    if not multiproc_dir:
        # Single-process fallback. Correct under one worker, and what the test
        # suite and `uvicorn --reload` run with.
        from prometheus_client import REGISTRY

        return REGISTRY

    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry, path=multiproc_dir)
    return registry


def render() -> tuple[bytes, str]:
    """Render the exposition document and its content type."""
    return generate_latest(build_registry()), CONTENT_TYPE_LATEST


def verify_multiprocess_ready() -> dict[str, object]:
    """Report whether cross-worker aggregation is actually engaged.

    Called at startup and surfaced on /readyz. A deployment running four
    workers with this misconfigured does not fail — it silently reports a
    quarter of its traffic, which is the kind of wrong that survives for
    months. Saying so out loud is the whole point.
    """
    multiproc_dir = os.environ.get(MULTIPROC_ENV)
    if not multiproc_dir:
        return {"mode": "single-process", "aggregated": False}

    status: dict[str, object] = {"mode": "multiprocess", "path": multiproc_dir}
    if not os.path.isdir(multiproc_dir):
        status["aggregated"] = False
        status["error"] = f"{MULTIPROC_ENV} is set but {multiproc_dir!r} is not a directory"
        logger.error("prometheus multiprocess directory missing", extra={"path": multiproc_dir})
        return status

    if not os.access(multiproc_dir, os.W_OK):
        status["aggregated"] = False
        status["error"] = f"{multiproc_dir!r} is not writable by this process"
        logger.error("prometheus multiprocess directory not writable", extra={"path": multiproc_dir})
        return status

    status["aggregated"] = True
    return status


def mark_worker_exit() -> None:
    """Release this worker's per-process metric files.

    Counters live on in the aggregate after a worker dies, which is what you
    want — a restart must not reset a total. Gauges keyed to a dead pid do not,
    so the library is told the process is going away.
    """
    if not os.environ.get(MULTIPROC_ENV):
        return
    try:
        multiprocess.mark_process_dead(os.getpid())
    except Exception:  # pragma: no cover - best effort during shutdown
        logger.warning("could not mark metrics process dead", exc_info=True)


# ---------------------------------------------------------------------------
# Request timing
# ---------------------------------------------------------------------------


def normalise_endpoint(request: Request) -> str:
    """Collapse a request to its route template.

    The raw path would be fine today because no route carries a parameter, but
    a label built from `request.url.path` is one `/v1/plans/{id}` away from
    unbounded cardinality. Reading the matched route keeps that from ever being
    a one-line mistake.
    """
    route = request.scope.get("route")
    path = getattr(route, "path", None)
    if path:
        return str(path)
    # No route matched — a 404. Bucketing them all together is deliberate:
    # labelling by the requested path lets any caller create series at will.
    return "<unmatched>"


async def timing_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """Record duration and status for every request."""
    started = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        # An unhandled exception still becomes a 500 downstream, so it is
        # recorded as one rather than vanishing from the histogram.
        REQUEST_DURATION.labels(endpoint=normalise_endpoint(request), status="500").observe(
            time.perf_counter() - started
        )
        raise

    REQUEST_DURATION.labels(
        endpoint=normalise_endpoint(request),
        status=str(response.status_code),
    ).observe(time.perf_counter() - started)
    return response
