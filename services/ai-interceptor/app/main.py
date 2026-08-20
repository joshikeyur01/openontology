"""FastAPI application: the OpenOntology commercial extension hook.

The open-core Go engine publishes Enriched Context Payloads to Kafka. This
service is the paid layer that turns one of those payloads into an actionable
command sequence: it authenticates the subscription, parses ``ontology_context``
and ``telemetry_snapshot``, formats a structured prompt, calls the LLM, and
returns a validated plan.
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from . import __version__
from .config import Settings, get_settings
from .idempotency import (
    PlanStore,
    PlanStoreUnavailable,
    build_plan_store,
    plan_store_stats,
)
from .llm import (
    PlannerError,
    RemediationPlanner,
    RemediationPromptContext,
    build_command_sequence,
    build_planner,
)
from .logging_config import configure_logging, current_request_id
from .metrics import (
    BUILD_INFO,
    COMMANDS_PLANNED,
    PLAN_CONFIDENCE,
    PLAN_REPLAYS,
    PLANS_ISSUED,
    mark_worker_exit,
    render as render_metrics,
    timing_middleware,
    verify_multiprocess_ready,
)
from .models import (
    CommandSequence,
    EnrichedContextPayload,
    ErrorResponse,
    HealthResponse,
    LicenseIntrospection,
)
from .redis_state import (
    build_limiter,
    close_redis,
    create_redis_client,
    limiter_stats,
    ping_redis,
)
from .security import (
    FEATURE_INTERCEPT,
    License,
    LicenseKeyMiddleware,
    LicenseRegistry,
    error_response,
)

logger = logging.getLogger(__name__)

# /metrics is exempt from the licence check. Scrapers do not hold subscriptions,
# and a metrics endpoint that 401s is a metrics endpoint nobody is scraping. It
# exposes counters and tenant names, never payloads — see SECURITY.md on not
# publishing the operational ports.
EXEMPT_PATHS = frozenset(
    {"/healthz", "/readyz", "/metrics", "/", "/docs", "/redoc", "/openapi.json"}
)
FEATURE_BY_PREFIX = {"/v1/intercept": FEATURE_INTERCEPT}


def require_license(request: Request) -> License:
    """Fetch the licence the middleware attached to this request."""
    license_record = getattr(request.state, "license", None)
    if license_record is None:  # pragma: no cover - middleware guarantees this
        raise RuntimeError("license missing from request state")
    return license_record


def get_planner(request: Request) -> RemediationPlanner:
    return request.app.state.planner


def get_plan_cache(request: Request) -> PlanStore:
    return request.app.state.plan_cache


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    registry = (
        LicenseRegistry.from_file(settings.license_registry_path)
        if settings.license_registry_path
        else LicenseRegistry.demo()
    )
    # The client is built here rather than in the lifespan because the limiter
    # is a middleware constructor argument. redis-py connects lazily, so no I/O
    # happens until the first request — and each uvicorn worker process, which
    # imports this module for itself, ends up with its own pool against the one
    # shared server.
    redis_client = create_redis_client(settings)
    limiter = build_limiter(settings, redis_client)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.settings = settings
        app.state.started_at = time.time()
        app.state.planner = build_planner(settings)
        app.state.redis = redis_client
        app.state.limiter = limiter
        app.state.plan_cache = build_plan_store(settings, redis_client)
        app.state.metrics_status = verify_multiprocess_ready()

        BUILD_INFO.labels(
            version=__version__,
            llm_provider=settings.llm_provider,
            llm_model=settings.llm_model,
            state_backend=settings.state_backend(),
        ).set(1)

        if redis_client is not None:
            try:
                await ping_redis(redis_client, settings.redis_op_timeout_seconds)
                app.state.redis_ready = True
            except Exception as exc:
                app.state.redis_ready = False
                # Fail fast where shared state is mandatory: a worker that came
                # up without it would meter quotas per process and re-plan
                # duplicates, which is precisely the bug Redis is here to fix.
                if settings.require_shared_state:
                    raise RuntimeError(
                        f"OO_REQUIRE_SHARED_STATE is set but {settings.redis_url} is unreachable: {exc}"
                    ) from exc
                logger.warning("redis unreachable at startup; running degraded", extra={"error": str(exc)})
        else:
            app.state.redis_ready = False

        logger.info(
            "ai interceptor ready",
            extra={
                "environment": settings.environment,
                "llm_provider": settings.llm_provider,
                "llm_model": settings.llm_model,
                "licenses_loaded": len(registry),
                "state_backend": settings.state_backend(),
                "rate_limit_fail_open": settings.rate_limit_fail_open,
                "idempotency_fail_open": settings.idempotency_fail_open,
                "metrics": app.state.metrics_status,
            },
        )
        yield
        await close_redis(redis_client)
        mark_worker_exit()
        logger.info("ai interceptor shutting down")

    app = FastAPI(
        title="OpenOntology Enterprise AI-Agent Interceptor",
        version=__version__,
        summary="Commercial extension that converts ontology mutations into command sequences.",
        docs_url="/docs" if settings.docs_enabled else None,
        redoc_url="/redoc" if settings.docs_enabled else None,
        openapi_url="/openapi.json" if settings.docs_enabled else None,
        lifespan=lifespan,
    )

    # Registered before LicenseKeyMiddleware and therefore the outer of the two,
    # because Starlette applies middleware in reverse order of registration.
    # Being outermost is the point: a request refused with 401 or 429 by the
    # licence layer has to appear in the latency histogram too, or the endpoint
    # looks healthy precisely while it is rejecting everything.
    app.middleware("http")(timing_middleware)

    app.add_middleware(
        LicenseKeyMiddleware,
        registry=registry,
        limiter=limiter,
        header_name=settings.license_header,
        exempt_paths=EXEMPT_PATHS,
        feature_by_prefix=FEATURE_BY_PREFIX,
    )

    _register_error_handlers(app)
    _register_routes(app, settings)
    return app


def _register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(RequestValidationError)
    async def _validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        logger.warning("payload rejected", extra={"errors": exc.errors()[:5]})
        return error_response(
            422,  # literal: Starlette renamed the constant across versions
            "payload_invalid",
            "The enriched context payload did not match the expected schema.",
            current_request_id(),
            hint="Verify schema_version and required fields against openontology.mutation.v1.",
        )

    @app.exception_handler(PlanStoreUnavailable)
    async def _plan_store_unavailable(request: Request, exc: PlanStoreUnavailable) -> JSONResponse:
        logger.error("idempotency store unavailable", extra={"detail": str(exc)})
        return error_response(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "idempotency_unavailable",
            str(exc),
            current_request_id(),
            hint="Retry the mutation; the offset is uncommitted so redelivery is safe.",
            headers={"Retry-After": "5"},
        )

    @app.exception_handler(PlannerError)
    async def _planner_error(request: Request, exc: PlannerError) -> JSONResponse:
        logger.error("planner failure", extra={"detail": str(exc)})
        return error_response(
            status.HTTP_502_BAD_GATEWAY,
            "planner_unavailable",
            str(exc),
            current_request_id(),
            hint="Retry; if it persists check the upstream model provider.",
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
        started_at = getattr(request.app.state, "started_at", time.time())
        return HealthResponse(
            status="ok",
            service=settings.service_name,
            version=__version__,
            environment=settings.environment,
            llm_provider=settings.llm_provider,
            llm_model=settings.llm_model,
            uptime_seconds=round(time.time() - started_at, 3),
            state_backend=settings.state_backend(),
        )

    @app.get("/readyz", tags=["ops"], summary="Readiness probe including shared state")
    async def readyz(request: Request) -> JSONResponse:
        """Report whether quota and idempotency state is genuinely shared.

        The engine and the command worker both expose /readyz; this makes the
        interceptor's shared-state posture observable in the same way, which is
        what the multi-worker quota check reads to confirm it is not silently
        testing four independent in-process limiters.
        """
        state = request.app.state
        client = getattr(state, "redis", None)
        started_at = getattr(state, "started_at", time.time())

        redis_status: dict[str, object] = {"configured": client is not None}
        ready = True

        if client is not None:
            probe = time.perf_counter()
            try:
                await ping_redis(client, settings.redis_op_timeout_seconds)
                redis_status["reachable"] = True
                redis_status["latency_ms"] = round((time.perf_counter() - probe) * 1000, 3)
            except Exception as exc:
                redis_status["reachable"] = False
                redis_status["error"] = str(exc)
                # Unreachable Redis is only fatal to readiness where the
                # deployment declared it mandatory; otherwise the process is
                # still serving, just with the failure policies engaged.
                ready = not settings.require_shared_state
        elif settings.require_shared_state:  # pragma: no cover - settings reject this combination
            ready = False

        body = {
            "status": "ready" if ready else "degraded",
            "service": settings.service_name,
            "version": __version__,
            "uptime_seconds": round(time.time() - started_at, 3),
            "state_backend": settings.state_backend(),
            "redis": redis_status,
            "rate_limiter": limiter_stats(getattr(state, "limiter", None)),
            "plan_store": plan_store_stats(getattr(state, "plan_cache", None)),
            # Four workers with multiprocess mode off does not fail — it
            # reports a quarter of the traffic, which is the kind of wrong that
            # survives for months. Saying so on /readyz is the point.
            "metrics": getattr(state, "metrics_status", {"mode": "unknown"}),
        }
        return JSONResponse(
            status_code=status.HTTP_200_OK if ready else status.HTTP_503_SERVICE_UNAVAILABLE,
            content=body,
        )

    @app.get(
        "/metrics",
        tags=["ops"],
        summary="Prometheus exposition",
        include_in_schema=False,
    )
    async def metrics() -> Response:
        body, content_type = render_metrics()
        return Response(content=body, media_type=content_type)

    @app.get(
        "/v1/license",
        response_model=LicenseIntrospection,
        tags=["commercial"],
        summary="Introspect the presented subscription",
        responses={401: {"model": ErrorResponse}, 402: {"model": ErrorResponse}},
    )
    async def introspect(license_record: License = Depends(require_license)) -> LicenseIntrospection:
        return LicenseIntrospection(
            key_id=license_record.key_id,
            tenant=license_record.tenant,
            tier=license_record.tier,
            features=sorted(license_record.features),
            quota_per_minute=license_record.quota_per_minute,
            expires_at=license_record.expires_at,
            valid=not license_record.is_expired(),
        )

    @app.post(
        "/v1/intercept",
        response_model=CommandSequence,
        status_code=status.HTTP_200_OK,
        tags=["commercial"],
        summary="Convert an ontology mutation into an actionable command sequence",
        responses={
            401: {"model": ErrorResponse},
            402: {"model": ErrorResponse},
            403: {"model": ErrorResponse},
            422: {"model": ErrorResponse},
            429: {"model": ErrorResponse},
            502: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
    )
    async def intercept(
        payload: EnrichedContextPayload,
        response: Response,
        license_record: License = Depends(require_license),
        planner: RemediationPlanner = Depends(get_planner),
        cache: PlanStore = Depends(get_plan_cache),
    ) -> CommandSequence:
        cache_key = (license_record.tenant, payload.event_id)

        # Fail closed: when the store cannot answer, get() raises rather than
        # reporting a miss, and the 503 handler turns that into a retry.
        if cached := await cache.get(cache_key):
            response.headers["X-Idempotent-Replay"] = "true"
            PLAN_REPLAYS.labels(tenant=license_record.tenant).inc()
            logger.info(
                "replaying cached plan",
                extra={"event_id": payload.event_id, "asset_id": payload.asset_id},
            )
            return CommandSequence.model_validate(cached)

        context = payload.ontology_context
        snapshot = payload.telemetry_snapshot
        logger.info(
            "intercepting ontology mutation",
            extra={
                "event_id": payload.event_id,
                "asset_id": payload.asset_id,
                "severity": payload.severity.value,
                "transition": payload.transition.value,
                "sensor_id": payload.rule.sensor_id,
                "observed_value": payload.rule.observed_value,
                "threshold": payload.rule.threshold,
                "operators": len(context.assigned_operators),
                "parent_systems": len(context.parent_systems),
                "snapshot_readings": len(snapshot.readings),
                "snapshot_complete": snapshot.complete,
                "context_degraded": payload.degraded,
            },
        )

        prompt_context = RemediationPromptContext(
            payload=payload,
            tenant=license_record.tenant,
            tier=license_record.tier,
            max_commands=settings.max_commands,
            policy_notes=_policy_notes(license_record, payload),
        )

        result = await planner.plan(prompt_context)
        envelope = build_command_sequence(payload, result, tenant=license_record.tenant)
        command_sequence = CommandSequence.model_validate(envelope)

        await cache.put(cache_key, command_sequence.model_dump(mode="json"))
        response.headers["X-Idempotent-Replay"] = "false"

        # Recorded after validation, so the histogram describes plans that were
        # actually issued rather than ones the model proposed and the envelope
        # rejected. `degraded` is a label because low confidence on a degraded
        # input is expected, while low confidence on a complete one is not, and
        # a single confidence distribution cannot tell those apart.
        PLANS_ISSUED.labels(
            tenant=license_record.tenant,
            tier=license_record.tier,
            severity=payload.severity.value,
            degraded=str(payload.degraded).lower(),
        ).inc()
        PLAN_CONFIDENCE.labels(
            tier=license_record.tier,
            severity=payload.severity.value,
        ).observe(command_sequence.confidence)
        for command in command_sequence.commands:
            COMMANDS_PLANNED.labels(action=command.action.value).inc()

        logger.info(
            "command sequence issued",
            extra={
                "plan_id": command_sequence.plan_id,
                "event_id": command_sequence.event_id,
                "commands": len(command_sequence.commands),
                "primary_action": command_sequence.commands[0].action.value,
                "confidence": command_sequence.confidence,
                "escalation_required": command_sequence.escalation.required,
            },
        )
        return command_sequence


def _policy_notes(license_record: License, payload: EnrichedContextPayload) -> list[str]:
    """Tenant- and payload-specific constraints injected into the prompt."""
    notes = [f"Tenant {license_record.tenant} is on the {license_record.tier} tier."]
    if license_record.tier != "ENTERPRISE":
        notes.append("Autonomous execution is not licensed; every command requires human approval.")
    if payload.degraded:
        notes.append("Graph or snapshot context is incomplete; prefer inspection over irreversible actions.")
    if payload.ontology_context.criticality == "SAFETY_CRITICAL":
        notes.append("Asset is safety critical; airworthiness/permit-to-work sign-off is mandatory.")
    return notes


app = create_app()


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    _settings = get_settings()
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        log_config=None,
        reload=_settings.environment == "local",
    )
