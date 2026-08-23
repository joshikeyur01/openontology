"""Service entrypoint for the GraphRAG interceptor.

``graphrag_interceptor.py`` is self-contained and already exposes its own
FastAPI app on its native contract (``EnrichedGraphPayload`` in,
``CommandActionResponse`` out). This wrapper adds the two things it needs to be
a deployable member of the topology rather than a library:

* **A mutation.v2 endpoint.** The closure loop speaks one contract to whichever
  interceptor it is pointed at. Mounting ``POST /v1/intercept`` here — taking a
  mutation, projecting it through :mod:`bridge`, and rendering the answer as the
  same command-sequence envelope the standard interceptor returns — makes this a
  drop-in alternative rather than a second protocol the worker has to learn.

* **Prometheus exposition**, so the dashboard covers both halves of the paid
  layer rather than only the one that shipped first.

The native surface is preserved under ``/v1/graphrag/intercept`` for a caller
that already speaks the richer contract and wants the full response — the fault
classification, cascade path, manual references and agent trace that the
worker's envelope has no place for.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

from fastapi import Depends, FastAPI, Request, Response, status
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, Counter, Histogram, generate_latest

import bridge
from graphrag_interceptor import (
    CommandCache,
    EnterpriseSubscription,
    MultiAgentGraphEngine,
    build_command_response,
    create_app,
    get_command_cache,
    get_engine,
    get_settings,
    verify_enterprise_subscription,
)

logger = logging.getLogger("openontology.graphrag.service")

# Naming matches the engine's and the standard interceptor's, so both halves of
# the paid layer land in one namespace on the dashboard.
REQUEST_DURATION = Histogram(
    "openontology_graphrag_request_duration_seconds",
    "Time to produce a command action, including both agent calls.",
    ["endpoint", "status"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

ACTIONS_ISSUED = Counter(
    "openontology_graphrag_actions_issued_total",
    "Command actions produced, by classification and action.",
    ["fault_classification", "action_type", "reversible"],
)

GUARDRAILS_APPLIED = Counter(
    "openontology_graphrag_guardrails_applied_total",
    "Server-side guardrails that fired after the planner.",
    ["guardrail"],
)

GRAPH_TRUST = Histogram(
    "openontology_graphrag_graph_trust",
    "How trustworthy the replicated topology was judged to be, 0 to 1.",
    buckets=(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0),
)

ADAPTER_REJECTIONS = Counter(
    "openontology_graphrag_adapter_rejections_total",
    "Mutations that could not be projected onto the GraphRAG contract.",
    ["reason"],
)


def build_service() -> FastAPI:
    settings = get_settings()

    # One billing export feeds every service. The module reads its own
    # OO_GRAPHRAG_LICENSE_REGISTRY_JSON, which also accepts a path; defaulting it
    # from the shared OO_LICENSE_REGISTRY_PATH is what stops this layer drifting
    # into a second, separately-maintained list of who has paid.
    if not os.environ.get("OO_GRAPHRAG_LICENSE_REGISTRY_JSON"):
        if shared := os.environ.get("OO_LICENSE_REGISTRY_PATH"):
            os.environ["OO_GRAPHRAG_LICENSE_REGISTRY_JSON"] = shared

    app = create_app()

    @app.middleware("http")
    async def _timing(request: Request, call_next):
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            REQUEST_DURATION.labels(endpoint=_route_of(request), status="500").observe(
                time.perf_counter() - started
            )
            raise
        REQUEST_DURATION.labels(
            endpoint=_route_of(request), status=str(response.status_code)
        ).observe(time.perf_counter() - started)
        return response

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(content=generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)

    @app.get("/readyz", tags=["ops"], summary="Readiness probe")
    async def readyz() -> JSONResponse:
        # Nothing external is required to plan: the deterministic kernel, the
        # retrieval corpus and the offline agent client are all in-process. The
        # probe reports what is configured rather than pretending to check a
        # dependency that does not exist.
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "status": "ready",
                "service": settings.service_name,
                "agent_provider": settings.agent_provider,
                "agent_model": settings.agent_model,
                "accepts": ["openontology.mutation.v1", "openontology.mutation.v2"],
            },
        )

    _register_mutation_route(app, settings)
    return app


def _route_of(request: Request) -> str:
    """Collapse to the route template so a 404 flood cannot create series."""
    route = request.scope.get("route")
    path = getattr(route, "path", None)
    return str(path) if path else "<unmatched>"


def _register_mutation_route(app: FastAPI, settings: Any) -> None:
    # The module's native route is remounted so the richer response stays
    # reachable, and the worker-facing contract takes the canonical path.
    #
    # Removed and re-added rather than repointed: Starlette compiles a route's
    # path into a regex at construction, so assigning to .path leaves the
    # compiled matcher untouched. The old route would keep matching
    # /v1/intercept, win on registration order, and validate raw mutations
    # against EnrichedGraphPayload — a 422 on every well-formed request.
    for route in list(app.router.routes):
        if getattr(route, "path", None) != "/v1/intercept":
            continue
        app.router.routes.remove(route)
        app.add_api_route(
            "/v1/graphrag/intercept",
            route.endpoint,
            methods=list(route.methods or {"POST"}),
            response_model=route.response_model,
            status_code=route.status_code,
            tags=list(route.tags or []),
            summary=route.summary,
            responses=route.responses,
        )

    @app.post(
        "/v1/intercept",
        tags=["commercial"],
        summary="Plan a command sequence from an ontology mutation",
    )
    async def intercept_mutation(
        mutation: dict[str, Any],
        response: Response,
        subscription: EnterpriseSubscription = Depends(verify_enterprise_subscription),
        engine: MultiAgentGraphEngine = Depends(get_engine),
        cache: CommandCache = Depends(get_command_cache),
    ) -> JSONResponse:
        event_id = str(mutation.get("event_id") or "")
        cache_key = (subscription.tenant, event_id)

        if cached := await cache.get(cache_key):
            response.headers["X-Idempotent-Replay"] = "true"
            action = bridge.CommandActionResponse.model_validate(cached)
            return JSONResponse(
                content=bridge.action_to_plan(action, model=settings.agent_model),
                headers={"X-Idempotent-Replay": "true"},
            )

        try:
            payload = bridge.mutation_to_graph_payload(mutation)
        except Exception as exc:
            # A projection failure is the caller's payload, not this service's
            # fault, so it is a 422 the worker classifies as terminal rather
            # than a 500 it would retry forever.
            ADAPTER_REJECTIONS.labels(reason=type(exc).__name__).inc()
            logger.warning(
                "mutation could not be projected onto the GraphRAG contract",
                extra={"event_id": event_id, "error": str(exc)},
            )
            return JSONResponse(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                content={
                    "error": {
                        "code": "mutation_not_projectable",
                        "message": f"could not project mutation onto the GraphRAG contract: {exc}",
                        "hint": "mutation.v2 with a resolved flow topology projects cleanly; "
                        "a v1 payload carries no upstream or downstream nodes.",
                    }
                },
            )

        outcome = await engine.run(payload)
        action = build_command_response(payload, outcome, tenant=subscription.tenant)
        await cache.put(cache_key, action.model_dump(mode="json"))

        ACTIONS_ISSUED.labels(
            fault_classification=action.fault_classification.value,
            action_type=action.action_type.value,
            reversible=str(action.reversible).lower(),
        ).inc()
        for guardrail in action.guardrails_applied:
            # Guardrail names are generated server-side from a closed set, so the
            # label space is bounded.
            GUARDRAILS_APPLIED.labels(guardrail=guardrail[:64]).inc()
        GRAPH_TRUST.observe(action.crdt_assessment.graph_trust)

        logger.info(
            "command action issued from mutation",
            extra={
                "event_id": action.event_id,
                "target_asset_id": action.target_asset_id,
                "action_type": action.action_type.value,
                "classification": action.fault_classification.value,
                "reversible": action.reversible,
                "graph_trust": action.crdt_assessment.graph_trust,
                "guardrails": len(action.guardrails_applied),
            },
        )

        return JSONResponse(
            content=bridge.action_to_plan(action, model=settings.agent_model),
            headers={"X-Idempotent-Replay": "false"},
        )


app = build_service()


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run("service:app", host="0.0.0.0", port=8010, log_config=None)
