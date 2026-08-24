#!/usr/bin/env python3
"""Offline checks for the closure loop's routing and failure model.

Standard library only, in the style of ``tools/produce_telemetry.py``: it needs
the service's own runtime dependencies and nothing else, so it runs inside the
built image without a test extra.

    docker run --rm -v "$PWD/services/command-worker:/app" -w /app \\
        -e OO_INTERCEPTOR_MODE=stub -e OO_KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \\
        --entrypoint python openontology-command-worker smoke_test.py

The two variables are for the module-level ``app = create_app()``, which reads
the ambient environment on import; every check below builds its own settings.

What it is here to prove is the part the stub hid. With an in-process simulator
there is no status code, no rate limit and no timeout, so the question "whose
fault is this failure" never had to be answered. Now it does, and the answer
decides whether a mutation is dead-lettered or redelivered — which is the
difference between an operator losing a command and an operator losing patience.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone

import httpx

from command_worker import (
    ActionRouter,
    CANONICAL_PLANS,
    HttpInterceptorClient,
    InterceptorError,
    LicenseRejected,
    Metrics,
    MutationEnvelope,
    RateGate,
    Settings,
    SubscriptionRegistry,
    build_gatekeeper,
    normalise_plan,
    parse_retry_after,
    retry_async,
)

FAILURES: list[str] = []
CHECKS = 0


def check(condition: bool, label: str) -> None:
    global CHECKS
    CHECKS += 1
    if not condition:
        FAILURES.append(label)
        print(f"  FAIL  {label}")
    else:
        print(f"  ok    {label}")


def section(title: str) -> None:
    print(f"\n{title}")
    print("-" * len(title))


def settings(**overrides) -> Settings:
    """Settings that ignore the ambient environment, so results are stable."""
    base = {
        "environment": "test",
        "kafka_bootstrap_servers": "kafka:29092",
        "license_key": "oo-live-standard-demo-key",
        "allowed_plans": "STANDARD,ENTERPRISE",
        "interceptor_mode": "http",
        "consumer_workers": 0,
    }
    base.update(overrides)
    return Settings(**base)


# ---------------------------------------------------------------------------
# 1. The two subscription vocabularies
# ---------------------------------------------------------------------------


def test_plan_vocabulary() -> None:
    section("subscription vocabulary")

    check(normalise_plan("SMALL_BUSINESS") == "STANDARD", "SMALL_BUSINESS folds into STANDARD")
    check(normalise_plan(" small-business ") == "STANDARD", "folding is case and separator tolerant")
    check(normalise_plan("ENTERPRISE") == "ENTERPRISE", "canonical names pass through")
    check(normalise_plan("PLATINUM") == "PLATINUM", "unknown plans survive to fail entitlement")

    registry = SubscriptionRegistry.fixtures()
    # These are the interceptor's LicenseRegistry.demo() tiers. If the two
    # tables ever drift again, this is where it shows up first.
    expected = {
        "oo-live-enterprise-demo-key": ("northwind-aerospace", "ENTERPRISE"),
        "oo-live-standard-demo-key": ("rotterdam-polymers", "STANDARD"),
        "oo-live-community-demo-key": ("community-user", "COMMUNITY"),
        "oo-live-expired-demo-key": ("lapsed-customer", "STANDARD"),
    }
    from command_worker import key_digest

    for key, (tenant, plan) in expected.items():
        record = registry.lookup(key_digest(key))
        check(record is not None, f"{key} is a known subscription")
        if record is not None:
            check(record.tenant == tenant, f"{key} -> tenant {tenant}")
            check(record.plan == plan, f"{key} -> plan {plan} (interceptor's tier name)")
            check(record.plan in CANONICAL_PLANS, f"{key} uses the canonical vocabulary")


def test_worker_accepts_its_own_licence() -> None:
    section("does the worker reject its own licence?")

    # The compose default: OO_LICENSE_KEY=oo-live-standard-demo-key.
    for allowed, label in (
        ("STANDARD,ENTERPRISE", "reconciled OO_ALLOWED_PLANS"),
        ("SMALL_BUSINESS,ENTERPRISE", "legacy OO_ALLOWED_PLANS still works"),
    ):
        gate = build_gatekeeper(settings(allowed_plans=allowed))
        try:
            subscription = gate.authorize({})
        except LicenseRejected as exc:
            check(False, f"{label}: rejected with {exc.code}")
            continue
        check(subscription.key_id == "lic_standard_demo", f"{label}: resolves lic_standard_demo")
        check(subscription.plan == "STANDARD", f"{label}: on the STANDARD plan")

    # And the paywall still bites where it should.
    gate = build_gatekeeper(settings(license_key="oo-live-community-demo-key"))
    try:
        gate.authorize({})
        check(False, "COMMUNITY is refused the closure loop")
    except LicenseRejected as exc:
        check(exc.code == "plan_not_entitled", "COMMUNITY is refused: plan_not_entitled")

    gate = build_gatekeeper(settings(license_key="oo-live-suspended-demo-key"))
    try:
        gate.authorize({})
        check(False, "a suspended subscription is refused")
    except LicenseRejected as exc:
        check(exc.code == "license_suspended", "a suspended subscription is refused before billing")


# ---------------------------------------------------------------------------
# 2. Whose fault is this failure?
# ---------------------------------------------------------------------------


def client(**overrides) -> HttpInterceptorClient:
    config = settings(**overrides)
    gate = RateGate(rpm=0, max_concurrency=4)
    return HttpInterceptorClient(config, httpx.AsyncClient(), gate, Metrics())


def test_failure_classification() -> None:
    section("failure classification: DLQ vs redeliver")

    subject = client()

    # (status, dead-lettered?, retried in-process?, label)
    matrix = [
        (400, True, False, "400 malformed payload"),
        (413, True, False, "413 payload too large"),
        (422, True, False, "422 contract violation"),
        (401, False, False, "401 missing/unknown key"),
        (402, False, False, "402 lapsed subscription"),
        (403, False, False, "403 tier not entitled"),
        (404, False, False, "404 misrouted endpoint"),
        (405, False, False, "405 wrong method"),
        (302, False, False, "302 redirect at the endpoint"),
        (408, False, True, "408 request timeout"),
        (429, False, True, "429 interceptor rate limit"),
        (500, False, True, "500 interceptor fault"),
        (502, False, True, "502 upstream model fault"),
        (503, False, True, "503 interceptor restarting"),
        (418, False, False, "an unrecognised status"),
    ]

    for status, dlq, retried, label in matrix:
        error = subject._failure(httpx.Response(status, text="{}"))
        check(error.permanent is dlq, f"{label}: {'dead-lettered' if dlq else 'redelivered, never DLQ'}")
        check(error.should_retry is retried, f"{label}: {'retried' if retried else 'not retried'} in process")

    # The property the task turns on, stated once as a whole.
    retryable = [row for row in matrix if not row[1]]
    errors = [subject._failure(httpx.Response(row[0], text="{}")) for row in retryable]
    check(
        not any(error.permanent for error in errors),
        "no retryable or environmental failure can reach the DLQ",
    )


def test_retry_after_is_honoured() -> None:
    section("Retry-After")

    check(parse_retry_after("12", cap=30) == 12.0, "delta-seconds parses")
    check(parse_retry_after(" 7 ", cap=30) == 7.0, "surrounding whitespace is tolerated")
    check(parse_retry_after("120", cap=30) == 30.0, "an over-long wait is clamped to the cap")
    check(parse_retry_after("-5", cap=30) == 0.0, "a negative wait floors at zero")
    check(parse_retry_after("soon", cap=30) is None, "garbage falls back to local backoff")
    check(parse_retry_after(None, cap=30) is None, "an absent header falls back to local backoff")
    check(parse_retry_after("nan", cap=30) is None, "NaN is rejected")

    future = datetime.now(tz=timezone.utc) + timedelta(seconds=10)
    stamp = future.strftime("%a, %d %b %Y %H:%M:%S GMT")
    parsed = parse_retry_after(stamp, cap=30)
    check(parsed is not None and 5.0 <= parsed <= 12.0, "an HTTP-date parses to a delta")

    subject = client()
    throttled = subject._failure(
        httpx.Response(429, headers={"Retry-After": "3"}, text="quota exceeded")
    )
    check(throttled.retry_after == 3.0, "a 429 carries its Retry-After onto the error")
    check(throttled.status_code == 429, "the status survives for the log line")


def test_retry_async_respects_the_exception() -> None:
    section("retry_async")

    async def scenario() -> None:
        attempts = 0

        async def not_retryable() -> None:
            nonlocal attempts
            attempts += 1
            raise InterceptorError("401", permanent=False, retryable=False, status_code=401)

        try:
            await retry_async(
                not_retryable,
                attempts=4,
                base_seconds=0.01,
                max_seconds=0.05,
                retry_on=(InterceptorError,),
                description="test",
            )
        except InterceptorError:
            pass
        check(attempts == 1, "a non-retryable error is not retried")

        attempts = 0

        async def retryable() -> str:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise InterceptorError("503", permanent=False, status_code=503)
            return "planned"

        result = await retry_async(
            retryable,
            attempts=4,
            base_seconds=0.01,
            max_seconds=0.05,
            retry_on=(InterceptorError,),
            description="test",
        )
        check(result == "planned" and attempts == 3, "a retryable error retries until it succeeds")

        # A server-named wait replaces the computed backoff, and the cap applies.
        started = time.monotonic()
        attempts = 0

        async def throttled() -> str:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise InterceptorError(
                    "429", permanent=False, status_code=429, retry_after=60.0
                )
            return "planned"

        await retry_async(
            throttled,
            attempts=3,
            base_seconds=0.01,
            max_seconds=0.02,
            retry_on=(InterceptorError,),
            description="test",
            retry_after_cap=0.2,
        )
        waited = time.monotonic() - started
        check(0.15 <= waited <= 1.0, "Retry-After overrides the computed backoff, under its cap")

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# 3. Backpressure
# ---------------------------------------------------------------------------


def test_rate_gate() -> None:
    section("rate gate")

    async def scenario() -> None:
        gate = RateGate(rpm=600, max_concurrency=4)  # 100ms apart
        started = time.monotonic()
        for _ in range(3):
            async with gate.slot():
                pass
        elapsed = time.monotonic() - started
        check(elapsed >= 0.18, f"three calls at 600rpm are paced apart ({elapsed:.3f}s)")

        gate = RateGate(rpm=0, max_concurrency=4)
        started = time.monotonic()
        async with gate.slot():
            pass
        check(time.monotonic() - started < 0.05, "an untuned gate does not pace")

        gate.apply_cooldown(0.25)
        started = time.monotonic()
        async with gate.slot():
            pass
        check(time.monotonic() - started >= 0.2, "a cooldown stalls the next caller")

        gate = RateGate(rpm=0, max_concurrency=4, margin=0.9)
        gate.retune(60, source="preflight")
        check(gate.rpm == 54, "the quota is adopted with its safety margin (60 -> 54)")
        gate.retune(600, source="x-ratelimit-limit")
        check(gate.rpm == 540, "an upgraded subscription retunes without a redeploy")

        # An explicitly configured ceiling is the operator's decision and must
        # survive both discovery paths.
        configured = settings(interceptor_max_rpm=120)
        gate = RateGate(
            rpm=configured.interceptor_max_rpm,
            max_concurrency=configured.interceptor_max_concurrency,
            margin=configured.interceptor_rate_margin,
        )
        check(gate.rpm == 108, "an explicit OO_INTERCEPTOR_MAX_RPM is applied with its margin")
        subject = HttpInterceptorClient(configured, httpx.AsyncClient(), gate, Metrics())
        subject._learn_quota(httpx.Response(200, headers={"X-RateLimit-Limit": "600"}))
        check(gate.rpm == 108, "X-RateLimit-Limit does not overrule an explicit ceiling")

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# 4. What actually goes on the wire
# ---------------------------------------------------------------------------


def mutation_payload() -> dict:
    now = datetime.now(tz=timezone.utc).isoformat()
    return {
        "event_id": "evt-smoke-0001",
        "schema_version": "openontology.mutation.v1",
        "producer": "ontology-resolution-engine",
        "emitted_at": now,
        "asset_id": "TURBOFAN-A320-0417",
        "transition": "RAISED",
        "severity": "HIGH",
        "breach_count": 1,
        "rule": {
            "rule_id": "rule-vibration",
            "sensor_id": "vibration_index",
            "operator": ">",
            "threshold": 8.5,
            "unit": "mm/s",
            "observed_value": 9.9,
            "exceeded_by": 1.4,
            "exceeded_pct": 16.5,
        },
        "telemetry_snapshot": {
            "trigger": {
                "sensor_id": "vibration_index",
                "value": 9.9,
                "unit": "mm/s",
                "observed_at": now,
                "age_seconds": 0.2,
            },
            "readings": [],
            "captured_at": now,
            "complete": True,
        },
        "ontology_context": {
            "asset_id": "TURBOFAN-A320-0417",
            "asset_name": "Fan module",
            "components": ["fan-bearing-1"],
            "assigned_operators": [],
            "parent_systems": [],
            "criticality": "HIGH",
        },
        "degraded": False,
        # An unmodelled field a newer engine might add.
        "engine_build": "1.4.0-rc2",
    }


def test_outbound_body() -> None:
    section("outbound body")

    payload = mutation_payload()
    payload["license_key"] = "oo-live-standard-demo-key"
    payload["headers"] = {"X-License-Key": "oo-live-standard-demo-key"}

    mutation = MutationEnvelope.model_validate(payload)
    subject = client()
    body = subject._body(mutation, payload)

    check("license_key" not in body, "the credential is stripped from the body")
    check("headers" not in body, "the header container is stripped from the body")
    check(body.get("engine_build") == "1.4.0-rc2", "an unmodelled engine field survives forwarding")
    check(
        body["telemetry_snapshot"]["trigger"]["observed_at"] is not None,
        "the snapshot the engine sent is forwarded intact",
    )

    # The regression this replaces: round-tripping through MutationEnvelope
    # rewrites anything it does not model as null, and the interceptor's
    # contract is the stricter of the two -- a null timestamp there is a 422,
    # which is a dead letter caused entirely by this worker.
    thin = {key: value for key, value in payload.items() if key != "telemetry_snapshot"}
    redumped = subject._body(MutationEnvelope.model_validate(thin), None)
    check(
        redumped["telemetry_snapshot"]["captured_at"] is None,
        "re-dumping invents nulls the engine never sent (why raw is forwarded)",
    )
    check(
        "engine_build" not in redumped,
        "re-dumping drops unmodelled fields (why raw is forwarded)",
    )


def test_degraded_context_is_not_a_dead_letter() -> None:
    section("degraded context (Go nil slices)")

    # Exactly what the engine publishes when the ontology graph is unreachable:
    # degraded=true and three nil slices, which Go marshals as null.
    payload = mutation_payload()
    payload["degraded"] = True
    payload["degraded_reason"] = 'graph_unavailable: asset not found in ontology graph'
    payload["ontology_context"] = {
        "asset_id": "TURBOFAN-A320-0417",
        "asset_name": "",
        "asset_class": "",
        "site": "",
        "criticality": "",
        "parent_systems": None,
        "components": None,
        "assigned_operators": None,
        "resolved_at": datetime.now(tz=timezone.utc).isoformat(),
        "source": "unavailable",
        "cache_hit": False,
    }
    payload["telemetry_snapshot"]["readings"] = None

    try:
        mutation = MutationEnvelope.model_validate(payload)
    except Exception as exc:  # noqa: BLE001 - the point is that nothing raises
        check(False, f"a degraded context parses instead of dead-lettering: {exc}")
        return

    check(mutation.ontology_context.parent_systems == [], "null parent_systems reads as empty")
    check(mutation.ontology_context.components == [], "null components reads as empty")
    check(mutation.ontology_context.assigned_operators == [], "null assigned_operators reads as empty")
    check(mutation.telemetry_snapshot.readings == [], "null readings reads as empty")
    check(mutation.degraded is True, "the degraded flag survives for the planner")

    async def scenario() -> None:
        config = settings(interceptor_mode="stub", interceptor_simulated_latency_ms=0)
        router = ActionRouter(config, None, None, None)
        subscription = build_gatekeeper(config).authorize({})
        plan = await router.route(mutation, subscription, payload)
        check(len(plan.commands) >= 1, "a degraded mutation still produces a plan")
        check(
            all(command.action not in {"ISOLATE", "SHUTDOWN"} for command in plan.commands),
            "thin context suppresses irreversible actions rather than the whole plan",
        )

    asyncio.run(scenario())


def test_stub_mode_still_works() -> None:
    section("stub mode is not regressed")

    async def scenario() -> None:
        config = settings(interceptor_mode="stub", interceptor_simulated_latency_ms=0)
        router = ActionRouter(config, None, None, None)
        check(router.mode == "stub", "the router selects the stub")

        gate = build_gatekeeper(config)
        subscription = gate.authorize({})
        mutation = MutationEnvelope.model_validate(mutation_payload())
        plan = await router.route(mutation, subscription, mutation_payload())

        check(plan.plan_id.startswith("plan_stub_"), "the stub mints a plan_stub_ id")
        check(plan.model == "stub-interceptor", "the stub identifies itself")
        check(len(plan.commands) >= 1, "the stub plans at least one command")
        check(
            plan.commands[0].action in {"SHIFT_SPEED", "INSPECT", "NOTIFY"},
            "a HIGH severity gets a proportionate action",
        )

    asyncio.run(scenario())


def main() -> int:
    print(f"command-worker smoke test — {datetime.now(tz=timezone.utc).isoformat()}")
    test_plan_vocabulary()
    test_worker_accepts_its_own_licence()
    test_failure_classification()
    test_retry_after_is_honoured()
    test_retry_async_respects_the_exception()
    test_rate_gate()
    test_outbound_body()
    test_degraded_context_is_not_a_dead_letter()
    test_stub_mode_still_works()

    print()
    if FAILURES:
        print(f"FAILED — {len(FAILURES)} of {CHECKS} checks")
        for failure in FAILURES:
            print(f"  - {failure}")
        return 1
    print(f"PASSED — {CHECKS} checks")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
