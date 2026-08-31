# The open-core boundary

This document exists so you never have to guess which half of OpenOntology you
are allowed to run, fork or sell. It states the boundary, then points at the
lines of code that enforce it.

## What is free

Everything under Apache-2.0 (see [LICENSE](../LICENSE) and [NOTICE](../NOTICE)):

- **`services/resolution-engine`** — the Go ontology resolution engine, in full.
  Ingestion, validation, live twin state, the anomaly rules, the alarm state
  machine, the Neo4j graph tier, the distributed idempotency filter and the
  graph CRDT replication core.
- **`services/command-worker`** — the closure loop.
- **`services/operator-console`** — the read-only operator surface.
- **`ops/`, `tools/`, `schemas/`, `docs/`**, the compose file, the Makefile and CI.

You may run all of it in production, modify it, fork it, embed it in a product,
and offer it to third parties as a hosted service. Apache-2.0, no additional
conditions.

**The open core is a complete, useful system on its own.** It ingests telemetry,
maintains digital twin state, evaluates rules, resolves graph context and
publishes enriched mutations. Anyone can consume `ontology.mutations` and write
their own planner against the published schema. Nothing is crippled to sell the
paid layer.

## What is commercial

Under BUSL-1.1 (see [LICENSE-BUSL-1.1](../LICENSE-BUSL-1.1)):

- **`services/ai-interceptor`** — subscription licensing, metering, prompt
  construction, and the validated command sequence.
- **`services/graphrag-interceptor`** — the semantic GraphRAG two-agent
  isolation layer.

BUSL-1.1 is source-available, not proprietary. You may read it, modify it,
self-host it and run it in production under the Additional Use Grant. What the
grant withholds is exactly one thing: offering these services to third parties
as a hosted or managed service. On **2030-08-13** they convert to Apache-2.0.

## Where the seam is enforced in code

The boundary is a **network boundary**, not a build flag or a runtime licence
check bolted onto one binary. This matters: it means the open half genuinely
does not contain the closed half, and can be verified to not contain it.

### 1. The open core never imports the commercial layer

There is no import of `app.main`, `app.llm` or `app.security` anywhere under
`services/resolution-engine` or `services/command-worker`. The worker reaches
the interceptor over HTTP only:

```
services/command-worker/command_worker.py    OO_INTERCEPTOR_URL + OO_INTERCEPTOR_PATH
```

Deleting `services/ai-interceptor` entirely leaves the open core building and
passing its tests. That is the check that keeps this claim true, and CI runs it
as the `Open-core boundary` job: it removes both commercial services, then
requires `go build ./...` and `go test ./...` to pass and the command worker's
smoke run to complete in stub mode.

One deliberate exception, stated so the claim stays exact:
`tools/export_schemas.py` **does** import the interceptor's Pydantic models,
because that is how the published schemas are generated from the parser that
actually enforces them. It is a dev-time generator, not part of the open-core
runtime — nothing in `services/resolution-engine`, `services/command-worker` or
`services/operator-console` depends on it, which is what the boundary job
proves by deleting the commercial layer and running them anyway.

### 2. The open core degrades rather than fails without the paid layer

`OO_INTERCEPTOR_MODE=stub` swaps the HTTP planner for an in-process simulator
that needs no interceptor, no subscription and no network. The closure loop
still runs end to end and still publishes to `ontology.commands`; the plans it
produces carry `plan_id` prefixed `plan_stub_` so their provenance is never
ambiguous — `make closure` reports the split.

An open-core project where the free half stops working without the paid half is
not open core. This is the property that makes the difference observable.

### 3. Entitlement is checked before any billable work

`services/ai-interceptor/app/security.py` — `LicenseKeyMiddleware` runs ahead of
the route, so an unentitled request never reaches the planner and never costs an
inference:

| Check | Failure code | HTTP |
|---|---|---|
| Key present (`X-License-Key` or `Authorization: Bearer`) | `license_key_missing` | 401 |
| Key recognised | `license_key_invalid` | 401 |
| Subscription current | `license_expired` | 402 |
| Tier includes `ai.intercept` | `feature_not_licensed` | 403 |
| Within sliding-window quota | `quota_exceeded` | 429 |

Keys are stored and compared as SHA-256 digests through `hmac.compare_digest`,
so the plaintext key never sits in memory beyond the request that presented it.

### 4. Tier decides which planner you reach

This is the sharpest edge of the boundary, and it is a routing decision in the
open-core half:

```
services/command-worker/command_worker.py    HttpInterceptorClient._destination
```

A STANDARD subscription is planned by `services/ai-interceptor`. An ENTERPRISE
subscription is routed instead to `services/graphrag-interceptor`, which runs a
deterministic topology kernel over the flow network and CRDT state, then two
agents over a retrieved maintenance corpus. On the same mutation the difference
is visible: a three-step plan against a ten-step isolation sequence carrying a
fault classification, a blast radius and a CRDT trust assessment.

Both speak one contract — mutation in, command sequence out — so this is a URL
swap, not a second code path. Unset `OO_ENTERPRISE_INTERCEPTOR_URL` and the
routing disappears: one interceptor for every tier.

That substitutability is the same promise made to anyone reimplementing the paid
layer. The GraphRAG service is an existence proof that the seam works, because
it is a second implementation of the same interface.

### 5. Tier governs capability, not just access

The tier is not a yes/no gate — it changes what the system is permitted to do:

- `services/ai-interceptor/app/main.py` — `_policy_notes()` injects
  `"Autonomous execution is not licensed; every command requires human
  approval."` into the prompt for any tier below ENTERPRISE.
- `services/command-worker/command_worker.py` — `OO_ALLOWED_PLANS` decides which
  tiers may drive the loop at all.

COMMUNITY is a real tier that resolves, authenticates and then returns 403
`feature_not_licensed`. It exists so the paywall is demonstrable rather than
asserted — see the demo keys in `.env.example`.

### 6. The registry is a seam, not a hardcode

One `OO_LICENSE_REGISTRY_PATH` feeds all three services, so a single billing
export decides who has paid for what rather than each layer keeping its own
list. `LicenseRegistry` ships with four demo subscriptions covering every branch a
caller can hit (entitled, unentitled, expired, throttled).
`OO_LICENSE_REGISTRY_PATH` replaces them with a JSON export from a real billing
system without a code change; `from_file` documents the shape. The command
worker reads the same file and accepts either `tier` or `plan` as the field
name, so one billing export feeds both services.

## If you are forking this

Fork the Apache-2.0 half freely. If you want the commercial half's behaviour
without its licence, the interface you need to reimplement is small and fully
published:

- accept `openontology.mutation.v1` (see [SCHEMAS.md](SCHEMAS.md))
- return `openontology.command-sequence.v1`
- speak the error envelope the worker classifies on: 402 and 403 terminal, 429
  and 502 retryable

Point `OO_INTERCEPTOR_URL` at your implementation. Nothing in the open core
needs to change, which is the point of putting the seam on a network boundary.
