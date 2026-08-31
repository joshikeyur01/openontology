# OpenOntology — Data Synchronization & AI Orchestration

Open-core digital twin platform for industrial assets.

* **Open core** — a Go *Ontology Resolution Engine* that keeps live twin state in Redis, evaluates anomaly rules against streaming telemetry, resolves the asset's graph neighbourhood, and publishes an **Enriched Context Payload**.
* **Commercial layer** — a FastAPI *Enterprise AI-Agent Interceptor* that authenticates a subscription, consumes that payload, and returns an **actionable command sequence** produced by an LLM under a strict tool schema.

```
                    ┌────────────────────┐
   edge gateways ──►│  telemetry.raw     │  (Kafka)
                    └─────────┬──────────┘
                              │
                  ┌───────────▼────────────────────────────────────┐
                  │  Ontology Resolution Engine (Go)               │
                  │                                               │
                  │  1. decode + validate                         │
                  │  2. HSET twin:<AssetID>:<SensorID>   ─► Redis  │
                  │  3. rules: vibration_index > 8.5              │
                  │           temperature_celsius > 110.0         │
                  │  4. alarm state machine (hysteresis, re-alert) │
                  │  5. graph context  ─► Neo4j (or fixtures)      │
                  │  6. serialize Enriched Context Payload        │
                  └───────────┬───────────────────────┬───────────┘
                              │                       │
                  ┌───────────▼────────┐   ┌──────────▼─────────┐
                  │ ontology.mutations │   │   telemetry.dlq    │
                  └───────────┬────────┘   └────────────────────┘
                              │
                  ┌───────────▼───────────────────────────────────┐
                  │  AI-Agent Interceptor (FastAPI) — PAID        │
                  │                                               │
                  │  license middleware: auth │ expiry │ feature   │
                  │                           │ quota              │
                  │  parse ontology_context + telemetry_snapshot   │
                  │  structured prompt ─► LLM ─► tool_use block    │
                  │  validated command sequence (ISOLATE, ...)     │
                  └───────────────────────────────────────────────┘
```

---

## Quick start

```bash
cp .env.example .env
make up          # build + start zookeeper, kafka, redis, engine, interceptor
make health      # both services report ready
make seed        # push a simulated telemetry run into telemetry.raw
make mutations   # watch enriched payloads land on ontology.mutations
make plan        # replay the newest mutation through the paid layer
make quota       # prove the licence quota holds across all four uvicorn workers
make smoke       # up + health + seed + assert both output topics carry records
make console     # open the read-only operator console
make dashboard   # open the provisioned Grafana pipeline dashboard
make targets     # confirm Prometheus is scraping every service
```

`make seed` ramps two assets from nominal into a threshold breach and back out
again, so a single run exercises `RAISED`, `ESCALATED` and `CLEARED`.

> **Telemetry must be keyed by asset.** `make seed` produces `<asset_id>|<json>`
> records and sets `parse.key=true`. Unkeyed records round-robin across the six
> partitions, so a channel's samples arrive interleaved and the engine's
> monotonic-timestamp guard correctly discards most of them as stale — measured
> at 81% dropped on an unkeyed run, 0% once keyed. Real gateways key by asset
> for the same reason.

| Endpoint | Purpose |
|---|---|
| `http://localhost:8081/readyz` | Engine readiness (includes a Redis ping) |
| `http://localhost:8081/stats` | Engine counters + effective configuration (JSON) |
| `http://localhost:8081/metrics` | Prometheus text exposition |
| `http://localhost:8000/healthz` | Interceptor liveness (unauthenticated) |
| `http://localhost:8000/docs` | Interceptor OpenAPI console |
| `http://localhost:8000/metrics` | Interceptor Prometheus exposition (aggregated across all four workers) |
| `http://localhost:8082/metrics` | Closure-loop Prometheus exposition |
| `http://localhost:8010/docs` | GraphRAG interceptor OpenAPI console (ENTERPRISE tier) |
| `http://localhost:8084/stats` | Edge replica — the second site's graph revision |
| `http://localhost:8083` | Operator console — live twin state, alarms and the commands produced |
| `http://localhost:3000` | Grafana — provisioned pipeline dashboard, no login |
| `http://localhost:9090` | Prometheus |

---

## 1. Go Ontology Resolution Engine

`services/resolution-engine` — one `main` package, one static binary.

| File | Responsibility |
|---|---|
| `main.go` | Wiring, signal handling, graceful drain, admin HTTP server |
| `config.go` | Environment parsing with accumulated validation errors |
| `model.go` | Wire types, identifier validation, rule evaluation |
| `state.go` | Sharded, mutex-guarded alarm state machine |
| `cache.go` | Redis live state (atomic Lua apply), snapshots, retry/backoff |
| `graph.go` | Fixture-backed graph stand-in (`GRAPH_PROVIDER=mock`) |
| `graph_neo4j.go` | Adapter putting the driver-backed resolver on the live path |
| `graph_cache.go` | The TTL cache both providers resolve through |
| `internal/graph/` | Pooled Neo4j resolver: blast radius and containment projections |
| `engine.go` | Consume → cache → evaluate → enrich → emit, plus DLQ routing |
| `metrics.go` | Lock-free counters, JSON and Prometheus renderers |

### Processing path

1. **Consume** `telemetry.raw` with a consumer group; `ENGINE_WORKERS` members share the partitions. Offsets are committed only after processing (at-least-once).
2. **Validate** `{asset_id, sensor_id, value, timestamp}`. Timestamps accept RFC3339 *or* epoch milliseconds. Identifiers are constrained to `[A-Za-z0-9][A-Za-z0-9._-]{0,127}` so the `twin:<asset>:<sensor>` key space stays unambiguous.
3. **Cache** via a Lua script that compares `observed_at_ms` server-side and rejects out-of-order samples atomically — no read-modify-write race. The sensor is registered in `twinindex:<asset>` so snapshots never need `SCAN`/`KEYS`.
4. **Evaluate** the rules: `vibration_index > 8.5` or `temperature_celsius > 110.0`. Severity is `CRITICAL` when the overshoot exceeds `RULE_CRITICAL_RATIO` (default 15%), otherwise `HIGH`.
5. **Mutate state** through the alarm state machine:

   | Condition | Emitted |
   |---|---|
   | first breach | `RAISED` |
   | severity increases while breached | `ESCALATED` |
   | still breached after `RULE_REALERT_INTERVAL` | `SUSTAINED` |
   | recovers below `limit × (1 − hysteresis)` | `CLEARED` |
   | anything else | *(absorbed — this is what stops a flapping sensor flooding the topic)* |

6. **Enrich** with the multi-variable Redis snapshot and the graph context (parent systems, components, assigned operators).
7. **Emit** to `ontology.mutations`, keyed by `AssetID` so all mutations for an asset stay ordered on one partition. The live twin's alarm hash (`twinalarm:<asset>:<sensor>`) is updated in the same step.

### Failure handling

| Failure | Behaviour |
|---|---|
| Malformed JSON / invalid fields | Published to `telemetry.dlq` with `dlq_reason` headers, offset committed — a poison pill never wedges a partition |
| Redis or Kafka error | Retried with exponential backoff + jitter (`ENGINE_MAX_ATTEMPTS`), then dead-lettered |
| Graph resolution fails | Mutation is emitted with `degraded: true` and `degraded_reason` — an operator would rather get a thin `CRITICAL` than none |
| Snapshot unavailable | Payload carries the trigger reading only, `telemetry_snapshot.complete: false` |
| Commit fails | Message is redelivered; every write is idempotent (monotonic timestamp in Redis, state machine suppresses duplicate transitions) |

### Concurrency

`StateTracker` is sharded 64 ways by FNV-1a hash of the twin key, so high-cardinality fleets do not serialise on one mutex. Counters are `atomic.Uint64`. The graph cache is `sync.RWMutex`-guarded and returns deep clones, so a cached context can never be mutated by a consumer goroutine. A janitor evicts idle channels every 10 minutes to bound memory.

### Configuration

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated bootstrap servers |
| `KAFKA_CONSUMER_GROUP` | `ontology-resolution-engine` | Consumer group id |
| `KAFKA_SOURCE_TOPIC` | `telemetry.raw` | Input topic |
| `KAFKA_MUTATION_TOPIC` | `ontology.mutations` | Enriched payload output |
| `KAFKA_DLQ_TOPIC` | `telemetry.dlq` | Dead-letter topic |
| `REDIS_ADDR` | `localhost:6379` | Live-state Redis |
| `TWIN_STATE_TTL` | `24h` | TTL on every twin key |
| `ENGINE_WORKERS` | `4` | Consumer-group members in this process |
| `ENGINE_MAX_ATTEMPTS` | `4` | Retries before dead-lettering |
| `ENGINE_OP_TIMEOUT` | `5s` | Per-message deadline |
| `RULE_VIBRATION_INDEX_MAX` | `8.5` | Vibration ceiling |
| `RULE_TEMPERATURE_CELSIUS_MAX` | `110.0` | Temperature ceiling |
| `RULE_CRITICAL_RATIO` | `0.15` | Overshoot fraction promoting `HIGH` → `CRITICAL` |
| `RULE_HYSTERESIS_RATIO` | `0.05` | Clearing deadband |
| `RULE_REALERT_INTERVAL` | `5m` | Re-assertion cadence for unresolved anomalies |
| `GRAPH_PROVIDER` | `mock` | `mock` (fixtures, no server) or `neo4j` (live graph) |
| `GRAPH_CACHE_TTL` | `5m` | Graph context cache lifetime |
| `GRAPH_QUERY_BUDGET` | `3s` | Hard ceiling on one traversal; must be ≤ `ENGINE_OP_TIMEOUT` |
| `GRAPH_SIMULATED_LATENCY` | `12ms` | Simulated round trip, `mock` only |
| `NEO4J_URI` | `bolt://neo4j:7687` | Bolt endpoint, `neo4j` only |
| `NEO4J_USERNAME` | `neo4j` | Bolt user |
| `NEO4J_PASSWORD` | — | Required when `GRAPH_PROVIDER=neo4j` |
| `NEO4J_DATABASE` | `neo4j` | Database to read from |
| `NEO4J_MAX_POOL_SIZE` | `32` | Concurrent Bolt connections |
| `NEO4J_CONNECT_TIMEOUT` | `10s` | Startup handshake ceiling |
| `HTTP_ADDR` | `:8081` | Admin listener |
| `LOG_LEVEL` | `info` | `debug\|info\|warn\|error` |

### The graph tier

Two providers satisfy the same `GraphResolver` interface, and `GRAPH_PROVIDER` picks between them. `/stats` and `/readyz` report which one is live, and `/metrics` carries it as `openontology_graph_provider_info{provider="…"}`.

**`mock`** — the binary's default, so `go run .` needs no graph server. Fixture-backed for the three known assets; anything else gets a deterministic synthesised context (`source: "neo4j-mock:synthesized"`) so arbitrary identifiers still exercise the full path.

**`neo4j`** — the compose default. `internal/graph` resolves the asset's containment subtree out of the live topology graph in one round trip (`CypherResolveOntologyNeighbourhood`: `:PART_OF` ancestry, `:HAS_COMPONENT` parts, `:RESPONSIBLE_FOR` accountability), and `graph_neo4j.go` projects it onto `OntologyContext`. Resolved contexts carry `source: "neo4j:live"`.

The graph sits on the ingestion hot path, so three things bound it:

* **The TTL cache fronts both providers.** A re-alerting asset re-traverses nothing; `make graph-stats` shows the hit rate.
* **`GRAPH_QUERY_BUDGET` is enforced twice** — once as the driver's transaction timeout, once at the adapter's call boundary, where it also covers connection acquisition and the driver's own retries. Config validation rejects a budget larger than `ENGINE_OP_TIMEOUT`, which would silently remove the bound.
* **Failure degrades, it never drops.** A resolution that errors, times out or finds no such asset produces `degraded: true` with the reason attached and the alarm still on the topic. The same holds at boot: an unreachable cluster logs loudly and keeps retrying rather than failing the process, because an engine that will not start drops every alarm.

The ontology itself is seeded from versioned revisions in `ops/neo4j/*.cypher`, applied by the `neo4j-seed` one-shot on `make up` and re-runnable with `make graph-seed`. Every statement is `MERGE` or `IF NOT EXISTS`, so re-applying is a no-op. The seeded fixtures mirror `defaultFixtures()` in `graph.go` exactly — a test asserts the two agree, so the same telemetry produces the same mutation under either provider, differing only in `source`.

To watch the degradation path: `make graph-kill`, `make seed`, `make mutations`, then `make graph-up`. The engine recovers without a restart; the Bolt driver re-opens its pool on its own.

### Enriched Context Payload

```json
{
  "event_id": "evt_9f2c1a...",
  "schema_version": "openontology.mutation.v1",
  "producer": "ontology-resolution-engine",
  "emitted_at": "2026-08-07T11:41:07.884Z",
  "asset_id": "TURBOFAN-A320-0417",
  "transition": "RAISED",
  "severity": "CRITICAL",
  "anomaly_active_since": "2026-08-07T11:41:07.884Z",
  "breach_count": 1,
  "rule": {
    "rule_id": "rule.vibration_index.max",
    "sensor_id": "vibration_index",
    "operator": ">",
    "threshold": 8.5,
    "unit": "mm/s",
    "observed_value": 11.4,
    "exceeded_by": 2.9,
    "exceeded_pct": 34.1176
  },
  "telemetry_snapshot": {
    "trigger": { "sensor_id": "vibration_index", "value": 11.4, "unit": "mm/s", "observed_at": "...", "age_seconds": 0.4 },
    "readings": [ { "sensor_id": "temperature_celsius", "value": 104.2, "unit": "degC", "observed_at": "...", "age_seconds": 1.1 } ],
    "captured_at": "...",
    "complete": true
  },
  "ontology_context": {
    "asset_id": "TURBOFAN-A320-0417",
    "asset_name": "CFM56-5B Turbofan #0417",
    "asset_class": "aero.propulsion.turbofan",
    "site": "MRO-TOULOUSE-B2",
    "criticality": "SAFETY_CRITICAL",
    "parent_systems": [ { "node_id": "SYS-PROP-A320-0417", "name": "Propulsion Subsystem (Engine 1)", "type": "Subsystem", "depth": 1 } ],
    "components": ["hpt_bearing_no3", "lpc_fan_module", "egt_harness", "fadec_channel_a"],
    "assigned_operators": [ { "operator_id": "OP-4471", "name": "L. Moreau", "role": "Lead Powerplant Engineer", "escalation_order": 1 } ],
    "maintenance_window": "2026-08-12T22:00:00Z/2026-08-13T04:00:00Z",
    "source": "neo4j:live",
    "cache_hit": false
  },
  "degraded": false,
  "source_partition": 3,
  "source_offset": 4211
}
```

---

## 2. FastAPI Enterprise AI-Agent Interceptor

`services/ai-interceptor` — the commercial extension hook.

### Licensing middleware

`LicenseKeyMiddleware` runs four checks before any billable work happens:

| Check | Failure | Status |
|---|---|---|
| Key present (`X-License-Key` or `Authorization: Bearer`) | `license_key_missing` | 401 |
| Key recognised | `license_key_invalid` | 401 |
| Subscription current | `license_expired` | 402 |
| Tier includes `ai.intercept` | `feature_not_licensed` | 403 |
| Within quota (sliding window) | `quota_exceeded` | 429 + `Retry-After` |

Keys are stored and compared as SHA-256 digests via `hmac.compare_digest`, so the plaintext key never persists beyond the request that presented it. Every response carries `X-Request-ID`, `X-Response-Time-ms`, `X-RateLimit-*` and `X-License-Tier`.

Demo keys (built-in registry; override with `OO_LICENSE_REGISTRY_PATH`):

| Key | Tenant | Tier | Result |
|---|---|---|---|
| `oo-live-enterprise-demo-key` | northwind-aerospace | ENTERPRISE | 200, 600 rpm |
| `oo-live-standard-demo-key` | rotterdam-polymers | STANDARD | 200, 60 rpm |
| `oo-live-community-demo-key` | community-user | COMMUNITY | 403 — proves the paywall |
| `oo-live-expired-demo-key` | lapsed-customer | STANDARD | 402 |

### Prompt and model call

`RemediationPromptContext` is a Pydantic model that owns its own rendering, so the exact bytes sent to the model are versioned with the schema. It parses `ontology_context` and `telemetry_snapshot` into a compact briefing (asset, anomaly, readings, escalation chain, tier constraints) and emits it as a fenced JSON block after a human-readable situation summary.

The response is constrained by a **tool schema** (`emit_command_sequence`, `strict: true`, `additionalProperties: false`) rather than parsed from prose, and forced with `tool_choice`, so the plan arrives as validated JSON rather than text to be salvaged.

That is the *only* thing this service asks of a language model — a forced tool call — and it is the whole provider contract:

```python
class LLMProvider(Protocol):
    """One request, one forced tool call, one structured answer."""

    name: str

    async def complete(
        self,
        *,
        model: str,
        system: str,
        user: str,
        tool: dict[str, Any],
        tool_name: str,
        max_tokens: int,
        effort: str,
    ) -> ToolCall: ...
```

Any tool-calling LLM API can satisfy it, and the planner never learns which one did. Two implementations ship:

| `OO_LLM_PROVIDER` | Implementation | Behaviour |
|---|---|---|
| `mock` (default) | `MockLLMClient`, in `app/llm.py` | Reads the briefing out of the prompt exactly as a model would, derives a deterministic plan, reports estimated token usage. No network, no key, no SDK. |
| `cloud` | `CloudLLMClient`, in `app/llm_cloud.py` | Forwards the same call to a live provider over HTTP. |

`app/llm_cloud.py` is the **only** module bound to a vendor — one lazy SDK import inside one class, plus the two provider extensions that do not generalise (a prompt-cache breakpoint on the byte-stable system prompt, and the reasoning-effort hint). Supporting a different provider means writing a sibling of that file and pointing `OO_LLM_PROVIDER` at it; the prompt, the tool schema, the planner, the validation and the API surface are untouched. Going live is `OO_LLM_PROVIDER=cloud` plus `OO_LLM_MODEL` and `OO_LLM_API_KEY` — one branch in `build_planner`, nothing else changes.

No vendor's model id is shipped as a default. `OO_LLM_MODEL` defaults to `openontology-mock-planner`, which is what actually produces the plan offline, and the service **refuses to start** in `cloud` mode until you name a model your provider serves.

The model's output is validated against `LLMPlan` (contiguous command sequences, enum-constrained actions and priorities, bounded confidence). Server-authoritative fields — `plan_id`, `model`, `usage`, `latency_ms` — are set by the service so the model cannot assert them.

### Response

```bash
curl -sS -X POST http://localhost:8000/v1/intercept \
  -H 'Content-Type: application/json' \
  -H 'X-License-Key: oo-live-enterprise-demo-key' \
  --data-binary @mutation.json
```

```json
{
  "plan_id": "plan_df71288fece0430cb493",
  "event_id": "evt_test_critical_0001",
  "asset_id": "TURBOFAN-A320-0417",
  "tenant": "northwind-aerospace",
  "model": "openontology-mock-planner",
  "severity": "CRITICAL",
  "confidence": 0.91,
  "commands": [
    {
      "sequence": 1,
      "target_component": "hpt_bearing_no3",
      "action": "ISOLATE",
      "priority": "CRITICAL",
      "assigned_to": "L. Moreau",
      "assigned_operator_id": "OP-4471",
      "parameters": { "isolation_scope": "Propulsion Subsystem (Engine 1)", "confirm_zero_energy": true },
      "expected_effect": "Remove hpt_bearing_no3 from load, halting broadband vibration excursion at 11.4 mm/s against a 8.5 limit.",
      "rollback": "Return to service only after inspection sign-off and a clean restart trend.",
      "deadline_seconds": 300
    }
  ],
  "escalation": { "required": true, "notify": ["S. Kaur"], "sla_seconds": 900 },
  "usage": { "input_tokens": 1089, "output_tokens": 632 }
}
```

Planning is idempotent per `(tenant, event_id)`: Kafka delivers at least once, so a repeated mutation replays the stored plan (`X-Idempotent-Replay: true`) instead of paying for a second inference.

### Shared state

Two things this service holds are correctness state, not caches: the licence quota and the idempotency record. Both used to live in process memory, which is correct for exactly one worker and silently wrong for two — nothing errors, the meter is just off by the replica count. The image now runs **four uvicorn workers** and both live in Redis.

Measured on the compose topology, firing 400 concurrent requests at the 60 rpm STANDARD subscription:

| Backend | Workers | Admitted |
|---|---|---|
| process-local (`OO_REDIS_URL` unset) | 4 | **240** — exactly 60 per worker, 4× the subscription |
| Redis (`OO_REDIS_URL` set) | 4 | **60** |

`make quota` runs the same check on every push through the compose smoke job. It defaults to the COMMUNITY key (10 rpm, 40 requests) rather than the STANDARD one, because it asserts an *exact* admitted count and must therefore be the only client presenting that key — the command worker validates its own subscription against `/v1/license` with the STANDARD key, and three of its checks landing mid-probe is the difference between 60 and 57.

**Quota.** `RedisSlidingWindowLimiter` keeps one sorted set per licence, `quota:<key_id>`, scored by observation time. Trim, count and admit happen inside a single Lua script, so the decision is atomic across every replica — a GET-then-SET limiter is not a limiter at all under concurrency, since every in-flight request reads the same count before any of them writes. The clock is Redis's own `TIME`, not the caller's, so skew between hosts cannot widen a tenant's window. Each key carries the window as its TTL, so the working set is bounded by traffic rather than by the number of licences ever issued.

**Idempotency.** `RedisPlanStore` keys plans as `plan:v1:<tenant>:<event_id>` with a TTL (`OO_IDEMPOTENCY_TTL_SECONDS`, default 24h) instead of the old LRU bound — replicas cannot agree on what is least recently used without another round trip, and a TTL states the actual invariant: a mutation is a duplicate for as long as its redelivery window is open. Writes use `SET NX`, so a stored plan is immutable for its lifetime and every later replay agrees on which plan it was.

**Failure policy, and why the two differ.** The Go engine's `idempotency_filter.go` fails *open*: dropping telemetry loses data no retry recovers, while a duplicate sample is absorbed by a monotonic cache write and a state machine that suppresses repeated transitions. Here:

| State | Policy | Reasoning |
|---|---|---|
| Quota | **fail open** — admit, count `failed_open` | A quota protects revenue and capacity. The cost of an outage is bounded and reversible: some requests go unmetered and the tenant is under-billed. Failing closed converts a metering outage into a total outage of the paid API for every tenant at once. |
| Idempotency | **fail closed** — 503 with `Retry-After` | Nothing is lost by refusing: the caller is a Kafka consumer whose offset is uncommitted, so it redelivers. A duplicate that gets through costs an inference and issues a second command sequence carrying `ISOLATE` and `SHUTDOWN` against a live asset, and nothing downstream absorbs it. |

Both are configurable (`OO_RATE_LIMIT_FAIL_OPEN`, `OO_IDEMPOTENCY_FAIL_OPEN`) — the defaults are an argument, not a law. Writing a plan is deliberately best effort even under the closed policy: the inference is already paid for, discarding it helps nobody, and the next delivery is gated by the read, which is still failing closed.

`GET /readyz` reports which backend is live, whether Redis answers, and the counters behind both policies.

Setting `OO_REQUIRE_SHARED_STATE=true` — as the compose file does — makes a worker that cannot reach Redis refuse to start, so a deployment cannot quietly regress to per-process quotas.

### Configuration

| Variable | Default | Meaning |
|---|---|---|
| `OO_LLM_PROVIDER` | `mock` | `mock` (offline, deterministic) or `cloud` (live provider) |
| `OO_LLM_MODEL` | `openontology-mock-planner` | Model id. Required — and validated — when provider is `cloud` |
| `OO_LLM_EFFORT` | `medium` | `low\|medium\|high\|xhigh\|max`, where the provider supports it |
| `OO_LLM_MAX_TOKENS` | `4096` | Output cap |
| `OO_LLM_TIMEOUT_SECONDS` | `30` | Planner deadline, enforced by the planner for every provider |
| `OO_LLM_API_KEY` | — | Required when provider is `cloud` |
| `OO_LICENSE_HEADER` | `X-License-Key` | Subscription header name |
| `OO_LICENSE_REGISTRY_PATH` | — | JSON registry replacing the demo keys |
| `OO_RATE_LIMIT_WINDOW_SECONDS` | `60` | Quota window |
| `OO_MAX_COMMANDS` | `6` | Upper bound on plan length |
| `OO_IDEMPOTENCY_CACHE_SIZE` | `512` | Plans held by the in-process fallback (0 disables) |
| `OO_LOG_LEVEL` | `INFO` | Log level |
| `OO_REDIS_URL` | — | Shared quota and idempotency state. Unset falls back to process-local state, correct for one worker only |
| `OO_REQUIRE_SHARED_STATE` | `false` | Refuse to start without a reachable Redis |
| `OO_QUOTA_KEY_PREFIX` | `quota:` | Sliding-window keyspace |
| `OO_PLAN_KEY_PREFIX` | `plan:` | Idempotency keyspace |
| `OO_IDEMPOTENCY_TTL_SECONDS` | `86400` | Lifetime of a stored plan |
| `OO_RATE_LIMIT_FAIL_OPEN` | `true` | Admit requests when the quota store is unreachable |
| `OO_IDEMPOTENCY_FAIL_OPEN` | `false` | Plan without a duplicate check when the store is unreachable |
| `OO_REDIS_OP_TIMEOUT_SECONDS` | `2` | Per-command deadline |
| `OO_REDIS_POOL_SIZE` | `16` | Connections per worker process |

---

## 3. Infrastructure

`docker-compose.yml` provisions Zookeeper, Kafka (dual INTERNAL/EXTERNAL listeners so host tooling and containers both work), Redis (AOF, `volatile-lru`), a one-shot `kafka-init` that creates the three topics with deliberate partition counts, and both services. Every dependency is gated on a healthcheck, and `resolution-engine` additionally waits for `kafka-init` to complete successfully.

Both application images run as non-root UID 10001 with a `HEALTHCHECK`. The Go image is a two-stage build producing a static binary on Alpine.

---

## Development

```bash
# Go
cd services/resolution-engine && go mod tidy && go vet ./... && go build ./...

# Go
cd services/resolution-engine && go test -race ./...

# Python — interceptor
cd services/ai-interceptor
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest tests -q
uvicorn app.main:app --reload

# The shared-state tests need a real Redis and skip themselves without one.
docker run -d --rm -p 6379:6379 redis:7.2-alpine
OO_TEST_REDIS_URL=redis://localhost:6379/0 OO_TEST_REQUIRE_REDIS=1 pytest tests -q

# Python — command worker
cd services/command-worker
OO_INTERCEPTOR_MODE=stub OO_KAFKA_BOOTSTRAP_SERVERS=kafka:29092 python smoke_test.py
```

---

## Production notes

Deliberate limitations, stated rather than hidden:

* **Two plans can still be produced for one event under an exact race.** The idempotency store closes the sequential case completely — a redelivery reads the stored plan and replays it. It does not close the window between a read that misses and the write that follows: two replicas handed the same mutation inside the planner's own latency both miss, and both plan. `SET NX` makes them agree afterwards on which plan every later replay returns, but the second inference has been paid for. Closing it properly needs a claim taken at read time and released on failure, as `idempotency_filter.go` does for ingestion; that is a different interface from `get`/`put` and has not been built here.
* **Compose runs a single-broker Kafka with `replication-factor 1`.** Fine for local end-to-end work, not for production durability.
* **Redis is a single node with `volatile-lru` and no replica.** Every key the interceptor writes carries a TTL, so quota windows and stored plans are both eviction candidates under memory pressure — an evicted plan means one duplicate inference, not a correctness failure, but a production deployment wants persistence and a failover target under this.
* **The licence registry is in-memory.** `LicenseRegistry` is the seam for the billing system of record; `from_file` shows the shape.
* **The seeded ontology is three assets.** It is enough to exercise every branch of the resolver, not a plant model. The `:FEEDS`/`:CONTROLS` flow network that `internal/graph`'s blast-radius projection (`ResolveAssetContext`) traverses is not seeded at all, so that projection has no live data behind it here — the mutation payload has no field for a blast radius, and inventing edges to fill one would be fiction.
* **Neo4j runs as a single community-edition node** with no replica and no backup. The engine degrades cleanly when it goes away, which is the property that matters locally, but a production deployment wants a cluster behind that Bolt URI.
* **Alarm timers use wall-clock time**, not event time, so a gateway with a skewed clock cannot suppress or spam re-alerts. Event time is preserved in the payload. Replaying historical data at speed therefore collapses `SUSTAINED` re-alerts — a `make seed` run replays 8 minutes of simulated time in seconds, so it never reaches the 5-minute re-alert interval.

## Continuous integration

`.github/workflows/ci.yml`, four jobs in parallel. Every action is pinned to a commit SHA rather than a tag; the version comment beside each is what humans read.

| Job | Does |
|---|---|
| **Go engine** | `gofmt -l` (fails on any output), `go vet`, `go build`, `go test -race ./...`. Go module and build caches keyed on `go.sum`. |
| **Python interceptor** | `pytest tests -q` against a real `redis:7.2-alpine` service container. `OO_TEST_REQUIRE_REDIS=1` turns the shared-state tests' skip into a failure, so a broken service block cannot report green. pip cache keyed on `requirements-dev.txt`. |
| **Python command worker** | `pytest smoke_test.py` *and* `python smoke_test.py`. Both, deliberately: the file's `check()` helper collects failures into a list rather than raising, so under pytest alone every test function returns cleanly whatever the checks found. pytest catches import and collection breakage; the script's exit code is the assertion that holds. |
| **Compose smoke** | `make up`, `make wait-health`, `make seed`, then asserts `ontology.mutations` and `ontology.commands` both received records, and that the licence quota still holds across the interceptor's four uvicorn workers. Logs dumped on failure, `make clean` always. |

The three fast jobs finish in about a minute each. The compose job is the long pole — pulling Kafka, ZooKeeper and Neo4j dominates it — and carries a 25 minute ceiling so a slow runner reports a real failure rather than a timeout.

## Verified

Built and exercised end to end on the compose topology:

| Check | Result |
|---|---|
| `gofmt -l` / `go vet` / `go build` (golang:1.22-alpine) | clean |
| `pytest tests -q` (interceptor, with Redis) | 48 passed |
| `python smoke_test.py` (command worker) | 95 checks passed |
| Quota, 4 uvicorn workers, 400 requests at 60 rpm | **exactly 60** admitted, rest `429` |
| Same probe with process-local state | 240 admitted — exactly 60 per worker, the bug this replaces |
| `make quota` (COMMUNITY key, 10 rpm, 40 requests) | 10 admitted, 30 × `429`, repeated 3× |
| One mutation posted 6 times across 4 workers | 1 inference, 5 × `X-Idempotent-Replay: true`, 1 `plan:` key in Redis |
| Idempotency store unreachable | `503 idempotency_unavailable`, `Retry-After: 5` |
| Quota store unreachable | request admitted, `failed_open` counter incremented |
| `go test -race ./...` | all three packages pass |
| 1920 telemetry events seeded | 1920 consumed, 1920 cached, **0 stale** |
| Rules evaluated / anomalies | 960 / 204 |
| Mutations emitted | 23 (`RAISED`, `ESCALATED`, `CLEARED`) |
| Severity promotion at 15% overshoot | `HIGH` → `CRITICAL` observed |
| Engine mutation → `POST /v1/intercept` | `ISOLATE` → `SHUTDOWN` → `INSPECT` → `NOTIFY`, assigned to `L. Moreau (OP-4471)` |
| COMMUNITY tier against the same payload | HTTP 403 `feature_not_licensed` |
