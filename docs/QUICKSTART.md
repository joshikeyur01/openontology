# Quickstart

From a cold clone to watching telemetry become maintenance commands. No API key,
no account, no cloud dependency — the default configuration runs the whole
topology offline with a deterministic planner.

## Requirements

| | |
|---|---|
| Docker with Compose v2 | `docker compose version` — needs v2, not `docker-compose` |
| Python 3 on the host | Only for `tools/` and a few Makefile targets |
| ~5 GB free RAM | Kafka, ZooKeeper, Neo4j, Redis, Prometheus and Grafana together |
| ~4 GB disk for images | First `make up` pulls them; later runs are cached |

You do **not** need Go, a Python virtualenv, an LLM provider API key, or a Neo4j
install. Everything runs in containers.

## Run it

```bash
git clone https://github.com/joshikeyur01/openontology.git
```

```bash
cd openontology && cp .env.example .env
```

```bash
make up
```

First run takes a few minutes — it pulls Kafka, ZooKeeper, Neo4j and Redis and
builds three service images. Then wait for everything to report ready:

```bash
make wait-health
```

Push a simulated telemetry run through it:

```bash
make seed
```

That ramps two assets from nominal into a threshold breach and back out, so a
single run exercises `RAISED`, `ESCALATED` and `CLEARED` rather than producing
noise that never crosses a line.

## See that it worked

Enriched mutations produced by the open-core engine:

```bash
make mutations
```

The command sequences the commercial layer planned from them:

```bash
make commands
```

Who planned what, and whether anything dead-lettered:

```bash
make closure
```

Live twin state for one asset, straight out of Redis:

```bash
make twin ASSET=TURBOFAN-A320-0417
```

## Watch it happen

```bash
make console
```

The operator console at http://localhost:8083. It leads with the thing the whole
pipeline exists to produce: the newest active alarm, and the command sequence
that was planned from it — each step with its expected effect, its rollback and
who it is assigned to. Below that, live twin state per asset and the two
activity feeds.

`make seed` ramps assets into breach and back out, so at rest everything reads
nominal. To leave assets alarming so there is something to look at, stop the
run before it recovers:

```bash
python3 tools/produce_telemetry.py --count 300 --assets TURBOFAN-A320-0418,HPP-PUMP-222 --anomaly-at 0.2 --recovery-at 1.0 | docker compose exec -T kafka kafka-console-producer --bootstrap-server localhost:29092 --topic telemetry.raw --property parse.key=true --property key.separator='|'
```

Use asset ids you have not seeded before. Replaying a previous run sends the
same timestamps, and the engine's monotonic guard correctly discards them as
stale.

## Watch it on the dashboard

```bash
make dashboard
```

Grafana at http://localhost:3000, already provisioned — no login, no
datasource to configure. Run `make seed` again with it open and you can watch
one burst move through the whole pipeline: ingestion spikes, anomalies become
RAISED and ESCALATED transitions, consumer lag on `ontology.mutations` climbs
while the interceptor plans and then drains back to zero, and commands come out
the far end broken down by action.

Confirm Prometheus is scraping every service:

```bash
make targets
```

## The 60-second tour

Replay the newest mutation through the paid layer and read the plan:

```bash
make plan
```

You should get an `ISOLATE` → `SHUTDOWN` → `INSPECT` → `NOTIFY` sequence with a
named accountable operator, a rollback for each step and a deadline.

Now prove the paywall is real. The COMMUNITY subscription authenticates
successfully and is then refused the feature:

```bash
curl -sS -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/v1/intercept -H 'X-License-Key: oo-live-community-demo-key' -H 'Content-Type: application/json' -d '{}'
```

`403`. An expired subscription gives `402`, a missing key `401`.

Prove the quota holds across all four uvicorn workers — a shared Redis window,
not four independent in-process limiters:

```bash
make quota
```

Watch the engine degrade rather than drop alarms when the graph goes away:

```bash
make graph-kill && make seed && make mutations
```

Mutations keep arriving carrying `degraded: true` and a reason. Bring it back:

```bash
make graph-up
```

The engine recovers without a restart; the Bolt driver re-opens its pool on its
own.

## See what the enterprise tier buys

The same mutation, planned by both tiers. First the standard interceptor:

```bash
make plan LICENSE_KEY=oo-live-standard-demo-key
```

Then the ENTERPRISE planner, which the closure loop routes to automatically for
an enterprise subscription:

```bash
make plan-graphrag
```

The second answers a question the first does not ask — which node is actually at
fault — and carries a fault classification, the blast radius, a CRDT trust
assessment and a full isolation sequence with an actor and a verification step
for each instruction.

## Survive a network partition

Two engine replicas run by default: a core site resolving from Neo4j, and an
edge site on local fixtures with no route to the shared topology store. They
exchange topology as a CRDT.

```bash
make replica
```

Both sites, their Lamport clocks and their graph revisions. Equal revisions mean
converged. Now cut the edge site off entirely:

```bash
make replica-split
```

Seed telemetry — only the core will see it — then check again. The sites have
diverged, and the edge is unreachable but still holding everything it had
learned. Reconnect:

```bash
make replica-heal
```

Within one sync interval `make replica` reports one digest across both sites,
and it stays there: an unchanged graph produces no further writes, so the clocks
stop moving. No coordination, no lock, no replay log — just a join.

## Endpoints

| URL | What |
|---|---|
| http://localhost:8081/stats | Engine counters and effective configuration |
| http://localhost:8081/metrics | Engine Prometheus exposition |
| http://localhost:8081/readyz | Engine readiness, includes a Redis ping |
| http://localhost:8000/docs | Interceptor OpenAPI console |
| http://localhost:8000/readyz | Interceptor readiness and shared-state backend |
| http://localhost:8082/stats | Closure-loop counters |
| http://localhost:8082/metrics | Closure-loop Prometheus exposition |
| http://localhost:8083 | Operator console — live twin state, alarms and commands |
| http://localhost:8084/stats | Edge replica — the second site's counters and graph revision |
| http://localhost:8010/docs | GraphRAG interceptor OpenAPI console (ENTERPRISE tier) |
| http://localhost:7474 | Neo4j browser (`neo4j` / `openontology`) |
| http://localhost:3000 | Grafana — the provisioned pipeline dashboard |
| http://localhost:9090 | Prometheus |

## Running against a real model

The default is a deterministic offline planner. It is not a stub: it implements
the same `LLMProvider` contract the live backend does — one request, one forced
tool call, one structured answer — and reads the same briefing out of the same
prompt, so the code path under test is the production one.

The service is provider-agnostic. It needs a tool-calling LLM API and nothing
more specific than that, so going live is configuration rather than code. Set
three values in `.env`:

```
OO_LLM_PROVIDER=cloud
OO_LLM_MODEL=<a model your provider serves>
OO_LLM_API_KEY=<your key>
```

`OO_LLM_MODEL` has no vendor default and is required here — the service refuses
to start in `cloud` mode rather than guess a model for you. Uncomment the
provider SDK in `services/ai-interceptor/requirements.txt`, then `make up`
again. Nothing else changes: it is one branch in `build_planner`, and
`app/llm_cloud.py` is the only module that knows a vendor exists.

The ENTERPRISE-tier GraphRAG planner works the same way, under its own settings
(`OO_GRAPHRAG_AGENT_PROVIDER=cloud`, `OO_GRAPHRAG_AGENT_MODEL`,
`OO_GRAPHRAG_AGENT_API_KEY`), so the two tiers can run on different providers.

## Tear down

```bash
make down
```

`make clean` also deletes the volumes, so the next `make up` starts from an
empty Kafka, Redis and Neo4j.

## If something goes wrong

**`make wait-health` times out.** Check what is actually up with
`docker compose ps`. Kafka needs ZooKeeper healthy first and Neo4j takes the
longest to accept Cypher; on a slow machine raise the budget with
`make wait-health HEALTH_TIMEOUT=300`.

**`make seed` reports a connection error.** The seed target produces through the
Kafka container, so the topology has to be up first. Run `make wait-health`.

**Mutations arrive but `make commands` is empty.** The worker plans through the
interceptor over HTTP; check `make closure` for its preflight state and
`docker compose logs command-worker`. `OO_INTERCEPTOR_MODE=stub` in `.env` runs
the loop without the interceptor at all, which isolates whether the problem is
the worker or the paid layer.

**Port already allocated.** The topology binds 3000, 6379, 7474, 7687, 8000,
8010, 8081, 8082, 8083, 8084, 9090 and 9092 on localhost. Stop whatever holds the port, or edit the `ports:`
mapping in `docker-compose.yml`.

**Everything is wedged and you want a clean slate.** `make clean` removes the
volumes; the next `make up` rebuilds from nothing.

## Next

- [ARCHITECTURE.md](ARCHITECTURE.md) — what the pieces are and why the boundaries fall where they do
- [SCHEMAS.md](SCHEMAS.md) — the payload contracts, with generated JSON Schema
- [OPEN-CORE.md](OPEN-CORE.md) — what is free, what is not, and where that is enforced
- [../CONTRIBUTING.md](../CONTRIBUTING.md) — running the tests and sending a change
