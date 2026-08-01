SHELL := /bin/bash
COMPOSE ?= docker compose
LICENSE_KEY ?= oo-live-enterprise-demo-key
GRAPHRAG_KEY ?= oo-live-graphrag-enterprise-key
SIM_COUNT ?= 240
SIM_ASSETS ?= TURBOFAN-A320-0417,HPP-PUMP-221
CLOSURE_SAMPLE ?= 400
# The compose network, used by the replica partition targets.
NETWORK ?= openontology
# The probe asserts an exact admitted count, so it must be the only client
# presenting its key. The COMMUNITY subscription is the only demo key no service
# in the topology uses: the command worker validates itself against /v1/license
# with the STANDARD key, which would silently eat slots mid-probe. Four workers
# with process-local state would admit 40 of the 40 requests below; one shared
# window admits 10. QUOTA_KEY_ID is the registry key_id the window is keyed on,
# cleared first so the count starts from zero.
QUOTA_KEY ?= oo-live-community-demo-key
QUOTA_KEY_ID ?= lic_community_demo
QUOTA_LIMIT ?= 10
QUOTA_REQUESTS ?= 40
# Seconds to wait for the topology to come up before giving up.
HEALTH_TIMEOUT ?= 180
TOPIC_TIMEOUT_MS ?= 60000
# Must match the NEO4J_PASSWORD docker-compose hands the graph and the engine.
NEO4J_PASSWORD ?= openontology

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: up
up: ## Build and start the full topology
	$(COMPOSE) up -d --build
	@echo "resolution engine : http://localhost:8081/stats"
	@echo "ai interceptor    : http://localhost:8000/docs"
	@echo "command worker    : http://localhost:8082/stats"
	@echo "operator console  : http://localhost:8083"
	@echo "edge replica      : http://localhost:8084/stats"
	@echo "graphrag (paid)   : http://localhost:8010/docs"
	@echo "topology graph    : http://localhost:7474 (neo4j / $(NEO4J_PASSWORD))"
	@echo "dashboard         : http://localhost:3000"
	@echo "prometheus        : http://localhost:9090"

.PHONY: down
down: ## Stop the topology (keeps volumes)
	$(COMPOSE) down

.PHONY: clean
clean: ## Stop the topology and delete volumes
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Tail engine, interceptor and command worker logs
	$(COMPOSE) logs -f resolution-engine ai-interceptor command-worker

.PHONY: topics
topics: ## List Kafka topics
	$(COMPOSE) exec kafka kafka-topics --bootstrap-server localhost:29092 --list

.PHONY: seed
seed: ## Push a simulated telemetry run into telemetry.raw
	python3 tools/produce_telemetry.py --count $(SIM_COUNT) --assets $(SIM_ASSETS) \
	  | $(COMPOSE) exec -T kafka kafka-console-producer \
	      --bootstrap-server localhost:29092 --topic telemetry.raw \
	      --property parse.key=true --property key.separator='|'

.PHONY: mutations
mutations: ## Tail the ontology.mutations topic
	$(COMPOSE) exec kafka kafka-console-consumer --bootstrap-server localhost:29092 \
	  --topic ontology.mutations --from-beginning --max-messages 20

.PHONY: commands
commands: ## Tail the ontology.commands topic
	$(COMPOSE) exec kafka kafka-console-consumer --bootstrap-server localhost:29092 \
	  --topic ontology.commands --from-beginning --max-messages 20

.PHONY: closure
closure: ## Show who planned the commands on ontology.commands, and the DLQ depth
	@echo "--- command worker ---"
	@curl -sS http://localhost:8082/stats | python3 -c "import json,sys; d=json.load(sys.stdin); c=d['config']; m=d['metrics']; \
	print('mode          :', c['interceptor_mode'], '->', c['interceptor_endpoint']); \
	print('entitled plans:', c['entitled_plans']); \
	print('preflight     :', d['preflight']['state'], '| worker', d['preflight']['worker_tenant'], d['preflight']['worker_plan'], \
	      '| interceptor', d['preflight']['interceptor_tenant'], d['preflight']['interceptor_tier']); \
	print('rate gate     :', d['rate_gate']); \
	print('plans/commands:', m['plans_requested'], '/', m['commands_issued'], '| dead-lettered:', m['dead_lettered']); \
	print('interceptor   :', m['interceptor'])"
	@echo "--- plan provenance on ontology.commands ---"
	@$(COMPOSE) exec -T kafka kafka-console-consumer --bootstrap-server localhost:29092 \
	  --topic ontology.commands --from-beginning --max-messages $(CLOSURE_SAMPLE) --timeout-ms 20000 2>/dev/null \
	  | python3 -c "import json,sys; rows=[json.loads(l) for l in sys.stdin if l.strip()]; \
	stub=[r for r in rows if r['plan_id'].startswith('plan_stub_')]; \
	real=[r for r in rows if not r['plan_id'].startswith('plan_stub_')]; \
	print(f'{len(rows)} commands: {len(real)} from the real interceptor (plan_*), {len(stub)} from the stub (plan_stub_*)'); \
	print('tenants:', sorted({r[\"tenant\"] for r in rows}), '| licences:', sorted({r[\"license_key_id\"] for r in rows}))"
	@echo "--- ontology.commands.dlq depth (0 is the healthy answer) ---"
	@$(COMPOSE) exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
	  --bootstrap-server localhost:29092 --topic ontology.commands.dlq 2>/dev/null \
	  | awk -F: '{s+=$$3} END {print s+0}'

.PHONY: dlq
dlq: ## Tail the dead-letter topics
	$(COMPOSE) exec kafka kafka-console-consumer --bootstrap-server localhost:29092 \
	  --topic telemetry.dlq --from-beginning --max-messages 20
	$(COMPOSE) exec kafka kafka-console-consumer --bootstrap-server localhost:29092 \
	  --topic ontology.commands.dlq --from-beginning --max-messages 20

.PHONY: graph-seed
graph-seed: ## Re-apply the versioned Cypher revisions in ops/neo4j
	$(COMPOSE) run --rm --build neo4j-seed

.PHONY: graph
graph: ## Show what the topology graph holds for one asset (ASSET=<id>)
	@test -n "$(ASSET)" || (echo "usage: make graph ASSET=TURBOFAN-A320-0417" && exit 1)
	@$(COMPOSE) exec -T neo4j cypher-shell -u neo4j -p $(NEO4J_PASSWORD) --format verbose \
	  "MATCH (a:Asset {id: '$(ASSET)'}) \
	   OPTIONAL MATCH (a)-[:PART_OF*1..4]->(s:System) \
	   OPTIONAL MATCH (a)-[:HAS_COMPONENT]->(c:Component) \
	   OPTIONAL MATCH (o:Operator)-[r:RESPONSIBLE_FOR]->(a) \
	   RETURN a.name AS asset, a.site AS site, a.criticality AS criticality, \
	          collect(DISTINCT s.name) AS parent_systems, \
	          collect(DISTINCT c.name) AS components, \
	          collect(DISTINCT o.id + ' (' + toString(r.escalation_order) + ')') AS operators"

.PHONY: graph-kill
graph-kill: ## Stop Neo4j, to watch the engine degrade instead of dropping alarms
	$(COMPOSE) stop neo4j
	@echo "neo4j stopped; mutations will carry degraded=true until 'make graph-up'"

.PHONY: graph-up
graph-up: ## Restart Neo4j after graph-kill
	$(COMPOSE) start neo4j

.PHONY: graph-stats
graph-stats: ## Show which graph provider is live and how it is behaving
	@curl -sS http://localhost:8081/stats | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["graph"], indent=2))'

.PHONY: twin
twin: ## Inspect live twin state in Redis (ASSET=<id>)
	@test -n "$(ASSET)" || (echo "usage: make twin ASSET=TURBOFAN-A320-0417" && exit 1)
	$(COMPOSE) exec redis redis-cli --scan --pattern 'twin:$(ASSET):*'
	@echo "--- vibration_index ---"
	$(COMPOSE) exec redis redis-cli hgetall 'twin:$(ASSET):vibration_index'

.PHONY: plan
plan: ## Replay the newest mutation through the paid interceptor
	@$(COMPOSE) exec -T kafka kafka-console-consumer --bootstrap-server localhost:29092 \
	  --topic ontology.mutations --from-beginning --max-messages 1 --timeout-ms 10000 2>/dev/null \
	  | curl -sS -X POST http://localhost:8000/v1/intercept \
	      -H 'Content-Type: application/json' \
	      -H 'X-License-Key: $(LICENSE_KEY)' \
	      --data-binary @- \
	  | python3 -m json.tool

.PHONY: plan-graphrag
plan-graphrag: ## Replay the newest mutation through the ENTERPRISE GraphRAG planner
	@$(COMPOSE) exec -T kafka kafka-console-consumer --bootstrap-server localhost:29092 \
	  --topic ontology.mutations --from-beginning --max-messages 1 --timeout-ms 10000 2>/dev/null \
	  | curl -sS -X POST http://localhost:8010/v1/intercept \
	      -H 'Content-Type: application/json' \
	      -H 'X-OpenOntology-License: $(GRAPHRAG_KEY)' \
	      --data-binary @- \
	  | python3 -m json.tool

.PHONY: health
health: ## Probe every service
	@curl -sS http://localhost:8081/readyz | python3 -m json.tool
	@curl -sS http://localhost:8000/readyz | python3 -m json.tool
	@curl -sS http://localhost:8082/readyz | python3 -m json.tool
	@curl -sS http://localhost:8083/readyz | python3 -m json.tool

.PHONY: wait-health
wait-health: ## Block until every service reports ready (HEALTH_TIMEOUT seconds)
	@echo "waiting up to $(HEALTH_TIMEOUT)s for the topology..."
	@deadline=$$(( $$(date +%s) + $(HEALTH_TIMEOUT) )); \
	for probe in http://localhost:8081/readyz http://localhost:8000/readyz http://localhost:8082/readyz http://localhost:8083/readyz; do \
	  until curl -fsS "$$probe" >/dev/null 2>&1; do \
	    if [ $$(date +%s) -ge $$deadline ]; then \
	      echo "timed out waiting for $$probe"; $(COMPOSE) ps; exit 1; \
	    fi; \
	    sleep 2; \
	  done; \
	  echo "  ready: $$probe"; \
	done

.PHONY: quota
quota: ## Prove the licence quota holds across all uvicorn workers
	@$(COMPOSE) exec -T redis redis-cli DEL 'quota:$(QUOTA_KEY_ID)' >/dev/null
	python3 tools/quota_probe.py --key $(QUOTA_KEY) \
	  --requests $(QUOTA_REQUESTS) --expect-allowed $(QUOTA_LIMIT)

.PHONY: assert-topics
assert-topics: ## Fail unless ontology.mutations and ontology.commands both carry records
	@set -e; \
	for topic in ontology.mutations ontology.commands; do \
	  echo "--- $$topic ---"; \
	  count=$$($(COMPOSE) exec -T kafka kafka-console-consumer \
	    --bootstrap-server localhost:29092 --topic $$topic --from-beginning \
	    --max-messages 1 --timeout-ms $(TOPIC_TIMEOUT_MS) 2>/dev/null | grep -c . || true); \
	  if [ "$$count" -lt 1 ]; then echo "FAIL: no records on $$topic"; exit 1; fi; \
	  echo "PASS: $$topic received records"; \
	done

.PHONY: smoke
smoke: ## Full end-to-end check: exactly what the compose CI job runs
	$(MAKE) up
	$(MAKE) wait-health
	$(MAKE) seed
	$(MAKE) assert-topics
	$(MAKE) quota

.PHONY: replica
replica: ## Show both replicas' graph revisions (equal = converged)
	@printf "  %-12s %-8s %-10s %s\n" SITE CLOCK VERTICES GRAPH_REVISION
	@for endpoint in http://localhost:8081 http://localhost:8084; do \
	  body=$$(curl -sS --max-time 5 "$$endpoint/stats" 2>/dev/null); \
	  if [ -z "$$body" ]; then \
	    echo "  (unreachable: $$endpoint — partitioned, or not running)"; \
	    continue; \
	  fi; \
	  echo "$$body" | python3 -c "import json,sys; \
	r=json.load(sys.stdin).get('replica',{}); \
	print('  {:<12} {:<8} {:<10} {}'.format(r.get('replica_id','-'), r.get('lamport_clock',0), r.get('live_vertices',0), (r.get('graph_revision') or '-')[:16]))"; \
	done
	@echo
	@core=$$(curl -sS --max-time 5 http://localhost:8081/stats 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('replica',{}).get('graph_revision',''))" 2>/dev/null); \
	edge=$$(curl -sS --max-time 5 http://localhost:8084/stats 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('replica',{}).get('graph_revision',''))" 2>/dev/null); \
	if [ -z "$$edge" ]; then \
	  echo "  PARTITIONED — the edge site is unreachable; it is holding its own state until the split heals"; \
	elif [ -n "$$core" ] && [ "$$core" = "$$edge" ]; then \
	  echo "  CONVERGED — both sites hold the same topology"; \
	else \
	  echo "  DIVERGED — the sites hold different topologies (run 'make replica-heal' and wait one sync interval)"; \
	fi

.PHONY: replica-split
replica-split: ## Partition the edge site off the network
	@docker network disconnect $(NETWORK) oo-resolution-engine-edge \
	  && echo "edge site partitioned." \
	  || echo "edge site was already disconnected."
	@echo
	@echo "This severs the whole uplink, not just the peer path: the edge site"
	@echo "loses Kafka and Redis along with its peer, which is what an aircraft"
	@echo "between uplinks actually experiences. It keeps the topology it had"
	@echo "already learned and re-offers it on reconnect."
	@echo
	@echo "Now seed telemetry — only the core will see it — then 'make replica'"
	@echo "to watch them diverge, and 'make replica-heal' to converge again."

.PHONY: replica-heal
replica-heal: ## Reconnect the partitioned replica and let anti-entropy run
	@# --alias is not optional. `docker network connect` registers only the
	@# container name, not the compose service alias, so without it the peer
	@# stays unresolvable ("no such host") and the split never heals — the
	@# containers are on the same network and still cannot find each other.
	@docker network connect --alias resolution-engine-edge $(NETWORK) oo-resolution-engine-edge \
	  && echo "edge site reconnected." \
	  || echo "edge site was already connected."
	@echo "convergence completes within one sync interval (10s); watch it with: make replica"

.PHONY: dedupe
dedupe: ## Show the distributed idempotency filter's counters
	@curl -sS http://localhost:8081/stats | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["idempotency"], indent=2))'

.PHONY: metrics
metrics: ## Print engine Prometheus metrics
	@curl -sS http://localhost:8081/metrics

.PHONY: console
console: ## Open the read-only operator console
	@echo "opening http://localhost:8083"
	@(command -v open >/dev/null && open http://localhost:8083) \
	  || (command -v xdg-open >/dev/null && xdg-open http://localhost:8083) \
	  || echo "open it manually: http://localhost:8083"

.PHONY: dashboard
dashboard: ## Open the Grafana pipeline dashboard
	@echo "opening http://localhost:3000/d/openontology-overview"
	@(command -v open >/dev/null && open http://localhost:3000/d/openontology-overview) \
	  || (command -v xdg-open >/dev/null && xdg-open http://localhost:3000/d/openontology-overview) \
	  || echo "open it manually: http://localhost:3000/d/openontology-overview"

.PHONY: targets
targets: ## Show whether Prometheus is actually scraping every service
	@curl -sS http://localhost:9090/api/v1/targets | python3 -c "import json,sys; \
	rows=json.load(sys.stdin)['data']['activeTargets']; \
	[print(f\"  {r['labels']['job']:<20} {r['health']:<8} {r.get('lastError','') or 'ok'}\") for r in sorted(rows, key=lambda r: r['labels']['job'])]; \
	bad=[r for r in rows if r['health'] != 'up']; \
	print(); print(f'{len(rows)-len(bad)}/{len(rows)} targets up'); \
	sys.exit(1 if bad else 0)"

.PHONY: test
test: ## Run the interceptor test suite
	cd services/ai-interceptor && python3 -m pytest tests -q

.PHONY: tidy
tidy: ## Resolve Go modules
	cd services/resolution-engine && go mod tidy

.PHONY: vet
vet: ## Build and vet the Go engine
	cd services/resolution-engine && go vet ./... && go build ./...

.PHONY: fmt
fmt: ## Format Go sources
	cd services/resolution-engine && gofmt -w .
