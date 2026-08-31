# Payload schemas

Two contracts cross service boundaries. Both are published as JSON Schema under
[`schemas/`](../schemas/) and both are **generated, never hand-edited**.

| Contract | Schema | Topic / endpoint | Produced by |
|---|---|---|---|
| Enriched Context Payload | [`openontology.mutation.v2`](../schemas/openontology.mutation.v2.schema.json) | `ontology.mutations` | Resolution engine (Go) |
| Actionable Command Sequence | [`openontology.command-sequence.v1`](../schemas/openontology.command-sequence.v1.schema.json) | `ontology.commands`, `POST /v1/intercept` | AI-agent interceptor |

## How they stay honest

The schemas are exported from the Pydantic models the interceptor actually
parses with, which makes them the **consumer's** view: what a caller must send
for the paid API to accept it.

```bash
python tools/export_schemas.py
```

That direction alone would let the Go producer drift. So the engine carries
`schema_conformance_test.go`, which marshals a fully populated mutation and
compares its key set against the committed schema in both directions — a field
the engine emits that the schema does not declare fails the build, and so does a
declared field the engine never emits. A rename on either side is a red test
rather than a runtime surprise for whoever wrote a consumer.

CI runs `python tools/export_schemas.py --check`, so a model change nobody
regenerated is also a red build.

## Enriched Context Payload

Published when an anomaly rule changes an asset's alarm state. Keyed by
`asset_id` on the topic, so every mutation for one asset stays ordered on one
partition.

```json
{
  "event_id": "evt_9f2c1a...",
  "schema_version": "openontology.mutation.v2",
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
    "trigger": {
      "sensor_id": "vibration_index",
      "value": 11.4,
      "unit": "mm/s",
      "observed_at": "2026-08-07T11:41:07.512Z",
      "age_seconds": 0.4
    },
    "readings": [
      {
        "sensor_id": "temperature_celsius",
        "value": 104.2,
        "unit": "degC",
        "observed_at": "2026-08-07T11:41:06.804Z",
        "age_seconds": 1.1
      }
    ],
    "captured_at": "2026-08-07T11:41:07.884Z",
    "complete": true
  },
  "ontology_context": {
    "asset_id": "TURBOFAN-A320-0417",
    "asset_name": "CFM56-5B Turbofan #0417",
    "asset_class": "aero.propulsion.turbofan",
    "model_number": "CFM56-5B4/P",
    "site": "MRO-TOULOUSE-B2",
    "criticality": "SAFETY_CRITICAL",
    "parent_systems": [
      {
        "node_id": "SYS-PROP-A320-0417",
        "name": "Propulsion Subsystem (Engine 1)",
        "type": "Subsystem",
        "depth": 1
      }
    ],
    "components": ["hpt_bearing_no3", "lpc_fan_module", "egt_harness", "fadec_channel_a"],
    "assigned_operators": [
      {
        "operator_id": "OP-4471",
        "name": "L. Moreau",
        "role": "Lead Powerplant Engineer",
        "escalation_order": 1
      }
    ],
    "maintenance_window": "2026-08-12T22:00:00Z/2026-08-13T04:00:00Z",
    "upstream_dependencies": [
      { "asset_id": "FUEL-PUMP-HP-0417", "name": "HP Fuel Pump #0417", "relation": "SUPPLIES", "hops": 1 }
    ],
    "downstream_impacts": [
      { "asset_id": "BLEED-AIR-MANIFOLD-1", "name": "Bleed Air Manifold 1", "relation": "IMPACTS", "hops": 1 },
      { "asset_id": "HYD-PUMP-GREEN-1", "name": "Green Hydraulic Pump 1", "relation": "IMPACTS", "hops": 1 }
    ],
    "blast_radius": 2,
    "source": "neo4j:live",
    "cache_hit": false
  },
  "degraded": false,
  "source_partition": 3,
  "source_offset": 4211
}
```

### Fields worth understanding

**`transition`** — the reason the mutation exists, not the asset's current
state. `RAISED`, `ESCALATED`, `SUSTAINED`, `CLEARED`. Anything that would not be
one of those is absorbed by the state machine, which is what stops a sensor
flapping either side of a threshold from flooding the topic.

**`severity`** — derived from overshoot, not declared. Past
`RULE_CRITICAL_RATIO` (default 15%) `HIGH` becomes `CRITICAL`.

**`telemetry_snapshot.complete`** — `false` means the multi-variable snapshot
could not be assembled and the payload carries the trigger reading only. The
mutation is still emitted; a consumer should plan more conservatively.

**`degraded` / `degraded_reason`** — graph resolution failed, timed out, or
found no such asset. The alarm is still on the topic. An operator would rather
have a thin `CRITICAL` than none.

**`ontology_context.source`** — provenance of the enrichment: `neo4j:live` from
the graph, `neo4j-mock:fixture` or `neo4j-mock:synthesized` from the fixture
provider. Never ambiguous about whether you are looking at real topology.

**`source_partition` / `source_offset`** — where the triggering telemetry sat on
`telemetry.raw`, so a mutation can always be traced back to its input.

## Actionable Command Sequence

Returned by `POST /v1/intercept` and republished to `ontology.commands`.

```json
{
  "plan_id": "plan_df71288fece0430cb493",
  "schema_version": "openontology.command-sequence.v1",
  "event_id": "evt_9f2c1a...",
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
      "parameters": {
        "isolation_scope": "Propulsion Subsystem (Engine 1)",
        "confirm_zero_energy": true
      },
      "expected_effect": "Remove hpt_bearing_no3 from load, halting broadband vibration excursion at 11.4 mm/s against a 8.5 limit.",
      "rollback": "Return to service only after inspection sign-off and a clean restart trend.",
      "deadline_seconds": 300
    }
  ],
  "escalation": {
    "required": true,
    "notify": ["S. Kaur"],
    "sla_seconds": 900
  },
  "usage": { "input_tokens": 1089, "output_tokens": 632 }
}
```

### Fields worth understanding

**`commands[].sequence`** — contiguous from 1 and validated as such. A plan with
a gap is rejected rather than executed with a missing step.

**`action`** — enum-constrained, not free text. The model emits it inside a
strict tool schema with `additionalProperties: false`, forced by `tool_choice`,
rather than in prose a parser has to scrape.

**`rollback`** — required on every command. A plan that cannot say how to undo a
step does not validate.

**`plan_id`, `model`, `usage`** — server-authoritative. Set by the service after
the call, so the model cannot assert what it cost or what produced it.

**`confidence`** — bounded 0–1 and reported, not acted on. Low confidence with a
`degraded` input is the signal to prefer inspection over irreversible actions;
that constraint is injected into the prompt by `_policy_notes()`.

## Idempotency and replay

Planning is idempotent per `(tenant, event_id)`. Kafka delivers at least once,
so a repeated mutation replays the stored plan — the response carries
`X-Idempotent-Replay: true` and no second inference is paid for.

## The flow topology (v2)

`mutation.v2` adds the process network around the asset, which is what turns
"this pump is vibrating" into "isolating this pump stops the reactor":

```json
"ontology_context": {
  "model_number": "A11VO-190",
  "upstream_dependencies": [
    { "asset_id": "SUCTION-STRAINER-S14", "name": "Suction Strainer S-14", "relation": "SUPPLIES", "hops": 1 },
    { "asset_id": "FEED-DRUM-D101", "name": "Feed Drum D-101", "relation": "SUPPLIES", "hops": 2 }
  ],
  "downstream_impacts": [
    { "asset_id": "HX-SHELL-TUBE-E220", "name": "Shell & Tube Exchanger E-220", "relation": "IMPACTS", "hops": 1 },
    { "asset_id": "REACTOR-R310", "name": "Polymerisation Reactor R-310", "relation": "IMPACTS", "hops": 2 },
    { "asset_id": "PRODUCT-COOLER-C450", "name": "Product Cooler C-450", "relation": "IMPACTS", "hops": 3 }
  ],
  "blast_radius": 3
}
```

**Both lists are ordered nearest-first, and the ordering is contract.** Index 0
is one hop away. A consumer walking a cascade back to its origin, or sizing a
containment decision, reads that ordering directly.

**`hops` separates an immediate consequence from a knock-on one.** The exchanger
a pump feeds directly is one hop; the cooler three stages on is three.

**Upstream follows `:FEEDS` only; downstream follows `:FEEDS` and `:CONTROLS`.**
A controller upstream is not a supplier — losing the PLC does not starve the
pump, so calling it an upstream dependency would say the asset is unfed when it
is merely unsupervised. Downstream, both matter: losing the asset stops what it
feeds *and* leaves what it controls running unsupervised, which is frequently
worse.

## Versioning

`schema_version` is carried on every payload and pinned by consumers. Additive
optional fields do not change it. A field that is removed, renamed, or changes
meaning or type produces a new version, and the interceptor accepts both for at
least one release so a mixed-version fleet keeps working during a rollout.

**v1 and v2 are both accepted today.** A fleet is not upgraded atomically — the
engine, the worker and the interceptor restart independently — so refusing v1
the moment v2 shipped would dead-letter every mutation still in flight from a
not-yet-restarted engine. Those are real alarms about real assets.

The reason the flow fields warranted a version bump despite being additive:
`EnrichedContextPayload.carries_flow_topology` is how a consumer tells an empty
blast radius that means *nothing is downstream* from one that means *this
producer does not report it*. Only the version distinguishes those, and a
planner that confused them would isolate an asset believing nothing depends on
it. Superseded schemas stay in `schemas/` as frozen artifacts and are not
regenerated — a published version is immutable by definition.
