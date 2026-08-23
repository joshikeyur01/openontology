"""Translation between the open-core wire contract and the GraphRAG layer.

The GraphRAG module was written against a richer, differently shaped payload
than the engine emits: a single ``parent_system`` rather than an ancestry list,
flat channel readings rather than a trigger-plus-snapshot structure, dependency
lists as bare node ids, and per-replica Lamport timelines. Every one of those is
now derivable from ``openontology.mutation.v2`` — the flow topology and the
replica observations were the two halves that were missing — so this module is
the projection, not an invention.

It translates in both directions:

* :func:`mutation_to_graph_payload` — ``mutation.v2`` in, ``EnrichedGraphPayload``
  out, so the command worker can call this service exactly as it calls the
  standard interceptor.
* :func:`action_to_plan` — ``CommandActionResponse`` out, rendered in the same
  command-sequence envelope the worker already consumes.

That symmetry is the point. It makes this service a drop-in alternative to
``services/ai-interceptor`` rather than a second protocol the closure loop has
to learn, which is the same substitutability ``docs/OPEN-CORE.md`` promises to
anyone reimplementing the paid layer.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from graphrag_interceptor import (
    ActionType,
    AssetStatus,
    CommandActionResponse,
    EnrichedGraphPayload,
)

# Mirrors _NODE_ID_RE in the GraphRAG module. Nodes that cannot satisfy it are
# dropped from the projection rather than failing the whole request: a single
# unmappable neighbour must not cost the alarm its plan.
_NODE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:\-]{1,127}$")

# The engine's actuation vocabulary, which the command worker validates against.
# GraphRAG speaks a narrower, more physical one; this is the correspondence.
#
# It is deliberately not an identity mapping: this layer is authorised for three
# interventions and the worker publishes seven, so translating rather than
# widening the worker's enum keeps the authorisation boundary where it is.
_ACTION_TO_WORKER = {
    ActionType.ISOLATE_VALVE: "ISOLATE",
    ActionType.EMERGENCY_SHUTDOWN: "SHUTDOWN",
    ActionType.DEGRADE_THROTTLE: "SHIFT_SPEED",
}

# Who a step is addressed to, rendered for the worker's assigned_to field.
_ACTOR_LABEL = {
    "HUMAN_TECHNICIAN": "Field technician",
    "HUMAN_SUPERVISOR": "Shift supervisor",
    "ROBOTIC_ACTUATOR": "Robotic actuator",
    "CONTROL_SYSTEM": "Control system",
}

# What a *reversible* step is, in the worker's vocabulary, by who performs it.
#
# GraphRAG carries one action_type for the whole response and a sequence of
# steps that carry it out. Stamping every step with the response's action makes
# the plan read wrong — "SHUTDOWN: notify the control room" is not a shutdown —
# and, worse, it flattens the one distinction an operator most needs, which is
# which steps can be undone. Only the irreversible steps carry the headline
# action; the reversible ones are labelled by what they actually are.
_REVERSIBLE_STEP_ACTION = {
    "CONTROL_SYSTEM": "SHIFT_SPEED",
    "ROBOTIC_ACTUATOR": "SHIFT_SPEED",
    "HUMAN_SUPERVISOR": "NOTIFY",
    "HUMAN_TECHNICIAN": "INSPECT",
}


def _valid_node(value: str | None) -> str | None:
    if not value:
        return None
    node = str(value).strip()
    return node if _NODE_ID_RE.match(node) else None


def _nearest_parent_system(context: dict[str, Any], asset_id: str) -> str:
    """Collapse the ancestry list to the single parent GraphRAG models.

    The immediate parent — lowest depth — is the containment scope an isolation
    decision is actually taken within. Falling back to the site and then to a
    synthesised id keeps the field populated, because it is required and an
    absent ancestry must not cost the alarm its plan.
    """
    systems = [s for s in context.get("parent_systems") or [] if isinstance(s, dict)]
    if systems:
        nearest = min(systems, key=lambda s: s.get("depth") or 0)
        if node := _valid_node(nearest.get("node_id")):
            return node
    if node := _valid_node(context.get("site")):
        return node
    return f"UNSCOPED:{asset_id}"


def _flow_ids(entries: Any, *, exclude: set[str]) -> list[str]:
    """Project flow refs to node ids, preserving the nearest-first ordering.

    That ordering is contract on both sides: the engine emits nearest-first and
    Agent 1 walks it to find a cascade origin.
    """
    out: list[str] = []
    seen: set[str] = set()
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        node = _valid_node(entry.get("asset_id"))
        if node is None or node in seen or node in exclude:
            continue
        seen.add(node)
        out.append(node)
    return out


def _channels(snapshot: dict[str, Any]) -> dict[str, float]:
    """Flatten the trigger and its snapshot into GraphRAG's channel map.

    The engine models a triggering reading plus a multi-variable snapshot;
    GraphRAG models one flat set of channels. The trigger is written last so it
    wins on collision — it is the reading that fired the rule, and the snapshot
    copy of the same channel may be marginally older.
    """
    channels: dict[str, float] = {}

    for reading in snapshot.get("readings") or []:
        if not isinstance(reading, dict):
            continue
        sensor, value = reading.get("sensor_id"), reading.get("value")
        if sensor and isinstance(value, (int, float)):
            channels[str(sensor)] = float(value)

    trigger = snapshot.get("trigger")
    if isinstance(trigger, dict):
        sensor, value = trigger.get("sensor_id"), trigger.get("value")
        if sensor and isinstance(value, (int, float)):
            channels[str(sensor)] = float(value)

    return channels


def _status_for(severity: str | None, degraded: bool) -> AssetStatus:
    """The asset's operational state, as the graph vertex would carry it.

    An alarming asset is ALARM; one whose context could not be fully resolved is
    UNKNOWN rather than OPERATIONAL, because claiming a healthy status on
    incomplete information is exactly the input that would license an
    over-confident plan.
    """
    if degraded:
        return AssetStatus.UNKNOWN
    if severity in {"CRITICAL", "HIGH"}:
        return AssetStatus.ALARM
    return AssetStatus.OPERATIONAL


def mutation_to_graph_payload(mutation: dict[str, Any]) -> EnrichedGraphPayload:
    """Project an ``openontology.mutation.v2`` record onto the GraphRAG contract.

    Raises ``ValueError`` (via pydantic) when the result cannot satisfy the
    inbound model — which is a 422 to the caller, not a silent degradation.
    """
    context = mutation.get("ontology_context") or {}
    snapshot = mutation.get("telemetry_snapshot") or {}
    asset_id = str(mutation.get("asset_id") or "").strip()

    # The asset must never appear in its own dependency chain, and a node cannot
    # be both upstream and downstream. A cyclic flow network can produce either,
    # and GraphRAG rejects both outright — so they are resolved here rather than
    # turned into a 422 for a payload the engine was right to emit.
    upstream = _flow_ids(context.get("upstream_dependencies"), exclude={asset_id})
    downstream = _flow_ids(context.get("downstream_impacts"), exclude={asset_id})
    # A node on both sides is a loop. It is kept downstream, because that is the
    # side a containment decision reads: what stops if this asset is isolated.
    upstream = [node for node in upstream if node not in set(downstream)]

    known = set(upstream) | set(downstream)

    payload: dict[str, Any] = {
        "event_id": mutation.get("event_id"),
        "timestamp": mutation.get("emitted_at") or datetime.now(tz=timezone.utc),
        "asset_metadata": {
            "uuid": asset_id,
            "name": context.get("asset_name") or asset_id,
            # Required and non-empty. The graph carries it on the asset vertex;
            # the asset class, then the id, are the fallbacks, because the field
            # drives maintenance-manual retrieval and an empty one would fail
            # validation on an otherwise complete payload.
            "model_number": (
                context.get("model_number")
                or context.get("asset_class")
                or asset_id
            ),
            "current_status": _status_for(mutation.get("severity"), bool(mutation.get("degraded"))),
        },
        "ontology_context": {
            "parent_system": _nearest_parent_system(context, asset_id),
            "physical_location": context.get("site") or "UNKNOWN-SITE",
            "upstream_dependencies": upstream,
            "downstream_impacts": downstream,
            # The first-call operator. GraphRAG models one accountable person;
            # the engine's list is ordered by escalation, so index 0 is who to
            # call, and the rest belong to an escalation chain this layer does
            # not model.
            "assigned_operator": _primary_operator(context),
            "criticality": context.get("criticality") or "UNKNOWN",
            "maintenance_window": context.get("maintenance_window"),
            "replica_observations": _observations(context, known),
        },
        "telemetry_snapshot": _channels(snapshot),
        "origin_replica": mutation.get("origin_replica"),
        "lamport_clock": mutation.get("lamport_clock") or 0,
        "graph_revision": mutation.get("graph_revision"),
        "degraded": bool(mutation.get("degraded")),
    }
    return EnrichedGraphPayload.model_validate(payload)


def _primary_operator(context: dict[str, Any]) -> str:
    operators = [o for o in context.get("assigned_operators") or [] if isinstance(o, dict)]
    if not operators:
        return "Unassigned"
    first = min(operators, key=lambda o: o.get("escalation_order") or 0)
    return str(first.get("name") or first.get("operator_id") or "Unassigned")


def _observations(context: dict[str, Any], known: set[str]) -> list[dict[str, Any]]:
    """Carry the per-replica Lamport timeline through unchanged.

    This is the field the guardrail layer reads to decide whether the topology
    is trustworthy enough to act irreversibly on, so it is passed through rather
    than summarised.
    """
    out: list[dict[str, Any]] = []
    for observation in context.get("replica_observations") or []:
        if not isinstance(observation, dict):
            continue
        replica = observation.get("replica_id")
        if not replica:
            continue
        out.append(
            {
                "replica_id": str(replica),
                "add_stamp": int(observation.get("add_stamp") or 0),
                "remove_stamp": int(observation.get("remove_stamp") or 0),
            }
        )
    return out


def action_to_plan(action: CommandActionResponse, *, model: str) -> dict[str, Any]:
    """Render a GraphRAG command as the command-sequence envelope.

    The worker's ``ActionPlan`` requires at least one command, so a response
    whose isolation sequence is empty is rendered as a single step carrying the
    action itself — a plan that validated but produced no instructions is a bug
    worth surfacing as a command an operator sees, not an empty list the worker
    would reject with a 422 it cannot diagnose.
    """
    worker_action = _ACTION_TO_WORKER.get(action.action_type, "INSPECT")
    priority = action.execution_priority.value

    commands: list[dict[str, Any]] = []
    for step in action.isolation_steps:
        # An irreversible step *is* the commanded action; a reversible one is
        # preparation, notification or verification around it.
        step_action = (
            worker_action
            if not step.reversible
            else _REVERSIBLE_STEP_ACTION.get(step.actor.value, "INSPECT")
        )
        commands.append(
            {
                "sequence": step.sequence,
                "target_component": step.target_component,
                "action": step_action,
                "priority": priority,
                "assigned_to": _ACTOR_LABEL.get(step.actor.value, step.actor.value),
                "expected_effect": step.instruction,
                # The step's verification is what an operator checks to confirm
                # the step worked; for a reversible step it is also how they
                # know it is safe to undo. An irreversible step says so plainly
                # rather than inventing a rollback that does not exist.
                "rollback": (
                    f"Reversible. Confirm by: {step.verification}"
                    if step.reversible
                    else "Not reversible without a maintenance intervention; "
                    f"confirm before proceeding by: {step.verification}"
                ),
                "deadline_seconds": step.estimated_seconds,
                "parameters": {
                    "actor": step.actor.value,
                    "reversible": step.reversible,
                    "manual_reference": step.manual_reference,
                },
            }
        )

    if not commands:
        commands.append(
            {
                "sequence": 1,
                "target_component": action.target_asset_id,
                "action": worker_action,
                "priority": priority,
                "assigned_to": "Shift supervisor",
                "expected_effect": action.rationale,
                "rollback": "Reversible." if action.reversible else "Not reversible.",
                "deadline_seconds": 900,
                "parameters": {},
            }
        )

    return {
        "plan_id": action.command_id,
        "event_id": action.event_id,
        "asset_id": action.source_asset_id,
        "tenant": action.tenant,
        "model": model,
        "confidence": action.confidence,
        "reasoning_summary": action.rationale,
        "commands": commands,
        "escalation": {
            "required": action.requires_human_authorization,
            "notify": [],
            "reason": (
                "Irreversible action against a live asset"
                if not action.reversible
                else "Human authorization required by tier policy"
            ),
            "sla_seconds": 900 if action.requires_human_authorization else 0,
        },
        # The evidence the plan rests on, plus every guardrail that fired. A
        # downgraded action is only auditable if the downgrade is reported.
        "evidence": list(action.evidence) + [
            f"guardrail: {applied}" for applied in action.guardrails_applied
        ],
        "latency_ms": action.latency_ms,
        # Provenance beyond the worker's envelope. It ignores unknown fields, so
        # these ride along for anything reading the topic directly.
        "fault_classification": action.fault_classification.value,
        "cascade_path": list(action.cascade_path),
        "blast_radius": list(action.blast_radius),
        "crdt_presence": action.crdt_assessment.presence.value,
    }
