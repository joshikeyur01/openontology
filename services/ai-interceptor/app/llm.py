"""LLM orchestration: enriched context in, actionable command sequence out.

The prompt is assembled from a Pydantic model (``RemediationPromptContext``)
so the exact bytes sent to the model are versioned alongside the schema, and
the response is constrained by a tool schema rather than parsed out of prose.

The model itself sits behind :class:`LLMProvider` — one method, one forced
tool call, one structured answer. Any tool-calling provider can implement it.
``MockLLMClient`` is the offline implementation shipped by default; the live
one lives in :mod:`app.llm_cloud`, the only module in this service that
imports a vendor SDK. Switching between them is one branch in
:func:`build_planner`, driven by ``OO_LLM_PROVIDER``; nothing else changes.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .config import Settings
from .models import (
    ActionType,
    Command,
    EnrichedContextPayload,
    Escalation,
    LLMPlan,
    Priority,
    Severity,
    TokenUsage,
    Transition,
)

logger = logging.getLogger(__name__)

TOOL_NAME = "emit_command_sequence"

DEFAULT_ALLOWED_ACTIONS: list[ActionType] = [
    ActionType.ISOLATE,
    ActionType.SHUTDOWN,
    ActionType.THROTTLE,
    ActionType.INSPECT,
    ActionType.SCHEDULE_MAINTENANCE,
    ActionType.NOTIFY,
    ActionType.ACKNOWLEDGE,
]

SYSTEM_PROMPT = """\
You are the OpenOntology remediation planner for industrial and aerospace digital twins.

You receive one ontology mutation: an asset that breached a monitored threshold, the
live multi-variable telemetry snapshot at the moment of the breach, and the asset's
graph context (parent systems, components, accountable operators).

Produce an executable command sequence for the maintenance and operations crew.

Rules:
- Act on named components from the ontology context. Never invent a component,
  operator, or identifier that is not present in the briefing.
- Order commands by execution order, starting at sequence 1.
- Reserve ISOLATE and SHUTDOWN for CRITICAL severity or safety-critical assets.
- Every command names an assignee drawn from the assigned operators; if none exist,
  assign to the site duty desk and require escalation.
- Set deadline_seconds proportional to severity: minutes for CRITICAL, hours otherwise.
- State confidence honestly. Degraded context or an incomplete telemetry snapshot
  must lower it.
- Return the plan exclusively through the emit_command_sequence tool.
"""

# Hand-written rather than derived from the Pydantic model: strict tool schemas
# forbid the $defs/$ref indirection that model_json_schema() emits for nested
# models, and the wire contract deserves to be explicit.
COMMAND_SEQUENCE_TOOL: dict[str, Any] = {
    "name": TOOL_NAME,
    "description": (
        "Emit the ordered, executable remediation command sequence for an anomalous "
        "industrial asset. Call this exactly once."
    ),
    "strict": True,
    "input_schema": {
        "type": "object",
        "properties": {
            "confidence": {
                "type": "number",
                "description": "Confidence in the plan, 0.0 to 1.0.",
            },
            "reasoning_summary": {
                "type": "string",
                "description": "Two to four sentences justifying the sequence, citing observed values.",
            },
            "commands": {
                "type": "array",
                "description": "Ordered commands, sequence starting at 1.",
                "items": {
                    "type": "object",
                    "properties": {
                        "sequence": {"type": "integer", "description": "1-based execution order."},
                        "target_component": {
                            "type": "string",
                            "description": "Component or subsystem from the ontology context.",
                        },
                        "action": {
                            "type": "string",
                            "enum": [action.value for action in DEFAULT_ALLOWED_ACTIONS],
                        },
                        "priority": {
                            "type": "string",
                            "enum": [priority.value for priority in Priority],
                        },
                        "assigned_to": {"type": "string", "description": "Assignee name or duty desk."},
                        "assigned_operator_id": {
                            "type": ["string", "null"],
                            "description": "Graph operator id, or null when unassigned.",
                        },
                        "parameters": {
                            "type": "object",
                            "description": "Action-specific parameters.",
                            "additionalProperties": True,
                        },
                        "expected_effect": {
                            "type": "string",
                            "description": "The physical outcome this command is expected to produce.",
                        },
                        "rollback": {
                            "type": ["string", "null"],
                            "description": "How to reverse the command, or null when irreversible.",
                        },
                        "deadline_seconds": {
                            "type": "integer",
                            "description": "Seconds within which the command must be executed.",
                        },
                    },
                    "required": [
                        "sequence",
                        "target_component",
                        "action",
                        "priority",
                        "assigned_to",
                        "assigned_operator_id",
                        "parameters",
                        "expected_effect",
                        "rollback",
                        "deadline_seconds",
                    ],
                    "additionalProperties": False,
                },
            },
            "escalation": {
                "type": "object",
                "properties": {
                    "required": {"type": "boolean"},
                    "notify": {"type": "array", "items": {"type": "string"}},
                    "reason": {"type": "string"},
                    "sla_seconds": {"type": "integer"},
                },
                "required": ["required", "notify", "reason", "sla_seconds"],
                "additionalProperties": False,
            },
            "evidence": {
                "type": "array",
                "description": "Telemetry or ontology facts the plan relies on.",
                "items": {"type": "string"},
            },
        },
        "required": ["confidence", "reasoning_summary", "commands", "escalation", "evidence"],
        "additionalProperties": False,
    },
}


class RemediationPromptContext(BaseModel):
    """Structured prompt input; rendering lives with the schema it renders."""

    model_config = ConfigDict(extra="forbid")

    payload: EnrichedContextPayload
    tenant: str
    tier: str
    max_commands: int = Field(default=6, ge=1, le=20)
    allowed_actions: list[ActionType] = Field(default_factory=lambda: list(DEFAULT_ALLOWED_ACTIONS))
    policy_notes: list[str] = Field(default_factory=list)

    def briefing(self) -> dict[str, Any]:
        """The machine-readable facts handed to the model."""
        payload = self.payload
        context = payload.ontology_context
        return {
            "event_id": payload.event_id,
            "asset": {
                "asset_id": context.asset_id or payload.asset_id,
                "name": context.asset_name,
                "class": context.asset_class,
                "site": context.site,
                "criticality": context.criticality,
                "maintenance_window": context.maintenance_window,
                "components": context.components,
                "parent_systems": [
                    {"node_id": node.node_id, "name": node.name, "type": node.type, "depth": node.depth}
                    for node in sorted(context.parent_systems, key=lambda n: n.depth)
                ],
            },
            "anomaly": {
                "severity": payload.severity.value,
                "transition": payload.transition.value,
                "breach_count": payload.breach_count,
                "active_since": payload.anomaly_active_since.isoformat()
                if payload.anomaly_active_since
                else None,
                "rule_id": payload.rule.rule_id,
                "sensor_id": payload.rule.sensor_id,
                "condition": f"{payload.rule.sensor_id} {payload.rule.operator} {payload.rule.threshold:g}",
                "observed_value": payload.rule.observed_value,
                "threshold": payload.rule.threshold,
                "unit": payload.rule.unit,
                "exceeded_by": payload.rule.exceeded_by,
                "exceeded_pct": payload.rule.exceeded_pct,
            },
            "telemetry_snapshot": {
                "captured_at": payload.telemetry_snapshot.captured_at.isoformat(),
                "complete": payload.telemetry_snapshot.complete,
                "readings": [
                    {
                        "sensor_id": reading.sensor_id,
                        "value": reading.value,
                        "unit": reading.unit,
                        "age_seconds": reading.age_seconds,
                    }
                    for reading in payload.telemetry_snapshot.readings
                ],
            },
            "operators": [
                {
                    "operator_id": operator.operator_id,
                    "name": operator.name,
                    "role": operator.role,
                    "shift": operator.shift,
                    "contact": operator.contact,
                    "escalation_order": operator.escalation_order,
                }
                for operator in context.escalation_chain
            ],
            "constraints": {
                "tenant": self.tenant,
                "tier": self.tier,
                "max_commands": self.max_commands,
                "allowed_actions": [action.value for action in self.allowed_actions],
                "context_degraded": payload.degraded,
                "degraded_reason": payload.degraded_reason,
                "policy_notes": self.policy_notes,
            },
        }

    def render_system(self) -> str:
        return SYSTEM_PROMPT

    def render_user(self) -> str:
        """Human-readable framing plus the machine-readable briefing."""
        payload = self.payload
        context = payload.ontology_context
        parent = context.immediate_parent
        operator = context.primary_operator

        headline = (
            f"{payload.severity.value} {payload.transition.value} on "
            f"{context.asset_name or payload.asset_id} "
            f"({payload.rule.sensor_id}={payload.rule.observed_value:g}"
            f"{(' ' + payload.rule.unit) if payload.rule.unit else ''}, "
            f"limit {payload.rule.threshold:g}, "
            f"{payload.rule.exceeded_pct:g}% over)."
        )
        situation = [
            headline,
            f"Site: {context.site or 'unknown'} | Criticality: {context.criticality}",
            f"Immediate parent system: {parent.name if parent else 'unmapped'}",
            f"Primary operator: {operator.name + ' (' + operator.role + ')' if operator else 'none assigned'}",
        ]
        if payload.degraded:
            situation.append(f"WARNING: context is degraded — {payload.degraded_reason}")

        return (
            "\n".join(situation)
            + "\n\nBriefing:\n```json\n"
            + json.dumps(self.briefing(), indent=2, sort_keys=True)
            + "\n```\n\nCall "
            + TOOL_NAME
            + " with at most "
            + str(self.max_commands)
            + " commands."
        )


# ---------------------------------------------------------------------------
# Provider interface
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ToolCall:
    """One provider turn: the tool input it produced, and what it cost."""

    tool_input: dict[str, Any]
    model: str
    input_tokens: int = 0
    output_tokens: int = 0


class LLMProvider(Protocol):
    """The entire surface this service needs from a language model.

    One request, one forced tool call, one structured answer. Any provider
    with tool calling can satisfy it, and nothing above this line — the
    planner, the prompt, the API layer — learns which one is behind it. Vendor
    SDKs are imported only by the implementation that speaks to that vendor.
    """

    #: Stable identifier for the backend, used in logs and health output.
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
    ) -> ToolCall:
        """Run one turn and return the input of the forced ``tool_name`` call."""
        ...


# ---------------------------------------------------------------------------
# Offline provider
# ---------------------------------------------------------------------------


class MockLLMClient:
    """Deterministic offline provider.

    Not a stub: it reads the same briefing a live model reads — the fenced
    JSON block in the user turn — and derives a schema-valid plan from it, so
    the code path under test is the production one. The same mutation always
    yields the same plan, which is what makes the commercial layer testable
    with no network egress, no API key and no vendor SDK installed.
    """

    name = "mock"

    _BRIEFING_PATTERN = re.compile(r"```json\s*(?P<body>\{.*?\})\s*```", re.DOTALL)

    def __init__(self, simulated_latency_ms: int = 40) -> None:
        self._latency = simulated_latency_ms / 1000.0

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
    ) -> ToolCall:
        if self._latency:
            await asyncio.sleep(self._latency)

        plan = _derive_plan(self._extract_briefing(user))
        plan_json = json.dumps(plan)

        return ToolCall(
            tool_input=plan,
            model=model,
            # Rough char/4 heuristic; a live provider reports exact counts.
            input_tokens=(len(system) + len(user)) // 4,
            output_tokens=len(plan_json) // 4,
        )

    @classmethod
    def _extract_briefing(cls, user: str) -> dict[str, Any]:
        match = cls._BRIEFING_PATTERN.search(user)
        if match is None:
            raise PlannerError("mock planner could not locate the briefing block in the prompt")
        return json.loads(match.group("body"))


# ---------------------------------------------------------------------------
# Deterministic planning used by the mock
# ---------------------------------------------------------------------------

# Components most likely implicated by each monitored channel, best match first.
_COMPONENT_AFFINITY: dict[str, tuple[str, ...]] = {
    "vibration_index": ("bearing", "spindle", "coupling", "rotor", "fan", "drive", "pump"),
    "temperature_celsius": ("bearing", "egt", "thermal", "coolant", "inverter", "seal", "motor"),
}

_SENSOR_LABEL: dict[str, str] = {
    "vibration_index": "broadband vibration",
    "temperature_celsius": "temperature",
}


def _select_component(sensor_id: str, components: list[str], fallback: str) -> str:
    for keyword in _COMPONENT_AFFINITY.get(sensor_id, ()):
        for component in components:
            if keyword in component.lower():
                return component
    return components[0] if components else fallback


def _derive_plan(briefing: dict[str, Any]) -> dict[str, Any]:
    """Produce a schema-valid plan from the briefing.

    This is the mock's substitute for model reasoning. It is deterministic on
    purpose: the same mutation always yields the same plan, which makes the
    commercial layer testable without a network call.
    """
    asset = briefing["asset"]
    anomaly = briefing["anomaly"]
    constraints = briefing["constraints"]
    operators: list[dict[str, Any]] = briefing.get("operators", [])
    snapshot = briefing.get("telemetry_snapshot", {})

    severity = anomaly["severity"]
    transition = anomaly["transition"]
    sensor_id = anomaly["sensor_id"]
    observed = anomaly["observed_value"]
    threshold = anomaly["threshold"]
    unit = anomaly.get("unit") or ""
    safety_critical = asset.get("criticality") == "SAFETY_CRITICAL"
    max_commands = int(constraints.get("max_commands", 6))
    allowed = set(constraints.get("allowed_actions", []))

    asset_label = asset.get("name") or asset["asset_id"]
    target = _select_component(sensor_id, asset.get("components", []), asset_label)
    parents = asset.get("parent_systems") or []
    parent_name = parents[0]["name"] if parents else asset_label

    primary = operators[0] if operators else None
    primary_name = primary["name"] if primary else "Site duty desk"
    primary_id = primary["operator_id"] if primary else None
    notify = [op["name"] for op in operators[1:]] or ["Site duty desk"]

    sensor_label = _SENSOR_LABEL.get(sensor_id, sensor_id)
    magnitude = f"{observed:g}{(' ' + unit) if unit else ''} against a {threshold:g} limit"

    commands: list[dict[str, Any]] = []

    def add(
        action: ActionType,
        priority: Priority,
        component: str,
        assignee: str,
        operator_id: str | None,
        effect: str,
        rollback: str | None,
        deadline: int,
        parameters: dict[str, Any] | None = None,
    ) -> None:
        if action.value not in allowed or len(commands) >= max_commands:
            return
        commands.append(
            {
                "sequence": len(commands) + 1,
                "target_component": component,
                "action": action.value,
                "priority": priority.value,
                "assigned_to": assignee,
                "assigned_operator_id": operator_id,
                "parameters": parameters or {},
                "expected_effect": effect,
                "rollback": rollback,
                "deadline_seconds": deadline,
            }
        )

    if transition == Transition.CLEARED.value:
        add(
            ActionType.ACKNOWLEDGE,
            Priority.LOW,
            target,
            primary_name,
            primary_id,
            f"Close the {sensor_label} alarm on {asset_label} after recovery below limit.",
            "Re-open the alarm if the reading breaches again within one shift.",
            3600,
            {"reason": "value recovered below the clearing threshold"},
        )
        add(
            ActionType.SCHEDULE_MAINTENANCE,
            Priority.LOW,
            target,
            primary_name,
            primary_id,
            f"Book a trend review of {target} at the next maintenance window.",
            "Cancel the work order if the trend review is not required.",
            86_400,
            {"maintenance_window": asset.get("maintenance_window")},
        )
        confidence = 0.72
        escalation_required = False
        escalation_reason = "Anomaly cleared; no escalation required."
        sla = 0
    elif severity == Severity.CRITICAL.value:
        add(
            ActionType.ISOLATE,
            Priority.CRITICAL,
            target,
            primary_name,
            primary_id,
            f"Remove {target} from load, halting {sensor_label} excursion at {magnitude}.",
            "Return to service only after inspection sign-off and a clean restart trend.",
            300,
            {
                "isolation_scope": parent_name,
                "confirm_zero_energy": True,
                "sensor_id": sensor_id,
                "observed_value": observed,
            },
        )
        if safety_critical:
            add(
                ActionType.SHUTDOWN,
                Priority.CRITICAL,
                parent_name,
                primary_name,
                primary_id,
                f"Bring {parent_name} to a controlled stop; the asset is safety critical.",
                "Restart per the approved return-to-service procedure.",
                600,
                {"mode": "controlled", "reason": f"{sensor_label} exceeded limit by "
                                                f"{anomaly.get('exceeded_pct', 0):g}%"},
            )
        add(
            ActionType.INSPECT,
            Priority.HIGH,
            target,
            primary_name,
            primary_id,
            f"Confirm the root cause of the {sensor_label} excursion on {target}.",
            None,
            1800,
            {"method": "borescope+thermography" if sensor_id == "temperature_celsius" else "vibration_survey"},
        )
        add(
            ActionType.NOTIFY,
            Priority.HIGH,
            asset_label,
            notify[0],
            operators[1]["operator_id"] if len(operators) > 1 else None,
            "Escalate to the accountable engineer and open an incident record.",
            None,
            600,
            {"channel": "incident_bridge", "recipients": notify},
        )
        confidence = 0.91
        escalation_required = True
        escalation_reason = (
            f"{sensor_label} on {asset_label} reached {magnitude}"
            f" ({anomaly.get('exceeded_pct', 0):g}% over limit)."
        )
        sla = 900
    else:
        add(
            ActionType.THROTTLE,
            Priority.HIGH,
            target,
            primary_name,
            primary_id,
            f"Reduce duty on {target} to bring {sensor_label} back below {threshold:g}{(' ' + unit) if unit else ''}.",
            "Restore the previous setpoint once the reading is stable below the limit.",
            900,
            {"target_reduction_pct": 20, "sensor_id": sensor_id},
        )
        add(
            ActionType.INSPECT,
            Priority.MEDIUM,
            target,
            primary_name,
            primary_id,
            f"Trend {sensor_label} on {target} for the remainder of the shift.",
            None,
            7200,
            {"sample_interval_seconds": 60},
        )
        add(
            ActionType.SCHEDULE_MAINTENANCE,
            Priority.MEDIUM,
            target,
            primary_name,
            primary_id,
            f"Raise a work order against {target} for the next maintenance window.",
            "Cancel the work order if the trend returns to nominal.",
            86_400,
            {"maintenance_window": asset.get("maintenance_window")},
        )
        confidence = 0.78
        escalation_required = False
        escalation_reason = "Threshold breach contained by throttling; monitoring in place."
        sla = 3600

    # Degraded context or an incomplete snapshot must be reflected honestly.
    if constraints.get("context_degraded"):
        confidence = round(confidence - 0.2, 2)
    if not snapshot.get("complete", False):
        confidence = round(confidence - 0.05, 2)
    confidence = max(0.05, min(1.0, confidence))

    if not commands:
        # Every allowed-action set must still yield something executable.
        commands.append(
            {
                "sequence": 1,
                "target_component": target,
                "action": ActionType.NOTIFY.value,
                "priority": Priority.HIGH.value,
                "assigned_to": primary_name,
                "assigned_operator_id": primary_id,
                "parameters": {"reason": "no permitted action available for this tier"},
                "expected_effect": "Hand the anomaly to a human for manual disposition.",
                "rollback": None,
                "deadline_seconds": 900,
            }
        )

    evidence = [
        f"{sensor_id} observed at {observed:g}{(' ' + unit) if unit else ''} "
        f"vs limit {threshold:g} ({anomaly.get('exceeded_pct', 0):g}% over)",
        f"transition={transition}, breach_count={anomaly.get('breach_count', 0)}",
        f"asset criticality={asset.get('criticality')}, site={asset.get('site')}",
    ]
    evidence.extend(
        f"{reading['sensor_id']}={reading['value']:g}"
        f"{(' ' + reading['unit']) if reading.get('unit') else ''}"
        f" (age {reading.get('age_seconds', 0):g}s)"
        for reading in snapshot.get("readings", [])
    )

    return {
        "confidence": confidence,
        "reasoning_summary": (
            f"{severity} {transition} on {asset_label}: {sensor_label} reached {magnitude}. "
            f"The plan acts on {target} within {parent_name}, assigns {primary_name}, and "
            f"{'escalates to the accountable engineer' if escalation_required else 'keeps the asset under observation'}."
        ),
        "commands": commands,
        "escalation": {
            "required": escalation_required,
            "notify": notify,
            "reason": escalation_reason,
            "sla_seconds": sla,
        },
        "evidence": evidence,
    }


# ---------------------------------------------------------------------------
# Planner
# ---------------------------------------------------------------------------


class PlannerError(RuntimeError):
    """Raised when the model does not return a usable command sequence."""


@dataclass(frozen=True)
class PlanResult:
    plan: LLMPlan
    usage: TokenUsage
    model: str
    latency_ms: float


class RemediationPlanner:
    """Formats the prompt, calls the provider, validates the structured reply.

    Provider-agnostic by construction: it holds an :class:`LLMProvider` and
    never inspects which implementation it got.
    """

    def __init__(
        self,
        provider: LLMProvider,
        *,
        model: str,
        max_tokens: int,
        effort: str,
        timeout_seconds: float,
    ) -> None:
        self._provider = provider
        self._model = model
        self._max_tokens = max_tokens
        self._effort = effort
        self._timeout = timeout_seconds

    @property
    def model(self) -> str:
        return self._model

    async def plan(self, context: RemediationPromptContext) -> PlanResult:
        started = time.perf_counter()

        # The deadline is enforced here rather than per provider, so every
        # backend is held to the same contract whatever its own SDK does.
        try:
            call = await asyncio.wait_for(
                self._provider.complete(
                    model=self._model,
                    system=context.render_system(),
                    user=context.render_user(),
                    tool=COMMAND_SEQUENCE_TOOL,
                    tool_name=TOOL_NAME,
                    max_tokens=self._max_tokens,
                    effort=self._effort,
                ),
                timeout=self._timeout,
            )
        except asyncio.TimeoutError as exc:
            raise PlannerError(f"planner timed out after {self._timeout}s") from exc
        except PlannerError:
            raise
        except Exception as exc:  # noqa: BLE001 - surfaced as a 502 by the caller
            raise PlannerError(f"planner call failed: {exc}") from exc

        latency_ms = (time.perf_counter() - started) * 1000

        try:
            plan = LLMPlan.model_validate(call.tool_input)
        except ValidationError as exc:
            raise PlannerError(f"planner emitted a malformed command sequence: {exc}") from exc

        usage = TokenUsage(
            input_tokens=call.input_tokens,
            output_tokens=call.output_tokens,
        )

        logger.info(
            "remediation plan generated",
            extra={
                "model": call.model,
                "commands": len(plan.commands),
                "confidence": plan.confidence,
                "latency_ms": round(latency_ms, 2),
                "input_tokens": usage.input_tokens,
                "output_tokens": usage.output_tokens,
            },
        )

        return PlanResult(
            plan=plan,
            usage=usage,
            model=call.model or self._model,
            latency_ms=latency_ms,
        )


def build_planner(settings: Settings) -> RemediationPlanner:
    """Construct the planner for the configured provider."""
    provider: LLMProvider
    if settings.require_live_llm():
        # Imported here, not at module scope: app.llm_cloud is the only module
        # bound to a vendor SDK, and the offline default has to stay importable
        # on a host where that SDK is not installed.
        from .llm_cloud import CloudLLMClient  # noqa: PLC0415 - optional dependency

        api_key = settings.llm_api_key.get_secret_value() if settings.llm_api_key else None
        provider = CloudLLMClient(api_key=api_key)
        logger.info(
            "planner using live provider",
            extra={"provider": provider.name, "model": settings.llm_model},
        )
    else:
        provider = MockLLMClient(simulated_latency_ms=settings.llm_simulated_latency_ms)
        logger.info(
            "planner using deterministic offline provider",
            extra={"provider": provider.name, "model": settings.llm_model},
        )

    return RemediationPlanner(
        provider,
        model=settings.llm_model,
        max_tokens=settings.llm_max_tokens,
        effort=settings.llm_effort,
        timeout_seconds=settings.llm_timeout_seconds,
    )


def build_command_sequence(
    payload: EnrichedContextPayload,
    result: PlanResult,
    *,
    tenant: str,
    plan_id: str | None = None,
) -> dict[str, Any]:
    """Compose the response envelope from the validated plan.

    Server-authoritative fields (identifiers, model, usage, latency) are set
    here so the model can never assert them.
    """
    return {
        "plan_id": plan_id or f"plan_{uuid.uuid4().hex[:20]}",
        "event_id": payload.event_id,
        "asset_id": payload.asset_id,
        "tenant": tenant,
        "model": result.model,
        "severity": payload.severity,
        "transition": payload.transition,
        "confidence": result.plan.confidence,
        "reasoning_summary": result.plan.reasoning_summary,
        "commands": [command.model_dump() for command in result.plan.commands],
        "escalation": result.plan.escalation.model_dump(),
        "evidence": result.plan.evidence,
        "context_degraded": payload.degraded,
        "latency_ms": round(result.latency_ms, 2),
        "usage": result.usage.model_dump(),
    }


__all__ = [
    "COMMAND_SEQUENCE_TOOL",
    "Command",
    "Escalation",
    "LLMProvider",
    "MockLLMClient",
    "PlanResult",
    "PlannerError",
    "RemediationPlanner",
    "RemediationPromptContext",
    "TOOL_NAME",
    "ToolCall",
    "build_command_sequence",
    "build_planner",
]
