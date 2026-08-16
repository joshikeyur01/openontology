"""Pydantic contracts.

Two families live here:

* the *inbound* Enriched Context Payload produced by the Go resolution engine
  (``ontology.mutations`` schema ``openontology.mutation.v1``);
* the *outbound* actionable command sequence returned to the caller.

``LLMPlan`` sits between them: it is the exact structure the model is required
to emit, kept separate from the response envelope so server-authoritative
fields (plan id, usage, model name) can never be hallucinated.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

SCHEMA_VERSION = "openontology.mutation.v2"

# Both are accepted for the length of a rollout. A fleet is not upgraded
# atomically: engines and interceptors restart independently, and refusing v1
# the moment this service ships would dead-letter every mutation still in flight
# from an engine that has not been restarted yet.
SUPPORTED_SCHEMA_VERSIONS = ("openontology.mutation.v1", "openontology.mutation.v2")


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


class _Inbound(BaseModel):
    """Base for engine-produced models.

    ``extra="ignore"`` is deliberate: the open-core engine may add fields in a
    minor release and the paid layer must not start rejecting traffic when it
    does.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True, str_strip_whitespace=True)


def null_collection_is_empty(value: Any) -> Any:
    """Read a JSON ``null`` collection as an absent one.

    Go marshals a nil slice as ``null``, not ``[]``, and the resolution engine
    leaves ``parent_systems``, ``components`` and ``assigned_operators`` nil
    whenever it could not reach the ontology graph. Such a payload is
    *degraded*, not malformed: the engine sets ``degraded: true`` so the planner
    can prefer inspection over an irreversible action, which is a decision this
    service is built to make. Rejecting it as a 422 would mean the mutations
    that most need a plan are the only ones that never get one.

    Mirrored in the command worker, which parses the same records off Kafka.
    """
    return [] if value is None else value


# ---------------------------------------------------------------------------
# Inbound: Enriched Context Payload
# ---------------------------------------------------------------------------


class Severity(str, Enum):
    INFO = "INFO"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class Transition(str, Enum):
    RAISED = "RAISED"
    ESCALATED = "ESCALATED"
    SUSTAINED = "SUSTAINED"
    CLEARED = "CLEARED"


class SensorReading(_Inbound):
    sensor_id: str
    value: float
    unit: str | None = None
    observed_at: datetime
    age_seconds: float = 0.0


class TelemetrySnapshot(_Inbound):
    trigger: SensorReading
    readings: list[SensorReading] = Field(default_factory=list)
    captured_at: datetime
    complete: bool = False

    def reading(self, sensor_id: str) -> SensorReading | None:
        for item in self.readings:
            if item.sensor_id == sensor_id:
                return item
        return None

    def as_lines(self) -> list[str]:
        return [
            f"{r.sensor_id}={r.value:g}{(' ' + r.unit) if r.unit else ''} "
            f"(age {r.age_seconds:g}s)"
            for r in self.readings
        ]

    _readings_tolerant = field_validator("readings", mode="before")(null_collection_is_empty)


class Operator(_Inbound):
    operator_id: str
    name: str
    role: str
    shift: str | None = None
    contact: str | None = None
    escalation_order: int = 0


class SystemNode(_Inbound):
    node_id: str
    name: str
    type: str
    depth: int = 0


class ReplicaObservation(_Inbound):
    """One replica's Lamport timeline for an asset vertex.

    An OR-Set element is live when its highest add stamp is strictly greater
    than its highest remove stamp — equality is a tie, and a tie is not
    liveness. A vertex whose stamps are close, or that replicas disagree about,
    is a topology still converging, which is not a basis for planning an
    irreversible action against.
    """

    replica_id: str
    add_stamp: int = 0
    remove_stamp: int = 0

    @property
    def is_live(self) -> bool:
        # Strictly greater: an equal pair is a tie, and a tie resolves to
        # removed, matching the Go CRDT's contract exactly.
        return self.add_stamp > self.remove_stamp


class FlowRef(_Inbound):
    """One asset in the process flow around the target.

    ``hops`` separates an immediate consequence from a knock-on one: the
    exchanger a pump feeds directly is one hop, the cooler three stages further
    on is three. A containment decision reads that number.
    """

    asset_id: str
    name: str = ""
    model_number: str = ""
    status: str = ""
    relation: Literal["SUPPLIES", "IMPACTS"] = "IMPACTS"
    hops: int = 1


class OntologyContext(_Inbound):
    asset_id: str
    asset_name: str = ""
    asset_class: str = ""
    site: str = ""
    criticality: str = "UNKNOWN"
    parent_systems: list[SystemNode] = Field(default_factory=list)
    components: list[str] = Field(default_factory=list)
    assigned_operators: list[Operator] = Field(default_factory=list)
    maintenance_window: str | None = None
    resolved_at: datetime | None = None
    source: str = "unknown"

    # --- mutation.v2: the process flow around the asset -------------------
    # Absent on v1 payloads, which is why they default to empty rather than
    # being required. EnrichedContextPayload.carries_flow_topology is how a
    # consumer tells "empty because there is nothing" from "empty because this
    # producer does not report it".
    model_number: str = ""
    upstream_dependencies: list[FlowRef] = Field(default_factory=list)
    downstream_impacts: list[FlowRef] = Field(default_factory=list)
    blast_radius: int = 0

    # Per-replica Lamport timeline for this asset's vertex, present when the
    # producing engine runs CRDT topology replication. Empty on a single-site
    # deployment, which is the common case.
    replica_observations: list[ReplicaObservation] = Field(default_factory=list)

    # A graph the engine could not reach yields three nil slices; see
    # :func:`null_collection_is_empty` for why that must not be a 422.
    _collections_tolerant = field_validator(
        "parent_systems",
        "components",
        "assigned_operators",
        "upstream_dependencies",
        "downstream_impacts",
        mode="before",
    )(null_collection_is_empty)
    cache_hit: bool = False

    @property
    def primary_operator(self) -> Operator | None:
        if not self.assigned_operators:
            return None
        return min(self.assigned_operators, key=lambda op: op.escalation_order)

    @property
    def escalation_chain(self) -> list[Operator]:
        return sorted(self.assigned_operators, key=lambda op: op.escalation_order)

    @property
    def topology_is_contested(self) -> bool:
        """Whether replicas disagree about this asset's presence.

        True when at least one replica has observed the vertex and at least one
        of them holds it tombstoned. The guardrail case: a contested topology
        should downgrade an irreversible action to a reversible one rather than
        act on a graph mid-convergence.
        """
        if len(self.replica_observations) < 2:
            return False
        verdicts = {observation.is_live for observation in self.replica_observations}
        return len(verdicts) > 1

    @property
    def nearest_upstream(self) -> FlowRef | None:
        """The asset feeding this one directly — the first suspect in a cascade."""
        return self.upstream_dependencies[0] if self.upstream_dependencies else None

    @property
    def immediate_parent(self) -> SystemNode | None:
        if not self.parent_systems:
            return None
        return min(self.parent_systems, key=lambda node: node.depth)


class RuleTrigger(_Inbound):
    rule_id: str
    sensor_id: str
    operator: str = ">"
    threshold: float
    unit: str | None = None
    observed_value: float
    exceeded_by: float = 0.0
    exceeded_pct: float = 0.0
    description: str | None = None


class EnrichedContextPayload(_Inbound):
    """The Go engine's mutation event, as accepted by ``POST /v1/intercept``."""

    event_id: str
    schema_version: str
    producer: str = "ontology-resolution-engine"
    emitted_at: datetime
    asset_id: str
    transition: Transition
    severity: Severity
    anomaly_active_since: datetime | None = None
    breach_count: int = 0
    rule: RuleTrigger
    telemetry_snapshot: TelemetrySnapshot
    ontology_context: OntologyContext
    degraded: bool = False
    degraded_reason: str | None = None
    source_partition: int | None = None
    source_offset: int | None = None

    # --- replication provenance (v2, optional) ----------------------------
    # origin_replica names the engine that produced this mutation,
    # lamport_clock is its logical clock at emission, and graph_revision is the
    # content hash of the topology it resolved against. Two mutations for the
    # same asset carrying different graph_revisions were planned against
    # different topologies.
    origin_replica: str | None = None
    lamport_clock: int = 0
    graph_revision: str | None = None

    @field_validator("schema_version")
    @classmethod
    def _known_schema(cls, value: str) -> str:
        if not value.startswith(SUPPORTED_SCHEMA_VERSIONS):
            supported = " or ".join(SUPPORTED_SCHEMA_VERSIONS)
            raise ValueError(f"unsupported schema_version {value!r}; expected {supported}")
        return value

    @property
    def carries_flow_topology(self) -> bool:
        """Whether this payload's producer populates the flow network.

        The distinction a planner needs: on v1 an empty blast radius means the
        producer does not report one, and on v2 it means there is genuinely
        nothing downstream. Treating the first as the second would license a
        containment action on the belief that isolating the asset stops nothing.
        """
        return not self.schema_version.startswith("openontology.mutation.v1")


# ---------------------------------------------------------------------------
# Outbound: actionable command sequence
# ---------------------------------------------------------------------------


class ActionType(str, Enum):
    ISOLATE = "ISOLATE"
    SHUTDOWN = "SHUTDOWN"
    THROTTLE = "THROTTLE"
    INSPECT = "INSPECT"
    SCHEDULE_MAINTENANCE = "SCHEDULE_MAINTENANCE"
    NOTIFY = "NOTIFY"
    ACKNOWLEDGE = "ACKNOWLEDGE"


class Priority(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class Command(BaseModel):
    """One executable instruction in the remediation sequence."""

    model_config = ConfigDict(extra="forbid")

    sequence: int = Field(ge=1, description="1-based execution order.")
    target_component: str = Field(min_length=1, description="Component or subsystem to act on.")
    action: ActionType
    priority: Priority
    assigned_to: str = Field(min_length=1, description="Human-readable assignee.")
    assigned_operator_id: str | None = Field(
        default=None, description="Graph operator id, when a named operator exists."
    )
    parameters: dict[str, Any] = Field(default_factory=dict)
    expected_effect: str = Field(min_length=1)
    rollback: str | None = None
    deadline_seconds: int = Field(default=900, ge=0, le=604_800)


class Escalation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    required: bool
    notify: list[str] = Field(default_factory=list)
    reason: str = ""
    sla_seconds: int = Field(default=0, ge=0)


class LLMPlan(BaseModel):
    """Exactly what the model is required to emit via the tool schema."""

    model_config = ConfigDict(extra="forbid")

    confidence: float = Field(ge=0.0, le=1.0)
    reasoning_summary: str = Field(min_length=1, max_length=2000)
    commands: list[Command] = Field(min_length=1)
    escalation: Escalation
    evidence: list[str] = Field(default_factory=list)

    @field_validator("commands")
    @classmethod
    def _sequences_are_contiguous(cls, commands: list[Command]) -> list[Command]:
        expected = list(range(1, len(commands) + 1))
        actual = [command.sequence for command in commands]
        if actual != expected:
            raise ValueError(f"command sequence must be contiguous from 1: got {actual}")
        return commands


class TokenUsage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    input_tokens: int = 0
    output_tokens: int = 0


class CommandSequence(BaseModel):
    """The interceptor's response: an actionable command sequence payload."""

    model_config = ConfigDict(extra="forbid")

    plan_id: str
    event_id: str
    asset_id: str
    tenant: str
    generated_at: datetime = Field(default_factory=_utcnow)
    model: str
    schema_version: Literal["openontology.command-sequence.v1"] = "openontology.command-sequence.v1"
    severity: Severity
    transition: Transition
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning_summary: str
    commands: list[Command]
    escalation: Escalation
    evidence: list[str] = Field(default_factory=list)
    context_degraded: bool = False
    latency_ms: float = 0.0
    usage: TokenUsage = Field(default_factory=TokenUsage)


class ErrorDetail(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str
    message: str
    hint: str | None = None


class ErrorResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    error: ErrorDetail
    request_id: str


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ok"]
    service: str
    version: str
    environment: str
    llm_provider: str
    llm_model: str
    uptime_seconds: float
    #: "redis" when the quota and idempotency records are shared across
    #: workers, "memory" when they are process-local and the service must
    #: therefore run exactly one worker.
    state_backend: Literal["redis", "memory"] = "memory"


class LicenseIntrospection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key_id: str
    tenant: str
    tier: str
    features: list[str]
    quota_per_minute: int
    expires_at: datetime
    valid: bool
