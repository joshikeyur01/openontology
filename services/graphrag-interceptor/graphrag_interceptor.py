"""MODULE 4 — Semantic GraphRAG Interceptor Layer.

The open-core Go engine (``services/resolution-engine``) replicates the physical
asset topology as a state-based graph CRDT and emits enriched mutations. This
module is the commercial brain that sits on top of that stream: it parses the
localised graph topology around one asset, correlates it against a technical
maintenance corpus using a GraphRAG retrieval pattern, and drives a two-agent
isolation loop that decides *which* physical node actually needs intervention.

The file is deliberately self-contained — schemas, CRDT resolution, retrieval,
agent transport, licensing and the HTTP surface all live here — so it can be
mounted next to ``app.main`` (``uvicorn app.graph_rag_interceptor:app``) or
lifted into its own service without untangling imports.

Pipeline::

    EnrichedGraphPayload
        -> deterministic topology kernel (traversal + OR-Set/Lamport resolution)
        -> Agent 1: Topology Isolator      (localized component vs inherited cascade)
        -> GraphRAG retrieval              (maintenance manual chunks, filtered by verdict)
        -> Agent 2: Strategic Action Planner (blast-radius-safe command sequence)
        -> safety guardrails               (server-authoritative, model cannot bypass)
        -> CommandActionResponse

Two properties are load-bearing and worth stating up front:

* **The deterministic kernel runs first, always.** Both agents receive its output
  as grounding, and its node set is the allowlist the agents' answers are checked
  against. A model may reinterpret the topology; it may not invent one.
* **Irreversible actions require a trusted graph.** If the CRDT state for the
  asset is contested, tombstoned-but-live, or the replicas are diverging, the
  guardrail layer downgrades EMERGENCY_SHUTDOWN to a reversible throttle and
  injects a reconciliation step. That check is applied after the planner, so no
  prompt injection or model error can route around it.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import math
import re
import time
from collections import OrderedDict, deque
from contextlib import asynccontextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Iterable, Literal, Mapping, Protocol, Sequence
from uuid import uuid4

from fastapi import Depends, FastAPI, Request, Response, Security, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = [
    "AssetMetadata",
    "OntologyContext",
    "TelemetrySnapshot",
    "EnrichedGraphPayload",
    "CommandActionResponse",
    "MultiAgentGraphEngine",
    "verify_enterprise_subscription",
    "create_app",
    "app",
]

MODULE_VERSION = "4.0.0"
RESPONSE_SCHEMA_VERSION = "openontology.command-action.v1"
LICENSE_HEADER = "X-OpenOntology-License"

#: Model id reported while the offline backend is reasoning. It names what
#: actually produced the plan; no vendor's model id is shipped as a default,
#: since it would be wrong for every provider but the one that owns it.
DETERMINISTIC_MODEL_ID = "openontology-rule-engine"

logger = logging.getLogger("openontology.graphrag")


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


# ===========================================================================
# 0. Runtime configuration and structured logging
# ===========================================================================


class InterceptorSettings(BaseSettings):
    """Process configuration; every value is environment driven."""

    model_config = SettingsConfigDict(
        env_prefix="OO_GRAPHRAG_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    service_name: str = "openontology-graphrag-interceptor"
    environment: str = "local"
    log_level: str = "INFO"
    docs_enabled: bool = True

    # --- commercial gating ------------------------------------------------
    license_header: str = LICENSE_HEADER
    license_registry_json: str | None = None
    quota_window_seconds: int = Field(default=60, ge=1, le=3600)

    # --- agent transport --------------------------------------------------
    #: ``deterministic`` runs the two agent frames as a rule engine in process:
    #: no network, no key, no SDK. ``cloud`` routes the same two forced tool
    #: calls through a real tool-calling provider; CloudToolClient is the only
    #: place that knows which vendor that is.
    agent_provider: Literal["deterministic", "cloud"] = "deterministic"
    #: Stamped onto every plan this service emits, so it is meaningful under
    #: both backends. The default names the offline rule engine rather than any
    #: vendor's model: pointing at ``cloud`` means naming a model that provider
    #: actually serves.
    agent_model: str = DETERMINISTIC_MODEL_ID
    agent_max_tokens: int = Field(default=4096, ge=512, le=64_000)
    agent_timeout_seconds: float = Field(default=30.0, gt=0, le=300)
    agent_max_attempts: int = Field(default=3, ge=1, le=5)
    agent_retry_backoff_seconds: float = Field(default=0.4, ge=0, le=10)
    simulated_latency_ms: int = Field(default=25, ge=0, le=5_000)
    agent_api_key: str | None = None

    # --- plan shaping -----------------------------------------------------
    retrieval_top_k: int = Field(default=4, ge=1, le=12)
    max_isolation_steps: int = Field(default=12, ge=3, le=40)
    min_graph_trust_for_irreversible: float = Field(default=0.65, ge=0.0, le=1.0)
    idempotency_cache_size: int = Field(default=512, ge=0, le=100_000)

    @field_validator("log_level")
    @classmethod
    def _normalise_log_level(cls, value: str) -> str:
        level = value.strip().upper()
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if level not in allowed:
            raise ValueError(f"log_level must be one of {sorted(allowed)}")
        return level


_settings_singleton: InterceptorSettings | None = None


def get_settings() -> InterceptorSettings:
    """Process-wide settings singleton (lazy so tests can patch the env)."""
    global _settings_singleton
    if _settings_singleton is None:
        _settings_singleton = InterceptorSettings()
    return _settings_singleton


_request_id_var: ContextVar[str] = ContextVar("graphrag_request_id", default="-")
_tenant_var: ContextVar[str] = ContextVar("graphrag_tenant", default="-")

_RESERVED_LOG_KEYS = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {
    "asctime",
    "message",
    "taskName",
}


class _JsonLogFormatter(logging.Formatter):
    """Single-line JSON records, correlated by request id and tenant."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": _request_id_var.get(),
            "tenant": _tenant_var.get(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED_LOG_KEYS:
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, separators=(",", ":"))


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(_JsonLogFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level.upper())
    for noisy in ("uvicorn.access", "uvicorn.error"):
        logging.getLogger(noisy).handlers = [handler]
        logging.getLogger(noisy).propagate = False


def bind_request_context(request_id: str, tenant: str = "-") -> tuple[Token[str], Token[str]]:
    return _request_id_var.set(request_id), _tenant_var.set(tenant)


def reset_request_context(tokens: tuple[Token[str], Token[str]]) -> None:
    _request_id_var.reset(tokens[0])
    _tenant_var.reset(tokens[1])


def current_request_id() -> str:
    return _request_id_var.get()


# ===========================================================================
# 1. Data validation and parsing (Pydantic v2 layer)
# ===========================================================================


class AssetStatus(str, Enum):
    """Operational state of a physical asset as held on the graph vertex."""

    OPERATIONAL = "OPERATIONAL"
    DEGRADED = "DEGRADED"
    ALARM = "ALARM"
    OFFLINE = "OFFLINE"
    MAINTENANCE = "MAINTENANCE"
    UNKNOWN = "UNKNOWN"

    @property
    def is_faulted(self) -> bool:
        return self in _FAULTED_STATUSES


_FAULTED_STATUSES = frozenset({AssetStatus.DEGRADED, AssetStatus.ALARM, AssetStatus.OFFLINE})


class FaultClassification(str, Enum):
    """Agent 1's verdict on where the failure actually originates."""

    LOCALIZED_COMPONENT = "LOCALIZED_COMPONENT"
    INHERITED_CASCADE = "INHERITED_CASCADE"
    INDETERMINATE = "INDETERMINATE"
    NO_FAULT_DETECTED = "NO_FAULT_DETECTED"


class ActionType(str, Enum):
    """The three physical interventions this layer is authorised to command."""

    ISOLATE_VALVE = "ISOLATE_VALVE"
    EMERGENCY_SHUTDOWN = "EMERGENCY_SHUTDOWN"
    DEGRADE_THROTTLE = "DEGRADE_THROTTLE"

    @property
    def is_irreversible(self) -> bool:
        return self is ActionType.EMERGENCY_SHUTDOWN


class ExecutionPriority(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    ROUTINE = "ROUTINE"


class StepActor(str, Enum):
    """Who or what executes an isolation step."""

    HUMAN_TECHNICIAN = "HUMAN_TECHNICIAN"
    HUMAN_SUPERVISOR = "HUMAN_SUPERVISOR"
    ROBOTIC_ACTUATOR = "ROBOTIC_ACTUATOR"
    CONTROL_SYSTEM = "CONTROL_SYSTEM"


class CRDTPresence(str, Enum):
    """Resolved liveness of the asset vertex after the OR-Set join."""

    LIVE = "LIVE"
    TOMBSTONED = "TOMBSTONED"
    UNOBSERVED = "UNOBSERVED"


class TieBreaker(str, Enum):
    REMOVED_WINS = "REMOVED_WINS"
    ADD_WINS = "ADD_WINS"
    NOT_REQUIRED = "NOT_REQUIRED"


class Band(str, Enum):
    """Where a telemetry reading sits against its engineering limits."""

    NOMINAL = "NOMINAL"
    WARN = "WARN"
    CRITICAL = "CRITICAL"
    HARD = "HARD"
    UNKNOWN = "UNKNOWN"


_BAND_RANK: dict[Band, int] = {
    Band.UNKNOWN: 0,
    Band.NOMINAL: 0,
    Band.WARN: 1,
    Band.CRITICAL: 2,
    Band.HARD: 3,
}


class _Inbound(BaseModel):
    """Base for engine-produced payloads.

    ``extra="ignore"`` on the nested structures is deliberate: the open-core
    engine may add fields in a minor release and the paid layer must not start
    rejecting production traffic when it does.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True, str_strip_whitespace=True)


_NODE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:\-]{1,127}$")
_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_CHANNEL_SEPARATOR = "::"


def _validate_node_id(value: str, *, field_name: str) -> str:
    node = value.strip()
    if not _NODE_ID_RE.match(node):
        raise ValueError(
            f"{field_name} {value!r} is not a valid graph node id "
            "(expected 2-128 chars of [A-Za-z0-9._:-], starting alphanumeric)"
        )
    return node


class AssetMetadata(_Inbound):
    """Identity of the physical asset the mutation fired on."""

    uuid: str = Field(
        min_length=2,
        max_length=128,
        description="Stable graph vertex id: an RFC-4122 UUID or a site asset slug.",
    )
    name: str = Field(min_length=1, max_length=256, description="Human-readable asset name.")
    model_number: str = Field(
        min_length=1,
        max_length=128,
        description="Manufacturer model number; drives maintenance-manual retrieval.",
    )
    current_status: AssetStatus = Field(
        default=AssetStatus.UNKNOWN, description="Operational state recorded on the vertex."
    )

    @field_validator("uuid")
    @classmethod
    def _canonical_identifier(cls, value: str) -> str:
        # Canonical UUIDs are normalised to lower case; site slugs (the form the
        # Go engine actually emits, e.g. "engine-esn-771402") pass through.
        candidate = value.strip()
        if _UUID_RE.match(candidate):
            return candidate.lower()
        return _validate_node_id(candidate, field_name="asset uuid")

    @property
    def model_family(self) -> str:
        """Leading token of the model number, used as a retrieval filter."""
        head = re.split(r"[-_/\s]", self.model_number.strip(), maxsplit=1)[0]
        return head.upper()


class ReplicaObservation(_Inbound):
    """One replica's Lamport timeline for the asset vertex.

    Mirrors the OR-Set element the Go engine replicates: per-replica *add* and
    *remove* stamps whose join is the pointwise maximum. Liveness is decided by
    comparing the two maxima, never by wall-clock time.
    """

    replica_id: str = Field(min_length=1, max_length=128)
    add_stamp: int = Field(default=0, ge=0, description="Highest Lamport add stamp seen by this replica.")
    remove_stamp: int = Field(default=0, ge=0, description="Highest Lamport remove stamp seen by this replica.")
    last_synced_at: datetime | None = None

    @property
    def local_presence(self) -> CRDTPresence:
        if self.add_stamp == 0 and self.remove_stamp == 0:
            return CRDTPresence.UNOBSERVED
        # Strictly greater: an equal pair is a tie, and a tie is not liveness.
        return CRDTPresence.LIVE if self.add_stamp > self.remove_stamp else CRDTPresence.TOMBSTONED


class OntologyContext(_Inbound):
    """The localised graph topology around the asset.

    ``upstream_dependencies`` is ordered nearest-first: index 0 feeds the asset
    directly, index 1 feeds index 0, and so on. Agent 1's traversal relies on
    that ordering to walk a cascade back to its origin.
    """

    parent_system: str = Field(min_length=1, max_length=128, description="Owning system vertex.")
    physical_location: str = Field(min_length=1, max_length=256, description="Site / unit / bay.")
    upstream_dependencies: list[str] = Field(
        default_factory=list,
        max_length=64,
        description="Feeding nodes, ordered nearest-first.",
    )
    downstream_impacts: list[str] = Field(
        default_factory=list,
        max_length=128,
        description="Nodes that consume this asset's output; the blast radius.",
    )
    assigned_operator: str = Field(min_length=1, max_length=128, description="Accountable operator.")

    # --- optional enrichments -------------------------------------------
    # Present when the engine has resolved neighbour state; absent payloads still
    # validate, and the kernel degrades to telemetry-only attribution.
    dependency_health: dict[str, AssetStatus] = Field(
        default_factory=dict,
        description="Known status of neighbour nodes, keyed by node id.",
    )
    replica_observations: list[ReplicaObservation] = Field(
        default_factory=list,
        max_length=64,
        description="Per-replica Lamport timeline for the asset vertex.",
    )
    criticality: str = Field(default="UNKNOWN", max_length=64)
    maintenance_window: str | None = Field(default=None, max_length=128)

    @field_validator("upstream_dependencies", "downstream_impacts")
    @classmethod
    def _validate_edges(cls, values: list[str]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for raw in values:
            node = _validate_node_id(raw, field_name="dependency node")
            if node in seen:  # duplicates would double-count the blast radius
                continue
            seen.add(node)
            ordered.append(node)
        return ordered

    @field_validator("parent_system")
    @classmethod
    def _validate_parent(cls, value: str) -> str:
        return _validate_node_id(value, field_name="parent_system")

    @property
    def nearest_upstream(self) -> str | None:
        return self.upstream_dependencies[0] if self.upstream_dependencies else None

    @property
    def blast_radius_size(self) -> int:
        return len(self.downstream_impacts)

    def upstream_chain_order(self) -> tuple[str, ...]:
        """Traversal order for the upstream walk: nearest node first.

        Named rather than inlined because the ordering *is* the contract — the
        cascade origin is the furthest faulting node in this sequence.
        """
        return tuple(self.upstream_dependencies)

    def neighbour_ids(self) -> frozenset[str]:
        return frozenset({self.parent_system, *self.upstream_dependencies, *self.downstream_impacts})


class TelemetrySnapshot(BaseModel):
    """Dynamic sensor channels captured at the moment of the mutation.

    Well-known channels are declared for editor and OpenAPI support; anything
    else the site publishes is accepted as an extra and coerced to ``float``.
    Channels may be *attributed* to a neighbour node with a ``node::channel``
    key (for example ``pump-114::vibration_index``), which is how the topology
    kernel sees upstream evidence without a second round trip to the graph.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    vibration_index: float | None = None
    temperature_celsius: float | None = None
    crdt_collision_rate: float | None = None

    @model_validator(mode="after")
    def _coerce_and_require_channels(self) -> "TelemetrySnapshot":
        extras = self.__pydantic_extra__
        if extras:
            coerced: dict[str, float] = {}
            for key, value in extras.items():
                if isinstance(value, bool) or not isinstance(value, (int, float, str)):
                    raise ValueError(f"telemetry channel {key!r} must be a float, got {type(value).__name__}")
                try:
                    number = float(value)
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"telemetry channel {key!r} is not numeric: {value!r}") from exc
                if not math.isfinite(number):
                    raise ValueError(f"telemetry channel {key!r} is not finite: {value!r}")
                coerced[key.strip()] = number
            extras.clear()
            extras.update(coerced)
        if not self.channels():
            raise ValueError("telemetry_snapshot must carry at least one sensor channel")
        return self

    def channels(self) -> dict[str, float]:
        """Every channel present, declared or dynamic."""
        merged: dict[str, float] = {}
        for name in ("vibration_index", "temperature_celsius", "crdt_collision_rate"):
            value = getattr(self, name)
            if value is not None:
                merged[name] = float(value)
        merged.update(self.__pydantic_extra__ or {})
        return merged

    def local_channels(self) -> dict[str, float]:
        """Channels belonging to the asset itself (no ``node::`` prefix)."""
        return {k: v for k, v in self.channels().items() if _CHANNEL_SEPARATOR not in k}

    def attributed_channels(self) -> dict[str, dict[str, float]]:
        """Channels attributed to neighbour nodes, keyed by node id."""
        attributed: dict[str, dict[str, float]] = {}
        for key, value in self.channels().items():
            if _CHANNEL_SEPARATOR not in key:
                continue
            node, _, channel = key.partition(_CHANNEL_SEPARATOR)
            node, channel = node.strip(), channel.strip()
            if not node or not channel:
                continue
            attributed.setdefault(node, {})[channel] = value
        return attributed

    def get(self, channel: str, default: float | None = None) -> float | None:
        return self.channels().get(channel, default)

    def as_lines(self) -> list[str]:
        return [f"{name}={value:g}" for name, value in sorted(self.local_channels().items())]


class EnrichedGraphPayload(_Inbound):
    """Master inbound contract for ``POST /v1/intercept``."""

    event_id: str = Field(min_length=1, max_length=128)
    timestamp: datetime = Field(description="When the mutation was emitted (RFC 3339, tz-aware).")
    asset_metadata: AssetMetadata
    ontology_context: OntologyContext
    telemetry_snapshot: TelemetrySnapshot

    # --- optional provenance ---------------------------------------------
    origin_replica: str | None = Field(default=None, max_length=128)
    lamport_clock: int = Field(default=0, ge=0)
    graph_revision: str | None = Field(default=None, max_length=128)
    degraded: bool = Field(default=False, description="Engine flag: context resolved incompletely.")

    @model_validator(mode="before")
    @classmethod
    def _accept_wire_key(cls, data: Any) -> Any:
        """Accept the engine's ``asset`` key for ``asset_metadata``.

        A ``Field(alias=...)`` would be the idiomatic spelling, but FastAPI
        re-derives a TypeAdapter per body field and pydantic then warns on every
        request that the alias has no effect in that context. Mapping the key
        here keeps both spellings valid and the request path quiet.
        """
        if isinstance(data, dict) and "asset" in data and "asset_metadata" not in data:
            data = dict(data)
            data["asset_metadata"] = data.pop("asset")
        return data

    @field_validator("timestamp")
    @classmethod
    def _tz_aware(cls, value: datetime) -> datetime:
        # A naive stamp from an edge site is assumed UTC rather than rejected;
        # ordering downstream is Lamport-based, so this only affects display.
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def _cross_check_topology(self) -> "EnrichedGraphPayload":
        asset_id = self.asset_metadata.uuid
        context = self.ontology_context

        if asset_id in context.upstream_dependencies or asset_id in context.downstream_impacts:
            raise ValueError(f"asset {asset_id!r} appears in its own dependency chain (self-loop)")

        overlap = set(context.upstream_dependencies) & set(context.downstream_impacts)
        if overlap:
            raise ValueError(
                f"nodes {sorted(overlap)} are both upstream and downstream of {asset_id!r}; "
                "the localised topology must be acyclic"
            )

        known = context.neighbour_ids() | {asset_id}

        unknown_health = sorted(set(context.dependency_health) - known)
        if unknown_health:
            raise ValueError(f"dependency_health references nodes outside the topology: {unknown_health}")

        unknown_attribution = sorted(set(self.telemetry_snapshot.attributed_channels()) - known)
        if unknown_attribution:
            raise ValueError(
                f"telemetry attributed to nodes outside the topology: {unknown_attribution}"
            )
        return self

    @property
    def known_nodes(self) -> frozenset[str]:
        return self.ontology_context.neighbour_ids() | {self.asset_metadata.uuid}


# ---------------------------------------------------------------------------
# Outbound and inter-agent contracts
# ---------------------------------------------------------------------------


class _Outbound(BaseModel):
    """Base for anything this service emits or requires an agent to emit."""

    model_config = ConfigDict(extra="forbid")


class CRDTAssessment(_Outbound):
    """Agent 1's reading of the replication state behind the topology."""

    presence: CRDTPresence
    tie_breaker_rule: TieBreaker
    contested: bool = Field(description="True when the highest add and remove stamps are equal.")
    highest_add_stamp: int = Field(ge=0)
    highest_remove_stamp: int = Field(ge=0)
    diverging_replicas: list[str] = Field(default_factory=list)
    collision_rate: float = Field(default=0.0, ge=0.0)
    graph_trust: float = Field(ge=0.0, le=1.0, description="0 = topology untrustworthy, 1 = fully converged.")
    explanation: str = Field(min_length=1, max_length=1200)


class TopologyIsolationVerdict(_Outbound):
    """Agent 1 output: where the fault lives, and how much of the graph to trust."""

    fault_classification: FaultClassification
    root_cause_node: str = Field(min_length=1, max_length=128)
    root_cause_rationale: str = Field(min_length=1, max_length=2000)
    cascade_path: list[str] = Field(
        default_factory=list,
        max_length=64,
        description="Origin-first traversal from the faulted upstream node to the asset.",
    )
    localized_components: list[str] = Field(default_factory=list, max_length=64)
    blast_radius: list[str] = Field(default_factory=list, max_length=128)
    severity_band: Band
    crdt_assessment: CRDTAssessment
    containment_hint: ActionType
    irreversible_permitted: bool
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: list[str] = Field(default_factory=list, max_length=64)


class IsolationStep(_Outbound):
    """One instruction in the physical isolation sequence."""

    sequence: int = Field(ge=1, description="1-based execution order.")
    actor: StepActor
    instruction: str = Field(min_length=8, max_length=600)
    target_component: str = Field(min_length=1, max_length=128)
    verification: str = Field(min_length=4, max_length=400)
    reversible: bool = True
    estimated_seconds: int = Field(default=120, ge=0, le=86_400)
    manual_reference: str | None = Field(default=None, max_length=160)

    def render(self) -> str:
        return f"{self.sequence}. [{self.actor.value}] {self.instruction} (verify: {self.verification})"


class StrategicActionPlan(_Outbound):
    """Agent 2 output: the command, before server-side guardrails are applied."""

    action_type: ActionType
    target_asset_id: str = Field(min_length=1, max_length=128)
    execution_priority: ExecutionPriority
    isolation_steps: list[IsolationStep] = Field(min_length=1, max_length=40)
    blast_radius_protection: list[str] = Field(default_factory=list, max_length=64)
    requires_human_authorization: bool = True
    rationale: str = Field(min_length=1, max_length=2000)
    manual_references: list[str] = Field(default_factory=list, max_length=32)
    confidence: float = Field(ge=0.0, le=1.0)

    @field_validator("isolation_steps")
    @classmethod
    def _contiguous_sequence(cls, steps: list[IsolationStep]) -> list[IsolationStep]:
        expected = list(range(1, len(steps) + 1))
        actual = [step.sequence for step in steps]
        if actual != expected:
            raise ValueError(f"isolation step sequence must be contiguous from 1: got {actual}")
        return steps


class RetrievedGuideline(_Outbound):
    """A maintenance-manual chunk surfaced by the GraphRAG lookup."""

    doc_id: str
    title: str
    source: str
    revision: str
    score: float = Field(ge=0.0)
    excerpt: str


class AgentTraceEntry(_Outbound):
    """Per-agent execution record; the audit trail a regulator asks for."""

    agent: str
    model: str
    provider: str
    latency_ms: float = Field(ge=0.0)
    attempts: int = Field(ge=1)
    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)


class CommandActionResponse(_Outbound):
    """The interceptor's answer: one actionable physical command."""

    command_id: str = Field(description="Server-generated UUID for this command.")
    target_asset_id: str = Field(description="Node requiring physical intervention.")
    action_type: ActionType
    isolation_steps: list[IsolationStep]
    execution_priority: ExecutionPriority

    # --- provenance and justification ------------------------------------
    schema_version: Literal["openontology.command-action.v1"] = RESPONSE_SCHEMA_VERSION
    event_id: str
    source_asset_id: str = Field(description="Asset the mutation fired on, which may differ from the target.")
    tenant: str
    issued_at: datetime = Field(default_factory=_utcnow)
    fault_classification: FaultClassification
    cascade_path: list[str] = Field(default_factory=list)
    blast_radius: list[str] = Field(default_factory=list)
    blast_radius_protection: list[str] = Field(default_factory=list)
    crdt_assessment: CRDTAssessment
    requires_human_authorization: bool = True
    reversible: bool = True
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str
    evidence: list[str] = Field(default_factory=list)
    manual_references: list[RetrievedGuideline] = Field(default_factory=list)
    guardrails_applied: list[str] = Field(default_factory=list)
    agent_trace: list[AgentTraceEntry] = Field(default_factory=list)
    latency_ms: float = Field(default=0.0, ge=0.0)


class ErrorDetail(_Outbound):
    code: str
    message: str
    hint: str | None = None


class ErrorEnvelope(_Outbound):
    error: ErrorDetail
    request_id: str


class HealthResponse(_Outbound):
    status: Literal["ok"]
    service: str
    module: Literal["semantic-graphrag-interceptor"] = "semantic-graphrag-interceptor"
    version: str
    environment: str
    agent_provider: str
    agent_model: str
    knowledge_chunks: int
    uptime_seconds: float


class SubscriptionIntrospection(_Outbound):
    key_id: str
    tenant: str
    tier: str
    features: list[str]
    quota_per_minute: int
    expires_at: datetime
    valid: bool


# ===========================================================================
# 2a. Deterministic topology kernel — engineering limits
# ===========================================================================


@dataclass(frozen=True)
class ChannelLimit:
    """Engineering limits for one telemetry channel.

    ``physical=False`` marks a *context integrity* channel: it describes how much
    the replicated graph can be trusted, never the physical health of the asset.
    Integrity breaches must never, on their own, justify an irreversible action.
    """

    channel: str
    unit: str
    warn: float
    critical: float
    hard: float
    physical: bool = True
    description: str = ""

    def band(self, value: float) -> Band:
        if value >= self.hard:
            return Band.HARD
        if value >= self.critical:
            return Band.CRITICAL
        if value >= self.warn:
            return Band.WARN
        return Band.NOMINAL

    def ratio(self, value: float) -> float:
        return value / self.critical if self.critical else 0.0


CHANNEL_LIMITS: dict[str, ChannelLimit] = {
    limit.channel: limit
    for limit in (
        ChannelLimit("vibration_index", "mm/s", 4.5, 7.0, 9.5, True, "Broadband RMS velocity."),
        ChannelLimit("temperature_celsius", "degC", 85.0, 110.0, 130.0, True, "Bearing / casing metal temperature."),
        ChannelLimit("pressure_bar", "bar", 12.0, 16.0, 19.0, True, "Line pressure at the asset outlet."),
        ChannelLimit("flow_deviation_pct", "%", 8.0, 18.0, 30.0, True, "Deviation from commanded flow."),
        ChannelLimit("rpm_deviation_pct", "%", 5.0, 12.0, 20.0, True, "Deviation from commanded shaft speed."),
        ChannelLimit("current_draw_amps", "A", 60.0, 85.0, 110.0, True, "Motor current draw."),
        ChannelLimit("acoustic_emission_db", "dB", 78.0, 92.0, 105.0, True, "Structure-borne acoustic emission."),
        ChannelLimit("oil_particulate_ppm", "ppm", 120.0, 260.0, 400.0, True, "Ferrous particulate in lubricant."),
        ChannelLimit("torque_nm", "N.m", 320.0, 420.0, 500.0, True, "Shaft torque."),
        ChannelLimit("differential_pressure_bar", "bar", 1.5, 2.8, 4.0, True, "Across-element differential."),
        ChannelLimit(
            "crdt_collision_rate", "ratio", 0.05, 0.15, 0.35, False, "Concurrent conflicting ops per merge."
        ),
        ChannelLimit("lamport_skew_ops", "ops", 25.0, 120.0, 400.0, False, "Clock spread across replicas."),
        ChannelLimit("replica_lag_seconds", "s", 30.0, 300.0, 1800.0, False, "Time since the slowest replica synced."),
    )
}


@dataclass(frozen=True)
class Exceedance:
    """One channel measured against its limits."""

    channel: str
    value: float
    band: Band
    limit: ChannelLimit | None
    node: str

    @property
    def physical(self) -> bool:
        return self.limit.physical if self.limit else True

    def describe(self) -> str:
        if self.limit is None:
            return f"{self.node}:{self.channel}={self.value:g} (no limit configured)"
        return (
            f"{self.node}:{self.channel}={self.value:g}{self.limit.unit} "
            f"[{self.band.value}; warn {self.limit.warn:g} / crit {self.limit.critical:g} / hard {self.limit.hard:g}]"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "node": self.node,
            "channel": self.channel,
            "value": self.value,
            "band": self.band.value,
            "unit": self.limit.unit if self.limit else None,
            "critical_limit": self.limit.critical if self.limit else None,
            "physical": self.physical,
        }


def evaluate_channels(channels: Mapping[str, float], *, node: str) -> list[Exceedance]:
    """Score channels against :data:`CHANNEL_LIMITS`, worst band first."""
    scored: list[Exceedance] = []
    for channel, value in channels.items():
        limit = CHANNEL_LIMITS.get(channel)
        band = limit.band(value) if limit else Band.UNKNOWN
        scored.append(Exceedance(channel=channel, value=value, band=band, limit=limit, node=node))
    scored.sort(key=lambda item: (-_BAND_RANK[item.band], item.channel))
    return scored


def worst_band(exceedances: Iterable[Exceedance]) -> Band:
    worst = Band.NOMINAL
    for item in exceedances:
        if _BAND_RANK[item.band] > _BAND_RANK[worst]:
            worst = item.band
    return worst


# ===========================================================================
# 2b. Deterministic topology kernel — CRDT / Lamport resolution
# ===========================================================================


def resolve_crdt_presence(
    observations: Sequence[ReplicaObservation],
    *,
    has_live_downstream: bool,
    collision_rate: float,
    telemetry_is_live: bool,
) -> CRDTAssessment:
    """Join the replicas' OR-Set timelines and decide vertex liveness.

    The join is the pointwise maximum of the per-replica add and remove stamps,
    exactly as the Go replication core does it. An element is live only when its
    highest add stamp is *strictly* greater than its highest remove stamp, so an
    equal pair is a genuine tie that a policy has to break.

    The tie-breaker is chosen from the topology rather than fixed:

    * A **leaf** vertex (nothing downstream depends on it) resolves
      ``REMOVED_WINS``, matching the replication core's documented default.
      A retire that races a re-add costs an operator one retry, not a
      permanently unreachable asset — resurrection is always available because
      a later add carries a strictly greater stamp.
    * A vertex with **live downstream dependents** resolves ``ADD_WINS``. Letting
      a concurrent retire tombstone a node that other assets are still fed by
      would erase a physically present asset from the operator's graph, which is
      the more dangerous failure. The override is recorded, costs confidence, and
      forces a reconciliation step into the plan.
    """
    if not observations:
        return CRDTAssessment(
            presence=CRDTPresence.UNOBSERVED,
            tie_breaker_rule=TieBreaker.NOT_REQUIRED,
            contested=False,
            highest_add_stamp=0,
            highest_remove_stamp=0,
            diverging_replicas=[],
            collision_rate=collision_rate,
            graph_trust=round(max(0.0, 0.7 - min(0.4, collision_rate * 2.0)), 3),
            explanation=(
                "No replica observations accompanied the mutation; vertex liveness is unverified. "
                "Treating the topology as provisional."
            ),
        )

    highest_add = max(obs.add_stamp for obs in observations)
    highest_remove = max(obs.remove_stamp for obs in observations)
    contested = highest_add == highest_remove and highest_add > 0

    if highest_add > highest_remove:
        presence = CRDTPresence.LIVE
        rule = TieBreaker.NOT_REQUIRED
        reason = f"highest add stamp {highest_add} exceeds highest remove stamp {highest_remove}"
    elif highest_remove > highest_add:
        presence = CRDTPresence.TOMBSTONED
        rule = TieBreaker.NOT_REQUIRED
        reason = f"highest remove stamp {highest_remove} exceeds highest add stamp {highest_add}"
    elif highest_add == 0:
        presence = CRDTPresence.UNOBSERVED
        rule = TieBreaker.NOT_REQUIRED
        reason = "no replica has stamped this vertex"
    elif has_live_downstream:
        presence = CRDTPresence.LIVE
        rule = TieBreaker.ADD_WINS
        reason = (
            f"add and remove stamps tie at {highest_add}; the vertex still feeds live downstream "
            "dependents, so add-wins keeps a physically present asset visible to operators"
        )
    else:
        presence = CRDTPresence.TOMBSTONED
        rule = TieBreaker.REMOVED_WINS
        reason = (
            f"add and remove stamps tie at {highest_add} on a leaf vertex; removed-wins matches the "
            "replication core, and a later add can always resurrect it"
        )

    # A replica whose own timeline disagrees with the joined verdict has not yet
    # absorbed the other side's state.
    diverging = sorted(
        obs.replica_id
        for obs in observations
        if obs.local_presence is not CRDTPresence.UNOBSERVED and obs.local_presence is not presence
    )

    trust = 1.0
    notes: list[str] = []
    if contested:
        trust -= 0.25
        notes.append("stamp tie required a policy tie-breaker")
    if rule is TieBreaker.ADD_WINS:
        trust -= 0.10
        notes.append("topology overrode the default removed-wins policy")
    if diverging:
        trust -= min(0.30, 0.10 * len(diverging))
        notes.append(f"{len(diverging)} replica(s) have not converged: {', '.join(diverging)}")
    if collision_rate > 0:
        trust -= min(0.40, collision_rate * 2.0)
        notes.append(f"merge collision rate {collision_rate:g}")
    if presence is CRDTPresence.TOMBSTONED and telemetry_is_live:
        # The graph says the asset is retired while the asset is still reporting.
        trust -= 0.25
        notes.append("vertex is tombstoned but still emitting telemetry")
    trust = round(max(0.0, min(1.0, trust)), 3)

    explanation = f"Vertex resolves {presence.value}: {reason}."
    if notes:
        explanation += " Trust reduced by: " + "; ".join(notes) + "."

    return CRDTAssessment(
        presence=presence,
        tie_breaker_rule=rule,
        contested=contested,
        highest_add_stamp=highest_add,
        highest_remove_stamp=highest_remove,
        diverging_replicas=diverging,
        collision_rate=collision_rate,
        graph_trust=trust,
        explanation=explanation,
    )


# ===========================================================================
# 2c. Deterministic topology kernel — traversal and classification
# ===========================================================================


@dataclass(frozen=True)
class TopologyAnalysis:
    """Everything the kernel derives before any model is consulted.

    Both agents receive this as grounding, and :attr:`known_nodes` is the
    allowlist their answers are validated against.
    """

    asset_id: str
    asset_name: str
    parent_system: str
    physical_location: str
    known_nodes: frozenset[str]
    upstream_chain: tuple[str, ...]
    downstream_impacts: tuple[str, ...]
    faulted_upstream: tuple[str, ...]
    cascade_path: tuple[str, ...]
    classification: FaultClassification
    root_cause_node: str
    localized_components: tuple[str, ...]
    physical_exceedances: tuple[Exceedance, ...]
    integrity_exceedances: tuple[Exceedance, ...]
    severity_band: Band
    crdt: CRDTAssessment
    blast_radius: tuple[str, ...]
    priority: ExecutionPriority
    recommended_action: ActionType
    irreversible_permitted: bool
    confidence: float
    evidence: tuple[str, ...]

    def to_briefing(self) -> dict[str, Any]:
        """Machine-readable grounding handed to both agents."""
        return {
            "asset_id": self.asset_id,
            "asset_name": self.asset_name,
            "parent_system": self.parent_system,
            "physical_location": self.physical_location,
            "upstream_chain_nearest_first": list(self.upstream_chain),
            "downstream_impacts": list(self.downstream_impacts),
            "faulted_upstream_nodes": list(self.faulted_upstream),
            "cascade_path_origin_first": list(self.cascade_path),
            "derived_classification": self.classification.value,
            "derived_root_cause_node": self.root_cause_node,
            "localized_components": list(self.localized_components),
            "physical_exceedances": [item.to_dict() for item in self.physical_exceedances],
            "integrity_exceedances": [item.to_dict() for item in self.integrity_exceedances],
            "severity_band": self.severity_band.value,
            "crdt_assessment": self.crdt.model_dump(mode="json"),
            "blast_radius": list(self.blast_radius),
            "derived_priority": self.priority.value,
            "derived_action": self.recommended_action.value,
            "irreversible_permitted": self.irreversible_permitted,
            "derived_confidence": self.confidence,
            "evidence": list(self.evidence),
            "permitted_nodes": sorted(self.known_nodes),
        }


def _node_is_faulted(
    node: str,
    *,
    health: Mapping[str, AssetStatus],
    attributed: Mapping[str, dict[str, float]],
) -> tuple[bool, list[str]]:
    """Is this neighbour node itself faulting? Returns (verdict, reasons)."""
    reasons: list[str] = []

    status = health.get(node)
    if status is not None and status.is_faulted:
        reasons.append(f"{node} reports status {status.value}")

    node_channels = attributed.get(node)
    if node_channels:
        scored = evaluate_channels(node_channels, node=node)
        for item in scored:
            if item.physical and _BAND_RANK[item.band] >= _BAND_RANK[Band.CRITICAL]:
                reasons.append(f"{item.describe()}")
    return bool(reasons), reasons


def analyse_topology(payload: EnrichedGraphPayload, settings: InterceptorSettings) -> TopologyAnalysis:
    """Traverse the localised graph and derive a defensible verdict.

    Pure and deterministic: the same payload always yields the same analysis,
    which is what makes the agents auditable rather than merely plausible.
    """
    asset = payload.asset_metadata
    context = payload.ontology_context
    telemetry = payload.telemetry_snapshot

    local_channels = telemetry.local_channels()
    attributed = telemetry.attributed_channels()
    scored = evaluate_channels(local_channels, node=asset.uuid)
    physical = tuple(item for item in scored if item.physical)
    integrity = tuple(item for item in scored if not item.physical)

    severity_band = worst_band(physical)
    integrity_band = worst_band(integrity)

    # --- CRDT / Lamport resolution ---------------------------------------
    live_downstream = [
        node
        for node in context.downstream_impacts
        if context.dependency_health.get(node, AssetStatus.UNKNOWN) is not AssetStatus.OFFLINE
    ]
    crdt = resolve_crdt_presence(
        context.replica_observations,
        has_live_downstream=bool(live_downstream),
        collision_rate=float(telemetry.get("crdt_collision_rate", 0.0) or 0.0),
        telemetry_is_live=bool(local_channels),
    )

    # --- upstream traversal ----------------------------------------------
    # upstream_dependencies is ordered nearest-first, so walking it forwards
    # moves away from the asset. The origin of a cascade is the furthest node
    # that is itself faulting: everything between it and the asset is a symptom.
    evidence: list[str] = []
    faulted_upstream: list[str] = []
    for node in context.upstream_chain_order():
        faulted, reasons = _node_is_faulted(node, health=context.dependency_health, attributed=attributed)
        if faulted:
            faulted_upstream.append(node)
            evidence.extend(reasons)

    # --- classification ---------------------------------------------------
    graph_untrusted = crdt.graph_trust < settings.min_graph_trust_for_irreversible
    local_physical_fault = _BAND_RANK[severity_band] >= _BAND_RANK[Band.WARN]

    if faulted_upstream:
        classification = FaultClassification.INHERITED_CASCADE
        origin = faulted_upstream[-1]  # furthest faulting node = fault origin
        cascade_index = context.upstream_dependencies.index(origin)
        # Origin-first: origin -> ... -> nearest upstream -> asset.
        cascade_path = tuple(reversed(context.upstream_dependencies[: cascade_index + 1])) + (asset.uuid,)
        root_cause = origin
        evidence.append(
            f"cascade traced {' -> '.join(cascade_path)}; intervening nodes are symptomatic, not causal"
        )
    elif local_physical_fault:
        classification = FaultClassification.LOCALIZED_COMPONENT
        cascade_path = ()
        root_cause = asset.uuid
        evidence.append(
            f"no upstream node is faulting; the breach is contained to {asset.uuid} "
            f"({', '.join(item.describe() for item in physical if _BAND_RANK[item.band] >= 1) or 'nominal'})"
        )
    elif _BAND_RANK[integrity_band] >= _BAND_RANK[Band.WARN] or graph_untrusted:
        classification = FaultClassification.INDETERMINATE
        cascade_path = ()
        root_cause = asset.uuid
        evidence.append(
            "no physical limit is breached, but the replicated topology is not trustworthy; "
            "attribution cannot be completed from this payload"
        )
    else:
        classification = FaultClassification.NO_FAULT_DETECTED
        cascade_path = ()
        root_cause = asset.uuid
        evidence.append("all channels nominal and the topology has converged")

    localized = tuple(
        item.channel for item in physical if _BAND_RANK[item.band] >= _BAND_RANK[Band.WARN]
    )

    # --- severity and action ---------------------------------------------
    blast_radius = tuple(context.downstream_impacts)
    hard_breach = _BAND_RANK[severity_band] >= _BAND_RANK[Band.HARD]
    critical_breach = _BAND_RANK[severity_band] >= _BAND_RANK[Band.CRITICAL]
    wide_blast = len(blast_radius) >= 3
    asset_alarming = asset.current_status in {AssetStatus.ALARM, AssetStatus.OFFLINE}

    if hard_breach or (critical_breach and (wide_blast or asset_alarming)):
        priority = ExecutionPriority.CRITICAL
    elif critical_breach or (classification is FaultClassification.INHERITED_CASCADE and local_physical_fault):
        priority = ExecutionPriority.HIGH
    elif local_physical_fault or classification is FaultClassification.INDETERMINATE:
        priority = ExecutionPriority.HIGH if wide_blast else ExecutionPriority.ROUTINE
    else:
        priority = ExecutionPriority.ROUTINE

    irreversible_permitted = (
        not graph_untrusted
        and crdt.presence is not CRDTPresence.UNOBSERVED
        and not (crdt.presence is CRDTPresence.TOMBSTONED and bool(local_channels))
    )

    if classification is FaultClassification.INDETERMINATE or not irreversible_permitted:
        # Never take an irreversible action on a topology we cannot trust.
        recommended = ActionType.DEGRADE_THROTTLE
    elif hard_breach and priority is ExecutionPriority.CRITICAL:
        # A hard limit is a permit-to-operate breach: continued operation is
        # unsafe at any blast radius. Note that a *wide* blast radius is
        # deliberately not a trigger here — taking many downstream consumers out
        # on an emergency trip is a reason for controlled isolation, not for a
        # faster one. Blast radius raises priority, never irreversibility.
        recommended = ActionType.EMERGENCY_SHUTDOWN
    elif classification in {
        FaultClassification.LOCALIZED_COMPONENT,
        FaultClassification.INHERITED_CASCADE,
    } and priority in {ExecutionPriority.CRITICAL, ExecutionPriority.HIGH}:
        recommended = ActionType.ISOLATE_VALVE
    else:
        recommended = ActionType.DEGRADE_THROTTLE

    # --- confidence -------------------------------------------------------
    confidence = 0.9
    if classification is FaultClassification.INDETERMINATE:
        confidence -= 0.35
    if payload.degraded:
        confidence -= 0.15
    if not context.replica_observations:
        confidence -= 0.10
    if classification is FaultClassification.INHERITED_CASCADE and not context.dependency_health:
        confidence -= 0.10  # attribution rests on attributed telemetry alone
    confidence = round(max(0.05, min(1.0, confidence * (0.6 + 0.4 * crdt.graph_trust))), 3)

    evidence.append(crdt.explanation)
    evidence.extend(item.describe() for item in physical[:6])
    evidence.extend(item.describe() for item in integrity[:3])

    return TopologyAnalysis(
        asset_id=asset.uuid,
        asset_name=asset.name,
        parent_system=context.parent_system,
        physical_location=context.physical_location,
        known_nodes=payload.known_nodes,
        upstream_chain=tuple(context.upstream_dependencies),
        downstream_impacts=tuple(context.downstream_impacts),
        faulted_upstream=tuple(faulted_upstream),
        cascade_path=cascade_path,
        classification=classification,
        root_cause_node=root_cause,
        localized_components=localized,
        physical_exceedances=physical,
        integrity_exceedances=integrity,
        severity_band=severity_band,
        crdt=crdt,
        blast_radius=blast_radius,
        priority=priority,
        recommended_action=recommended,
        irreversible_permitted=irreversible_permitted,
        confidence=confidence,
        evidence=tuple(dict.fromkeys(evidence)),  # stable order, de-duplicated
    )


# ===========================================================================
# 3. GraphRAG retrieval — technical maintenance manual corpus
# ===========================================================================


@dataclass(frozen=True)
class ProcedureStep:
    """A manual step template. Placeholders are filled from the live topology."""

    actor: StepActor
    instruction: str
    verification: str
    reversible: bool = True
    estimated_seconds: int = 120


@dataclass(frozen=True)
class ManualChunk:
    """One retrievable chunk of a technical maintenance manual."""

    doc_id: str
    title: str
    source: str
    revision: str
    body: str
    procedure: tuple[ProcedureStep, ...]
    model_families: tuple[str, ...] = ()          # empty => applies to every family
    applicable_actions: tuple[ActionType, ...] = ()  # empty => applies to every action
    classifications: tuple[FaultClassification, ...] = ()
    tags: tuple[str, ...] = ()
    supplementary: bool = False  # supplementary chunks augment, never lead

    def indexable_text(self) -> str:
        return " ".join(
            (
                self.title,
                self.body,
                " ".join(self.tags),
                " ".join(self.model_families),
                " ".join(action.value for action in self.applicable_actions),
                " ".join(item.value for item in self.classifications),
            )
        )

    def excerpt(self, limit: int = 320) -> str:
        text = " ".join(self.body.split())
        return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


MAINTENANCE_CORPUS: tuple[ManualChunk, ...] = (
    ManualChunk(
        doc_id="MM-ROT-004-7.3",
        title="Rotating Equipment: Vibration Excursion Containment",
        source="MAINT-MAN-ROT-004 sec. 7.3",
        revision="Rev G",
        body=(
            "Broadband velocity above the critical limit on a rotating assembly indicates bearing "
            "distress, coupling misalignment or rotor imbalance. Reduce shaft speed before any "
            "isolation attempt: closing a discharge path against a rotating assembly at speed "
            "drives the machine into surge. Confirm the reduced-speed vibration signature before "
            "committing to isolation, then close the discharge valve and lock the actuator."
        ),
        procedure=(
            ProcedureStep(
                StepActor.CONTROL_SYSTEM,
                "Ramp {target_name} ({target}) to 60 percent of commanded speed over 90 seconds; "
                "hold and re-sample the vibration signature.",
                "Vibration velocity trends downward within two sample windows.",
                reversible=True,
                estimated_seconds=120,
            ),
            ProcedureStep(
                StepActor.ROBOTIC_ACTUATOR,
                "Close the discharge isolation valve on {target} to 20 percent, then to fully shut "
                "in two stages, monitoring differential pressure between stages.",
                "Valve position feedback reads shut and downstream pressure decays.",
                reversible=True,
                estimated_seconds=180,
            ),
            ProcedureStep(
                StepActor.HUMAN_TECHNICIAN,
                "Apply mechanical lockout to the {target} actuator at {location} and tag the "
                "isolation against the work order.",
                "Lockout tag photographed and attached to the work order record.",
                reversible=True,
                estimated_seconds=300,
            ),
        ),
        model_families=("TRB", "PMP", "CMP", "FAN"),
        applicable_actions=(ActionType.ISOLATE_VALVE, ActionType.DEGRADE_THROTTLE),
        classifications=(FaultClassification.LOCALIZED_COMPONENT,),
        tags=("vibration", "bearing", "rotating", "isolation", "surge"),
    ),
    ManualChunk(
        doc_id="MM-THM-011-2.1",
        title="Hard Thermal Limit: Emergency Shutdown Sequence",
        source="MAINT-MAN-THM-011 sec. 2.1",
        revision="Rev D",
        body=(
            "A metal temperature at or above the hard limit is a permit-to-operate breach. Trip the "
            "asset on its own emergency stop rather than by removing supply: an upstream cut leaves "
            "the rotor windmilling without lubrication. Maintain auxiliary lubrication and cooling "
            "through the coast-down, and evacuate the immediate radius before the trip lands."
        ),
        procedure=(
            ProcedureStep(
                StepActor.HUMAN_SUPERVISOR,
                "Clear personnel from the {location} radius and confirm the evacuation over the "
                "unit channel before the trip is armed.",
                "Muster count confirmed against the site access log.",
                reversible=True,
                estimated_seconds=180,
            ),
            ProcedureStep(
                StepActor.CONTROL_SYSTEM,
                "Arm and execute the emergency stop on {target_name} ({target}); hold auxiliary "
                "lubrication and cooling online through the full coast-down.",
                "Shaft speed reaches zero with lube oil pressure maintained above minimum.",
                reversible=False,
                estimated_seconds=240,
            ),
            ProcedureStep(
                StepActor.ROBOTIC_ACTUATOR,
                "Shut the supply isolation valve on {target} only after coast-down completes.",
                "Valve position feedback reads shut and inlet pressure decays to ambient.",
                reversible=False,
                estimated_seconds=120,
            ),
            ProcedureStep(
                StepActor.HUMAN_TECHNICIAN,
                "Record metal temperature at 5-minute intervals until it falls below the warn "
                "limit; do not break containment before then.",
                "Three consecutive readings below the warn limit are logged.",
                reversible=True,
                estimated_seconds=1800,
            ),
        ),
        applicable_actions=(ActionType.EMERGENCY_SHUTDOWN,),
        classifications=(FaultClassification.LOCALIZED_COMPONENT, FaultClassification.INHERITED_CASCADE),
        tags=("temperature", "thermal", "shutdown", "trip", "coast-down", "emergency"),
    ),
    ManualChunk(
        doc_id="MM-CAS-021-4.4",
        title="Upstream Feed Isolation for Inherited Cascade Faults",
        source="MAINT-MAN-CAS-021 sec. 4.4",
        revision="Rev C",
        body=(
            "When the disturbance originates upstream, isolating the reporting asset removes the "
            "symptom and leaves the source live to propagate along its remaining paths. Stabilise "
            "the reporting asset first so it survives the transient, then isolate at the origin "
            "node, then verify that the intervening nodes recover on their own. Intervening nodes "
            "that do not recover after origin isolation are independent faults and need their own "
            "work orders."
        ),
        procedure=(
            ProcedureStep(
                StepActor.CONTROL_SYSTEM,
                "Hold {asset_name} ({asset}) at reduced load to ride out the upstream transient; "
                "do not isolate the reporting asset yet.",
                "Reporting asset remains within its warn band at reduced load.",
                reversible=True,
                estimated_seconds=120,
            ),
            ProcedureStep(
                StepActor.ROBOTIC_ACTUATOR,
                "Close the outlet isolation valve at the cascade origin {target} to arrest "
                "propagation along {cascade_summary}.",
                "Origin outlet valve reads shut and its downstream pressure decays.",
                reversible=True,
                estimated_seconds=240,
            ),
            ProcedureStep(
                StepActor.HUMAN_TECHNICIAN,
                "Walk the cascade path {cascade_summary} and confirm each intervening node returns "
                "to nominal once the origin is isolated.",
                "Every intervening node is nominal, or is raised as an independent fault.",
                reversible=True,
                estimated_seconds=900,
            ),
        ),
        applicable_actions=(ActionType.ISOLATE_VALVE, ActionType.EMERGENCY_SHUTDOWN),
        classifications=(FaultClassification.INHERITED_CASCADE,),
        tags=("cascade", "upstream", "propagation", "origin", "isolation"),
    ),
    ManualChunk(
        doc_id="MM-DEG-008-3.2",
        title="Throttle to Safe State under Degraded Ontology Confidence",
        source="MAINT-MAN-DEG-008 sec. 3.2",
        revision="Rev B",
        body=(
            "Where the asset register cannot be trusted -- an unresolved replication conflict, a "
            "tombstoned vertex still reporting, or diverging replicas -- no irreversible field "
            "action may be authorised. Throttle to the documented safe state, which is reversible "
            "from the control room, and force a graph reconciliation before escalating. Throttling "
            "buys time without committing the site to an action that cannot be undone."
        ),
        procedure=(
            ProcedureStep(
                StepActor.CONTROL_SYSTEM,
                "Throttle {target_name} ({target}) to the documented safe-state setpoint and "
                "inhibit automatic setpoint recovery.",
                "Setpoint reads safe-state and the recovery inhibit is latched.",
                reversible=True,
                estimated_seconds=90,
            ),
            ProcedureStep(
                StepActor.HUMAN_TECHNICIAN,
                "Verify the asset register entry for {target} against the physical nameplate at "
                "{location} and report the discrepancy.",
                "Nameplate photograph attached to the reconciliation ticket.",
                reversible=True,
                estimated_seconds=600,
            ),
        ),
        applicable_actions=(ActionType.DEGRADE_THROTTLE,),
        classifications=(FaultClassification.INDETERMINATE, FaultClassification.NO_FAULT_DETECTED),
        tags=("degraded", "safe-state", "throttle", "reversible", "reconciliation", "crdt"),
    ),
    ManualChunk(
        doc_id="MM-BLR-002-5.1",
        title="Blast Radius Protection for Downstream Consumers",
        source="MAINT-MAN-BLR-002 sec. 5.1",
        revision="Rev F",
        body=(
            "Before any supply interruption, place every downstream consumer into a state that "
            "tolerates loss of feed. Consumers taken out from under load trip on their own "
            "protection and turn one isolation into a unit-wide outage. Sequence the downstream "
            "transfer before the isolation, never after."
        ),
        procedure=(
            ProcedureStep(
                StepActor.CONTROL_SYSTEM,
                "Transfer downstream consumers {downstream_summary} to standby feed or reduced "
                "load before the isolation lands.",
                "Each downstream consumer reports a stable state on standby feed.",
                reversible=True,
                estimated_seconds=180,
            ),
            ProcedureStep(
                StepActor.HUMAN_SUPERVISOR,
                "Notify {operator} and the {parent} control room that {downstream_count} downstream "
                "node(s) are exposed by this action.",
                "Acknowledgement received from the control room.",
                reversible=True,
                estimated_seconds=120,
            ),
        ),
        tags=("blast", "downstream", "consumers", "standby", "protection"),
        supplementary=True,
    ),
    ManualChunk(
        doc_id="MM-CRDT-014-1.6",
        title="CRDT Reconciliation Before Irreversible Field Action",
        source="MAINT-MAN-CRDT-014 sec. 1.6",
        revision="Rev A",
        body=(
            "The replicated asset graph converges without coordination, so a field crew may be "
            "acting on a vertex whose liveness is still contested. Where the add and remove stamps "
            "tie, or replicas disagree, force a reconciliation pass and re-read the vertex before "
            "committing an irreversible action. A tie resolves to removed on a leaf and to added "
            "where live downstream dependents exist; either way the resolution must be recorded "
            "against the work order."
        ),
        procedure=(
            ProcedureStep(
                StepActor.CONTROL_SYSTEM,
                "Force a reconciliation pass across the replicas holding {target} and re-read the "
                "vertex once the merge settles.",
                "Vertex liveness is uncontested on every replica after the merge.",
                reversible=True,
                estimated_seconds=120,
            ),
            ProcedureStep(
                StepActor.HUMAN_SUPERVISOR,
                "Record the applied tie-breaker rule and the resolved liveness against the work "
                "order before authorising field work on {target}.",
                "Tie-breaker rule and resolved presence captured in the work order.",
                reversible=True,
                estimated_seconds=180,
            ),
        ),
        tags=("crdt", "lamport", "reconciliation", "replica", "tie-breaker", "convergence"),
        supplementary=True,
    ),
    ManualChunk(
        doc_id="MM-LOTO-006-9.2",
        title="Valve Actuator Lockout and Tagout",
        source="MAINT-MAN-LOTO-006 sec. 9.2",
        revision="Rev H",
        body=(
            "A commanded-shut valve is not an isolation until its actuator is mechanically locked "
            "and the stored energy is bled. Prove the isolation by attempting to open against the "
            "lock, then bleed the trapped volume to a safe receiver."
        ),
        procedure=(
            ProcedureStep(
                StepActor.HUMAN_TECHNICIAN,
                "Lock the {target} actuator in the shut position and attempt a controlled open to "
                "prove the lock holds.",
                "Actuator does not move under the open command; lock proven.",
                reversible=True,
                estimated_seconds=420,
            ),
            ProcedureStep(
                StepActor.HUMAN_TECHNICIAN,
                "Bleed trapped volume downstream of {target} to the safe receiver and confirm zero "
                "gauge pressure.",
                "Local gauge reads zero and the bleed is witnessed.",
                reversible=True,
                estimated_seconds=300,
            ),
        ),
        applicable_actions=(ActionType.ISOLATE_VALVE, ActionType.EMERGENCY_SHUTDOWN),
        tags=("lockout", "tagout", "valve", "actuator", "stored-energy", "loto"),
        supplementary=True,
    ),
    ManualChunk(
        doc_id="MM-HEX-017-6.5",
        title="Heat Exchanger Fouling and Differential Pressure",
        source="MAINT-MAN-HEX-017 sec. 6.5",
        revision="Rev C",
        body=(
            "Rising differential pressure with rising outlet temperature indicates tube-side "
            "fouling. Throttle rather than isolate while the bundle is hot: a cold-side isolation "
            "on a hot bundle thermally shocks the tube sheet."
        ),
        procedure=(
            ProcedureStep(
                StepActor.CONTROL_SYSTEM,
                "Reduce throughput across {target_name} ({target}) in 10 percent decrements until "
                "differential pressure falls below the warn limit.",
                "Differential pressure below warn limit for two sample windows.",
                reversible=True,
                estimated_seconds=300,
            ),
            ProcedureStep(
                StepActor.HUMAN_TECHNICIAN,
                "Raise a bundle cleaning work order against {target} for the next window at "
                "{location}.",
                "Work order number recorded against the asset.",
                reversible=True,
                estimated_seconds=600,
            ),
        ),
        model_families=("HX", "HEX", "COOL"),
        applicable_actions=(ActionType.DEGRADE_THROTTLE, ActionType.ISOLATE_VALVE),
        tags=("fouling", "differential", "exchanger", "thermal-shock", "throttle"),
    ),
    ManualChunk(
        doc_id="MM-CTL-009-2.8",
        title="Control Loop Saturation and Actuator Wind-Up",
        source="MAINT-MAN-CTL-009 sec. 2.8",
        revision="Rev E",
        body=(
            "A saturated loop reports a deviation the plant cannot close. Break the loop to manual "
            "at the last known good output before the integrator winds further; isolating the "
            "actuator while the loop is saturated produces a step change on release."
        ),
        procedure=(
            ProcedureStep(
                StepActor.CONTROL_SYSTEM,
                "Place the {target} control loop in manual at the last known good output and clear "
                "the integrator.",
                "Loop reads manual, output matches the last good value, integrator cleared.",
                reversible=True,
                estimated_seconds=60,
            ),
            ProcedureStep(
                StepActor.HUMAN_TECHNICIAN,
                "Stroke the {target} actuator locally through its range and record the response "
                "against the commissioning curve.",
                "Stroke response is within the commissioning tolerance.",
                reversible=True,
                estimated_seconds=900,
            ),
        ),
        model_families=("CTRL", "FADEC", "PLC"),
        applicable_actions=(ActionType.DEGRADE_THROTTLE,),
        tags=("control", "saturation", "wind-up", "manual", "loop"),
    ),
    ManualChunk(
        doc_id="MM-CLS-001-8.9",
        title="Post-Intervention Verification and Work Order Capture",
        source="MAINT-MAN-CLS-001 sec. 8.9",
        revision="Rev J",
        body=(
            "Every intervention closes the same way: prove the intended physical state, write the "
            "resulting state back to the asset graph so the next mutation is evaluated against "
            "reality, and hand the asset to the accountable operator with an explicit next action."
        ),
        procedure=(
            ProcedureStep(
                StepActor.HUMAN_TECHNICIAN,
                "Confirm the achieved physical state of {target} and write the resulting status "
                "back to the asset graph vertex.",
                "Graph vertex status matches the observed physical state.",
                reversible=True,
                estimated_seconds=240,
            ),
            ProcedureStep(
                StepActor.HUMAN_SUPERVISOR,
                "Hand {target} to {operator} with the recorded evidence and the agreed next "
                "action.",
                "Handover acknowledged by the accountable operator.",
                reversible=True,
                estimated_seconds=180,
            ),
        ),
        tags=("verification", "closure", "handover", "work-order", "graph-writeback"),
        supplementary=True,
    ),
)


_TOKEN_RE = re.compile(r"[a-z0-9]+")
_EMBEDDING_DIM = 256


def _tokenize(text: str) -> list[str]:
    return _TOKEN_RE.findall(text.lower())


def _embed(text: str, dim: int = _EMBEDDING_DIM) -> tuple[float, ...]:
    """Deterministic signed hashing embedding.

    A real deployment swaps this for a sentence-transformer or a hosted
    embedding endpoint; the store's contract does not change when it does. What
    matters here is that retrieval is reproducible, offline and dependency-free.
    """
    vector = [0.0] * dim
    for token in _tokenize(text):
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
        value = int.from_bytes(digest, "big")
        bucket = value % dim
        sign = 1.0 if (value >> 63) & 1 else -1.0
        vector[bucket] += sign
    norm = math.sqrt(sum(component * component for component in vector))
    if norm == 0.0:
        return tuple(vector)
    return tuple(component / norm for component in vector)


def _cosine(left: Sequence[float], right: Sequence[float]) -> float:
    return sum(a * b for a, b in zip(left, right))


class MaintenanceManualVectorStore:
    """Mock vector store over the technical maintenance corpus.

    Retrieval is hybrid: cosine similarity over hashed embeddings, a lexical
    overlap term that keeps rare technical vocabulary meaningful at this corpus
    size, and hard metadata filters derived from the topology verdict. The
    filters are what makes this *GraphRAG* rather than plain RAG — the graph
    decides which slice of the manual is admissible before ranking begins.
    """

    def __init__(self, corpus: Sequence[ManualChunk] = MAINTENANCE_CORPUS) -> None:
        self._corpus = tuple(corpus)
        self._embeddings: dict[str, tuple[float, ...]] = {
            chunk.doc_id: _embed(chunk.indexable_text()) for chunk in self._corpus
        }
        self._token_sets: dict[str, frozenset[str]] = {
            chunk.doc_id: frozenset(_tokenize(chunk.indexable_text())) for chunk in self._corpus
        }

    def __len__(self) -> int:
        return len(self._corpus)

    def _score(
        self,
        chunk: ManualChunk,
        query_vector: Sequence[float],
        query_tokens: frozenset[str],
        *,
        model_family: str | None,
        action: ActionType | None,
        classification: FaultClassification | None,
    ) -> float:
        cosine = _cosine(query_vector, self._embeddings[chunk.doc_id])
        chunk_tokens = self._token_sets[chunk.doc_id]
        overlap = len(query_tokens & chunk_tokens) / max(1, len(query_tokens))
        score = 0.55 * max(0.0, cosine) + 0.45 * overlap

        if model_family and chunk.model_families:
            score += 0.25 if model_family in chunk.model_families else -0.20
        if action and chunk.applicable_actions:
            score += 0.30 if action in chunk.applicable_actions else -0.35
        if classification and chunk.classifications:
            score += 0.20 if classification in chunk.classifications else -0.15
        return round(score, 6)

    async def search(
        self,
        query: str,
        *,
        top_k: int = 4,
        model_family: str | None = None,
        action: ActionType | None = None,
        classification: FaultClassification | None = None,
        include_supplementary: bool = True,
        latency_ms: int = 0,
    ) -> list[tuple[ManualChunk, float]]:
        """Rank the corpus for one query. Async to mirror a networked store."""
        if latency_ms:
            await asyncio.sleep(latency_ms / 1000)

        query_vector = _embed(query)
        query_tokens = frozenset(_tokenize(query))

        ranked: list[tuple[ManualChunk, float]] = []
        for chunk in self._corpus:
            if chunk.supplementary and not include_supplementary:
                continue
            score = self._score(
                chunk,
                query_vector,
                query_tokens,
                model_family=model_family,
                action=action,
                classification=classification,
            )
            if score > 0:
                ranked.append((chunk, score))

        # Ties broken by doc_id so identical payloads retrieve identically.
        ranked.sort(key=lambda pair: (-pair[1], pair[0].doc_id))
        return ranked[:top_k]


# ===========================================================================
# 4a. Agent system frames and structured output contracts
# ===========================================================================

AGENT_ONE = "topology_isolator"
AGENT_TWO = "strategic_action_planner"

TOPOLOGY_TOOL_NAME = "emit_topology_verdict"
ACTION_TOOL_NAME = "emit_action_plan"

AGENT_ONE_SYSTEM_PROMPT = """\
You are the OpenOntology TOPOLOGY ISOLATOR.

Your sole purpose is attribution: given one localised graph neighbourhood and the
telemetry captured at the moment of the mutation, decide whether the failure is a
LOCALIZED_COMPONENT fault in the reporting asset or an INHERITED_CASCADE arriving
from an upstream dependency. You do not plan work, you do not command equipment,
and you do not recommend maintenance. You attribute the fault and you state how
far the topology can be trusted.

TRAVERSAL RULES (deterministic, apply in order):
1. `upstream_chain_nearest_first` is ordered nearest-first: index 0 feeds the
   reporting asset directly, index 1 feeds index 0, and so on. Walk it forwards.
2. A node is faulting when its reported status is DEGRADED, ALARM or OFFLINE, or
   when telemetry attributed to it breaches a CRITICAL or HARD limit.
3. If any upstream node is faulting, the classification is INHERITED_CASCADE and
   the root cause is the FURTHEST faulting node in the chain. Every node between
   that origin and the reporting asset is a symptom, never the cause.
4. If no upstream node is faulting and the reporting asset breaches a physical
   limit, the classification is LOCALIZED_COMPONENT and the root cause is the
   reporting asset.
5. If only context-integrity channels are breached, or the graph cannot be
   trusted, the classification is INDETERMINATE. Do not guess an origin.
6. If nothing is breached and the graph has converged, return NO_FAULT_DETECTED.

CRDT AND LAMPORT RULES (you must evaluate these explicitly):
- The asset vertex is an OR-Set element replicated with per-replica Lamport add
  and remove stamps. The join is the pointwise maximum of those stamps.
- The vertex is LIVE only when the highest add stamp is STRICTLY GREATER than the
  highest remove stamp. Equal stamps are a genuine tie, not liveness.
- Break a tie from the topology, not from wall-clock time:
  * leaf vertex, nothing live downstream -> REMOVED_WINS (the replication core's
    default; a later add always resurrects the vertex);
  * live downstream dependents exist -> ADD_WINS, because tombstoning a vertex
    that still feeds live consumers erases a physically present asset from the
    operator's graph. Record the override and lower your confidence.
- Treat these as trust-destroying and reflect them in `graph_trust`: a contested
  tie, replicas that disagree with the joined verdict, a non-zero
  `crdt_collision_rate`, and a vertex that resolves TOMBSTONED while still
  emitting telemetry.
- `irreversible_permitted` is false whenever the graph cannot be trusted. An
  irreversible field action on an unresolved vertex is never acceptable.

GROUNDING:
You are given `derived_analysis`, the deterministic kernel's own traversal. Treat
it as the default answer. You may depart from it, but only by citing a specific
fact in the briefing that it mis-weighted, and you must say so in
`root_cause_rationale`. Every node you name must appear in `permitted_nodes`.
Never invent a node, a channel or a replica.

Return your verdict exclusively through the emit_topology_verdict tool.
"""

AGENT_TWO_SYSTEM_PROMPT = """\
You are the OpenOntology STRATEGIC ACTION PLANNER.

You receive a completed attribution verdict from the Topology Isolator and a set
of retrieved technical maintenance manual guidelines. Your purpose is to convert
that verdict into one physical command and the exact ordered step sequence that
executes it without widening the blast radius. You do not re-attribute the fault:
the verdict's `root_cause_node` is where intervention lands, and if the fault was
inherited, that node is upstream of the asset that reported it.

COMMAND SELECTION:
- ISOLATE_VALVE: the fault is attributed and containable at a single node, and
  isolating it stops the disturbance.
- EMERGENCY_SHUTDOWN: a hard limit is breached, or a critical breach threatens a
  wide blast radius, and continued operation is unsafe. Irreversible.
- DEGRADE_THROTTLE: the safe reversible fallback. Use it whenever the verdict is
  INDETERMINATE, `irreversible_permitted` is false, or throttling genuinely
  resolves the condition.
You may only choose EMERGENCY_SHUTDOWN when the verdict permits irreversible
action AND the execution priority is CRITICAL.

STEP SEQUENCING (blast-radius discipline):
1. Protect downstream consumers BEFORE the supply is interrupted. Consumers cut
   out from under load trip on their own protection and turn one isolation into a
   unit-wide outage.
2. Stabilise before you isolate: reduce speed, load or throughput first.
3. Isolate at the attributed root cause, not at the node that noticed.
4. Prove the isolation physically — position feedback, lockout, bled pressure —
   rather than trusting a command acknowledgement.
5. Close by writing the achieved state back to the asset graph and handing over
   to the accountable operator.
Every step names its actor: HUMAN_TECHNICIAN, HUMAN_SUPERVISOR, ROBOTIC_ACTUATOR
or CONTROL_SYSTEM. Mark a step irreversible only when it truly is, and cite the
manual reference the step came from.

CONSTRAINTS:
- Ground every step in the retrieved guidelines. Do not invent a procedure that
  the corpus does not support.
- `target_asset_id` and every `target_component` must appear in
  `permitted_nodes`.
- Sequence numbers start at 1 and are contiguous.
- Require human authorisation for any irreversible action, any CRITICAL priority,
  and any plan built on a graph the isolator did not fully trust.
- State confidence honestly; a degraded graph or a thin retrieval must lower it.

Return your plan exclusively through the emit_action_plan tool.
"""


TOPOLOGY_VERDICT_TOOL: dict[str, Any] = {
    "name": TOPOLOGY_TOOL_NAME,
    "description": (
        "Emit the topology isolation verdict for one ontology mutation: where the fault "
        "originates and how far the replicated graph can be trusted. Call this exactly once."
    ),
    "strict": True,
    "input_schema": {
        "type": "object",
        "properties": {
            "fault_classification": {
                "type": "string",
                "enum": [item.value for item in FaultClassification],
            },
            "root_cause_node": {
                "type": "string",
                "description": "Node requiring attribution; must appear in permitted_nodes.",
            },
            "root_cause_rationale": {
                "type": "string",
                "description": "Two to five sentences citing the traversal and the observed values.",
            },
            "cascade_path": {
                "type": "array",
                "description": "Origin-first path from the cascade origin to the reporting asset; empty when localized.",
                "items": {"type": "string"},
            },
            "localized_components": {
                "type": "array",
                "description": "Channels or components implicated on the reporting asset.",
                "items": {"type": "string"},
            },
            "blast_radius": {
                "type": "array",
                "description": "Downstream nodes exposed if the fault propagates.",
                "items": {"type": "string"},
            },
            "severity_band": {"type": "string", "enum": [item.value for item in Band]},
            "crdt_assessment": {
                "type": "object",
                "properties": {
                    "presence": {"type": "string", "enum": [item.value for item in CRDTPresence]},
                    "tie_breaker_rule": {"type": "string", "enum": [item.value for item in TieBreaker]},
                    "contested": {"type": "boolean"},
                    "highest_add_stamp": {"type": "integer"},
                    "highest_remove_stamp": {"type": "integer"},
                    "diverging_replicas": {"type": "array", "items": {"type": "string"}},
                    "collision_rate": {"type": "number"},
                    "graph_trust": {"type": "number", "description": "0.0 to 1.0."},
                    "explanation": {"type": "string"},
                },
                "required": [
                    "presence",
                    "tie_breaker_rule",
                    "contested",
                    "highest_add_stamp",
                    "highest_remove_stamp",
                    "diverging_replicas",
                    "collision_rate",
                    "graph_trust",
                    "explanation",
                ],
                "additionalProperties": False,
            },
            "containment_hint": {"type": "string", "enum": [item.value for item in ActionType]},
            "irreversible_permitted": {"type": "boolean"},
            "confidence": {"type": "number", "description": "0.0 to 1.0."},
            "evidence": {"type": "array", "items": {"type": "string"}},
        },
        "required": [
            "fault_classification",
            "root_cause_node",
            "root_cause_rationale",
            "cascade_path",
            "localized_components",
            "blast_radius",
            "severity_band",
            "crdt_assessment",
            "containment_hint",
            "irreversible_permitted",
            "confidence",
            "evidence",
        ],
        "additionalProperties": False,
    },
}

ACTION_PLAN_TOOL: dict[str, Any] = {
    "name": ACTION_TOOL_NAME,
    "description": (
        "Emit the physical isolation command and its ordered step sequence. Call this exactly once."
    ),
    "strict": True,
    "input_schema": {
        "type": "object",
        "properties": {
            "action_type": {"type": "string", "enum": [item.value for item in ActionType]},
            "target_asset_id": {
                "type": "string",
                "description": "Node requiring physical intervention; must appear in permitted_nodes.",
            },
            "execution_priority": {
                "type": "string",
                "enum": [item.value for item in ExecutionPriority],
            },
            "isolation_steps": {
                "type": "array",
                "description": "Ordered instructions, sequence starting at 1.",
                "items": {
                    "type": "object",
                    "properties": {
                        "sequence": {"type": "integer", "description": "1-based execution order."},
                        "actor": {"type": "string", "enum": [item.value for item in StepActor]},
                        "instruction": {"type": "string"},
                        "target_component": {"type": "string"},
                        "verification": {
                            "type": "string",
                            "description": "The observable proof that the step completed.",
                        },
                        "reversible": {"type": "boolean"},
                        "estimated_seconds": {"type": "integer"},
                        "manual_reference": {
                            "type": ["string", "null"],
                            "description": "Source citation, or null when the step is generic.",
                        },
                    },
                    "required": [
                        "sequence",
                        "actor",
                        "instruction",
                        "target_component",
                        "verification",
                        "reversible",
                        "estimated_seconds",
                        "manual_reference",
                    ],
                    "additionalProperties": False,
                },
            },
            "blast_radius_protection": {
                "type": "array",
                "description": "Explicit measures protecting the downstream nodes.",
                "items": {"type": "string"},
            },
            "requires_human_authorization": {"type": "boolean"},
            "rationale": {"type": "string"},
            "manual_references": {
                "type": "array",
                "description": "doc_id values of the guidelines this plan relies on.",
                "items": {"type": "string"},
            },
            "confidence": {"type": "number", "description": "0.0 to 1.0."},
        },
        "required": [
            "action_type",
            "target_asset_id",
            "execution_priority",
            "isolation_steps",
            "blast_radius_protection",
            "requires_human_authorization",
            "rationale",
            "manual_references",
            "confidence",
        ],
        "additionalProperties": False,
    },
}


# ===========================================================================
# 4b. Agent transport
# ===========================================================================


class AgentExecutionError(RuntimeError):
    """Raised when an agent cannot be made to produce a usable structured answer."""


@dataclass(frozen=True)
class LLMResult:
    payload: dict[str, Any]
    input_tokens: int = 0
    output_tokens: int = 0


class StructuredLLMClient(Protocol):
    """Transport contract shared by the live and deterministic backends."""

    provider: str
    model: str

    async def emit(
        self,
        *,
        agent: str,
        system: str,
        briefing: Mapping[str, Any],
        tool: Mapping[str, Any],
    ) -> LLMResult:
        """Run one agent turn and return the tool input it produced."""
        ...


class CloudToolClient:
    """Live backend: one forced tool call per agent turn.

    This class is the service's entire vendor surface. Everything else — the
    kernel, the retrieval layer, the agent loop — speaks
    :class:`StructuredLLMClient` and never learns which provider answered, so
    supporting another one means writing a sibling of this class and nothing
    else. The SDK is imported lazily for the same reason it is confined here:
    the module stays importable, and the deterministic backend stays usable,
    on a host with no SDK installed and no network egress.
    """

    provider = "cloud"

    def __init__(self, *, api_key: str, model: str, max_tokens: int) -> None:
        try:
            from anthropic import AsyncAnthropic  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover - depends on host install
            raise AgentExecutionError(
                "OO_GRAPHRAG_AGENT_PROVIDER=cloud requires the provider SDK listed in "
                "requirements.txt; install it or fall back to the deterministic provider"
            ) from exc
        self._client = AsyncAnthropic(api_key=api_key)
        self.model = model
        self._max_tokens = max_tokens

    async def emit(
        self,
        *,
        agent: str,
        system: str,
        briefing: Mapping[str, Any],
        tool: Mapping[str, Any],
    ) -> LLMResult:
        tool_name = str(tool["name"])
        response = await self._client.messages.create(
            model=self.model,
            max_tokens=self._max_tokens,
            system=system,
            messages=[
                {
                    "role": "user",
                    "content": json.dumps(briefing, default=str, separators=(",", ":")),
                }
            ],
            tools=[dict(tool)],
            tool_choice={"type": "tool", "name": tool_name},
        )

        for block in response.content:
            if getattr(block, "type", None) == "tool_use" and getattr(block, "name", None) == tool_name:
                payload = getattr(block, "input", None)
                if not isinstance(payload, dict):
                    raise AgentExecutionError(f"agent {agent} returned a non-object tool input")
                usage = getattr(response, "usage", None)
                return LLMResult(
                    payload=payload,
                    input_tokens=int(getattr(usage, "input_tokens", 0) or 0),
                    output_tokens=int(getattr(usage, "output_tokens", 0) or 0),
                )

        stop_reason = getattr(response, "stop_reason", "unknown")
        raise AgentExecutionError(
            f"agent {agent} did not call {tool_name} (stop_reason={stop_reason})"
        )


class _TemplateContext(dict):
    """Format mapping that degrades to a safe phrase instead of raising."""

    def __missing__(self, key: str) -> str:  # pragma: no cover - defensive
        logger.warning("manual template referenced an unknown placeholder", extra={"placeholder": key})
        return "the affected component"


def _render(template: str, context: Mapping[str, Any]) -> str:
    return " ".join(template.format_map(_TemplateContext(context)).split())


def _summarise(nodes: Sequence[str], *, limit: int = 4, empty: str = "no downstream consumers") -> str:
    if not nodes:
        return empty
    head = list(nodes[:limit])
    if len(nodes) > limit:
        return ", ".join(head) + f" and {len(nodes) - limit} more"
    return ", ".join(head)


def _estimate_tokens(payload: Any) -> int:
    """Rough token accounting for the offline backend (~4 chars per token)."""
    return max(1, len(json.dumps(payload, default=str)) // 4)


class DeterministicReasoningClient:
    """Offline backend that executes the same two agent frames as a rule engine.

    This is not a stub: it consumes exactly the briefing a live model receives —
    the kernel's traversal, the CRDT resolution, the retrieved manual chunks —
    and composes its answer from them. That makes the whole pipeline runnable,
    testable and reproducible with no network egress, and it doubles as the
    reference behaviour a live model is compared against in review.
    """

    provider = "deterministic"

    def __init__(self, *, model: str = DETERMINISTIC_MODEL_ID, simulated_latency_ms: int = 0) -> None:
        self.model = model
        self._latency_ms = simulated_latency_ms

    async def emit(
        self,
        *,
        agent: str,
        system: str,
        briefing: Mapping[str, Any],
        tool: Mapping[str, Any],
    ) -> LLMResult:
        if self._latency_ms:
            await asyncio.sleep(self._latency_ms / 1000)

        tool_name = str(tool["name"])
        if tool_name == TOPOLOGY_TOOL_NAME:
            payload = self._topology_verdict(briefing)
        elif tool_name == ACTION_TOOL_NAME:
            payload = self._action_plan(briefing)
        else:  # pragma: no cover - guarded by the engine's own tool table
            raise AgentExecutionError(f"deterministic backend has no handler for tool {tool_name!r}")

        return LLMResult(
            payload=payload,
            input_tokens=_estimate_tokens(briefing) + _estimate_tokens(system),
            output_tokens=_estimate_tokens(payload),
        )

    # -- Agent 1 ----------------------------------------------------------

    @staticmethod
    def _topology_verdict(briefing: Mapping[str, Any]) -> dict[str, Any]:
        analysis: Mapping[str, Any] = briefing["derived_analysis"]
        classification = FaultClassification(analysis["derived_classification"])
        cascade = list(analysis["cascade_path_origin_first"])
        crdt: Mapping[str, Any] = analysis["crdt_assessment"]
        asset_id = str(analysis["asset_id"])
        root = str(analysis["derived_root_cause_node"])

        physical = [item for item in analysis["physical_exceedances"] if item["band"] in {"WARN", "CRITICAL", "HARD"}]
        breach_summary = (
            "; ".join(
                f"{item['channel']} at {item['value']:g}{item['unit'] or ''} ({item['band']})"
                for item in physical[:4]
            )
            or "no physical limit breached"
        )

        if classification is FaultClassification.INHERITED_CASCADE:
            rationale = (
                f"Walking the upstream chain nearest-first, "
                f"{_summarise(list(analysis['faulted_upstream_nodes']), empty='no node')} is faulting; "
                f"the furthest faulting node {root} is therefore the origin and every node between it "
                f"and {asset_id} is symptomatic. The reporting asset shows {breach_summary}, which is "
                f"consistent with an inherited disturbance rather than an independent failure. "
                f"Intervention belongs at {root}, not at {asset_id}."
            )
        elif classification is FaultClassification.LOCALIZED_COMPONENT:
            rationale = (
                f"No node in the upstream chain "
                f"({_summarise(list(analysis['upstream_chain_nearest_first']), empty='none declared')}) "
                f"reports a fault or breaches an attributed limit, so the disturbance did not arrive "
                f"from upstream. {asset_id} itself shows {breach_summary}, which contains the fault to "
                f"the reporting asset and its components "
                f"({_summarise(list(analysis['localized_components']), empty='unspecified')})."
            )
        elif classification is FaultClassification.INDETERMINATE:
            rationale = (
                f"No physical limit is breached on {asset_id} and no upstream node is faulting, but the "
                f"replicated topology cannot be trusted at {crdt['graph_trust']:g}: {crdt['explanation']} "
                f"Attribution is withheld rather than guessed, and no irreversible action is permitted "
                f"on this vertex until the graph reconciles."
            )
        else:
            rationale = (
                f"All channels on {asset_id} are within their warn limits and the vertex has converged "
                f"({crdt['explanation']}). The mutation is recorded but no fault is attributable."
            )

        return {
            "fault_classification": classification.value,
            "root_cause_node": root,
            "root_cause_rationale": rationale,
            "cascade_path": cascade,
            "localized_components": list(analysis["localized_components"]),
            "blast_radius": list(analysis["blast_radius"]),
            "severity_band": analysis["severity_band"],
            "crdt_assessment": dict(crdt),
            "containment_hint": analysis["derived_action"],
            "irreversible_permitted": bool(analysis["irreversible_permitted"]),
            "confidence": float(analysis["derived_confidence"]),
            "evidence": list(analysis["evidence"])[:24],
        }

    # -- Agent 2 ----------------------------------------------------------

    @staticmethod
    def _action_plan(briefing: Mapping[str, Any]) -> dict[str, Any]:
        verdict: Mapping[str, Any] = briefing["verdict"]
        topology: Mapping[str, Any] = briefing["topology"]
        guidelines: Sequence[Mapping[str, Any]] = briefing.get("retrieved_guidelines", ())
        constraints: Mapping[str, Any] = briefing.get("constraints", {})

        action = ActionType(verdict["containment_hint"])
        classification = FaultClassification(verdict["fault_classification"])
        priority = ExecutionPriority(topology["derived_priority"])
        crdt: Mapping[str, Any] = verdict["crdt_assessment"]
        graph_trust = float(crdt["graph_trust"])
        max_steps = int(constraints.get("max_isolation_steps", 12))

        target = str(verdict["root_cause_node"])
        asset_id = str(topology["asset_id"])
        downstream = list(verdict.get("blast_radius") or topology["blast_radius"])
        cascade = list(verdict.get("cascade_path") or ())

        context = {
            "target": target,
            "target_name": topology["asset_name"] if target == asset_id else target,
            "asset": asset_id,
            "asset_name": topology["asset_name"],
            "parent": topology["parent_system"],
            "location": topology["physical_location"],
            "operator": briefing.get("assigned_operator", "the accountable operator"),
            "downstream_summary": _summarise(downstream),
            "downstream_count": len(downstream),
            "cascade_summary": " -> ".join(cascade) if cascade else f"{target} -> {asset_id}",
            "model_number": briefing.get("model_number", "the installed model"),
        }

        def steps_from(chunk: Mapping[str, Any]) -> list[dict[str, Any]]:
            rendered: list[dict[str, Any]] = []
            for step in chunk["procedure"]:
                rendered.append(
                    {
                        "sequence": 0,  # renumbered once the sequence is assembled
                        "actor": step["actor"],
                        "instruction": _render(step["instruction"], context),
                        "target_component": target,
                        "verification": _render(step["verification"], context),
                        "reversible": bool(step["reversible"]),
                        "estimated_seconds": int(step["estimated_seconds"]),
                        "manual_reference": f"{chunk['source']} ({chunk['doc_id']})",
                    }
                )
            return rendered

        def find(predicate) -> Mapping[str, Any] | None:
            for chunk in guidelines:
                if predicate(chunk):
                    return chunk
            return None

        lead = find(
            lambda chunk: not chunk["supplementary"]
            and (not chunk["applicable_actions"] or action.value in chunk["applicable_actions"])
        )
        blast_chunk = find(lambda chunk: chunk["supplementary"] and "downstream" in chunk["tags"])
        crdt_chunk = find(lambda chunk: chunk["supplementary"] and "crdt" in chunk["tags"])
        loto_chunk = find(lambda chunk: chunk["supplementary"] and "loto" in chunk["tags"])
        closure_chunk = find(lambda chunk: chunk["supplementary"] and "closure" in chunk["tags"])

        assembled: list[dict[str, Any]] = []
        cited: list[str] = []

        # 1. Reconcile the graph before anything irreversible is contemplated.
        if crdt_chunk and (crdt["contested"] or crdt["diverging_replicas"] or graph_trust < 0.75):
            assembled.extend(steps_from(crdt_chunk))
            cited.append(str(crdt_chunk["doc_id"]))

        # 2. Protect the blast radius before the supply is interrupted.
        if blast_chunk and downstream:
            assembled.extend(steps_from(blast_chunk))
            cited.append(str(blast_chunk["doc_id"]))

        # 3. The action itself, from the highest-ranked applicable procedure.
        if lead:
            assembled.extend(steps_from(lead))
            cited.append(str(lead["doc_id"]))
        else:
            assembled.append(
                {
                    "sequence": 0,
                    "actor": StepActor.CONTROL_SYSTEM.value,
                    "instruction": _render(
                        "Hold {target_name} ({target}) at its documented safe-state setpoint; no "
                        "retrieved procedure covers this combination of fault and asset family.",
                        context,
                    ),
                    "verification": "Setpoint reads safe-state and is latched against recovery.",
                    "target_component": target,
                    "reversible": True,
                    "estimated_seconds": 120,
                    "manual_reference": None,
                }
            )

        # 4. Prove the isolation mechanically.
        if loto_chunk and action in {ActionType.ISOLATE_VALVE, ActionType.EMERGENCY_SHUTDOWN}:
            assembled.extend(steps_from(loto_chunk))
            cited.append(str(loto_chunk["doc_id"]))

        # 5. Close out: write the achieved state back to the graph, hand over.
        closure: list[dict[str, Any]] = []
        if closure_chunk:
            closure = steps_from(closure_chunk)
            cited.append(str(closure_chunk["doc_id"]))

        # Truncate the middle rather than the closure: the write-back is what
        # keeps the next mutation grounded in reality.
        budget = max(1, max_steps - len(closure))
        assembled = assembled[:budget] + closure
        for index, step in enumerate(assembled, start=1):
            step["sequence"] = index

        protection = [
            f"{node} transferred to standby feed or reduced load before the isolation lands"
            for node in downstream[:6]
        ]
        if len(downstream) > 6:
            protection.append(f"{len(downstream) - 6} further downstream node(s) covered by the same transfer")
        if not downstream:
            protection.append("No downstream consumers are exposed by this action")
        if cascade:
            protection.append(
                f"Cascade path {' -> '.join(cascade)} re-walked after isolation to catch independent faults"
            )

        requires_authorization = (
            action.is_irreversible
            or priority is ExecutionPriority.CRITICAL
            or graph_trust < 0.75
            or any(not step["reversible"] for step in assembled)
        )

        if classification is FaultClassification.INHERITED_CASCADE:
            rationale = (
                f"The isolator attributed this to {target} upstream of the reporting asset {asset_id}, so "
                f"{action.value} lands on the origin; isolating {asset_id} would remove the symptom and "
                f"leave the source live. {len(downstream)} downstream node(s) are protected before the "
                f"supply is interrupted."
            )
        elif classification is FaultClassification.INDETERMINATE:
            rationale = (
                f"Attribution is incomplete and graph trust is {graph_trust:g}, so the plan is limited to "
                f"the reversible {action.value} on {target} plus a forced reconciliation. No irreversible "
                f"field action is authorised until the vertex resolves."
            )
        else:
            rationale = (
                f"The fault is contained to {target}, so {action.value} is applied locally at "
                f"{context['location']} after stabilising the asset and protecting "
                f"{len(downstream)} downstream node(s)."
            )
        if lead:
            rationale += f" Procedure grounded in {lead['source']}."

        confidence = float(verdict["confidence"]) * (1.0 if lead else 0.7)
        if graph_trust < 0.75:
            confidence *= 0.9

        return {
            "action_type": action.value,
            "target_asset_id": target,
            "execution_priority": priority.value,
            "isolation_steps": assembled,
            "blast_radius_protection": protection,
            "requires_human_authorization": requires_authorization,
            "rationale": rationale,
            "manual_references": list(dict.fromkeys(cited)),
            "confidence": round(max(0.05, min(1.0, confidence)), 3),
        }


def build_agent_client(settings: InterceptorSettings) -> StructuredLLMClient:
    """Select the transport. Falls back loudly, never silently."""
    if settings.agent_provider == "cloud":
        if not settings.agent_api_key:
            raise AgentExecutionError(
                "OO_GRAPHRAG_AGENT_PROVIDER=cloud requires OO_GRAPHRAG_AGENT_API_KEY"
            )
        return CloudToolClient(
            api_key=settings.agent_api_key,
            model=settings.agent_model,
            max_tokens=settings.agent_max_tokens,
        )
    return DeterministicReasoningClient(simulated_latency_ms=settings.simulated_latency_ms)


# ===========================================================================
# 5. Multi-agent execution engine (the isolation loop)
# ===========================================================================


@dataclass(frozen=True)
class EngineOutcome:
    """Everything the loop produced, ready to be shaped into a response."""

    analysis: TopologyAnalysis
    verdict: TopologyIsolationVerdict
    plan: StrategicActionPlan
    guidelines: tuple[tuple[ManualChunk, float], ...]
    guardrails: tuple[str, ...]
    trace: tuple[AgentTraceEntry, ...]
    latency_ms: float


def _serialise_chunk(chunk: ManualChunk, score: float) -> dict[str, Any]:
    """Chunk representation handed to Agent 2 — body *and* procedure template."""
    return {
        "doc_id": chunk.doc_id,
        "title": chunk.title,
        "source": chunk.source,
        "revision": chunk.revision,
        "score": round(score, 4),
        "body": chunk.body,
        "tags": list(chunk.tags),
        "model_families": list(chunk.model_families),
        "applicable_actions": [action.value for action in chunk.applicable_actions],
        "classifications": [item.value for item in chunk.classifications],
        "supplementary": chunk.supplementary,
        "procedure": [
            {
                "actor": step.actor.value,
                "instruction": step.instruction,
                "verification": step.verification,
                "reversible": step.reversible,
                "estimated_seconds": step.estimated_seconds,
            }
            for step in chunk.procedure
        ],
    }


class MultiAgentGraphEngine:
    """Two-agent isolation loop over a GraphRAG-retrieved maintenance corpus.

    Agent 1 attributes the fault against the graph; retrieval is then filtered by
    *that verdict* before Agent 2 plans the physical command. Running retrieval
    after attribution is the point of the GraphRAG pattern here: the manual slice
    a cascade fault needs is not the slice a localised bearing failure needs, and
    filtering on the verdict keeps irrelevant procedures out of the planner's
    context entirely.
    """

    def __init__(
        self,
        client: StructuredLLMClient,
        vector_store: MaintenanceManualVectorStore,
        settings: InterceptorSettings,
    ) -> None:
        self._client = client
        self._store = vector_store
        self._settings = settings

    # -- public API --------------------------------------------------------

    async def run(self, payload: EnrichedGraphPayload) -> EngineOutcome:
        started = time.perf_counter()
        trace: list[AgentTraceEntry] = []

        analysis = analyse_topology(payload, self._settings)
        logger.info(
            "topology kernel resolved",
            extra={
                "event_id": payload.event_id,
                "asset_id": analysis.asset_id,
                "classification": analysis.classification.value,
                "root_cause_node": analysis.root_cause_node,
                "severity_band": analysis.severity_band.value,
                "graph_trust": analysis.crdt.graph_trust,
                "crdt_presence": analysis.crdt.presence.value,
                "tie_breaker": analysis.crdt.tie_breaker_rule.value,
                "blast_radius": len(analysis.blast_radius),
            },
        )

        verdict = await self._run_topology_isolator(payload, analysis, trace)
        guidelines = await self._retrieve(payload, verdict)
        plan = await self._run_action_planner(payload, analysis, verdict, guidelines, trace)
        plan, guardrails = self._enforce_safety_invariants(plan, verdict, analysis)

        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        return EngineOutcome(
            analysis=analysis,
            verdict=verdict,
            plan=plan,
            guidelines=tuple(guidelines),
            guardrails=tuple(guardrails),
            trace=tuple(trace),
            latency_ms=latency_ms,
        )

    # -- Agent 1 -----------------------------------------------------------

    async def _run_topology_isolator(
        self,
        payload: EnrichedGraphPayload,
        analysis: TopologyAnalysis,
        trace: list[AgentTraceEntry],
    ) -> TopologyIsolationVerdict:
        context = payload.ontology_context
        briefing: dict[str, Any] = {
            "task": "Attribute the fault and assess graph trust. Do not plan work.",
            "event": {
                "event_id": payload.event_id,
                "timestamp": payload.timestamp.isoformat(),
                "origin_replica": payload.origin_replica,
                "lamport_clock": payload.lamport_clock,
                "engine_reported_degraded": payload.degraded,
            },
            "asset_metadata": payload.asset_metadata.model_dump(mode="json"),
            "ontology_context": context.model_dump(mode="json"),
            "telemetry_snapshot": {
                "local_channels": payload.telemetry_snapshot.local_channels(),
                "attributed_channels": payload.telemetry_snapshot.attributed_channels(),
            },
            "channel_limits": {
                name: {
                    "unit": limit.unit,
                    "warn": limit.warn,
                    "critical": limit.critical,
                    "hard": limit.hard,
                    "physical": limit.physical,
                }
                for name, limit in CHANNEL_LIMITS.items()
            },
            "derived_analysis": analysis.to_briefing(),
            "permitted_nodes": sorted(analysis.known_nodes),
        }

        verdict = await self._invoke(
            agent=AGENT_ONE,
            system=AGENT_ONE_SYSTEM_PROMPT,
            briefing=briefing,
            tool=TOPOLOGY_VERDICT_TOOL,
            model=TopologyIsolationVerdict,
            trace=trace,
            validate=lambda result: self._validate_verdict(result, analysis),
        )

        logger.info(
            "topology isolator verdict",
            extra={
                "event_id": payload.event_id,
                "classification": verdict.fault_classification.value,
                "root_cause_node": verdict.root_cause_node,
                "cascade_depth": len(verdict.cascade_path),
                "confidence": verdict.confidence,
                "irreversible_permitted": verdict.irreversible_permitted,
                "agreed_with_kernel": verdict.fault_classification is analysis.classification
                and verdict.root_cause_node == analysis.root_cause_node,
            },
        )
        return verdict

    @staticmethod
    def _validate_verdict(verdict: TopologyIsolationVerdict, analysis: TopologyAnalysis) -> None:
        """Reject a verdict that leaves the graph it was given."""
        cited = {verdict.root_cause_node, *verdict.cascade_path, *verdict.blast_radius}
        unknown = sorted(cited - analysis.known_nodes)
        if unknown:
            raise AgentExecutionError(
                f"topology isolator named nodes outside the localised graph: {unknown}"
            )
        if verdict.fault_classification is FaultClassification.INHERITED_CASCADE and not verdict.cascade_path:
            raise AgentExecutionError("an inherited cascade verdict must carry a cascade path")

    # -- GraphRAG retrieval ------------------------------------------------

    async def _retrieve(
        self, payload: EnrichedGraphPayload, verdict: TopologyIsolationVerdict
    ) -> list[tuple[ManualChunk, float]]:
        """Two filtered queries, run concurrently, merged by score.

        The primary query asks for the procedure that fits the verdict; the
        secondary asks for the blast-radius and reconciliation material that any
        plan touching live downstream consumers or a contested vertex needs.
        """
        asset = payload.asset_metadata
        breached = ", ".join(
            item.channel for item in analyse_channels_for_query(payload)
        ) or "no breached channel"

        primary_query = (
            f"{verdict.fault_classification.value} on {asset.name} model {asset.model_number} "
            f"at {payload.ontology_context.physical_location}; breached channels {breached}; "
            f"severity {verdict.severity_band.value}; containment {verdict.containment_hint.value}"
        )
        secondary_terms = ["downstream consumers blast radius standby feed protection closure handover"]
        if verdict.crdt_assessment.contested or verdict.crdt_assessment.diverging_replicas:
            secondary_terms.append("crdt lamport reconciliation replica tie-breaker convergence")
        if verdict.containment_hint in {ActionType.ISOLATE_VALVE, ActionType.EMERGENCY_SHUTDOWN}:
            secondary_terms.append("lockout tagout valve actuator stored energy")
        secondary_query = " ".join(secondary_terms)

        primary, secondary = await asyncio.gather(
            self._store.search(
                primary_query,
                top_k=self._settings.retrieval_top_k,
                model_family=asset.model_family,
                action=verdict.containment_hint,
                classification=verdict.fault_classification,
                include_supplementary=False,
            ),
            self._store.search(
                secondary_query,
                top_k=self._settings.retrieval_top_k,
                include_supplementary=True,
            ),
        )

        merged: dict[str, tuple[ManualChunk, float]] = {}
        for chunk, score in (*primary, *secondary):
            existing = merged.get(chunk.doc_id)
            if existing is None or score > existing[1]:
                merged[chunk.doc_id] = (chunk, score)

        # Lead procedures first, then supplementary material, each by score.
        ranked = sorted(
            merged.values(),
            key=lambda pair: (pair[0].supplementary, -pair[1], pair[0].doc_id),
        )
        logger.info(
            "graphrag retrieval complete",
            extra={
                "event_id": payload.event_id,
                "retrieved": [chunk.doc_id for chunk, _ in ranked],
                "top_score": round(ranked[0][1], 4) if ranked else 0.0,
                "model_family": asset.model_family,
            },
        )
        return ranked

    # -- Agent 2 -----------------------------------------------------------

    async def _run_action_planner(
        self,
        payload: EnrichedGraphPayload,
        analysis: TopologyAnalysis,
        verdict: TopologyIsolationVerdict,
        guidelines: Sequence[tuple[ManualChunk, float]],
        trace: list[AgentTraceEntry],
    ) -> StrategicActionPlan:
        briefing: dict[str, Any] = {
            "task": "Convert the isolation verdict into one physical command and its step sequence.",
            "event_id": payload.event_id,
            "verdict": verdict.model_dump(mode="json"),
            "topology": analysis.to_briefing(),
            "assigned_operator": payload.ontology_context.assigned_operator,
            "model_number": payload.asset_metadata.model_number,
            "maintenance_window": payload.ontology_context.maintenance_window,
            "retrieved_guidelines": [_serialise_chunk(chunk, score) for chunk, score in guidelines],
            "constraints": {
                "max_isolation_steps": self._settings.max_isolation_steps,
                "permitted_actions": [action.value for action in ActionType],
                "irreversible_permitted": verdict.irreversible_permitted,
                "min_graph_trust_for_irreversible": self._settings.min_graph_trust_for_irreversible,
            },
            "permitted_nodes": sorted(analysis.known_nodes),
        }

        plan = await self._invoke(
            agent=AGENT_TWO,
            system=AGENT_TWO_SYSTEM_PROMPT,
            briefing=briefing,
            tool=ACTION_PLAN_TOOL,
            model=StrategicActionPlan,
            trace=trace,
            validate=lambda result: self._validate_plan(result, analysis),
        )

        logger.info(
            "strategic action plan drafted",
            extra={
                "event_id": payload.event_id,
                "action_type": plan.action_type.value,
                "target_asset_id": plan.target_asset_id,
                "priority": plan.execution_priority.value,
                "steps": len(plan.isolation_steps),
                "confidence": plan.confidence,
            },
        )
        return plan

    @staticmethod
    def _validate_plan(plan: StrategicActionPlan, analysis: TopologyAnalysis) -> None:
        cited = {plan.target_asset_id, *(step.target_component for step in plan.isolation_steps)}
        unknown = sorted(cited - analysis.known_nodes)
        if unknown:
            raise AgentExecutionError(f"action planner targeted nodes outside the graph: {unknown}")

    # -- shared invocation -------------------------------------------------

    async def _invoke(
        self,
        *,
        agent: str,
        system: str,
        briefing: Mapping[str, Any],
        tool: Mapping[str, Any],
        model: type[BaseModel],
        trace: list[AgentTraceEntry],
        validate,
    ) -> Any:
        """One agent turn: timeout, bounded retries, schema and graph validation.

        A malformed or out-of-graph answer is retried with backoff rather than
        accepted, because a plan that names a node the operator cannot find is
        worse than no plan at all.
        """
        settings = self._settings
        started = time.perf_counter()
        last_error: Exception | None = None

        for attempt in range(1, settings.agent_max_attempts + 1):
            try:
                result = await asyncio.wait_for(
                    self._client.emit(agent=agent, system=system, briefing=briefing, tool=tool),
                    timeout=settings.agent_timeout_seconds,
                )
                parsed = model.model_validate(result.payload)
                validate(parsed)
            except asyncio.TimeoutError as exc:
                last_error = AgentExecutionError(
                    f"agent {agent} timed out after {settings.agent_timeout_seconds:g}s"
                )
                logger.warning("agent turn timed out", extra={"agent": agent, "attempt": attempt})
                _ = exc
            except ValidationError as exc:
                last_error = AgentExecutionError(
                    f"agent {agent} emitted a structurally invalid answer: "
                    f"{exc.errors()[:3]}"
                )
                logger.warning(
                    "agent turn failed schema validation",
                    extra={"agent": agent, "attempt": attempt, "errors": exc.errors()[:3]},
                )
            except AgentExecutionError as exc:
                last_error = exc
                logger.warning(
                    "agent turn rejected", extra={"agent": agent, "attempt": attempt, "detail": str(exc)}
                )
            except Exception as exc:  # transport-level failure
                last_error = AgentExecutionError(f"agent {agent} transport failure: {exc}")
                logger.warning(
                    "agent transport failure",
                    extra={"agent": agent, "attempt": attempt, "detail": str(exc)},
                )
            else:
                trace.append(
                    AgentTraceEntry(
                        agent=agent,
                        model=self._client.model,
                        provider=self._client.provider,
                        latency_ms=round((time.perf_counter() - started) * 1000, 2),
                        attempts=attempt,
                        input_tokens=result.input_tokens,
                        output_tokens=result.output_tokens,
                    )
                )
                return parsed

            if attempt < settings.agent_max_attempts and settings.agent_retry_backoff_seconds:
                await asyncio.sleep(settings.agent_retry_backoff_seconds * attempt)

        assert last_error is not None  # the loop cannot exit without one
        raise last_error

    # -- guardrails --------------------------------------------------------

    def _enforce_safety_invariants(
        self,
        plan: StrategicActionPlan,
        verdict: TopologyIsolationVerdict,
        analysis: TopologyAnalysis,
    ) -> tuple[StrategicActionPlan, list[str]]:
        """Server-authoritative corrections applied after the planner.

        These are invariants, not preferences: they run on every plan regardless
        of which backend produced it, so neither a model error nor instructions
        smuggled through the payload can route around them.
        """
        applied: list[str] = []
        updates: dict[str, Any] = {}
        action = plan.action_type
        priority = plan.execution_priority
        steps = list(plan.isolation_steps)

        # 1. Intervention must land on the attributed root cause.
        if plan.target_asset_id != verdict.root_cause_node:
            applied.append(
                f"target redirected from {plan.target_asset_id} to the attributed root cause "
                f"{verdict.root_cause_node}"
            )
            updates["target_asset_id"] = verdict.root_cause_node

        # 2. No irreversible action on a graph we cannot trust.
        trust = verdict.crdt_assessment.graph_trust
        untrusted = trust < self._settings.min_graph_trust_for_irreversible
        if action.is_irreversible and (not verdict.irreversible_permitted or untrusted):
            applied.append(
                f"EMERGENCY_SHUTDOWN downgraded to DEGRADE_THROTTLE: graph trust {trust:g} is below the "
                f"{self._settings.min_graph_trust_for_irreversible:g} threshold for irreversible action"
            )
            action = ActionType.DEGRADE_THROTTLE
            updates["action_type"] = action

            # Drop the irreversible steps rather than relabelling them: an
            # instruction that reads "trip the unit" is not made reversible by
            # flipping a boolean, and handing that contradiction to a field crew
            # is worse than handing them a shorter plan.
            retained = [step for step in steps if step.reversible]
            dropped = len(steps) - len(retained)
            if dropped:
                applied.append(
                    f"{dropped} irreversible step(s) removed from the sequence to match the downgraded action"
                )
            steps = retained or [
                IsolationStep(
                    sequence=1,
                    actor=StepActor.CONTROL_SYSTEM,
                    instruction=(
                        f"Hold {verdict.root_cause_node} at its documented safe-state setpoint and inhibit "
                        "automatic recovery until the asset graph reconciles."
                    ),
                    target_component=verdict.root_cause_node,
                    verification="Setpoint reads safe-state and the recovery inhibit is latched.",
                    reversible=True,
                    estimated_seconds=120,
                    manual_reference=None,
                )
            ]

        # 3. An irreversible command is CRITICAL by definition.
        if action.is_irreversible and priority is not ExecutionPriority.CRITICAL:
            applied.append("execution priority raised to CRITICAL for an irreversible command")
            priority = ExecutionPriority.CRITICAL
            updates["execution_priority"] = priority

        # 4. A CRITICAL kernel severity may not be planned as ROUTINE.
        if analysis.priority is ExecutionPriority.CRITICAL and priority is ExecutionPriority.ROUTINE:
            applied.append("execution priority raised to HIGH: the kernel scored this event CRITICAL")
            priority = ExecutionPriority.HIGH
            updates["execution_priority"] = priority

        # 5. Every step must be reachable and countable.
        if len(steps) > self._settings.max_isolation_steps:
            applied.append(
                f"isolation sequence truncated from {len(steps)} to "
                f"{self._settings.max_isolation_steps} steps"
            )
            steps = steps[: self._settings.max_isolation_steps]

        # 6. A plan without a verification-bearing closing step is not executable.
        if steps and not any(step.actor is StepActor.HUMAN_SUPERVISOR for step in steps):
            applied.append("supervisor handover appended: no step returned the asset to a human owner")
            steps = steps + [
                IsolationStep(
                    sequence=len(steps) + 1,
                    actor=StepActor.HUMAN_SUPERVISOR,
                    instruction=(
                        f"Hand {updates.get('target_asset_id', plan.target_asset_id)} to the accountable "
                        "operator with the recorded evidence and the agreed next action."
                    ),
                    target_component=str(updates.get("target_asset_id", plan.target_asset_id)),
                    verification="Handover acknowledged by the accountable operator.",
                    reversible=True,
                    estimated_seconds=180,
                    manual_reference=None,
                )
            ]

        # 7. Human authorisation is mandatory for irreversible work, critical
        #    work, and anything planned on a graph the isolator did not trust.
        if not plan.requires_human_authorization:
            reason = None
            if action.is_irreversible or any(not step.reversible for step in steps):
                reason = "the sequence contains irreversible work"
            elif priority is ExecutionPriority.CRITICAL:
                reason = "the command is CRITICAL priority"
            elif untrusted:
                reason = f"graph trust {trust:g} is below the threshold for unattended execution"
            if reason:
                applied.append(f"human authorisation forced: {reason}")
                updates["requires_human_authorization"] = True

        if applied:
            steps = [step.model_copy(update={"sequence": index}) for index, step in enumerate(steps, start=1)]
            updates["isolation_steps"] = steps
            # A corrected plan is a less certain plan.
            updates["confidence"] = round(max(0.05, plan.confidence * 0.9), 3)
            plan = plan.model_copy(update=updates)
            logger.info("safety guardrails applied", extra={"guardrails": applied})

        return plan, applied


def analyse_channels_for_query(payload: EnrichedGraphPayload) -> list[Exceedance]:
    """Breached local channels, worst first — used to shape the retrieval query."""
    scored = evaluate_channels(payload.telemetry_snapshot.local_channels(), node=payload.asset_metadata.uuid)
    return [item for item in scored if _BAND_RANK[item.band] >= _BAND_RANK[Band.WARN]]


# ===========================================================================
# 6. Commercial gating — the monetisation seam
# ===========================================================================

FEATURE_GRAPHRAG = "ai.graphrag.intercept"
FEATURE_AUTONOMOUS = "ai.graphrag.autonomous"


class SubscriptionError(Exception):
    """Rejection by the commercial layer, carrying its own HTTP mapping."""

    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        *,
        hint: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.hint = hint
        self.headers = headers or {}


def license_digest(key: str) -> str:
    """SHA-256 hex digest of a plaintext licence key."""
    return hashlib.sha256(key.strip().encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class EnterpriseSubscription:
    key_id: str
    key_digest: str
    tenant: str
    tier: str
    quota_per_minute: int
    expires_at: datetime
    features: frozenset[str] = field(default_factory=frozenset)

    def is_expired(self, now: datetime | None = None) -> bool:
        return (now or _utcnow()) >= self.expires_at

    def has(self, feature: str) -> bool:
        return feature in self.features


class SubscriptionRegistry:
    """The internal allowed list of enterprise licences.

    Keys are held only as digests, so the plaintext a customer presents never
    persists past the request that carried it. A production deployment swaps
    this for the billing system of record; :meth:`lookup` is the whole contract.
    """

    def __init__(self, subscriptions: Sequence[EnterpriseSubscription]) -> None:
        self._by_digest = {item.key_digest: item for item in subscriptions}

    def __len__(self) -> int:
        return len(self._by_digest)

    def lookup(self, presented_key: str) -> EnterpriseSubscription | None:
        presented = license_digest(presented_key)
        record = self._by_digest.get(presented)
        if record is None:
            return None
        # Constant-time confirmation; the dict hit above is the fast path.
        if not hmac.compare_digest(record.key_digest, presented):
            return None
        return record

    @classmethod
    def from_json(cls, raw: str) -> "SubscriptionRegistry":
        """Load from ``OO_GRAPHRAG_LICENSE_REGISTRY_JSON`` (inline JSON or a path)."""
        text = raw.strip()
        if not text.startswith("["):
            from pathlib import Path

            text = Path(text).read_text(encoding="utf-8")
        entries = json.loads(text)
        subscriptions = [
            EnterpriseSubscription(
                key_id=entry["key_id"],
                key_digest=entry.get("key_digest") or license_digest(entry["key"]),
                tenant=entry["tenant"],
                tier=entry["tier"],
                quota_per_minute=int(entry["quota_per_minute"]),
                expires_at=datetime.fromisoformat(entry["expires_at"]),
                features=frozenset(entry.get("features", [])),
            )
            for entry in entries
        ]
        return cls(subscriptions)

    @classmethod
    def demo(cls) -> "SubscriptionRegistry":
        """Built-in tiers for local development and the compose topology.

        The COMMUNITY entry is the paywall made concrete: the open-core engine
        publishes mutations to anyone, but this layer answers only for tiers that
        carry ``ai.graphrag.intercept``.
        """
        far_future = _utcnow() + timedelta(days=365)
        yesterday = _utcnow() - timedelta(days=1)
        return cls(
            [
                EnterpriseSubscription(
                    key_id="lic_graphrag_enterprise",
                    key_digest=license_digest("oo-live-graphrag-enterprise-key"),
                    tenant="northwind-aerospace",
                    tier="ENTERPRISE",
                    quota_per_minute=600,
                    expires_at=far_future,
                    features=frozenset({FEATURE_GRAPHRAG, FEATURE_AUTONOMOUS}),
                ),
                EnterpriseSubscription(
                    key_id="lic_graphrag_business",
                    key_digest=license_digest("oo-live-graphrag-business-key"),
                    tenant="rotterdam-polymers",
                    tier="BUSINESS",
                    quota_per_minute=60,
                    expires_at=far_future,
                    features=frozenset({FEATURE_GRAPHRAG}),
                ),
                EnterpriseSubscription(
                    key_id="lic_graphrag_community",
                    key_digest=license_digest("oo-live-graphrag-community-key"),
                    tenant="community-user",
                    tier="COMMUNITY",
                    quota_per_minute=10,
                    expires_at=far_future,
                    features=frozenset(),
                ),
                EnterpriseSubscription(
                    key_id="lic_graphrag_lapsed",
                    key_digest=license_digest("oo-live-graphrag-lapsed-key"),
                    tenant="lapsed-customer",
                    tier="BUSINESS",
                    quota_per_minute=60,
                    expires_at=yesterday,
                    features=frozenset({FEATURE_GRAPHRAG}),
                ),
            ]
        )


class SlidingWindowQuota:
    """Per-subscription request quota over a sliding window.

    Process-local, which is correct for a single worker. Multi-replica
    deployments back this with Redis; the interface does not change.
    """

    def __init__(self, window_seconds: int = 60) -> None:
        self._window = float(window_seconds)
        self._hits: dict[str, deque[float]] = {}
        self._lock = asyncio.Lock()

    async def check(self, key: str, limit: int) -> tuple[bool, int, float]:
        """Record a hit and report ``(allowed, remaining, retry_after)``."""
        now = time.monotonic()
        cutoff = now - self._window
        async with self._lock:
            bucket = self._hits.setdefault(key, deque())
            while bucket and bucket[0] <= cutoff:
                bucket.popleft()
            if len(bucket) >= limit:
                return False, 0, max(0.0, bucket[0] + self._window - now)
            bucket.append(now)
            return True, max(0, limit - len(bucket)), 0.0


_license_header_scheme = APIKeyHeader(
    name=LICENSE_HEADER,
    scheme_name="OpenOntology enterprise licence",
    description="Enterprise licence key issued with the commercial subscription.",
    auto_error=False,
)
_bearer_scheme = HTTPBearer(
    scheme_name="Bearer licence",
    description="The same licence key, presented as a bearer token.",
    auto_error=False,
)

_default_registry: SubscriptionRegistry | None = None
_default_quota: SlidingWindowQuota | None = None


def _fallback_registry() -> SubscriptionRegistry:
    global _default_registry
    if _default_registry is None:
        settings = get_settings()
        _default_registry = (
            SubscriptionRegistry.from_json(settings.license_registry_json)
            if settings.license_registry_json
            else SubscriptionRegistry.demo()
        )
    return _default_registry


def _fallback_quota() -> SlidingWindowQuota:
    global _default_quota
    if _default_quota is None:
        _default_quota = SlidingWindowQuota(get_settings().quota_window_seconds)
    return _default_quota


async def verify_enterprise_subscription(
    request: Request,
    response: Response,
    license_key: str | None = Security(_license_header_scheme),
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer_scheme),
) -> EnterpriseSubscription:
    """Gate the premium layer: authenticate, check expiry, entitlement and quota.

    Accepts the key either as ``X-OpenOntology-License`` or as an
    ``Authorization: Bearer`` token, because fleet gateways and hand-rolled
    edge-site clients tend to disagree about which is idiomatic.
    """
    registry: SubscriptionRegistry = getattr(request.app.state, "subscriptions", None) or _fallback_registry()
    quota: SlidingWindowQuota = getattr(request.app.state, "quota", None) or _fallback_quota()

    presented = (license_key or "").strip() or (credentials.credentials.strip() if credentials else "")
    if not presented:
        raise SubscriptionError(
            status.HTTP_401_UNAUTHORIZED,
            "license_key_missing",
            f"Provide an enterprise licence via the {LICENSE_HEADER} header or a bearer token.",
            hint="Contact sales@openontology.io for a Module 4 licence.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    subscription = registry.lookup(presented)
    if subscription is None:
        logger.warning("rejected unknown licence key", extra={"path": request.url.path})
        raise SubscriptionError(
            status.HTTP_401_UNAUTHORIZED,
            "license_key_invalid",
            "The supplied licence key is not on the enterprise allowed list.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if subscription.is_expired():
        raise SubscriptionError(
            status.HTTP_402_PAYMENT_REQUIRED,
            "license_expired",
            f"Subscription for tenant {subscription.tenant} expired at "
            f"{subscription.expires_at.isoformat()}.",
            hint="Renew the subscription to restore access to the GraphRAG interceptor.",
        )

    if not subscription.has(FEATURE_GRAPHRAG):
        raise SubscriptionError(
            status.HTTP_403_FORBIDDEN,
            "feature_not_licensed",
            f"Tier {subscription.tier} does not include '{FEATURE_GRAPHRAG}'.",
            hint=f"Upgrade to a tier that includes '{FEATURE_GRAPHRAG}'.",
        )

    allowed, remaining, retry_after = await quota.check(subscription.key_id, subscription.quota_per_minute)
    if not allowed:
        raise SubscriptionError(
            status.HTTP_429_TOO_MANY_REQUESTS,
            "quota_exceeded",
            f"Rate limit of {subscription.quota_per_minute} requests/minute exceeded.",
            headers={
                "Retry-After": str(max(1, int(retry_after) + 1)),
                "X-RateLimit-Limit": str(subscription.quota_per_minute),
                "X-RateLimit-Remaining": "0",
            },
        )

    _tenant_var.set(subscription.tenant)
    request.state.subscription = subscription
    response.headers["X-RateLimit-Limit"] = str(subscription.quota_per_minute)
    response.headers["X-RateLimit-Remaining"] = str(remaining)
    response.headers["X-License-Tier"] = subscription.tier
    return subscription


# ===========================================================================
# 7. Command generation and API layer
# ===========================================================================


def build_command_response(
    payload: EnrichedGraphPayload,
    outcome: EngineOutcome,
    *,
    tenant: str,
) -> CommandActionResponse:
    """Assemble the wire response.

    Server-authoritative fields — command id, tenant, timing, trace — are set
    here rather than taken from any agent, so they cannot be hallucinated.
    """
    plan = outcome.plan
    verdict = outcome.verdict

    by_id = {chunk.doc_id: (chunk, score) for chunk, score in outcome.guidelines}
    cited = [by_id[doc_id] for doc_id in plan.manual_references if doc_id in by_id]
    if not cited:
        cited = list(outcome.guidelines[:3])

    references = [
        RetrievedGuideline(
            doc_id=chunk.doc_id,
            title=chunk.title,
            source=chunk.source,
            revision=chunk.revision,
            score=round(score, 4),
            excerpt=chunk.excerpt(),
        )
        for chunk, score in cited
    ]

    reversible = not plan.action_type.is_irreversible and all(
        step.reversible for step in plan.isolation_steps
    )

    return CommandActionResponse(
        command_id=f"cmd_{uuid4().hex}",
        target_asset_id=plan.target_asset_id,
        action_type=plan.action_type,
        isolation_steps=plan.isolation_steps,
        execution_priority=plan.execution_priority,
        event_id=payload.event_id,
        source_asset_id=payload.asset_metadata.uuid,
        tenant=tenant,
        fault_classification=verdict.fault_classification,
        cascade_path=list(verdict.cascade_path),
        blast_radius=list(verdict.blast_radius),
        blast_radius_protection=list(plan.blast_radius_protection),
        crdt_assessment=verdict.crdt_assessment,
        requires_human_authorization=plan.requires_human_authorization,
        reversible=reversible,
        confidence=plan.confidence,
        rationale=f"{verdict.root_cause_rationale} {plan.rationale}".strip(),
        evidence=list(verdict.evidence),
        manual_references=references,
        guardrails_applied=list(outcome.guardrails),
        agent_trace=list(outcome.trace),
        latency_ms=outcome.latency_ms,
    )


class CommandCache:
    """Bounded idempotency cache keyed by ``(tenant, event_id)``.

    Kafka delivers at least once, so the same mutation legitimately arrives
    twice. Replaying the stored command keeps the physical instruction stable —
    an operator must never be handed two different plans for one event — and
    avoids paying for a second pair of inferences.
    """

    def __init__(self, max_entries: int) -> None:
        self._max = max_entries
        self._data: OrderedDict[tuple[str, str], dict[str, Any]] = OrderedDict()
        self._lock = asyncio.Lock()

    async def get(self, key: tuple[str, str]) -> dict[str, Any] | None:
        if self._max == 0:
            return None
        async with self._lock:
            value = self._data.get(key)
            if value is not None:
                self._data.move_to_end(key)
            return value

    async def put(self, key: tuple[str, str], value: dict[str, Any]) -> None:
        if self._max == 0:
            return
        async with self._lock:
            self._data[key] = value
            self._data.move_to_end(key)
            while len(self._data) > self._max:
                self._data.popitem(last=False)


def error_response(
    status_code: int,
    code: str,
    message: str,
    request_id: str,
    *,
    hint: str | None = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    """Uniform error envelope for every rejection path."""
    detail: dict[str, Any] = {"code": code, "message": message}
    if hint:
        detail["hint"] = hint
    response = JSONResponse(
        status_code=status_code,
        content={"error": detail, "request_id": request_id},
        headers=dict(headers or {}),
    )
    response.headers["X-Request-ID"] = request_id
    return response


def get_engine(request: Request) -> MultiAgentGraphEngine:
    return request.app.state.engine


def get_command_cache(request: Request) -> CommandCache:
    return request.app.state.command_cache


def create_app(settings: InterceptorSettings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    registry = (
        SubscriptionRegistry.from_json(settings.license_registry_json)
        if settings.license_registry_json
        else SubscriptionRegistry.demo()
    )
    vector_store = MaintenanceManualVectorStore()

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        application.state.settings = settings
        application.state.started_at = time.time()
        application.state.subscriptions = registry
        application.state.quota = SlidingWindowQuota(settings.quota_window_seconds)
        application.state.vector_store = vector_store
        application.state.engine = MultiAgentGraphEngine(
            build_agent_client(settings), vector_store, settings
        )
        application.state.command_cache = CommandCache(settings.idempotency_cache_size)
        logger.info(
            "graphrag interceptor ready",
            extra={
                "environment": settings.environment,
                "agent_provider": settings.agent_provider,
                "agent_model": settings.agent_model,
                "knowledge_chunks": len(vector_store),
                "subscriptions_loaded": len(registry),
            },
        )
        yield
        logger.info("graphrag interceptor shutting down")

    application = FastAPI(
        title="OpenOntology Semantic GraphRAG Interceptor",
        version=MODULE_VERSION,
        summary="Module 4: multi-agent root-cause isolation over a replicated asset graph.",
        docs_url="/docs" if settings.docs_enabled else None,
        redoc_url="/redoc" if settings.docs_enabled else None,
        openapi_url="/openapi.json" if settings.docs_enabled else None,
        lifespan=lifespan,
    )

    _register_middleware(application)
    _register_error_handlers(application)
    _register_routes(application, settings, vector_store)
    return application


def _register_middleware(application: FastAPI) -> None:
    @application.middleware("http")
    async def correlate(request: Request, call_next):
        request_id = request.headers.get("X-Request-ID") or uuid4().hex
        tokens = bind_request_context(request_id)
        started = time.perf_counter()
        try:
            request.state.request_id = request_id
            response = await call_next(request)
            elapsed = (time.perf_counter() - started) * 1000
            response.headers["X-Request-ID"] = request_id
            response.headers["X-Response-Time-ms"] = f"{elapsed:.2f}"
            logger.info(
                "request completed",
                extra={
                    "path": request.url.path,
                    "method": request.method,
                    "status": response.status_code,
                    "duration_ms": round(elapsed, 2),
                },
            )
            return response
        finally:
            reset_request_context(tokens)


def _register_error_handlers(application: FastAPI) -> None:
    @application.exception_handler(SubscriptionError)
    async def _subscription_error(request: Request, exc: SubscriptionError) -> JSONResponse:
        logger.warning(
            "request rejected by the commercial layer",
            extra={"status": exc.status_code, "reason": exc.code},
        )
        return error_response(
            exc.status_code,
            exc.code,
            exc.message,
            current_request_id(),
            hint=exc.hint,
            headers=exc.headers,
        )

    @application.exception_handler(RequestValidationError)
    async def _request_validation_error(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        errors = exc.errors()[:5]
        logger.warning("payload rejected", extra={"errors": errors})
        return error_response(
            422,  # literal: the Starlette constant was renamed across versions
            "payload_invalid",
            "The enriched graph payload did not match the expected schema.",
            current_request_id(),
            hint=_first_validation_hint(errors),
        )

    @application.exception_handler(ValidationError)
    async def _model_validation_error(request: Request, exc: ValidationError) -> JSONResponse:
        # A model built outside request parsing (an agent answer, a cache
        # replay) failed validation: that is ours, not the caller's.
        logger.error("internal model validation failure", extra={"errors": exc.errors()[:3]})
        return error_response(
            status.HTTP_502_BAD_GATEWAY,
            "response_invalid",
            "The interceptor produced a response that failed its own contract.",
            current_request_id(),
            hint="Retry; if it persists the agent backend is emitting malformed plans.",
        )

    @application.exception_handler(AgentExecutionError)
    async def _agent_error(request: Request, exc: AgentExecutionError) -> JSONResponse:
        logger.error("agent execution failed", extra={"detail": str(exc)})
        return error_response(
            status.HTTP_502_BAD_GATEWAY,
            "agent_unavailable",
            str(exc),
            current_request_id(),
            hint="Retry; if it persists check the upstream model provider.",
        )

    @application.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("unhandled error", extra={"path": request.url.path})
        return error_response(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "internal_error",
            "An unexpected error occurred.",
            current_request_id(),
        )


def _first_validation_hint(errors: Sequence[Mapping[str, Any]]) -> str:
    if not errors:  # pragma: no cover - FastAPI always supplies at least one
        return "Check the payload against the EnrichedGraphPayload schema."
    first = errors[0]
    location = ".".join(str(part) for part in first.get("loc", ()) if part != "body")
    return f"{location or 'payload'}: {first.get('msg', 'invalid value')}"


def _register_routes(
    application: FastAPI,
    settings: InterceptorSettings,
    vector_store: MaintenanceManualVectorStore,
) -> None:
    @application.get("/healthz", response_model=HealthResponse, tags=["ops"], summary="Liveness probe")
    async def healthz(request: Request) -> HealthResponse:
        started_at = getattr(request.app.state, "started_at", time.time())
        return HealthResponse(
            status="ok",
            service=settings.service_name,
            version=MODULE_VERSION,
            environment=settings.environment,
            agent_provider=settings.agent_provider,
            agent_model=settings.agent_model,
            knowledge_chunks=len(vector_store),
            uptime_seconds=round(time.time() - started_at, 3),
        )

    @application.get(
        "/v1/subscription",
        response_model=SubscriptionIntrospection,
        tags=["commercial"],
        summary="Introspect the presented enterprise licence",
        responses={
            401: {"model": ErrorEnvelope},
            402: {"model": ErrorEnvelope},
            403: {"model": ErrorEnvelope},
            429: {"model": ErrorEnvelope},
        },
    )
    async def introspect(
        subscription: EnterpriseSubscription = Depends(verify_enterprise_subscription),
    ) -> SubscriptionIntrospection:
        return SubscriptionIntrospection(
            key_id=subscription.key_id,
            tenant=subscription.tenant,
            tier=subscription.tier,
            features=sorted(subscription.features),
            quota_per_minute=subscription.quota_per_minute,
            expires_at=subscription.expires_at,
            valid=not subscription.is_expired(),
        )

    @application.post(
        "/v1/intercept",
        response_model=CommandActionResponse,
        status_code=status.HTTP_200_OK,
        tags=["commercial"],
        summary="Isolate the root cause and emit a physical command action",
        responses={
            401: {"model": ErrorEnvelope},
            402: {"model": ErrorEnvelope},
            403: {"model": ErrorEnvelope},
            422: {"model": ErrorEnvelope},
            429: {"model": ErrorEnvelope},
            502: {"model": ErrorEnvelope},
        },
    )
    async def intercept(
        payload: EnrichedGraphPayload,
        response: Response,
        subscription: EnterpriseSubscription = Depends(verify_enterprise_subscription),
        engine: MultiAgentGraphEngine = Depends(get_engine),
        cache: CommandCache = Depends(get_command_cache),
    ) -> CommandActionResponse:
        cache_key = (subscription.tenant, payload.event_id)

        if cached := await cache.get(cache_key):
            response.headers["X-Idempotent-Replay"] = "true"
            logger.info(
                "replaying cached command",
                extra={"event_id": payload.event_id, "asset_id": payload.asset_metadata.uuid},
            )
            return CommandActionResponse.model_validate(cached)

        context = payload.ontology_context
        logger.info(
            "intercepting enriched graph payload",
            extra={
                "event_id": payload.event_id,
                "asset_id": payload.asset_metadata.uuid,
                "asset_status": payload.asset_metadata.current_status.value,
                "model_number": payload.asset_metadata.model_number,
                "parent_system": context.parent_system,
                "upstream": len(context.upstream_dependencies),
                "downstream": len(context.downstream_impacts),
                "channels": len(payload.telemetry_snapshot.channels()),
                "replicas": len(context.replica_observations),
                "engine_degraded": payload.degraded,
            },
        )

        outcome = await engine.run(payload)
        command = build_command_response(payload, outcome, tenant=subscription.tenant)

        await cache.put(cache_key, command.model_dump(mode="json"))
        response.headers["X-Idempotent-Replay"] = "false"
        response.headers["X-Fault-Classification"] = command.fault_classification.value
        response.headers["X-Execution-Priority"] = command.execution_priority.value

        logger.info(
            "command action issued",
            extra={
                "command_id": command.command_id,
                "event_id": command.event_id,
                "target_asset_id": command.target_asset_id,
                "action_type": command.action_type.value,
                "priority": command.execution_priority.value,
                "steps": len(command.isolation_steps),
                "confidence": command.confidence,
                "guardrails": len(command.guardrails_applied),
                "latency_ms": command.latency_ms,
            },
        )
        return command


app = create_app()


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    _settings = get_settings()
    uvicorn.run(
        "app.graph_rag_interceptor:app",
        host="0.0.0.0",
        port=8100,
        log_config=None,
        reload=_settings.environment == "local",
    )
