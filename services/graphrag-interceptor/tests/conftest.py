"""Shared fixtures for the GraphRAG interceptor suite."""

from __future__ import annotations

import copy
import os
import sys
from pathlib import Path
from typing import Any

import pytest

# The service is a directory, not an installed package.
SERVICE_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SERVICE_ROOT))

# The deterministic agent client is the default, but the suite must never depend
# on ambient configuration deciding that for it: a developer with the cloud
# provider exported would otherwise have the tests make paid network calls.
os.environ.setdefault("OO_GRAPHRAG_AGENT_PROVIDER", "deterministic")
os.environ["OO_GRAPHRAG_AGENT_PROVIDER"] = "deterministic"
os.environ.pop("OO_GRAPHRAG_LICENSE_REGISTRY_JSON", None)
os.environ.pop("OO_LICENSE_REGISTRY_PATH", None)

ENTERPRISE_KEY = "oo-live-graphrag-enterprise-key"
BUSINESS_KEY = "oo-live-graphrag-business-key"
COMMUNITY_KEY = "oo-live-graphrag-community-key"
EXPIRED_KEY = "oo-live-graphrag-lapsed-key"


# A realistic openontology.mutation.v2 record: the pump alarming, with the flow
# topology and replica timelines the engine actually emits.
MUTATION_V2: dict[str, Any] = {
    "event_id": "evt_test_graphrag_0001",
    "schema_version": "openontology.mutation.v2",
    "producer": "ontology-resolution-engine",
    "emitted_at": "2026-08-13T21:35:34.617392Z",
    "asset_id": "HPP-PUMP-221",
    "transition": "RAISED",
    "severity": "CRITICAL",
    "anomaly_active_since": "2026-08-13T21:35:34.617392Z",
    "breach_count": 1,
    "rule": {
        "rule_id": "rule.vibration_index.max",
        "sensor_id": "vibration_index",
        "operator": ">",
        "threshold": 8.5,
        "unit": "mm/s",
        "observed_value": 11.4,
        "exceeded_by": 2.9,
        "exceeded_pct": 34.1,
    },
    "telemetry_snapshot": {
        "trigger": {
            "sensor_id": "vibration_index",
            "value": 11.4,
            "unit": "mm/s",
            "observed_at": "2026-08-13T21:35:34.117392Z",
            "age_seconds": 0.5,
        },
        "readings": [
            {"sensor_id": "temperature_celsius", "value": 104.2, "unit": "degC",
             "observed_at": "2026-08-13T21:35:33.117392Z", "age_seconds": 1.5},
            {"sensor_id": "oil_pressure_bar", "value": 4.2, "unit": "bar",
             "observed_at": "2026-08-13T21:35:33.117392Z", "age_seconds": 1.5},
        ],
        "captured_at": "2026-08-13T21:35:34.617392Z",
        "complete": True,
    },
    "ontology_context": {
        "asset_id": "HPP-PUMP-221",
        "asset_name": "Hydraulic Power Pack Pump 221",
        "asset_class": "industrial.hydraulics.pump",
        "model_number": "A11VO-190",
        "site": "PLANT-ROTTERDAM-L4",
        "criticality": "HIGH",
        "parent_systems": [
            {"node_id": "SYS-HYD-L4", "name": "Line 4 Hydraulic Loop", "type": "Subsystem", "depth": 1},
            {"node_id": "PLANT-ROTTERDAM", "name": "Rotterdam Plant", "type": "Site", "depth": 3},
        ],
        "components": ["drive_coupling", "thrust_bearing"],
        "assigned_operators": [
            {"operator_id": "OP-8815", "name": "M. Okafor", "role": "Line Supervisor", "escalation_order": 2},
            {"operator_id": "OP-8801", "name": "J. de Vries", "role": "Maintenance Technician", "escalation_order": 1},
        ],
        "upstream_dependencies": [
            {"asset_id": "SUCTION-STRAINER-S14", "name": "Suction Strainer S-14",
             "model_number": "STR-S14-316L", "status": "OPERATIONAL", "relation": "SUPPLIES", "hops": 1},
            {"asset_id": "FEED-DRUM-D101", "name": "Feed Drum D-101",
             "model_number": "DRM-D101-CS", "status": "OPERATIONAL", "relation": "SUPPLIES", "hops": 2},
        ],
        "downstream_impacts": [
            {"asset_id": "HX-SHELL-TUBE-E220", "name": "Exchanger E-220",
             "model_number": "HX-E220-BEM", "status": "OPERATIONAL", "relation": "IMPACTS", "hops": 1},
            {"asset_id": "REACTOR-R310", "name": "Reactor R-310",
             "model_number": "CSTR-R310-GL", "status": "OPERATIONAL", "relation": "IMPACTS", "hops": 2},
        ],
        "blast_radius": 2,
        "replica_observations": [
            {"replica_id": "core-site", "add_stamp": 147, "remove_stamp": 0},
            {"replica_id": "edge-site", "add_stamp": 54, "remove_stamp": 0},
        ],
        "maintenance_window": "2026-08-09T02:00:00Z/2026-08-09T06:00:00Z",
        "source": "neo4j:live",
        "cache_hit": False,
    },
    "degraded": False,
    "origin_replica": "core-site",
    "lamport_clock": 148,
    "graph_revision": "c2d6e22b0beab27bad95156ca7795fab642449f2c2bbadb0b1b2758043800c1e",
    "source_partition": 2,
    "source_offset": 1773,
}


@pytest.fixture
def mutation() -> dict[str, Any]:
    """A fresh deep copy, so a test that mutates it cannot affect another."""
    return copy.deepcopy(MUTATION_V2)


@pytest.fixture
def client():
    from fastapi.testclient import TestClient

    from service import build_service

    with TestClient(build_service()) as test_client:
        yield test_client


def post(client, mutation: dict[str, Any], key: str | None = ENTERPRISE_KEY):
    from graphrag_interceptor import LICENSE_HEADER

    headers = {LICENSE_HEADER: key} if key else {}
    return client.post("/v1/intercept", json=mutation, headers=headers)
