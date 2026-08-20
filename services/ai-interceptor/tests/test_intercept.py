"""End-to-end tests for the commercial interceptor (mock planner)."""

from __future__ import annotations

import copy
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.models import EnrichedContextPayload

ENTERPRISE_KEY = "oo-live-enterprise-demo-key"
COMMUNITY_KEY = "oo-live-community-demo-key"
EXPIRED_KEY = "oo-live-expired-demo-key"

NOW = datetime.now(tz=timezone.utc).isoformat()

CRITICAL_PAYLOAD: dict = {
    "event_id": "evt_test_critical_0001",
    "schema_version": "openontology.mutation.v1",
    "producer": "ontology-resolution-engine",
    "emitted_at": NOW,
    "asset_id": "TURBOFAN-A320-0417",
    "transition": "RAISED",
    "severity": "CRITICAL",
    "anomaly_active_since": NOW,
    "breach_count": 1,
    "rule": {
        "rule_id": "rule.vibration_index.max",
        "sensor_id": "vibration_index",
        "operator": ">",
        "threshold": 8.5,
        "unit": "mm/s",
        "observed_value": 11.4,
        "exceeded_by": 2.9,
        "exceeded_pct": 34.1176,
        "description": "ISO 10816 broadband vibration index ceiling",
    },
    "telemetry_snapshot": {
        "trigger": {
            "sensor_id": "vibration_index",
            "value": 11.4,
            "unit": "mm/s",
            "observed_at": NOW,
            "age_seconds": 0.4,
        },
        "readings": [
            {"sensor_id": "temperature_celsius", "value": 104.2, "unit": "degC", "observed_at": NOW, "age_seconds": 1.1},
            {"sensor_id": "vibration_index", "value": 11.4, "unit": "mm/s", "observed_at": NOW, "age_seconds": 0.4},
        ],
        "captured_at": NOW,
        "complete": True,
    },
    "ontology_context": {
        "asset_id": "TURBOFAN-A320-0417",
        "asset_name": "CFM56-5B Turbofan #0417",
        "asset_class": "aero.propulsion.turbofan",
        "site": "MRO-TOULOUSE-B2",
        "criticality": "SAFETY_CRITICAL",
        "parent_systems": [
            {"node_id": "SYS-PROP-A320-0417", "name": "Propulsion Subsystem (Engine 1)", "type": "Subsystem", "depth": 1},
            {"node_id": "AIRFRAME-A320-MSN4412", "name": "Airframe A320 MSN4412", "type": "System", "depth": 2},
        ],
        "components": ["hpt_bearing_no3", "lpc_fan_module", "egt_harness"],
        "assigned_operators": [
            {"operator_id": "OP-4471", "name": "L. Moreau", "role": "Lead Powerplant Engineer", "shift": "B", "contact": "+33-5-6100-4471", "escalation_order": 1},
            {"operator_id": "OP-2210", "name": "S. Kaur", "role": "Reliability Engineer", "shift": "B", "contact": "+33-5-6100-2210", "escalation_order": 2},
        ],
        "maintenance_window": "2026-08-12T22:00:00Z/2026-08-13T04:00:00Z",
        "resolved_at": NOW,
        "source": "neo4j-mock:fixture",
        "cache_hit": False,
    },
    "degraded": False,
    "source_partition": 0,
    "source_offset": 4211,
}


@pytest.fixture()
def client() -> TestClient:
    with TestClient(create_app(), raise_server_exceptions=False) as test_client:
        yield test_client


def post_intercept(client: TestClient, payload: dict, key: str = ENTERPRISE_KEY):
    return client.post("/v1/intercept", json=payload, headers={"X-License-Key": key})


def test_healthz_is_unauthenticated(client: TestClient) -> None:
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.headers["X-Request-ID"]


def test_missing_license_key_is_rejected(client: TestClient) -> None:
    response = client.post("/v1/intercept", json=CRITICAL_PAYLOAD)
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "license_key_missing"


def test_unknown_license_key_is_rejected(client: TestClient) -> None:
    response = post_intercept(client, CRITICAL_PAYLOAD, key="not-a-real-key")
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "license_key_invalid"


def test_expired_license_is_payment_required(client: TestClient) -> None:
    response = post_intercept(client, CRITICAL_PAYLOAD, key=EXPIRED_KEY)
    assert response.status_code == 402
    assert response.json()["error"]["code"] == "license_expired"


def test_unlicensed_tier_cannot_reach_the_planner(client: TestClient) -> None:
    response = post_intercept(client, CRITICAL_PAYLOAD, key=COMMUNITY_KEY)
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "feature_not_licensed"


def test_bearer_token_is_accepted(client: TestClient) -> None:
    response = client.post(
        "/v1/intercept",
        json=CRITICAL_PAYLOAD,
        headers={"Authorization": f"Bearer {ENTERPRISE_KEY}"},
    )
    assert response.status_code == 200


def test_critical_vibration_yields_isolate_command(client: TestClient) -> None:
    response = post_intercept(client, CRITICAL_PAYLOAD)
    assert response.status_code == 200

    body = response.json()
    assert body["asset_id"] == "TURBOFAN-A320-0417"
    assert body["tenant"] == "northwind-aerospace"
    assert body["severity"] == "CRITICAL"
    assert body["commands"][0]["action"] == "ISOLATE"
    assert body["commands"][0]["priority"] == "CRITICAL"
    assert body["commands"][0]["target_component"] == "hpt_bearing_no3"
    assert body["commands"][0]["assigned_to"] == "L. Moreau"
    assert body["commands"][0]["assigned_operator_id"] == "OP-4471"
    assert [c["sequence"] for c in body["commands"]] == list(range(1, len(body["commands"]) + 1))
    assert body["escalation"]["required"] is True
    assert body["usage"]["input_tokens"] > 0
    assert response.headers["X-License-Tier"] == "ENTERPRISE"
    assert response.headers["X-Idempotent-Replay"] == "false"


def test_safety_critical_asset_adds_controlled_shutdown(client: TestClient) -> None:
    body = post_intercept(client, CRITICAL_PAYLOAD).json()
    actions = [command["action"] for command in body["commands"]]
    assert "SHUTDOWN" in actions


def test_repeated_event_id_replays_the_cached_plan(client: TestClient) -> None:
    first = post_intercept(client, CRITICAL_PAYLOAD)
    second = post_intercept(client, CRITICAL_PAYLOAD)

    assert second.status_code == 200
    assert second.headers["X-Idempotent-Replay"] == "true"
    assert second.json()["plan_id"] == first.json()["plan_id"]


def test_high_severity_prefers_throttling(client: TestClient) -> None:
    payload = copy.deepcopy(CRITICAL_PAYLOAD)
    payload["event_id"] = "evt_test_high_0002"
    payload["severity"] = "HIGH"
    payload["rule"]["observed_value"] = 8.9
    payload["rule"]["exceeded_by"] = 0.4
    payload["rule"]["exceeded_pct"] = 4.7

    body = post_intercept(client, payload).json()
    assert body["commands"][0]["action"] == "THROTTLE"
    assert body["escalation"]["required"] is False


def test_cleared_transition_acknowledges(client: TestClient) -> None:
    payload = copy.deepcopy(CRITICAL_PAYLOAD)
    payload["event_id"] = "evt_test_cleared_0003"
    payload["transition"] = "CLEARED"
    payload["severity"] = "INFO"

    body = post_intercept(client, payload).json()
    assert body["commands"][0]["action"] == "ACKNOWLEDGE"
    assert body["escalation"]["required"] is False


def test_degraded_context_lowers_confidence(client: TestClient) -> None:
    baseline = post_intercept(client, CRITICAL_PAYLOAD).json()

    degraded = copy.deepcopy(CRITICAL_PAYLOAD)
    degraded["event_id"] = "evt_test_degraded_0004"
    degraded["degraded"] = True
    degraded["degraded_reason"] = "graph_unavailable: dial tcp: connection refused"

    body = post_intercept(client, degraded).json()
    assert body["context_degraded"] is True
    assert body["confidence"] < baseline["confidence"]


def test_unknown_schema_version_is_rejected(client: TestClient) -> None:
    payload = copy.deepcopy(CRITICAL_PAYLOAD)
    payload["event_id"] = "evt_test_schema_0005"
    payload["schema_version"] = "openontology.mutation.v9"

    response = post_intercept(client, payload)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "payload_invalid"


def test_v1_payloads_are_still_accepted(client: TestClient) -> None:
    """A fleet is not upgraded atomically.

    The engine and this service restart independently, so mutations produced by
    a not-yet-restarted v1 engine are in flight when a v2 interceptor comes up.
    Refusing them would dead-letter real alarms over a version string.
    """
    payload = copy.deepcopy(CRITICAL_PAYLOAD)
    payload["event_id"] = "evt_test_schema_v1_0006"
    payload["schema_version"] = "openontology.mutation.v1"

    response = post_intercept(client, payload)
    assert response.status_code == 200
    assert response.json()["commands"]


def test_v2_flow_topology_is_parsed(client: TestClient) -> None:
    """The v2 additions survive the round trip rather than being dropped."""
    payload = copy.deepcopy(CRITICAL_PAYLOAD)
    payload["event_id"] = "evt_test_schema_v2_0007"
    payload["schema_version"] = "openontology.mutation.v2"
    payload["ontology_context"]["model_number"] = "A11VO-190"
    payload["ontology_context"]["upstream_dependencies"] = [
        {"asset_id": "SUCTION-STRAINER-S14", "name": "Suction Strainer S-14",
         "relation": "SUPPLIES", "hops": 1},
    ]
    payload["ontology_context"]["downstream_impacts"] = [
        {"asset_id": "HX-SHELL-TUBE-E220", "name": "Exchanger E-220",
         "relation": "IMPACTS", "hops": 1},
        {"asset_id": "REACTOR-R310", "name": "Reactor R-310",
         "relation": "IMPACTS", "hops": 2},
    ]
    payload["ontology_context"]["blast_radius"] = 2

    response = post_intercept(client, payload)
    assert response.status_code == 200

    parsed = EnrichedContextPayload.model_validate(payload)
    assert parsed.carries_flow_topology is True
    assert parsed.ontology_context.blast_radius == 2
    assert parsed.ontology_context.nearest_upstream.asset_id == "SUCTION-STRAINER-S14"
    # Nearest-first ordering is the contract a cascade walk depends on.
    assert [d.hops for d in parsed.ontology_context.downstream_impacts] == [1, 2]


def test_v1_payload_reports_no_flow_topology(client: TestClient) -> None:
    """An empty blast radius on v1 means "not reported", not "nothing downstream".

    Only the version distinguishes those, and a planner that confused them would
    isolate an asset believing nothing depends on it.
    """
    payload = copy.deepcopy(CRITICAL_PAYLOAD)
    payload["event_id"] = "evt_test_schema_v1_0008"
    payload["schema_version"] = "openontology.mutation.v1"

    parsed = EnrichedContextPayload.model_validate(payload)
    assert parsed.carries_flow_topology is False
    assert parsed.ontology_context.blast_radius == 0
    assert parsed.ontology_context.downstream_impacts == []


def test_license_introspection(client: TestClient) -> None:
    response = client.get("/v1/license", headers={"X-License-Key": ENTERPRISE_KEY})
    assert response.status_code == 200

    body = response.json()
    assert body["tenant"] == "northwind-aerospace"
    assert body["tier"] == "ENTERPRISE"
    assert "ai.intercept" in body["features"]
    assert body["valid"] is True
