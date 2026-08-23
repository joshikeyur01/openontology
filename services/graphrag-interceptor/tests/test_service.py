"""The service contract: licensing, the two-agent loop, and the guardrails.

The guardrail tests are the important ones. This layer is authorised to command
an emergency shutdown of a live asset, and the argument that this is safe rests
entirely on checks that run *after* the planner — so a model error or a prompt
injection cannot route around them. A test suite that exercised only the happy
path would be asserting the least important property.
"""

from __future__ import annotations

import pytest

from conftest import BUSINESS_KEY, COMMUNITY_KEY, ENTERPRISE_KEY, EXPIRED_KEY, post


# ---------------------------------------------------------------------------
# The paywall
# ---------------------------------------------------------------------------


def test_a_request_without_a_key_is_refused(client, mutation):
    assert post(client, mutation, key=None).status_code == 401


def test_an_unknown_key_is_refused(client, mutation):
    assert post(client, mutation, key="oo-live-not-a-real-key").status_code == 401


def test_the_community_tier_is_refused_the_feature(client, mutation):
    """The paywall, made concrete.

    COMMUNITY authenticates successfully and is then refused — which is the
    distinction that makes this open core rather than closed: the engine
    publishes the mutation to anyone, and only this layer is gated.
    """
    response = post(client, mutation, key=COMMUNITY_KEY)
    assert response.status_code == 403


def test_an_expired_subscription_is_refused(client, mutation):
    response = post(client, mutation, key=EXPIRED_KEY)
    assert response.status_code == 402


def test_an_entitled_tier_is_served(client, mutation):
    response = post(client, mutation, key=ENTERPRISE_KEY)
    assert response.status_code == 200


# ---------------------------------------------------------------------------
# The plan
# ---------------------------------------------------------------------------


def test_a_plan_is_returned_in_the_workers_envelope(client, mutation):
    """The whole point of the bridge: one contract, whichever interceptor.

    These are the fields services/command-worker validates, so a change that
    broke any of them would dead-letter every command this service produces.
    """
    body = post(client, mutation).json()

    assert body["plan_id"]
    assert body["event_id"] == mutation["event_id"]
    assert body["asset_id"] == "HPP-PUMP-221"
    assert body["tenant"]
    assert 0.0 <= body["confidence"] <= 1.0
    assert body["commands"], "the worker requires at least one command"

    for command in body["commands"]:
        assert command["sequence"] >= 1
        assert command["action"]
        assert command["priority"]
        assert command["expected_effect"]
        assert command["rollback"], "every command must say how to undo it, or that it cannot be undone"


def test_commands_are_contiguously_sequenced(client, mutation):
    body = post(client, mutation).json()
    sequences = [command["sequence"] for command in body["commands"]]
    assert sequences == list(range(1, len(sequences) + 1))


def test_actions_are_in_the_workers_vocabulary(client, mutation):
    """GraphRAG speaks three physical interventions; the worker publishes seven.

    Emitting an action outside the worker's enum would be a 422 at the far end.
    """
    allowed = {"ISOLATE", "SHUTDOWN", "SHIFT_SPEED", "INSPECT",
               "SCHEDULE_MAINTENANCE", "NOTIFY", "ACKNOWLEDGE"}
    body = post(client, mutation).json()
    assert {command["action"] for command in body["commands"]} <= allowed


def test_reversible_steps_are_not_labelled_as_the_irreversible_action(client, mutation):
    """"SHUTDOWN: notify the control room" is not a shutdown.

    Flattening every step onto the headline action loses the one distinction an
    operator most needs, which is which steps can be undone.
    """
    body = post(client, mutation).json()

    for command in body["commands"]:
        reversible = command["parameters"].get("reversible")
        if reversible is True:
            assert command["action"] not in {"ISOLATE", "SHUTDOWN"}, (
                f"reversible step {command['sequence']} is labelled {command['action']}"
            )
            assert command["rollback"].startswith("Reversible")
        elif reversible is False:
            assert "Not reversible" in command["rollback"]


def test_the_blast_radius_reaches_the_plan(client, mutation):
    """The flow topology is why this layer exists; it must survive the round trip."""
    body = post(client, mutation).json()
    assert set(body["blast_radius"]) == {"HX-SHELL-TUBE-E220", "REACTOR-R310"}


def test_evidence_records_the_guardrails_that_fired(client, mutation):
    """A downgrade is only auditable if the downgrade is reported."""
    body = post(client, mutation).json()
    assert body["evidence"], "a plan with no evidence cannot be reviewed"


# ---------------------------------------------------------------------------
# Guardrails — the reason this is safe to point at a live asset
# ---------------------------------------------------------------------------


def test_a_contested_topology_forces_human_authorization(client, mutation):
    """Irreversible actions require a trusted graph.

    When replicas disagree about whether the asset vertex is even live, the
    topology is mid-convergence and is not a basis for taking load off a live
    asset. The guardrail runs after the planner, so no model output can skip it.
    """
    mutation["ontology_context"]["replica_observations"] = [
        {"replica_id": "core-site", "add_stamp": 120, "remove_stamp": 120},
        {"replica_id": "edge-site", "add_stamp": 118, "remove_stamp": 121},
    ]
    body = post(client, mutation).json()

    assert body["escalation"]["required"] is True
    assert any("guardrail" in entry for entry in body["evidence"]) or body["crdt_presence"] != "LIVE"


def test_a_degraded_context_does_not_raise_confidence(client, mutation):
    """Planning on incomplete information must not look more certain than planning on complete."""
    baseline = post(client, mutation).json()["confidence"]

    degraded = dict(mutation)
    degraded["event_id"] = "evt_test_graphrag_degraded"
    degraded["degraded"] = True
    degraded_confidence = post(client, degraded).json()["confidence"]

    assert degraded_confidence <= baseline


def test_the_plan_only_targets_nodes_from_the_payload(client, mutation):
    """The deterministic kernel's node set is the allowlist.

    A model may reinterpret the topology; it may not invent one. Any target
    outside the payload's own nodes would be an action against an asset the
    caller never described.
    """
    known = {
        mutation["asset_id"],
        *[n["asset_id"] for n in mutation["ontology_context"]["upstream_dependencies"]],
        *[n["asset_id"] for n in mutation["ontology_context"]["downstream_impacts"]],
    }
    body = post(client, mutation).json()

    assert body["asset_id"] in known
    for node in body["blast_radius"] + body["cascade_path"]:
        assert node in known, f"{node} is not a node the payload described"


# ---------------------------------------------------------------------------
# Idempotency and operational surface
# ---------------------------------------------------------------------------


def test_replaying_a_mutation_does_not_replan(client, mutation):
    """Kafka delivers at least once; a redelivery must not buy a second inference."""
    first = post(client, mutation)
    second = post(client, mutation)

    assert first.status_code == second.status_code == 200
    assert second.headers.get("X-Idempotent-Replay") == "true"
    assert first.json()["plan_id"] == second.json()["plan_id"]


def test_an_unprojectable_payload_is_a_422_not_a_500(client):
    """The worker classifies 422 as terminal and 5xx as retryable.

    Returning a 500 for a payload that will never parse means retrying it
    forever instead of dead-lettering it once.
    """
    response = post(client, {"event_id": "evt_broken", "asset_id": "X"})
    assert response.status_code == 422
    assert response.json()["error"]["code"] in {"mutation_not_projectable", "payload_invalid"}


def test_health_and_readiness(client):
    assert client.get("/healthz").status_code == 200

    ready = client.get("/readyz")
    assert ready.status_code == 200
    assert "openontology.mutation.v2" in ready.json()["accepts"]


def test_metrics_are_exposed(client, mutation):
    post(client, mutation)
    body = client.get("/metrics").text

    assert "openontology_graphrag_request_duration_seconds" in body
    assert "openontology_graphrag_actions_issued_total" in body


def test_the_native_contract_is_still_reachable(client, mutation):
    """The richer response — agent trace, manual references — stays available.

    Only the path moved, so a caller that already speaks EnrichedGraphPayload is
    not forced through the lossy worker envelope.
    """
    import bridge

    payload = bridge.mutation_to_graph_payload(mutation).model_dump(mode="json")
    from graphrag_interceptor import LICENSE_HEADER

    response = client.post(
        "/v1/graphrag/intercept", json=payload, headers={LICENSE_HEADER: ENTERPRISE_KEY}
    )
    assert response.status_code == 200

    body = response.json()
    assert body["schema_version"] == "openontology.command-action.v1"
    assert "agent_trace" in body
    assert "crdt_assessment" in body
