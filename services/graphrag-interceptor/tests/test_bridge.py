"""The projection between the open-core contract and the GraphRAG layer.

These are the tests that matter most for this service. The two-agent loop is
only as good as what it is handed, and every field below was a real shape
mismatch between what the engine emits and what the module was written against.
"""

from __future__ import annotations

import copy

import pytest

import bridge
from conftest import MUTATION_V2


def project(mutation):
    return bridge.mutation_to_graph_payload(mutation)


def test_projects_a_complete_mutation(mutation):
    payload = project(mutation)

    assert payload.event_id == "evt_test_graphrag_0001"
    assert payload.asset_metadata.uuid == "HPP-PUMP-221"
    assert payload.asset_metadata.name == "Hydraulic Power Pack Pump 221"
    assert payload.asset_metadata.model_number == "A11VO-190"
    assert payload.ontology_context.physical_location == "PLANT-ROTTERDAM-L4"
    assert payload.graph_revision == mutation["graph_revision"]
    assert payload.origin_replica == "core-site"
    assert payload.lamport_clock == 148


def test_parent_system_collapses_to_the_immediate_parent(mutation):
    """The ancestry list is ordered by depth; the isolation scope is the nearest.

    Taking any other element would scope a containment decision to the plant
    when it belongs to the hydraulic loop.
    """
    payload = project(mutation)
    assert payload.ontology_context.parent_system == "SYS-HYD-L4"


def test_flow_ordering_is_preserved(mutation):
    """Nearest-first is contract on both sides.

    The engine emits it that way and Agent 1 walks the upstream list to find a
    cascade origin, so a reordering here would make it attribute the fault to
    the wrong node.
    """
    payload = project(mutation)
    assert payload.ontology_context.upstream_dependencies == [
        "SUCTION-STRAINER-S14",
        "FEED-DRUM-D101",
    ]
    assert payload.ontology_context.downstream_impacts == [
        "HX-SHELL-TUBE-E220",
        "REACTOR-R310",
    ]
    assert payload.ontology_context.nearest_upstream == "SUCTION-STRAINER-S14"


def test_primary_operator_is_the_first_call_not_the_first_listed(mutation):
    """Escalation order decides, not array position.

    The fixture deliberately lists the supervisor (order 2) before the
    technician (order 1) — an engine is free to emit them unsorted.
    """
    payload = project(mutation)
    assert payload.ontology_context.assigned_operator == "J. de Vries"


def test_channels_flatten_trigger_and_snapshot(mutation):
    payload = project(mutation)
    channels = payload.telemetry_snapshot.channels()

    assert channels["vibration_index"] == pytest.approx(11.4)
    assert channels["temperature_celsius"] == pytest.approx(104.2)
    assert channels["oil_pressure_bar"] == pytest.approx(4.2)


def test_the_trigger_wins_over_a_stale_snapshot_copy(mutation):
    """The trigger is the reading that fired the rule.

    The snapshot may carry an older value for the same channel; taking it would
    plan against a reading the rule did not fire on.
    """
    mutation["telemetry_snapshot"]["readings"].append(
        {"sensor_id": "vibration_index", "value": 3.0, "unit": "mm/s",
         "observed_at": "2026-08-13T21:30:00Z", "age_seconds": 300}
    )
    payload = project(mutation)
    assert payload.telemetry_snapshot.channels()["vibration_index"] == pytest.approx(11.4)


def test_replica_observations_pass_through(mutation):
    payload = project(mutation)
    observations = payload.ontology_context.replica_observations

    assert {o.replica_id for o in observations} == {"core-site", "edge-site"}
    assert all(o.local_presence.value == "LIVE" for o in observations)


def test_a_tombstoned_replica_is_carried_as_tombstoned(mutation):
    """The guardrail layer reads this to refuse an irreversible action.

    Strictly greater is liveness; equal stamps are a tie and a tie resolves to
    removed. Getting that backwards would let a plan act on a vertex the fleet
    is in the middle of deleting.
    """
    mutation["ontology_context"]["replica_observations"] = [
        {"replica_id": "core-site", "add_stamp": 10, "remove_stamp": 12},
        {"replica_id": "edge-site", "add_stamp": 10, "remove_stamp": 10},
    ]
    payload = project(mutation)
    presence = {o.replica_id: o.local_presence.value for o in payload.ontology_context.replica_observations}

    assert presence["core-site"] == "TOMBSTONED"
    assert presence["edge-site"] == "TOMBSTONED", "equal stamps are a tie, and a tie is not liveness"


def test_a_self_referential_flow_edge_is_dropped(mutation):
    """A cyclic graph can put an asset in its own dependency chain.

    GraphRAG rejects that outright, so it is resolved here — the engine was not
    wrong to emit what the graph actually contains.
    """
    mutation["ontology_context"]["downstream_impacts"].append(
        {"asset_id": "HPP-PUMP-221", "name": "itself", "relation": "IMPACTS", "hops": 3}
    )
    payload = project(mutation)
    assert "HPP-PUMP-221" not in payload.ontology_context.downstream_impacts


def test_a_node_on_both_sides_is_kept_downstream(mutation):
    """A loop in the flow network puts a node upstream and downstream at once.

    It is kept downstream because that is the side a containment decision reads:
    what stops if this asset is isolated.
    """
    mutation["ontology_context"]["upstream_dependencies"].append(
        {"asset_id": "REACTOR-R310", "name": "Reactor R-310", "relation": "SUPPLIES", "hops": 3}
    )
    payload = project(mutation)

    assert "REACTOR-R310" in payload.ontology_context.downstream_impacts
    assert "REACTOR-R310" not in payload.ontology_context.upstream_dependencies


def test_model_number_falls_back_rather_than_failing(mutation):
    """model_number is required and non-empty, and drives manual retrieval.

    An asset the graph has no model number for must still produce a plan.
    """
    del mutation["ontology_context"]["model_number"]
    payload = project(mutation)
    assert payload.asset_metadata.model_number == "industrial.hydraulics.pump"

    del mutation["ontology_context"]["asset_class"]
    payload = project(mutation)
    assert payload.asset_metadata.model_number == "HPP-PUMP-221"


def test_an_unresolvable_context_still_projects(mutation):
    """The degraded path is the one the engine works hardest to deliver.

    An empty ancestry, no operators and no flow is exactly what a
    graph-unavailable mutation carries, and it must not be the one payload the
    paid layer refuses.
    """
    context = mutation["ontology_context"]
    context["parent_systems"] = []
    context["assigned_operators"] = []
    context["upstream_dependencies"] = []
    context["downstream_impacts"] = []
    context["site"] = ""
    mutation["degraded"] = True

    payload = project(mutation)

    assert payload.degraded is True
    assert payload.ontology_context.assigned_operator == "Unassigned"
    assert payload.ontology_context.physical_location == "UNKNOWN-SITE"
    assert payload.asset_metadata.current_status.value == "UNKNOWN", (
        "a degraded context must not claim the asset is OPERATIONAL"
    )


def test_a_v1_mutation_projects_without_flow(mutation):
    """v1 predates the flow topology. It should still plan, with no blast radius."""
    mutation["schema_version"] = "openontology.mutation.v1"
    for field in ("upstream_dependencies", "downstream_impacts", "replica_observations"):
        mutation["ontology_context"].pop(field, None)

    payload = project(mutation)
    assert payload.ontology_context.upstream_dependencies == []
    assert payload.ontology_context.downstream_impacts == []
    assert payload.ontology_context.blast_radius_size == 0


def test_a_mutation_with_no_channels_is_rejected(mutation):
    """A payload with nothing to evaluate is not plannable, and should say so."""
    mutation["telemetry_snapshot"] = {"trigger": None, "readings": []}
    with pytest.raises(Exception):
        project(mutation)


def test_malformed_node_ids_are_dropped_not_fatal(mutation):
    """One unmappable neighbour must not cost the alarm its plan."""
    mutation["ontology_context"]["downstream_impacts"].append(
        {"asset_id": "not a valid node id!", "name": "bad", "relation": "IMPACTS", "hops": 1}
    )
    payload = project(mutation)

    assert payload.ontology_context.downstream_impacts == ["HX-SHELL-TUBE-E220", "REACTOR-R310"]
