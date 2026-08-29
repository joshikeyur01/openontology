package main

import (
	"encoding/json"
	"testing"
)

// The degraded path is the one the engine works hardest to still deliver: the
// graph is unreachable, and the argument for emitting anyway is that an
// operator would rather have a thin CRITICAL than none.
//
// That argument only holds if the thin payload is parseable. Go marshals a nil
// slice as null rather than [], and the published contract declares these
// fields as arrays, so a consumer validating against openontology.mutation.v1
// rejects them with a type error and dead-letters the alarm — losing exactly
// the mutations the degradation was designed to preserve.
//
// This was a real failure, not a hypothetical: 36 mutations were dead-lettered
// with "Input should be a valid list" before it was found.
func TestDegradedContextMarshalsEmptyArraysNotNull(t *testing.T) {
	// Built the way the graph-unavailable branch builds it: identifier and
	// provenance only, every collection left at its zero value.
	context := OntologyContext{
		AssetID: "TURBOFAN-A320-0417",
		Source:  "unavailable",
	}
	context.normaliseCollections()

	encoded, err := json.Marshal(context)
	if err != nil {
		t.Fatalf("marshal degraded context: %v", err)
	}

	var generic map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &generic); err != nil {
		t.Fatalf("unmarshal degraded context: %v", err)
	}

	for _, field := range []string{"parent_systems", "components", "assigned_operators"} {
		raw, present := generic[field]
		if !present {
			t.Errorf("%s missing from the degraded payload entirely", field)
			continue
		}
		if string(raw) == "null" {
			t.Errorf("%s marshalled as null; the contract declares an array, and a "+
				"consumer validating against it will dead-letter this mutation", field)
		}
		if string(raw) != "[]" {
			t.Errorf("%s = %s, want []", field, raw)
		}
	}
}

// Normalisation must not disturb a context that already carries data — an
// over-eager implementation that replaced rather than filled would silently
// erase the enrichment on the healthy path, which is the larger bug.
func TestNormaliseCollectionsPreservesResolvedContext(t *testing.T) {
	context := OntologyContext{
		AssetID:           "TURBOFAN-A320-0417",
		ParentSystems:     []SystemNode{{NodeID: "SYS-PROP-A320-0417", Name: "Propulsion", Depth: 1}},
		Components:        []string{"hpt_bearing_no3", "lpc_fan_module"},
		AssignedOperators: []Operator{{OperatorID: "OP-4471", Name: "L. Moreau", EscalationOrder: 1}},
	}
	context.normaliseCollections()

	if got := len(context.ParentSystems); got != 1 {
		t.Errorf("ParentSystems length = %d, want 1", got)
	}
	if got := len(context.Components); got != 2 {
		t.Errorf("Components length = %d, want 2", got)
	}
	if got := len(context.AssignedOperators); got != 1 {
		t.Errorf("AssignedOperators length = %d, want 1", got)
	}
	if context.Components[0] != "hpt_bearing_no3" {
		t.Errorf("Components[0] = %q, want hpt_bearing_no3", context.Components[0])
	}
}

// A whole payload, not just the context, so a future field that is a slice and
// gets left nil on the degraded path is caught here rather than in a consumer's
// dead-letter queue.
func TestDegradedPayloadHasNoNullArrays(t *testing.T) {
	payload := EnrichedContextPayload{
		EventID:        newEventID(),
		SchemaVersion:  SchemaVersion,
		Producer:       Producer,
		AssetID:        "TURBOFAN-A320-0417",
		Transition:     TransitionRaised,
		Severity:       SeverityCritical,
		Degraded:       true,
		DegradedReason: "graph_unavailable: dial tcp: connection refused",
		OntologyContext: OntologyContext{
			AssetID: "TURBOFAN-A320-0417",
			Source:  "unavailable",
		},
	}
	payload.OntologyContext.normaliseCollections()

	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	var generic map[string]any
	if err := json.Unmarshal(encoded, &generic); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	var nulls []string
	var walk func(prefix string, value any)
	walk = func(prefix string, value any) {
		switch typed := value.(type) {
		case map[string]any:
			for key, nested := range typed {
				name := key
				if prefix != "" {
					name = prefix + "." + key
				}
				walk(name, nested)
			}
		case nil:
			nulls = append(nulls, prefix)
		}
	}
	walk("", generic)

	// telemetry_snapshot.readings is populated on the real path; here the
	// struct is zero-valued, so it is expected and excluded. Any other null is
	// a field a strict consumer could reject.
	allowed := map[string]bool{"telemetry_snapshot.readings": true}
	for _, field := range nulls {
		if !allowed[field] {
			t.Errorf("degraded payload carries null at %s; a consumer validating "+
				"the published schema may reject it", field)
		}
	}
}
