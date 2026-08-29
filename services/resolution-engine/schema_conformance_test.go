package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// The published contracts in schemas/ are generated from the interceptor's
// Pydantic models — the consumer's view of the wire. This file is what stops
// that view drifting from the producer's.
//
// A JSON Schema validator would be the obvious tool, but it would mean a
// dependency in go.mod for one test. The drift that actually happens is a field
// renamed, added or dropped on one side only, and comparing the emitted key set
// against the schema's declared properties catches all three without one.

// The current contract. Superseded versions stay in schemas/ as frozen
// artifacts; the producer is only ever checked against the one it emits, which
// SchemaVersion in model.go names.
const mutationSchemaPath = "../../schemas/openontology.mutation.v2.schema.json"

type jsonSchema struct {
	Properties           map[string]json.RawMessage `json:"properties"`
	Required             []string                   `json:"required"`
	AdditionalProperties *bool                      `json:"additionalProperties"`
}

func loadMutationSchema(t *testing.T) jsonSchema {
	t.Helper()

	raw, err := os.ReadFile(filepath.FromSlash(mutationSchemaPath))
	if err != nil {
		t.Fatalf("read published schema: %v\n"+
			"schemas/ is generated; run: python tools/export_schemas.py", err)
	}

	var schema jsonSchema
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatalf("parse published schema: %v", err)
	}
	if len(schema.Properties) == 0 {
		t.Fatal("published schema declares no properties; it is probably truncated")
	}
	return schema
}

// marshalledKeys returns the JSON object keys the engine actually emits for a
// fully populated mutation.
func marshalledKeys(t *testing.T, payload EnrichedContextPayload) map[string]bool {
	t.Helper()

	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal mutation: %v", err)
	}

	var generic map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &generic); err != nil {
		t.Fatalf("unmarshal mutation: %v", err)
	}

	keys := make(map[string]bool, len(generic))
	for key := range generic {
		keys[key] = true
	}
	return keys
}

// samplePayload is a mutation with every optional field populated, so the key
// set under test is the widest the engine can produce. DegradedReason is
// omitempty, and a degraded=false payload would hide a rename of it.
func samplePayload() EnrichedContextPayload {
	return EnrichedContextPayload{
		EventID:        newEventID(),
		SchemaVersion:  SchemaVersion,
		Producer:       Producer,
		AssetID:        "TURBOFAN-A320-0417",
		Transition:     TransitionRaised,
		Severity:       SeverityCritical,
		BreachCount:    1,
		Degraded:       true,
		DegradedReason: "graph resolution exceeded its budget",

		// Replication provenance is omitempty, so a payload without it would
		// hide a rename of any of these three from the check below.
		OriginReplica: "core-site",
		LamportClock:  42,
		GraphRevision: "8f1c...",

		OntologyContext: OntologyContext{
			AssetID: "TURBOFAN-A320-0417",
			ReplicaObservations: []ReplicaObservation{
				{ReplicaID: "core-site", AddStamp: 42},
			},
		},
	}
}

func TestEmittedMutationMatchesPublishedSchema(t *testing.T) {
	schema := loadMutationSchema(t)
	emitted := marshalledKeys(t, samplePayload())

	var undeclared []string
	for key := range emitted {
		if _, ok := schema.Properties[key]; !ok {
			undeclared = append(undeclared, key)
		}
	}
	sort.Strings(undeclared)

	if len(undeclared) > 0 {
		t.Errorf("engine emits fields the published schema does not declare: %v\n"+
			"either the field is internal and should carry json:\"-\", or the "+
			"contract moved and schemas/ needs regenerating from the interceptor "+
			"models: python tools/export_schemas.py", undeclared)
	}

	var missing []string
	for key := range schema.Properties {
		if !emitted[key] {
			missing = append(missing, key)
		}
	}
	sort.Strings(missing)

	if len(missing) > 0 {
		t.Errorf("published schema declares fields the engine never emits: %v\n"+
			"a consumer written against the contract would read these as absent", missing)
	}
}

// The required list is the half of the contract a consumer will reject on, so
// it gets its own assertion: a required field the engine omits is a 422 at the
// paid API rather than a field a caller can shrug off.
func TestEveryRequiredSchemaFieldIsEmitted(t *testing.T) {
	schema := loadMutationSchema(t)
	emitted := marshalledKeys(t, samplePayload())

	var missing []string
	for _, key := range schema.Required {
		if !emitted[key] {
			missing = append(missing, key)
		}
	}
	sort.Strings(missing)

	if len(missing) > 0 {
		t.Fatalf("published schema requires fields the engine does not emit: %v", missing)
	}
}
