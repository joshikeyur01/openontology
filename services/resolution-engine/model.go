package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	// SchemaVersion is carried on every emitted mutation. Consumers (notably
	// the commercial AI interceptor) pin against it.
	//
	// v2 adds the process flow around the asset — upstream_dependencies,
	// downstream_impacts, blast_radius — plus model_number. The fields are
	// additive, but the version moved anyway: a consumer that plans a
	// containment action needs to know whether an empty blast radius means
	// "nothing downstream" or "this producer does not populate it", and only a
	// version can tell it which.
	SchemaVersion = "openontology.mutation.v2"

	// SchemaVersionV1 is the previous contract, still accepted by the
	// interceptor for the length of a rollout.
	SchemaVersionV1 = "openontology.mutation.v1"

	// Producer identifies the emitting component in the mutation payload.
	Producer = "ontology-resolution-engine"

	// Sensor identifiers governed by the built-in anomaly rules.
	SensorVibrationIndex     = "vibration_index"
	SensorTemperatureCelsius = "temperature_celsius"
)

// identifierPattern constrains asset and sensor identifiers. Redis keys are
// built as twin:<asset>:<sensor>, so separators inside an identifier would
// make the key space ambiguous.
var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// Severity ranks how far past a limit a reading has travelled.
type Severity string

const (
	SeverityInfo     Severity = "INFO"
	SeverityHigh     Severity = "HIGH"
	SeverityCritical Severity = "CRITICAL"
)

// Rank orders severities so escalation can be detected numerically.
func (s Severity) Rank() int {
	switch s {
	case SeverityCritical:
		return 3
	case SeverityHigh:
		return 2
	case SeverityInfo:
		return 1
	default:
		return 0
	}
}

// EventTime accepts RFC3339 strings and epoch milliseconds, because edge
// gateways in the field disagree about timestamp encoding. It always
// serialises back out as RFC3339 with nanosecond precision in UTC.
type EventTime struct {
	time.Time
}

func (t *EventTime) UnmarshalJSON(b []byte) error {
	raw := strings.TrimSpace(string(b))
	switch raw {
	case "", "null", `""`:
		return errors.New("timestamp is required")
	}

	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return fmt.Errorf("timestamp: %w", err)
		}
		layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05"}
		for _, layout := range layouts {
			if parsed, err := time.Parse(layout, s); err == nil {
				t.Time = parsed.UTC()
				return nil
			}
		}
		return fmt.Errorf("timestamp %q is not RFC3339 or epoch milliseconds", s)
	}

	millis, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fmt.Errorf("timestamp %q is not RFC3339 or epoch milliseconds", raw)
	}
	t.Time = time.UnixMilli(millis).UTC()
	return nil
}

func (t EventTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Time.UTC().Format(time.RFC3339Nano))
}

// TelemetryEvent is one multi-variable sample as published on telemetry.raw.
type TelemetryEvent struct {
	AssetID   string    `json:"asset_id"`
	SensorID  string    `json:"sensor_id"`
	Value     float64   `json:"value"`
	Unit      string    `json:"unit,omitempty"`
	Timestamp EventTime `json:"timestamp"`
}

// Normalize trims and canonicalises identifiers before validation so that
// "  Turbofan-A320-0417 " and "VIBRATION_INDEX" resolve to the same twin.
func (e *TelemetryEvent) Normalize() {
	e.AssetID = strings.TrimSpace(e.AssetID)
	e.SensorID = strings.ToLower(strings.TrimSpace(e.SensorID))
	e.Unit = strings.TrimSpace(e.Unit)
}

// Validate enforces the exact typing contract the rest of the pipeline relies
// on. A failure here routes the message to the dead-letter topic rather than
// wedging the partition.
func (e TelemetryEvent) Validate() error {
	var errs []error
	if !identifierPattern.MatchString(e.AssetID) {
		errs = append(errs, fmt.Errorf("asset_id %q must match %s", e.AssetID, identifierPattern))
	}
	if !identifierPattern.MatchString(e.SensorID) {
		errs = append(errs, fmt.Errorf("sensor_id %q must match %s", e.SensorID, identifierPattern))
	}
	if math.IsNaN(e.Value) || math.IsInf(e.Value, 0) {
		errs = append(errs, errors.New("value must be a finite float64"))
	}
	if e.Timestamp.IsZero() {
		errs = append(errs, errors.New("timestamp must be present and non-zero"))
	}
	return errors.Join(errs...)
}

// CacheKey is the live-state key mandated by the ontology spec.
func (e TelemetryEvent) CacheKey() string { return twinKey(e.AssetID, e.SensorID) }

func twinKey(assetID, sensorID string) string {
	return "twin:" + assetID + ":" + sensorID
}

// assetIndexKey holds the set of sensors seen for an asset. It lives in its
// own namespace so a sensor can never collide with the index itself, and it
// removes any need for SCAN/KEYS when building a snapshot.
func assetIndexKey(assetID string) string { return "twinindex:" + assetID }

// alarmKey holds the mutated alarm state for one twin channel.
func alarmKey(assetID, sensorID string) string {
	return "twinalarm:" + assetID + ":" + sensorID
}

// Threshold is a single "greater than" anomaly rule.
type Threshold struct {
	RuleID      string  `json:"rule_id"`
	SensorID    string  `json:"sensor_id"`
	Limit       float64 `json:"limit"`
	Unit        string  `json:"unit"`
	Description string  `json:"description"`
}

// RuleEvaluation is the verdict for one reading against its threshold.
type RuleEvaluation struct {
	Threshold   Threshold
	Observed    float64
	Breached    bool
	Severity    Severity
	ExceededBy  float64
	ExceededPct float64
}

// RuleEngine evaluates readings against the configured thresholds. It holds no
// mutable state, so a single value is safe to share across all workers.
type RuleEngine struct {
	thresholds    map[string]Threshold
	criticalRatio float64
}

// NewRuleEngine builds the evaluator from configuration.
func NewRuleEngine(cfg Config) RuleEngine {
	return RuleEngine{thresholds: cfg.Thresholds, criticalRatio: cfg.CriticalRatio}
}

// Governs reports whether a sensor has an anomaly rule attached.
func (r RuleEngine) Governs(sensorID string) bool {
	_, ok := r.thresholds[sensorID]
	return ok
}

// Evaluate applies the rule for the reading's sensor. The second return value
// is false when no rule governs the sensor, in which case the reading is
// cached but never considered for a state mutation.
func (r RuleEngine) Evaluate(ev TelemetryEvent) (RuleEvaluation, bool) {
	threshold, ok := r.thresholds[ev.SensorID]
	if !ok {
		return RuleEvaluation{}, false
	}

	eval := RuleEvaluation{
		Threshold: threshold,
		Observed:  ev.Value,
		Severity:  SeverityInfo,
	}
	if ev.Value <= threshold.Limit {
		return eval, true
	}

	eval.Breached = true
	eval.ExceededBy = ev.Value - threshold.Limit
	if threshold.Limit != 0 {
		eval.ExceededPct = eval.ExceededBy / math.Abs(threshold.Limit)
	}
	if eval.ExceededPct >= r.criticalRatio {
		eval.Severity = SeverityCritical
	} else {
		eval.Severity = SeverityHigh
	}
	return eval, true
}

// Trigger renders the evaluation into the wire representation.
func (e RuleEvaluation) Trigger() RuleTrigger {
	return RuleTrigger{
		RuleID:        e.Threshold.RuleID,
		SensorID:      e.Threshold.SensorID,
		Operator:      ">",
		Threshold:     e.Threshold.Limit,
		Unit:          e.Threshold.Unit,
		ObservedValue: e.Observed,
		ExceededBy:    round(e.ExceededBy, 6),
		ExceededPct:   round(e.ExceededPct*100, 4),
		Description:   e.Threshold.Description,
	}
}

// ---------------------------------------------------------------------------
// Enriched Context Payload — the contract with ontology.mutations consumers.
// ---------------------------------------------------------------------------

// SensorReading is one live value read back out of the Redis state cache.
type SensorReading struct {
	SensorID   string    `json:"sensor_id"`
	Value      float64   `json:"value"`
	Unit       string    `json:"unit,omitempty"`
	ObservedAt time.Time `json:"observed_at"`
	AgeSeconds float64   `json:"age_seconds"`
}

// TelemetrySnapshot is the multi-variable state of the asset at mutation time.
type TelemetrySnapshot struct {
	Trigger    SensorReading   `json:"trigger"`
	Readings   []SensorReading `json:"readings"`
	CapturedAt time.Time       `json:"captured_at"`
	Complete   bool            `json:"complete"`
}

// Operator is a human accountable for the asset, resolved from the graph.
type Operator struct {
	OperatorID      string `json:"operator_id"`
	Name            string `json:"name"`
	Role            string `json:"role"`
	Shift           string `json:"shift,omitempty"`
	Contact         string `json:"contact,omitempty"`
	EscalationOrder int    `json:"escalation_order"`
}

// SystemNode is one ancestor in the asset's containment hierarchy.
type SystemNode struct {
	NodeID string `json:"node_id"`
	Name   string `json:"name"`
	Type   string `json:"type"`
	Depth  int    `json:"depth"`
}

// OntologyContext is the graph neighbourhood of the anomalous asset.
type OntologyContext struct {
	AssetID           string       `json:"asset_id"`
	AssetName         string       `json:"asset_name"`
	AssetClass        string       `json:"asset_class"`
	Site              string       `json:"site"`
	Criticality       string       `json:"criticality"`
	ModelNumber       string       `json:"model_number,omitempty"`
	ParentSystems     []SystemNode `json:"parent_systems"`
	Components        []string     `json:"components"`
	AssignedOperators []Operator   `json:"assigned_operators"`

	// Process flow around the asset, nearest first. Upstream is what supplies
	// it; Downstream is what stops or runs unsupervised if it is isolated.
	// BlastRadius is len(Downstream), carried explicitly so a consumer can size
	// a containment decision without walking the list.
	UpstreamDependencies []FlowRef `json:"upstream_dependencies"`
	DownstreamImpacts    []FlowRef `json:"downstream_impacts"`
	BlastRadius          int       `json:"blast_radius"`

	// ReplicaObservations is the per-replica Lamport timeline for this asset's
	// vertex. Empty when replication is disabled, which is the single-site case.
	ReplicaObservations []ReplicaObservation `json:"replica_observations,omitempty"`
	MaintenanceWindow   string               `json:"maintenance_window,omitempty"`
	ResolvedAt          time.Time            `json:"resolved_at"`
	Source              string               `json:"source"`
	CacheHit            bool                 `json:"cache_hit"`
}

// FlowRef is one asset in the process flow around the target.
//
// Hops distinguishes an immediate consequence from a knock-on one: a pump that
// feeds the exchanger directly is one hop, the cooler four stages downstream is
// four. A containment decision reads that number.
type FlowRef struct {
	AssetID  string `json:"asset_id"`
	Name     string `json:"name,omitempty"`
	Model    string `json:"model_number,omitempty"`
	Status   string `json:"status,omitempty"`
	Relation string `json:"relation"`
	Hops     int    `json:"hops"`
}

// normaliseCollections replaces nil slices with empty ones.
//
// This is not cosmetic. Go marshals a nil slice as JSON null, not as [], and
// the published contract declares these three fields as arrays. A consumer
// validating against openontology.mutation.v1 rejects null with a type error,
// so a payload assembled without them — the degraded path, which leaves the
// context almost empty by design — is unparseable at the far end and gets
// dead-lettered.
//
// That failure lands exactly backwards: mutations emitted while the graph is
// unavailable are the ones the engine works hardest to still deliver, and they
// were the only ones the closure loop threw away.
func (o *OntologyContext) normaliseCollections() {
	if o.ParentSystems == nil {
		o.ParentSystems = []SystemNode{}
	}
	if o.Components == nil {
		o.Components = []string{}
	}
	if o.AssignedOperators == nil {
		o.AssignedOperators = []Operator{}
	}
	if o.UpstreamDependencies == nil {
		o.UpstreamDependencies = []FlowRef{}
	}
	if o.DownstreamImpacts == nil {
		o.DownstreamImpacts = []FlowRef{}
	}
}

// Clone deep-copies the context so cached values can never be mutated by a
// consumer goroutine.
func (o OntologyContext) Clone() OntologyContext {
	out := o
	out.ParentSystems = cloneSlice(o.ParentSystems)
	out.Components = cloneSlice(o.Components)
	out.UpstreamDependencies = cloneSlice(o.UpstreamDependencies)
	out.DownstreamImpacts = cloneSlice(o.DownstreamImpacts)
	out.AssignedOperators = cloneSlice(o.AssignedOperators)
	return out
}

// cloneSlice copies s while preserving the difference between nil and empty.
// append([]T(nil), empty...) returns nil, which would put "parent_systems":
// null on the wire for an asset the graph holds with no modelled parents —
// a shape consumers ranging over the list do not expect.
func cloneSlice[T any](s []T) []T {
	if s == nil {
		return nil
	}
	return append(make([]T, 0, len(s)), s...)
}

// PrimaryOperator returns the lowest escalation order operator, which the AI
// layer uses as the default assignee.
func (o OntologyContext) PrimaryOperator() (Operator, bool) {
	best := Operator{}
	found := false
	for _, op := range o.AssignedOperators {
		if !found || op.EscalationOrder < best.EscalationOrder {
			best, found = op, true
		}
	}
	return best, found
}

// RuleTrigger describes exactly which rule fired and by how much.
type RuleTrigger struct {
	RuleID        string  `json:"rule_id"`
	SensorID      string  `json:"sensor_id"`
	Operator      string  `json:"operator"`
	Threshold     float64 `json:"threshold"`
	Unit          string  `json:"unit,omitempty"`
	ObservedValue float64 `json:"observed_value"`
	ExceededBy    float64 `json:"exceeded_by"`
	ExceededPct   float64 `json:"exceeded_pct"`
	Description   string  `json:"description,omitempty"`
}

// EnrichedContextPayload is what lands on ontology.mutations.
type EnrichedContextPayload struct {
	EventID            string            `json:"event_id"`
	SchemaVersion      string            `json:"schema_version"`
	Producer           string            `json:"producer"`
	EmittedAt          time.Time         `json:"emitted_at"`
	AssetID            string            `json:"asset_id"`
	Transition         TransitionKind    `json:"transition"`
	Severity           Severity          `json:"severity"`
	AnomalyActiveSince time.Time         `json:"anomaly_active_since"`
	BreachCount        uint64            `json:"breach_count"`
	Rule               RuleTrigger       `json:"rule"`
	TelemetrySnapshot  TelemetrySnapshot `json:"telemetry_snapshot"`
	OntologyContext    OntologyContext   `json:"ontology_context"`
	Degraded           bool              `json:"degraded"`
	DegradedReason     string            `json:"degraded_reason,omitempty"`

	// Replication provenance. OriginReplica names the engine that produced this
	// mutation, LamportClock is its logical clock at emission, and GraphRevision
	// is the content hash of the topology it resolved against.
	//
	// Together they answer a question a consumer planning an irreversible action
	// has to ask: was the graph this decision rests on the converged one, or a
	// site's local view mid-partition? Two mutations carrying different
	// graph_revisions for the same asset were planned against different
	// topologies.
	OriginReplica   string `json:"origin_replica,omitempty"`
	LamportClock    int64  `json:"lamport_clock,omitempty"`
	GraphRevision   string `json:"graph_revision,omitempty"`
	SourcePartition int    `json:"source_partition"`
	SourceOffset    int64  `json:"source_offset"`
}

// newEventID mints a collision-resistant identifier for one mutation.
func newEventID() string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand failing is fatal-adjacent; fall back to a timestamp so
		// the pipeline degrades instead of dropping the mutation.
		return fmt.Sprintf("evt_%d", time.Now().UnixNano())
	}
	return "evt_" + hex.EncodeToString(buf)
}

// round trims float noise so payloads stay readable and diffable.
func round(v float64, places int) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	factor := math.Pow(10, float64(places))
	return math.Round(v*factor) / factor
}
