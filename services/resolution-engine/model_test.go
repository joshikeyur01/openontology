package main

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// RuleEngine
// ---------------------------------------------------------------------------

// boundaryRules uses a limit of 100 and a critical ratio of 0.2 so the promotion
// boundary lands on values that are exactly representable in float64. That makes
// "one ulp below" a meaningful assertion rather than a coin toss.
func boundaryRules() RuleEngine {
	return RuleEngine{
		thresholds: map[string]Threshold{
			SensorTemperatureCelsius: {
				RuleID:   "rule.temperature_celsius.max",
				SensorID: SensorTemperatureCelsius,
				Limit:    100,
				Unit:     "degC",
			},
		},
		criticalRatio: 0.2,
	}
}

// TestRuleEngineSeverityAtCriticalBoundary walks the promotion boundary one ulp
// at a time. The rule is exceeded_pct >= critical_ratio, so the boundary value
// itself must promote.
func TestRuleEngineSeverityAtCriticalBoundary(t *testing.T) {
	const boundary = 120.0 // 100 * (1 + 0.2)

	cases := []struct {
		name         string
		value        float64
		wantBreached bool
		wantSeverity Severity
		wantPct      float64
	}{
		{"far below the limit", 20, false, SeverityInfo, 0},
		{"exactly on the limit does not breach", 100, false, SeverityInfo, 0},
		{"one ulp above the limit breaches as HIGH", math.Nextafter(100, math.Inf(1)), true, SeverityHigh, 0},
		{"midway through the HIGH band", 110, true, SeverityHigh, 0.1},
		{"one ulp below the critical boundary stays HIGH", math.Nextafter(boundary, 0), true, SeverityHigh, 0.2},
		{"exactly on the critical boundary promotes", boundary, true, SeverityCritical, 0.2},
		{"one ulp above the critical boundary promotes", math.Nextafter(boundary, math.Inf(1)), true, SeverityCritical, 0.2},
		{"far above the critical boundary", 500, true, SeverityCritical, 4},
	}

	rules := boundaryRules()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eval, governed := rules.Evaluate(reading("KILN-1", SensorTemperatureCelsius, tc.value, baseTime))
			if !governed {
				t.Fatal("temperature_celsius must be governed by a rule")
			}
			if eval.Breached != tc.wantBreached {
				t.Fatalf("Breached = %t, want %t", eval.Breached, tc.wantBreached)
			}
			if eval.Severity != tc.wantSeverity {
				t.Fatalf("Severity = %s, want %s (exceeded_pct=%.17g, ratio=%.17g)",
					eval.Severity, tc.wantSeverity, eval.ExceededPct, rules.criticalRatio)
			}
			if got := round(eval.ExceededPct, 6); got != tc.wantPct {
				t.Fatalf("ExceededPct = %g, want %g", got, tc.wantPct)
			}
			if eval.Observed != tc.value {
				t.Fatalf("Observed = %g, want %g", eval.Observed, tc.value)
			}
		})
	}
}

// TestRuleEngineCriticalRatioZeroPromotesEverything covers the degenerate
// configuration: with a ratio of zero every breach is immediately CRITICAL,
// because exceeded_pct is always >= 0.
func TestRuleEngineCriticalRatioZeroPromotesEverything(t *testing.T) {
	rules := boundaryRules()
	rules.criticalRatio = 0

	eval, _ := rules.Evaluate(reading("KILN-1", SensorTemperatureCelsius, math.Nextafter(100, math.Inf(1)), baseTime))
	if eval.Severity != SeverityCritical {
		t.Fatalf("Severity = %s, want CRITICAL when the critical ratio is 0", eval.Severity)
	}
}

func TestRuleEngineGovernance(t *testing.T) {
	rules := NewRuleEngine(testConfig("127.0.0.1:0"))

	for _, sensor := range []string{SensorVibrationIndex, SensorTemperatureCelsius} {
		if !rules.Governs(sensor) {
			t.Errorf("Governs(%q) = false, want true", sensor)
		}
	}
	for _, sensor := range []string{"oil_pressure", "", "VIBRATION_INDEX"} {
		if rules.Governs(sensor) {
			t.Errorf("Governs(%q) = true, want false", sensor)
		}
	}

	eval, governed := rules.Evaluate(reading("PUMP-221", "oil_pressure", 9000, baseTime))
	if governed {
		t.Fatal("Evaluate reported an ungoverned sensor as governed")
	}
	if eval != (RuleEvaluation{}) {
		t.Fatalf("Evaluate returned %+v for an ungoverned sensor, want the zero value", eval)
	}
}

func TestRuleEvaluationTrigger(t *testing.T) {
	rules := NewRuleEngine(testConfig("127.0.0.1:0"))
	eval, _ := rules.Evaluate(reading("PUMP-221", SensorVibrationIndex, 10.2, baseTime))

	trigger := eval.Trigger()
	if trigger.RuleID != "rule.vibration_index.max" || trigger.SensorID != SensorVibrationIndex {
		t.Fatalf("Trigger identity = %+v", trigger)
	}
	if trigger.Operator != ">" {
		t.Fatalf("Operator = %q, want \">\"", trigger.Operator)
	}
	if trigger.Threshold != testVibrationLimit || trigger.ObservedValue != 10.2 {
		t.Fatalf("Trigger values = %+v", trigger)
	}
	if trigger.ExceededBy != 1.7 {
		t.Fatalf("ExceededBy = %g, want 1.7 (rounded to 6 places)", trigger.ExceededBy)
	}
	if trigger.ExceededPct != 20.0 {
		t.Fatalf("ExceededPct = %g, want 20 (percent, rounded to 4 places)", trigger.ExceededPct)
	}
	if trigger.Unit != "mm/s" {
		t.Fatalf("Unit = %q, want the threshold's unit", trigger.Unit)
	}
}

// ---------------------------------------------------------------------------
// EventTime
// ---------------------------------------------------------------------------

// TestEventTimeUnmarshal covers every encoding the field gateways are known to
// emit, plus the shapes that must be rejected outright.
func TestEventTimeUnmarshal(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		want    time.Time
		wantErr bool
	}{
		// RFC3339 / RFC3339Nano
		{name: "RFC3339 UTC", input: `"2026-08-13T10:15:30Z"`, want: time.Date(2026, 8, 13, 10, 15, 30, 0, time.UTC)},
		{name: "RFC3339 with offset is normalised to UTC", input: `"2026-08-13T12:15:30+02:00"`, want: time.Date(2026, 8, 13, 10, 15, 30, 0, time.UTC)},
		{name: "RFC3339 with negative offset", input: `"2026-08-13T05:15:30-05:00"`, want: time.Date(2026, 8, 13, 10, 15, 30, 0, time.UTC)},
		{name: "RFC3339Nano", input: `"2026-08-13T10:15:30.123456789Z"`, want: time.Date(2026, 8, 13, 10, 15, 30, 123456789, time.UTC)},
		{name: "RFC3339 with milliseconds", input: `"2026-08-13T10:15:30.250Z"`, want: time.Date(2026, 8, 13, 10, 15, 30, 250000000, time.UTC)},
		// Zoneless local formats the parser also accepts, read as UTC.
		{name: "naive datetime", input: `"2026-08-13T10:15:30"`, want: time.Date(2026, 8, 13, 10, 15, 30, 0, time.UTC)},
		{name: "naive datetime with fraction", input: `"2026-08-13T10:15:30.5"`, want: time.Date(2026, 8, 13, 10, 15, 30, 500000000, time.UTC)},

		// Epoch milliseconds
		{name: "epoch millis", input: `1755079800000`, want: time.UnixMilli(1755079800000).UTC()},
		{name: "epoch millis with sub-second precision", input: `1755079800123`, want: time.UnixMilli(1755079800123).UTC()},
		{name: "epoch zero is the unix epoch, not the zero time", input: `0`, want: time.UnixMilli(0).UTC()},
		{name: "negative epoch millis predate 1970", input: `-1000`, want: time.UnixMilli(-1000).UTC()},

		// Rejected
		{name: "empty JSON string", input: `""`, wantErr: true},
		{name: "null", input: `null`, wantErr: true},
		{name: "free text", input: `"not-a-time"`, wantErr: true},
		{name: "date only", input: `"2026-08-13"`, wantErr: true},
		// RFC 3339 permits a lowercase "t"/"z"; Go's time package does not, so
		// a gateway emitting them is dead-lettered rather than misread.
		{name: "lowercase t and z separators", input: `"2026-08-13t10:15:30z"`, wantErr: true},
		{name: "epoch seconds as a bare number are read as millis, not rejected", input: `1755079800`, want: time.UnixMilli(1755079800).UTC()},
		{name: "epoch millis quoted as a string", input: `"1755079800000"`, wantErr: true},
		{name: "float", input: `1755079800.5`, wantErr: true},
		{name: "boolean", input: `true`, wantErr: true},
		{name: "object", input: `{}`, wantErr: true},
		{name: "array", input: `[]`, wantErr: true},
		{name: "unterminated string", input: `"2026-08-13T10:15:30Z`, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got EventTime
			err := json.Unmarshal([]byte(tc.input), &got)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("Unmarshal(%s) = %s, want an error", tc.input, got.Time)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unmarshal(%s): %v", tc.input, err)
			}
			if !got.Time.Equal(tc.want) {
				t.Fatalf("Unmarshal(%s) = %s, want %s", tc.input, got.Time, tc.want)
			}
			if got.Time.Location() != time.UTC {
				t.Fatalf("Unmarshal(%s) kept location %s, want UTC", tc.input, got.Time.Location())
			}
		})
	}
}

// TestEventTimeUnmarshalInsideAnEvent proves the decoder is reached through the
// real message shape, and that a bad timestamp fails the whole decode rather
// than silently zeroing the field.
func TestEventTimeUnmarshalInsideAnEvent(t *testing.T) {
	var ev TelemetryEvent
	if err := json.Unmarshal([]byte(`{"asset_id":"PUMP-221","sensor_id":"vibration_index","value":9.1,"timestamp":1755079800000}`), &ev); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !ev.Timestamp.Time.Equal(time.UnixMilli(1755079800000).UTC()) {
		t.Fatalf("timestamp = %s", ev.Timestamp.Time)
	}

	if err := json.Unmarshal([]byte(`{"asset_id":"PUMP-221","sensor_id":"vibration_index","value":9.1,"timestamp":"tuesday"}`), &ev); err == nil {
		t.Fatal("decoding a malformed timestamp succeeded, want an error routing the message to the DLQ")
	}
}

func TestEventTimeMarshalRoundTrip(t *testing.T) {
	cases := []time.Time{
		time.Date(2026, 8, 13, 10, 15, 30, 0, time.UTC),
		time.Date(2026, 8, 13, 10, 15, 30, 123456789, time.UTC),
		time.Date(2026, 8, 13, 12, 15, 30, 0, time.FixedZone("CEST", 2*60*60)),
	}

	for _, want := range cases {
		body, err := json.Marshal(EventTime{Time: want})
		if err != nil {
			t.Fatalf("Marshal(%s): %v", want, err)
		}
		if !strings.HasSuffix(string(body), `Z"`) {
			t.Fatalf("Marshal(%s) = %s, want a UTC RFC3339Nano string", want, body)
		}

		var got EventTime
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("Unmarshal(%s): %v", body, err)
		}
		if !got.Time.Equal(want) {
			t.Fatalf("round trip of %s produced %s", want, got.Time)
		}
	}
}

// ---------------------------------------------------------------------------
// Identifier validation and the twin key space
// ---------------------------------------------------------------------------

// TestValidateRejectsIdentifiersThatBreakTheKeySpace is the load-bearing case:
// Redis keys are twin:<asset>:<sensor>, so anything containing a separator, or
// anything empty, would make the key space ambiguous.
func TestValidateRejectsIdentifiersThatBreakTheKeySpace(t *testing.T) {
	cases := []struct {
		name    string
		id      string
		wantOK  bool
		because string
	}{
		{name: "plain alphanumeric", id: "PUMP221", wantOK: true},
		{name: "hyphens and digits", id: "TURBOFAN-A320-0417", wantOK: true},
		{name: "underscores", id: "vibration_index", wantOK: true},
		{name: "dots", id: "line4.pump.221", wantOK: true},
		{name: "single character", id: "A", wantOK: true},
		{name: "128 characters", id: strings.Repeat("a", 128), wantOK: true},

		{name: "empty", id: "", because: "an empty segment collapses the key"},
		{name: "whitespace only", id: "   ", because: "trimmed to empty"},
		{name: "embedded colon", id: "PUMP:221", because: "adds a separator to twin:<asset>:<sensor>"},
		{name: "leading colon", id: ":PUMP221", because: "adds a separator"},
		{name: "trailing colon", id: "PUMP221:", because: "adds a separator"},
		{name: "the key prefix itself", id: "twin:PUMP:vibration_index", because: "would shadow a real twin key"},
		{name: "embedded space", id: "PUMP 221", because: "not in the identifier alphabet"},
		{name: "embedded newline", id: "PUMP221\nvibration", because: "not in the identifier alphabet"},
		// Normalize strips surrounding whitespace, so a trailing newline never
		// reaches the pattern. TestIdentifierPatternIsAnchoredAtEndOfText covers
		// what happens if it ever did.
		{name: "trailing newline is trimmed before validation", id: "PUMP221\n", wantOK: true},
		{name: "leading hyphen", id: "-PUMP221", because: "must start with an alphanumeric"},
		{name: "leading dot", id: ".PUMP221", because: "must start with an alphanumeric"},
		{name: "leading underscore", id: "_PUMP221", because: "must start with an alphanumeric"},
		{name: "129 characters", id: strings.Repeat("a", 129), because: "exceeds the 128 character budget"},
		{name: "non-ascii", id: "PÜMP221", because: "not in the identifier alphabet"},
		{name: "glob metacharacter", id: "PUMP*", because: "not in the identifier alphabet"},
		{name: "pipe, the fingerprint separator", id: "PUMP|221", because: "not in the identifier alphabet"},
	}

	for _, tc := range cases {
		t.Run("asset/"+tc.name, func(t *testing.T) {
			ev := TelemetryEvent{AssetID: tc.id, SensorID: SensorVibrationIndex, Value: 1, Timestamp: EventTime{Time: baseTime}}
			ev.Normalize()
			assertValidity(t, ev, tc.wantOK, tc.because)
		})
		t.Run("sensor/"+tc.name, func(t *testing.T) {
			ev := TelemetryEvent{AssetID: "PUMP-221", SensorID: tc.id, Value: 1, Timestamp: EventTime{Time: baseTime}}
			ev.Normalize()
			assertValidity(t, ev, tc.wantOK, tc.because)
		})
	}
}

// TestIdentifierPatternIsAnchoredAtEndOfText guards the subtlety that makes the
// pattern safe: Go anchors $ at end of text rather than before a trailing
// newline, so an identifier smuggling a newline past normalisation still cannot
// match and inject a separator into the key space.
func TestIdentifierPatternIsAnchoredAtEndOfText(t *testing.T) {
	for _, id := range []string{"PUMP221\n", "PUMP221\n\n", "PUMP221\r\n", "PUMP\n221"} {
		if identifierPattern.MatchString(id) {
			t.Errorf("identifierPattern matched %q", id)
		}
	}
	if !identifierPattern.MatchString("PUMP221") {
		t.Error("identifierPattern rejected a legal identifier")
	}
}

func assertValidity(t *testing.T, ev TelemetryEvent, wantOK bool, because string) {
	t.Helper()
	err := ev.Validate()
	if wantOK && err != nil {
		t.Fatalf("Validate() rejected a legal identifier: %v", err)
	}
	if !wantOK && err == nil {
		t.Fatalf("Validate() accepted %q; it must be rejected because %s", ev.AssetID+"/"+ev.SensorID, because)
	}
}

// TestValidateOtherFields covers the non-identifier half of the contract.
func TestValidateOtherFields(t *testing.T) {
	cases := map[string]struct {
		mutate  func(*TelemetryEvent)
		wantErr bool
	}{
		"valid":               {mutate: func(*TelemetryEvent) {}},
		"zero value is fine":  {mutate: func(e *TelemetryEvent) { e.Value = 0 }},
		"negative value":      {mutate: func(e *TelemetryEvent) { e.Value = -40 }},
		"NaN value":           {mutate: func(e *TelemetryEvent) { e.Value = math.NaN() }, wantErr: true},
		"positive infinity":   {mutate: func(e *TelemetryEvent) { e.Value = math.Inf(1) }, wantErr: true},
		"negative infinity":   {mutate: func(e *TelemetryEvent) { e.Value = math.Inf(-1) }, wantErr: true},
		"zero timestamp":      {mutate: func(e *TelemetryEvent) { e.Timestamp = EventTime{} }, wantErr: true},
		"unit is not policed": {mutate: func(e *TelemetryEvent) { e.Unit = "" }},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ev := reading("PUMP-221", SensorVibrationIndex, 9.0, baseTime)
			tc.mutate(&ev)
			err := ev.Validate()
			if tc.wantErr != (err != nil) {
				t.Fatalf("Validate() error = %v, wantErr = %t", err, tc.wantErr)
			}
		})
	}
}

// TestValidateReportsEveryProblemAtOnce: an operator debugging a gateway should
// see the whole list, not one failure per redeploy.
func TestValidateReportsEveryProblemAtOnce(t *testing.T) {
	ev := TelemetryEvent{AssetID: "bad:asset", SensorID: "bad:sensor", Value: math.NaN()}
	err := ev.Validate()
	if err == nil {
		t.Fatal("Validate() accepted a wholly invalid event")
	}
	for _, want := range []string{"asset_id", "sensor_id", "finite float64", "timestamp"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestNormalize(t *testing.T) {
	cases := []struct {
		name                            string
		in                              TelemetryEvent
		wantAsset, wantSensor, wantUnit string
	}{
		{
			name:       "trims the asset and lower-cases the sensor",
			in:         TelemetryEvent{AssetID: "  Turbofan-A320-0417 ", SensorID: " VIBRATION_INDEX ", Unit: " mm/s "},
			wantAsset:  "Turbofan-A320-0417",
			wantSensor: "vibration_index",
			wantUnit:   "mm/s",
		},
		{
			name:       "asset case is preserved, so twin keys stay case sensitive",
			in:         TelemetryEvent{AssetID: "PUMP-221", SensorID: "Vibration_Index"},
			wantAsset:  "PUMP-221",
			wantSensor: "vibration_index",
		},
		{
			name:       "tabs and newlines are trimmed from the edges",
			in:         TelemetryEvent{AssetID: "\tPUMP-221\n", SensorID: "\nVIBRATION_INDEX\t"},
			wantAsset:  "PUMP-221",
			wantSensor: "vibration_index",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := tc.in
			ev.Normalize()
			if ev.AssetID != tc.wantAsset || ev.SensorID != tc.wantSensor || ev.Unit != tc.wantUnit {
				t.Fatalf("Normalize() = (%q, %q, %q), want (%q, %q, %q)",
					ev.AssetID, ev.SensorID, ev.Unit, tc.wantAsset, tc.wantSensor, tc.wantUnit)
			}
		})
	}
}

// TestNormalizeMakesUppercaseSensorsUsable ties normalisation to the rules: a
// gateway shouting VIBRATION_INDEX must still hit the rule keyed on lowercase.
func TestNormalizeMakesUppercaseSensorsUsable(t *testing.T) {
	rules := NewRuleEngine(testConfig("127.0.0.1:0"))
	ev := TelemetryEvent{AssetID: "PUMP-221", SensorID: "VIBRATION_INDEX", Value: 9.0, Timestamp: EventTime{Time: baseTime}}

	if _, governed := rules.Evaluate(ev); governed {
		t.Fatal("an un-normalised sensor id matched a rule; the test is not proving anything")
	}
	ev.Normalize()
	if _, governed := rules.Evaluate(ev); !governed {
		t.Fatal("a normalised sensor id did not match its rule")
	}
}

// TestKeySpaceIsUnambiguous is the property the identifier pattern exists to
// guarantee: every validated (asset, sensor) pair renders a key with exactly the
// separators the reader expects, and distinct pairs never collide.
func TestKeySpaceIsUnambiguous(t *testing.T) {
	assets := []string{"A", "A-B", "A.B", "A_B", "PUMP221", strings.Repeat("z", 128)}
	sensors := []string{"v", "vibration_index", "temperature_celsius", "a.b-c_d"}

	seen := make(map[string]string)
	for _, asset := range assets {
		for _, sensor := range sensors {
			ev := TelemetryEvent{AssetID: asset, SensorID: sensor, Value: 1, Timestamp: EventTime{Time: baseTime}}
			if err := ev.Validate(); err != nil {
				t.Fatalf("fixture (%q, %q) is not valid: %v", asset, sensor, err)
			}

			key := ev.CacheKey()
			if got := strings.Count(key, ":"); got != 2 {
				t.Fatalf("CacheKey(%q, %q) = %q has %d colons, want exactly 2", asset, sensor, key, got)
			}
			if !strings.HasPrefix(key, "twin:") {
				t.Fatalf("CacheKey(%q, %q) = %q, want the twin: namespace", asset, sensor, key)
			}

			pair := asset + "\x00" + sensor
			if prior, clash := seen[key]; clash && prior != pair {
				t.Fatalf("key %q is produced by both %q and %q", key, prior, pair)
			}
			seen[key] = pair
		}
	}
}

// TestKeyNamespacesDoNotOverlap: a claim, an index entry and an alarm record
// must never be able to shadow live twin state.
func TestKeyNamespacesDoNotOverlap(t *testing.T) {
	asset, sensor := "PUMP-221", SensorVibrationIndex

	keys := map[string]string{
		"twin":   twinKey(asset, sensor),
		"index":  assetIndexKey(asset),
		"alarm":  alarmKey(asset, sensor),
		"dedupe": DefaultIdempotencyKeyPrefix + "deadbeef",
	}

	seen := make(map[string]string, len(keys))
	for name, key := range keys {
		if prior, clash := seen[key]; clash {
			t.Fatalf("%s and %s render the same key %q", prior, name, key)
		}
		seen[key] = name
	}
	for name, key := range keys {
		for otherName, other := range keys {
			if name == otherName {
				continue
			}
			if strings.HasPrefix(key, other) {
				t.Fatalf("%s key %q is prefixed by the %s key %q", name, key, otherName, other)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Payload helpers
// ---------------------------------------------------------------------------

func TestSeverityRank(t *testing.T) {
	cases := map[Severity]int{
		SeverityCritical: 3,
		SeverityHigh:     2,
		SeverityInfo:     1,
		Severity(""):     0,
		Severity("WAT"):  0,
	}
	for severity, want := range cases {
		if got := severity.Rank(); got != want {
			t.Errorf("Severity(%q).Rank() = %d, want %d", severity, got, want)
		}
	}
	if !(SeverityCritical.Rank() > SeverityHigh.Rank() && SeverityHigh.Rank() > SeverityInfo.Rank()) {
		t.Fatal("severity ranks are not strictly ordered")
	}
}

// TestOntologyContextCloneIsDeep guards the graph cache: a consumer goroutine
// mutating its copy must not corrupt the cached entry other workers will read.
func TestOntologyContextCloneIsDeep(t *testing.T) {
	original := richContext()
	clone := original.Clone()

	clone.ParentSystems[0].Name = "MUTATED"
	clone.Components[0] = "MUTATED"
	clone.AssignedOperators[0].Name = "MUTATED"
	clone.Components = append(clone.Components, "extra")

	if original.ParentSystems[0].Name == "MUTATED" {
		t.Error("Clone shares the ParentSystems backing array")
	}
	if original.Components[0] == "MUTATED" || len(original.Components) != 2 {
		t.Error("Clone shares the Components backing array")
	}
	if original.AssignedOperators[0].Name == "MUTATED" {
		t.Error("Clone shares the AssignedOperators backing array")
	}
}

// TestCloneDistinguishesNilFromEmpty guards the JSON contract: an asset the
// graph holds with no modelled parents must serialise as [] rather than null,
// or a consumer ranging over parent_systems breaks on it.
func TestCloneDistinguishesNilFromEmpty(t *testing.T) {
	empty := OntologyContext{
		ParentSystems:     []SystemNode{},
		Components:        []string{},
		AssignedOperators: []Operator{},
	}.Clone()

	if empty.ParentSystems == nil || empty.Components == nil || empty.AssignedOperators == nil {
		t.Fatalf("Clone turned empty slices into nil: %+v", empty)
	}
	body, err := json.Marshal(empty)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, want := range []string{`"parent_systems":[]`, `"components":[]`, `"assigned_operators":[]`} {
		if !strings.Contains(string(body), want) {
			t.Errorf("encoded context %s is missing %s", body, want)
		}
	}

	absent := OntologyContext{}.Clone()
	if absent.ParentSystems != nil || absent.Components != nil || absent.AssignedOperators != nil {
		t.Fatalf("Clone invented slices for an absent neighbourhood: %+v", absent)
	}
}

func TestPrimaryOperator(t *testing.T) {
	t.Run("lowest escalation order wins regardless of slice order", func(t *testing.T) {
		op, ok := richContext().PrimaryOperator()
		if !ok {
			t.Fatal("PrimaryOperator reported no operator")
		}
		if op.OperatorID != "OP-8801" {
			t.Fatalf("PrimaryOperator = %s, want OP-8801 (escalation order 1)", op.OperatorID)
		}
	})

	t.Run("no operators", func(t *testing.T) {
		if _, ok := (OntologyContext{}).PrimaryOperator(); ok {
			t.Fatal("PrimaryOperator reported an operator for an empty context")
		}
	})
}

func TestRound(t *testing.T) {
	cases := []struct {
		value  float64
		places int
		want   float64
	}{
		{1.23456789, 6, 1.234568},
		{1.5, 0, 2},
		{-1.23456789, 4, -1.2346},
		{0.1 + 0.2, 6, 0.3},
		{math.NaN(), 6, 0},
		{math.Inf(1), 6, 0},
		{math.Inf(-1), 6, 0},
	}
	for _, tc := range cases {
		if got := round(tc.value, tc.places); got != tc.want {
			t.Errorf("round(%v, %d) = %v, want %v", tc.value, tc.places, got, tc.want)
		}
	}
}

func TestNewEventID(t *testing.T) {
	seen := make(map[string]bool, 1000)
	for i := 0; i < 1000; i++ {
		id := newEventID()
		if !strings.HasPrefix(id, "evt_") {
			t.Fatalf("newEventID() = %q, want an evt_ prefix", id)
		}
		if len(id) != len("evt_")+24 {
			t.Fatalf("newEventID() = %q, want 12 hex-encoded bytes", id)
		}
		if seen[id] {
			t.Fatalf("newEventID() collided on %q", id)
		}
		seen[id] = true
	}
}
