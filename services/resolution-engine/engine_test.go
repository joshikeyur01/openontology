package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// ---------------------------------------------------------------------------
// buildPayload: the degraded paths
//
// An enrichment failure must never suppress the alarm. An operator would rather
// receive a thin CRITICAL notification than none at all, so every one of these
// cases has to produce a payload that still round-trips as a mutation.
// ---------------------------------------------------------------------------

func TestBuildPayloadDegradedPaths(t *testing.T) {
	errGraph := errors.New("bolt://neo4j:7687: connection refused")

	cases := map[string]struct {
		graphErr       error
		breakRedis     bool
		wantDegraded   bool
		wantReasons    []string
		wantNotReasons []string
	}{
		"fully enriched": {},
		"graph unavailable": {
			graphErr:       errGraph,
			wantDegraded:   true,
			wantReasons:    []string{"graph_unavailable", "connection refused"},
			wantNotReasons: []string{"snapshot_unavailable"},
		},
		"snapshot unavailable": {
			breakRedis:     true,
			wantDegraded:   true,
			wantReasons:    []string{"snapshot_unavailable"},
			wantNotReasons: []string{"graph_unavailable"},
		},
		"both unavailable": {
			graphErr:     errGraph,
			breakRedis:   true,
			wantDegraded: true,
			wantReasons:  []string{"snapshot_unavailable", "graph_unavailable"},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fixture := newTestEngine(t, &stubGraph{context: richContext(), err: tc.graphErr})
			ctx := context.Background()

			ev := reading("PUMP-221", SensorVibrationIndex, 12.0, baseTime)
			if _, err := fixture.cache.Apply(ctx, ev); err != nil {
				t.Fatalf("seeding the twin: %v", err)
			}
			if tc.breakRedis {
				fixture.mr.Close()
			}

			eval, _ := fixture.engine.rules.Evaluate(ev)
			transition := Transition{
				Kind:        TransitionRaised,
				Severity:    SeverityCritical,
				ActiveSince: baseTime,
				BreachCount: 1,
			}
			now := baseTime.Add(2 * time.Second)

			payload, err := fixture.engine.buildPayload(ctx, ev, eval, transition, mustMessage(t, ev), now)
			if err != nil {
				t.Fatalf("buildPayload returned an error instead of degrading: %v", err)
			}

			// 1. The payload is a usable mutation whatever failed.
			assertMutationIsWellFormed(t, payload, ev, transition)

			// 2. Degradation is reported honestly.
			if payload.Degraded != tc.wantDegraded {
				t.Errorf("Degraded = %t, want %t (reason %q)", payload.Degraded, tc.wantDegraded, payload.DegradedReason)
			}
			if tc.wantDegraded && payload.DegradedReason == "" {
				t.Error("Degraded is set but DegradedReason is empty; the consumer cannot tell what was missing")
			}
			if !tc.wantDegraded && payload.DegradedReason != "" {
				t.Errorf("DegradedReason = %q on a fully enriched payload", payload.DegradedReason)
			}
			for _, want := range tc.wantReasons {
				if !strings.Contains(payload.DegradedReason, want) {
					t.Errorf("DegradedReason = %q, want it to mention %q", payload.DegradedReason, want)
				}
			}
			for _, unwanted := range tc.wantNotReasons {
				if strings.Contains(payload.DegradedReason, unwanted) {
					t.Errorf("DegradedReason = %q, want it not to mention %q", payload.DegradedReason, unwanted)
				}
			}

			// 3. Each tier degrades independently of the other.
			if tc.graphErr != nil {
				if payload.OntologyContext.Source != "unavailable" {
					t.Errorf("OntologyContext.Source = %q, want \"unavailable\"", payload.OntologyContext.Source)
				}
				if payload.OntologyContext.AssetID != ev.AssetID {
					t.Errorf("OntologyContext.AssetID = %q, want %q even when the graph is down",
						payload.OntologyContext.AssetID, ev.AssetID)
				}
				if !payload.OntologyContext.ResolvedAt.Equal(now) {
					t.Errorf("OntologyContext.ResolvedAt = %s, want %s", payload.OntologyContext.ResolvedAt, now)
				}
				if got := fixture.metrics.GraphDegraded.Load(); got != 1 {
					t.Errorf("GraphDegraded metric = %d, want 1", got)
				}
			} else {
				if payload.OntologyContext.AssetName == "" || len(payload.OntologyContext.AssignedOperators) == 0 {
					t.Errorf("OntologyContext was not populated: %+v", payload.OntologyContext)
				}
				if got := fixture.metrics.GraphDegraded.Load(); got != 0 {
					t.Errorf("GraphDegraded metric = %d, want 0", got)
				}
			}

			if tc.breakRedis {
				if payload.TelemetrySnapshot.Complete {
					t.Error("TelemetrySnapshot.Complete is true although the snapshot failed")
				}
				if len(payload.TelemetrySnapshot.Readings) != 1 {
					t.Errorf("Readings = %+v, want the triggering reading as the fallback",
						payload.TelemetrySnapshot.Readings)
				}
			} else {
				if !payload.TelemetrySnapshot.Complete {
					t.Error("TelemetrySnapshot.Complete is false although the snapshot succeeded")
				}
				if len(payload.TelemetrySnapshot.Readings) == 0 {
					t.Error("Readings is empty although the snapshot succeeded")
				}
			}
		})
	}
}

// assertMutationIsWellFormed checks the invariants every consumer of
// ontology.mutations relies on, degraded or not.
func assertMutationIsWellFormed(t *testing.T, payload EnrichedContextPayload, ev TelemetryEvent, transition Transition) {
	t.Helper()

	if !strings.HasPrefix(payload.EventID, "evt_") {
		t.Errorf("EventID = %q", payload.EventID)
	}
	if payload.SchemaVersion != SchemaVersion {
		t.Errorf("SchemaVersion = %q, want %q", payload.SchemaVersion, SchemaVersion)
	}
	if payload.Producer != Producer {
		t.Errorf("Producer = %q, want %q", payload.Producer, Producer)
	}
	if payload.AssetID != ev.AssetID {
		t.Errorf("AssetID = %q, want %q", payload.AssetID, ev.AssetID)
	}
	if payload.Transition != transition.Kind || payload.Severity != transition.Severity {
		t.Errorf("transition/severity = %s/%s, want %s/%s",
			payload.Transition, payload.Severity, transition.Kind, transition.Severity)
	}
	if payload.BreachCount != transition.BreachCount {
		t.Errorf("BreachCount = %d, want %d", payload.BreachCount, transition.BreachCount)
	}
	if payload.Rule.RuleID == "" || payload.Rule.SensorID != ev.SensorID {
		t.Errorf("Rule = %+v, want the firing rule", payload.Rule)
	}
	if payload.Rule.ObservedValue != ev.Value {
		t.Errorf("Rule.ObservedValue = %g, want %g", payload.Rule.ObservedValue, ev.Value)
	}
	if payload.TelemetrySnapshot.Trigger.SensorID != ev.SensorID {
		t.Errorf("TelemetrySnapshot.Trigger = %+v, want the triggering sensor", payload.TelemetrySnapshot.Trigger)
	}
	if len(payload.TelemetrySnapshot.Readings) == 0 {
		t.Error("TelemetrySnapshot.Readings is empty; a mutation must always carry at least its trigger")
	}
	if payload.SourcePartition != 3 || payload.SourceOffset != 4242 {
		t.Errorf("source coordinates = %d/%d, want 3/4242", payload.SourcePartition, payload.SourceOffset)
	}

	// The mutation has to survive the wire.
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("payload does not marshal: %v", err)
	}
	var decoded EnrichedContextPayload
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("payload does not round trip: %v", err)
	}
	if decoded.EventID != payload.EventID || decoded.Degraded != payload.Degraded {
		t.Errorf("payload changed across a JSON round trip")
	}
	if payload.Degraded && !strings.Contains(string(body), `"degraded_reason"`) {
		t.Error("degraded_reason was omitted from the encoded mutation")
	}
}

// TestBuildPayloadPrefersTheEventUnit covers the small piece of enrichment
// logic that decides which unit a consumer sees.
func TestBuildPayloadPrefersTheEventUnit(t *testing.T) {
	fixture := newTestEngine(t, &stubGraph{context: richContext()})
	ctx := context.Background()

	cases := map[string]struct {
		unit string
		want string
	}{
		"the gateway's unit wins":              {unit: "in/s", want: "in/s"},
		"the threshold's unit is the fallback": {unit: "", want: "mm/s"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ev := reading("PUMP-221", SensorVibrationIndex, 12.0, baseTime)
			ev.Unit = tc.unit

			eval, _ := fixture.engine.rules.Evaluate(ev)
			payload, err := fixture.engine.buildPayload(ctx, ev, eval, Transition{Kind: TransitionRaised}, mustMessage(t, ev), baseTime)
			if err != nil {
				t.Fatalf("buildPayload: %v", err)
			}
			if got := payload.TelemetrySnapshot.Trigger.Unit; got != tc.want {
				t.Fatalf("trigger unit = %q, want %q", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// process: the paths that commit the offset without emitting
// ---------------------------------------------------------------------------

// TestProcessRejectsPoisonMessages: a message that can never succeed must be
// dead-lettered and its offset committed, or it wedges the partition forever.
func TestProcessRejectsPoisonMessages(t *testing.T) {
	cases := map[string]string{
		"truncated json":                `{"asset_id":"PUMP-221",`,
		"not an object":                 `"just a string"`,
		"missing timestamp":             `{"asset_id":"PUMP-221","sensor_id":"vibration_index","value":9.1}`,
		"malformed timestamp":           `{"asset_id":"PUMP-221","sensor_id":"vibration_index","value":9.1,"timestamp":"tuesday"}`,
		"asset id breaks the key space": `{"asset_id":"PUMP:221","sensor_id":"vibration_index","value":9.1,"timestamp":"2026-08-13T10:00:00Z"}`,
		"empty sensor id":               `{"asset_id":"PUMP-221","sensor_id":"","value":9.1,"timestamp":"2026-08-13T10:00:00Z"}`,
		"non finite value":              `{"asset_id":"PUMP-221","sensor_id":"vibration_index","value":1e999,"timestamp":"2026-08-13T10:00:00Z"}`,
		"timestamp at the epoch cannot identify an event": `{"asset_id":"PUMP-221","sensor_id":"vibration_index","value":9.1,"timestamp":0}`,
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			fixture := newTestEngine(t, &stubGraph{context: richContext()})
			msg := kafka.Message{Topic: "telemetry.raw", Partition: 3, Offset: 4242, Value: []byte(body)}

			if err := fixture.engine.process(context.Background(), msg); err != nil {
				t.Fatalf("process returned %v, want nil so the poison pill's offset is committed", err)
			}
			if got := fixture.metrics.EventsRejected.Load(); got != 1 {
				t.Errorf("EventsRejected = %d, want 1", got)
			}
			if got := fixture.metrics.DLQMessages.Load(); got != 1 {
				t.Errorf("DLQMessages = %d, want 1", got)
			}
			if got := fixture.metrics.CacheWrites.Load(); got != 0 {
				t.Errorf("CacheWrites = %d, want 0 — a rejected message must not reach the cache", got)
			}
		})
	}
}

// TestProcessSuppressesDuplicatesBeforeTheCache proves the filter is a
// structural step at the head of the loop, not an afterthought.
func TestProcessSuppressesDuplicatesBeforeTheCache(t *testing.T) {
	fixture := newTestEngine(t, &stubGraph{context: richContext()})
	ctx := context.Background()

	// An ungoverned sensor lets process run to completion without needing a
	// broker, so the assertion is about the filter and nothing else.
	ev := reading("PUMP-221", "oil_pressure", 210.0, baseTime)
	msg := mustMessage(t, ev)

	if err := fixture.engine.process(ctx, msg); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	if got := fixture.metrics.CacheWrites.Load(); got != 1 {
		t.Fatalf("CacheWrites = %d after the first delivery, want 1", got)
	}

	for i := 0; i < 3; i++ {
		if err := fixture.engine.process(ctx, msg); err != nil {
			t.Fatalf("redelivery %d: %v", i+1, err)
		}
	}
	if got := fixture.metrics.EventsDuplicate.Load(); got != 3 {
		t.Errorf("EventsDuplicate = %d, want 3", got)
	}
	if got := fixture.metrics.CacheWrites.Load(); got != 1 {
		t.Errorf("CacheWrites = %d, want 1 — redeliveries must not reach the cache", got)
	}
	if got := fixture.metrics.EventsConsumed.Load(); got != 4 {
		t.Errorf("EventsConsumed = %d, want 4", got)
	}
}

// TestProcessDropsOutOfOrderReadings covers the interaction between the
// idempotency filter (which only sees exact redeliveries) and the cache's
// staleness guard (which catches genuinely out-of-order samples).
func TestProcessDropsOutOfOrderReadings(t *testing.T) {
	fixture := newTestEngine(t, &stubGraph{context: richContext()})
	ctx := context.Background()

	// Below the threshold, so nothing is ever published.
	newer := reading("PUMP-221", SensorVibrationIndex, 2.0, baseTime.Add(time.Minute))
	older := reading("PUMP-221", SensorVibrationIndex, 1.0, baseTime)

	if err := fixture.engine.process(ctx, mustMessage(t, newer)); err != nil {
		t.Fatalf("newer reading: %v", err)
	}
	if err := fixture.engine.process(ctx, mustMessage(t, older)); err != nil {
		t.Fatalf("older reading returned %v, want nil so the offset is committed", err)
	}

	if got := fixture.metrics.EventsStale.Load(); got != 1 {
		t.Errorf("EventsStale = %d, want 1", got)
	}
	if got := fixture.metrics.RulesEvaluated.Load(); got != 1 {
		t.Errorf("RulesEvaluated = %d, want 1 — a stale reading must not reach the rules", got)
	}

	value := fixture.mr.HGet(newer.CacheKey(), "value")
	if value != "2" {
		t.Errorf("twin value = %q, want the newer reading to still own the channel", value)
	}
}

// TestProcessCachesUngovernedSensorsWithoutAlarming: sensors with no rule are
// still part of the twin, they simply never produce a mutation.
func TestProcessCachesUngovernedSensorsWithoutAlarming(t *testing.T) {
	fixture := newTestEngine(t, &stubGraph{context: richContext()})

	ev := reading("PUMP-221", "oil_pressure", 9999.0, baseTime)
	if err := fixture.engine.process(context.Background(), mustMessage(t, ev)); err != nil {
		t.Fatalf("process: %v", err)
	}

	if got := fixture.metrics.CacheWrites.Load(); got != 1 {
		t.Errorf("CacheWrites = %d, want 1", got)
	}
	if got := fixture.metrics.RulesEvaluated.Load(); got != 0 {
		t.Errorf("RulesEvaluated = %d, want 0", got)
	}
	if got := fixture.metrics.MutationsEmitted.Load(); got != 0 {
		t.Errorf("MutationsEmitted = %d, want 0", got)
	}
	if total, _ := fixture.tracker.Tracked(); total != 0 {
		t.Errorf("the state tracker holds %d channels for an ungoverned sensor, want 0", total)
	}
	if !fixture.mr.Exists(ev.CacheKey()) {
		t.Error("the reading was not cached")
	}
}

// TestProcessAbsorbsHealthyReadings: a governed sensor reading below its limit
// updates the twin and stops there.
func TestProcessAbsorbsHealthyReadings(t *testing.T) {
	fixture := newTestEngine(t, &stubGraph{context: richContext()})

	ev := reading("PUMP-221", SensorVibrationIndex, 2.0, baseTime)
	if err := fixture.engine.process(context.Background(), mustMessage(t, ev)); err != nil {
		t.Fatalf("process: %v", err)
	}

	if got := fixture.metrics.RulesEvaluated.Load(); got != 1 {
		t.Errorf("RulesEvaluated = %d, want 1", got)
	}
	if got := fixture.metrics.Anomalies.Load(); got != 0 {
		t.Errorf("Anomalies = %d, want 0", got)
	}
	if got := fixture.metrics.MutationsEmitted.Load(); got != 0 {
		t.Errorf("MutationsEmitted = %d, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// process: the rollback path
// ---------------------------------------------------------------------------

// TestProcessRollsBackEverythingItClaimedOnFailure is the correctness property
// behind at-least-once delivery.
//
// When a mutation cannot be published, Kafka will redeliver the message. That
// redelivery is only useful if the attempt that failed left no trace that makes
// the retry look like a duplicate — neither the distributed idempotency claim
// nor the in-memory alarm transition. If either survives, a transient Kafka or
// Redis blip turns a CRITICAL alarm into silence: the retry is absorbed, no
// mutation is emitted, and nothing is dead-lettered.
func TestProcessRollsBackEverythingItClaimedOnFailure(t *testing.T) {
	fixture := newTestEngine(t, &stubGraph{context: richContext()})
	ctx := context.Background()

	ev := reading("PUMP-221", SensorVibrationIndex, 12.0, baseTime)
	msg := mustMessage(t, ev)

	// Poison the alarm record with a string, so the HSET inside MarkAlarm fails
	// with WRONGTYPE. That is a deterministic, fast stand-in for the transient
	// infrastructure failure this path exists to survive.
	if err := fixture.cache.Redis().Set(ctx, alarmKey(ev.AssetID, ev.SensorID), "wrong-type", 0).Err(); err != nil {
		t.Fatalf("poisoning the alarm key: %v", err)
	}

	err := fixture.engine.process(ctx, msg)
	if err == nil {
		t.Fatal("process succeeded although the alarm write failed; the caller would commit the offset and lose the alarm")
	}

	// 1. The idempotency claim must be gone, or the redelivery is discarded.
	fingerprint, ferr := fixture.dedupe.fingerprintFor(ev)
	if ferr != nil {
		t.Fatalf("fingerprintFor: %v", ferr)
	}
	if fixture.mr.Exists(fixture.dedupe.Key(fingerprint)) {
		t.Error("the idempotency claim survived the failure; the redelivery will be dropped as a duplicate")
	}

	// 2. The alarm transition must be gone, or the redelivery is absorbed by the
	//    state machine as a duplicate transition and never re-emitted.
	total, anomalous := fixture.tracker.Tracked()
	if anomalous != 0 || total != 0 {
		t.Errorf("the state tracker still holds (%d total, %d anomalous) after a failed publish; "+
			"the retry will see the channel already ANOMALOUS and absorb the RAISED", total, anomalous)
	}

	// 3. With the fault cleared, the retry must re-emit rather than be absorbed.
	//    MarkAlarm runs before the publish, so its record proves the state
	//    machine produced the transition again.
	fixture.mr.Del(alarmKey(ev.AssetID, ev.SensorID))
	_ = fixture.engine.process(ctx, msg) // the publish still fails; only the pre-publish effect matters

	if got := fixture.mr.HGet(alarmKey(ev.AssetID, ev.SensorID), "transition"); got != string(TransitionRaised) {
		t.Fatalf("after the fault cleared the retry recorded transition %q, want RAISED — "+
			"the alarm was swallowed instead of being retried", got)
	}
}

// TestProcessKeepsTheClaimWhenItSucceeds is the other half of the contract: a
// successful attempt must leave its claim in place so the redelivery Kafka
// performs after a failed *commit* is suppressed.
func TestProcessKeepsTheClaimWhenItSucceeds(t *testing.T) {
	fixture := newTestEngine(t, &stubGraph{context: richContext()})
	ctx := context.Background()

	ev := reading("PUMP-221", "oil_pressure", 210.0, baseTime)
	if err := fixture.engine.process(ctx, mustMessage(t, ev)); err != nil {
		t.Fatalf("process: %v", err)
	}

	fingerprint, err := fixture.dedupe.fingerprintFor(ev)
	if err != nil {
		t.Fatalf("fingerprintFor: %v", err)
	}
	if !fixture.mr.Exists(fixture.dedupe.Key(fingerprint)) {
		t.Fatal("a successful attempt released its claim; a redelivery would be reprocessed")
	}
}

// TestProcessDoesNotRollBackWhenRedisIsGone: a cache outage fails the event
// before any claim work is meaningful, and must surface as an error so the
// retry/dead-letter machinery takes over rather than silently committing.
func TestProcessSurfacesCacheOutages(t *testing.T) {
	fixture := newTestEngine(t, &stubGraph{context: richContext()})
	fixture.mr.Close()

	ev := reading("PUMP-221", SensorVibrationIndex, 12.0, baseTime)
	err := fixture.engine.process(context.Background(), mustMessage(t, ev))
	if err == nil {
		t.Fatal("process returned nil with Redis unreachable, want an error so the message is retried")
	}
	if got := fixture.metrics.EventsRejected.Load(); got != 0 {
		t.Errorf("EventsRejected = %d, want 0 — an outage is not a poison pill", got)
	}
}

// TestProcessWithRetryDeadLettersAPersistentFailure covers the layer above
// process: once the attempt budget is spent, the message must land on the DLQ
// so an unhealthy dependency cannot wedge the partition indefinitely.
func TestProcessWithRetryDeadLettersAPersistentFailure(t *testing.T) {
	fixture := newTestEngine(t, &stubGraph{context: richContext()})
	ctx := context.Background()

	ev := reading("PUMP-221", SensorVibrationIndex, 12.0, baseTime)
	if err := fixture.cache.Redis().Set(ctx, alarmKey(ev.AssetID, ev.SensorID), "wrong-type", 0).Err(); err != nil {
		t.Fatalf("poisoning the alarm key: %v", err)
	}

	fixture.engine.processWithRetry(ctx, mustMessage(t, ev), discardLogger())

	if got := fixture.metrics.ProcessErrors.Load(); got != 1 {
		t.Errorf("ProcessErrors = %d, want 1", got)
	}
	if got := fixture.metrics.DLQMessages.Load(); got != 1 {
		t.Errorf("DLQMessages = %d, want 1", got)
	}
	if total, anomalous := fixture.tracker.Tracked(); total != 0 || anomalous != 0 {
		t.Errorf("Tracked() = (%d, %d) after a dead-lettered message, want (0, 0)", total, anomalous)
	}
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

func TestFirstNonEmpty(t *testing.T) {
	cases := []struct {
		in   []string
		want string
	}{
		{nil, ""},
		{[]string{}, ""},
		{[]string{"", ""}, ""},
		{[]string{"a", "b"}, "a"},
		{[]string{"", "b"}, "b"},
		{[]string{"", "", "c"}, "c"},
	}
	for _, tc := range cases {
		if got := firstNonEmpty(tc.in...); got != tc.want {
			t.Errorf("firstNonEmpty(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestAppendReason(t *testing.T) {
	cases := []struct{ existing, addition, want string }{
		{"", "a", "a"},
		{"a", "b", "a; b"},
		{"a; b", "c", "a; b; c"},
	}
	for _, tc := range cases {
		if got := appendReason(tc.existing, tc.addition); got != tc.want {
			t.Errorf("appendReason(%q, %q) = %q, want %q", tc.existing, tc.addition, got, tc.want)
		}
	}
}

func TestTruncate(t *testing.T) {
	cases := []struct {
		in   string
		max  int
		want string
	}{
		{"short", 900, "short"},
		{"exact", 5, "exact"},
		{"toolong", 4, "tool..."},
		{"", 0, ""},
	}
	for _, tc := range cases {
		if got := truncate(tc.in, tc.max); got != tc.want {
			t.Errorf("truncate(%q, %d) = %q, want %q", tc.in, tc.max, got, tc.want)
		}
	}
}

func TestSleepCtx(t *testing.T) {
	if !sleepCtx(context.Background(), time.Millisecond) {
		t.Error("sleepCtx returned false for a completed sleep")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sleepCtx(ctx, time.Hour) {
		t.Error("sleepCtx returned true for a cancelled context")
	}
}

// TestRunStateJanitorPrunesAndStops covers the background eviction loop.
func TestRunStateJanitorPrunesAndStops(t *testing.T) {
	fixture := newTestEngine(t, &stubGraph{context: richContext()})

	rules := NewRuleEngine(testConfig(fixture.mr.Addr()))
	eval, _ := rules.Evaluate(reading("PUMP-221", SensorVibrationIndex, 1.0, baseTime))
	fixture.tracker.Evaluate("twin:PUMP-221:vibration_index", eval, time.Now().Add(-48*time.Hour))

	if total, _ := fixture.tracker.Tracked(); total != 1 {
		t.Fatalf("the tracker holds %d channels, want 1", total)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		fixture.engine.RunStateJanitor(ctx, time.Millisecond)
	}()

	deadline := time.After(2 * time.Second)
	for {
		if total, _ := fixture.tracker.Tracked(); total == 0 {
			break
		}
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("the janitor did not evict the idle channel")
		case <-time.After(time.Millisecond):
		}
	}

	if got := fixture.metrics.StatePruned.Load(); got == 0 {
		t.Error("StatePruned metric was not incremented")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunStateJanitor did not return when its context was cancelled")
	}
}
