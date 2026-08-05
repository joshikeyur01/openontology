package main

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Scripted driver
//
// Every alarm-machine property in this file is a sequence of readings fed to
// one channel, so the tests are written as scripts: an offset from baseTime, a
// value, and the transition the tracker must emit (empty meaning "absorbed").
// ---------------------------------------------------------------------------

type step struct {
	after time.Duration
	value float64
	want  TransitionKind // "" means the reading must not produce a mutation
	// severity is checked only when non-empty.
	severity Severity
}

// evaluate is the single call site for StateTracker.Evaluate in these tests, so
// the emission decision is compared the same way everywhere.
func evaluate(tr *StateTracker, key string, eval RuleEvaluation, now time.Time) (Transition, bool) {
	transition, emit, _ := tr.Evaluate(key, eval, now)
	return transition, emit
}

// runScript feeds the steps through the real RuleEngine so severity promotion
// and the state machine are exercised together, and returns every emitted
// transition in order.
func runScript(t *testing.T, tr *StateTracker, rules RuleEngine, key string, steps []step) []Transition {
	t.Helper()

	var emitted []Transition
	for i, s := range steps {
		ev := reading("PUMP-221", SensorVibrationIndex, s.value, baseTime.Add(s.after))
		eval, governed := rules.Evaluate(ev)
		if !governed {
			t.Fatalf("step %d: sensor %q is not governed by a rule", i, ev.SensorID)
		}

		transition, emit := evaluate(tr, key, eval, baseTime.Add(s.after))

		switch {
		case s.want == "" && emit:
			t.Fatalf("step %d (t+%s value=%g): emitted %s, want the reading absorbed",
				i, s.after, s.value, transition.Kind)
		case s.want != "" && !emit:
			t.Fatalf("step %d (t+%s value=%g): absorbed, want %s", i, s.after, s.value, s.want)
		case s.want != "" && transition.Kind != s.want:
			t.Fatalf("step %d (t+%s value=%g): emitted %s, want %s",
				i, s.after, s.value, transition.Kind, s.want)
		}
		if emit {
			if s.severity != "" && transition.Severity != s.severity {
				t.Fatalf("step %d (t+%s value=%g): severity %s, want %s",
					i, s.after, s.value, transition.Severity, s.severity)
			}
			emitted = append(emitted, transition)
		}
	}
	return emitted
}

func newScriptTracker() (*StateTracker, RuleEngine) {
	cfg := testConfig("127.0.0.1:0")
	return NewStateTracker(cfg.ReAlertInterval, cfg.HysteresisRatio), NewRuleEngine(cfg)
}

// ---------------------------------------------------------------------------
// Transition edges
// ---------------------------------------------------------------------------

// TestTransitionEdges walks every edge of the alarm state machine.
func TestTransitionEdges(t *testing.T) {
	cases := map[string][]step{
		"raised on the first breach": {
			{after: 0, value: 8.6, want: TransitionRaised, severity: SeverityHigh},
		},
		"raised straight to critical": {
			{after: 0, value: 12, want: TransitionRaised, severity: SeverityCritical},
		},
		"escalated when severity increases while anomalous": {
			{after: 0, value: 8.6, want: TransitionRaised, severity: SeverityHigh},
			{after: time.Second, value: 12, want: TransitionEscalated, severity: SeverityCritical},
		},
		"further breaches at the same severity are absorbed": {
			{after: 0, value: 8.6, want: TransitionRaised},
			{after: time.Second, value: 8.7, want: ""},
			{after: 2 * time.Second, value: 9.0, want: ""},
		},
		"sustained once the re-alert interval elapses": {
			{after: 0, value: 8.6, want: TransitionRaised},
			{after: testReAlertInterval - time.Millisecond, value: 8.6, want: ""},
			{after: testReAlertInterval, value: 8.6, want: TransitionSustained, severity: SeverityHigh},
		},
		"cleared once past the hysteresis band": {
			{after: 0, value: 8.6, want: TransitionRaised},
			{after: time.Second, value: 1.0, want: TransitionCleared, severity: SeverityInfo},
		},
		"a healthy channel never emits": {
			{after: 0, value: 1.0, want: ""},
			{after: time.Second, value: 8.5, want: ""},
			{after: 2 * time.Second, value: 0, want: ""},
		},
		"clear then re-raise is a fresh episode": {
			{after: 0, value: 8.6, want: TransitionRaised},
			{after: time.Second, value: 1.0, want: TransitionCleared},
			{after: 2 * time.Second, value: 8.6, want: TransitionRaised},
		},
		"a value exactly on the limit does not breach": {
			{after: 0, value: testVibrationLimit, want: ""},
		},
	}

	for name, steps := range cases {
		t.Run(name, func(t *testing.T) {
			tr, rules := newScriptTracker()
			runScript(t, tr, rules, "twin:PUMP-221:vibration_index", steps)
		})
	}
}

// TestRaisedCarriesEpisodeMetadata pins the fields consumers read off a RAISED.
func TestRaisedCarriesEpisodeMetadata(t *testing.T) {
	tr, rules := newScriptTracker()
	key := "twin:PUMP-221:vibration_index"

	emitted := runScript(t, tr, rules, key, []step{
		{after: 0, value: 8.6, want: TransitionRaised},
		{after: time.Second, value: 8.7, want: ""},
		{after: 2 * time.Second, value: 12, want: TransitionEscalated},
	})

	if len(emitted) != 2 {
		t.Fatalf("emitted %d transitions, want 2", len(emitted))
	}
	raised, escalated := emitted[0], emitted[1]

	if !raised.ActiveSince.Equal(baseTime) {
		t.Errorf("RAISED ActiveSince = %s, want %s", raised.ActiveSince, baseTime)
	}
	if !escalated.ActiveSince.Equal(baseTime) {
		t.Errorf("ESCALATED ActiveSince = %s, want the episode start %s", escalated.ActiveSince, baseTime)
	}
	if raised.BreachCount != 1 {
		t.Errorf("RAISED BreachCount = %d, want 1", raised.BreachCount)
	}
	if escalated.BreachCount != 3 {
		t.Errorf("ESCALATED BreachCount = %d, want 3 (every breaching sample counts)", escalated.BreachCount)
	}
}

// ---------------------------------------------------------------------------
// Suppression: the properties that keep ontology.mutations from flooding
// ---------------------------------------------------------------------------

// TestOscillationAroundThresholdDoesNotStorm is the headline suppression
// property: a sensor parked on its limit, dithering either side of it, must
// alarm exactly once.
func TestOscillationAroundThresholdDoesNotStorm(t *testing.T) {
	tr, rules := newScriptTracker()
	key := "twin:PUMP-221:vibration_index"

	// 8.4 sits inside the hysteresis band (clearAt is 8.075), so every dip is a
	// recovery the tracker must refuse to act on.
	var steps []step
	for i := 0; i < 200; i++ {
		value := 8.6
		if i%2 == 1 {
			value = 8.4
		}
		steps = append(steps, step{after: time.Duration(i) * time.Second, value: value})
	}
	steps[0].want = TransitionRaised

	emitted := runScript(t, tr, rules, key, steps)
	if len(emitted) != 1 {
		t.Fatalf("200 oscillating readings emitted %d mutations, want exactly 1 RAISED", len(emitted))
	}
}

// TestHysteresisBandHoldsAlarm pins the release point exactly. The alarm is
// held while the reading is above limit*(1-ratio) and released at or below it.
func TestHysteresisBandHoldsAlarm(t *testing.T) {
	justInside := math.Nextafter(testClearAt, math.Inf(1))   // clearAt + 1 ulp
	justOutside := math.Nextafter(testClearAt, math.Inf(-1)) // clearAt - 1 ulp

	cases := map[string]struct {
		value float64
		want  TransitionKind
	}{
		"well inside the band holds":             {value: 8.4, want: ""},
		"one ulp above the release point holds":  {value: justInside, want: ""},
		"exactly at the release point clears":    {value: testClearAt, want: TransitionCleared},
		"one ulp below the release point clears": {value: justOutside, want: TransitionCleared},
		"well below the release point clears":    {value: 1.0, want: TransitionCleared},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tr, rules := newScriptTracker()
			runScript(t, tr, rules, "twin:PUMP-221:vibration_index", []step{
				{after: 0, value: 9.0, want: TransitionRaised},
				{after: time.Second, value: tc.value, want: tc.want},
			})
		})
	}
}

// TestHysteresisIsDisabledAtRatioZero shows the band collapses onto the limit
// when the ratio is zero, which is the "no hysteresis" configuration.
func TestHysteresisIsDisabledAtRatioZero(t *testing.T) {
	tr := NewStateTracker(testReAlertInterval, 0)
	rules := NewRuleEngine(testConfig("127.0.0.1:0"))
	runScript(t, tr, rules, "twin:PUMP-221:vibration_index", []step{
		{after: 0, value: 9.0, want: TransitionRaised},
		{after: time.Second, value: testVibrationLimit, want: TransitionCleared},
	})
}

// TestSustainedRespectsReAlertInterval covers the periodic re-assertion timer,
// including the boundary and the disabled case.
func TestSustainedRespectsReAlertInterval(t *testing.T) {
	t.Run("fires on the interval and restarts the timer", func(t *testing.T) {
		tr, rules := newScriptTracker()
		runScript(t, tr, rules, "twin:PUMP-221:vibration_index", []step{
			{after: 0, value: 8.6, want: TransitionRaised},
			{after: testReAlertInterval - time.Nanosecond, value: 8.6, want: ""},
			{after: testReAlertInterval, value: 8.6, want: TransitionSustained},
			{after: testReAlertInterval + time.Second, value: 8.6, want: ""},
			{after: 2*testReAlertInterval - time.Nanosecond, value: 8.6, want: ""},
			{after: 2 * testReAlertInterval, value: 8.6, want: TransitionSustained},
		})
	})

	t.Run("a zero interval disables re-assertion", func(t *testing.T) {
		tr := NewStateTracker(0, testHysteresisRatio)
		rules := NewRuleEngine(testConfig("127.0.0.1:0"))
		runScript(t, tr, rules, "twin:PUMP-221:vibration_index", []step{
			{after: 0, value: 8.6, want: TransitionRaised},
			{after: time.Hour, value: 8.6, want: ""},
			{after: 24 * time.Hour, value: 8.6, want: ""},
		})
	})

	t.Run("an escalation restarts the interval", func(t *testing.T) {
		tr, rules := newScriptTracker()
		runScript(t, tr, rules, "twin:PUMP-221:vibration_index", []step{
			{after: 0, value: 8.6, want: TransitionRaised},
			{after: testReAlertInterval - time.Minute, value: 12, want: TransitionEscalated},
			// The re-alert clock now runs from the escalation, not the raise.
			{after: testReAlertInterval, value: 12, want: ""},
			{after: 2*testReAlertInterval - time.Minute, value: 12, want: TransitionSustained},
		})
	})
}

// TestSeverityFlappingDoesNotStormEscalations is the same suppression property
// as TestOscillationAroundThresholdDoesNotStorm, applied to the *critical*
// boundary rather than the alarm boundary. A reading dithering across
// limit*(1+criticalRatio) must not re-emit ESCALATED on every upswing: once an
// episode has reached a severity, only a genuinely higher severity is news.
func TestSeverityFlappingDoesNotStormEscalations(t *testing.T) {
	tr, rules := newScriptTracker()
	key := "twin:PUMP-221:vibration_index"

	// 9.8 is CRITICAL (exceeds 8.5 by 15.3%), 9.7 is HIGH (14.1%).
	steps := []step{{after: 0, value: 9.8, want: TransitionRaised, severity: SeverityCritical}}
	for i := 1; i < 100; i++ {
		value := 9.7
		if i%2 == 0 {
			value = 9.8
		}
		steps = append(steps, step{after: time.Duration(i) * time.Second, value: value})
	}

	emitted := runScript(t, tr, rules, key, steps)
	if len(emitted) != 1 {
		kinds := make([]TransitionKind, 0, len(emitted))
		for _, e := range emitted {
			kinds = append(kinds, e.Kind)
		}
		t.Fatalf("100 readings dithering across the critical boundary emitted %d mutations (%v), want exactly 1 RAISED",
			len(emitted), kinds)
	}
}

// TestEscalationStillFiresOnceForARealSeverityIncrease guards the fix above
// from over-suppressing: a genuine INFO->HIGH->CRITICAL climb must still emit.
func TestEscalationStillFiresOnceForARealSeverityIncrease(t *testing.T) {
	tr, rules := newScriptTracker()
	runScript(t, tr, rules, "twin:PUMP-221:vibration_index", []step{
		{after: 0, value: 8.6, want: TransitionRaised, severity: SeverityHigh},
		{after: time.Second, value: 8.7, want: ""},
		{after: 2 * time.Second, value: 12, want: TransitionEscalated, severity: SeverityCritical},
		{after: 3 * time.Second, value: 13, want: ""},
	})
}

// TestSeverityLatchResetsOnNewEpisode proves the escalation high-water mark is
// scoped to one alarm episode: after a CLEAR, a HIGH breach raises again and a
// later CRITICAL still escalates.
func TestSeverityLatchResetsOnNewEpisode(t *testing.T) {
	tr, rules := newScriptTracker()
	runScript(t, tr, rules, "twin:PUMP-221:vibration_index", []step{
		{after: 0, value: 12, want: TransitionRaised, severity: SeverityCritical},
		{after: time.Second, value: 1.0, want: TransitionCleared},
		{after: 2 * time.Second, value: 8.6, want: TransitionRaised, severity: SeverityHigh},
		{after: 3 * time.Second, value: 12, want: TransitionEscalated, severity: SeverityCritical},
	})
}

// TestBreachCountIsCumulativeAcrossEpisodes pins current behaviour rather than
// asserting a desired one: ActiveSince is reset when an episode ends but
// BreachCount is not, so the two fields on the same Transition describe
// different windows. See the accompanying report.
func TestBreachCountIsCumulativeAcrossEpisodes(t *testing.T) {
	tr, rules := newScriptTracker()
	emitted := runScript(t, tr, rules, "twin:PUMP-221:vibration_index", []step{
		{after: 0, value: 8.6, want: TransitionRaised},
		{after: time.Second, value: 8.7, want: ""},
		{after: 2 * time.Second, value: 1.0, want: TransitionCleared},
		{after: 3 * time.Second, value: 8.6, want: TransitionRaised},
	})

	reraised := emitted[len(emitted)-1]
	if reraised.BreachCount != 3 {
		t.Fatalf("BreachCount on the second episode's RAISED = %d, want 3 (cumulative across episodes)",
			reraised.BreachCount)
	}
	if !reraised.ActiveSince.Equal(baseTime.Add(3 * time.Second)) {
		t.Fatalf("ActiveSince on the second episode's RAISED = %s, want the new episode's start",
			reraised.ActiveSince)
	}
}

// ---------------------------------------------------------------------------
// Rollback
// ---------------------------------------------------------------------------

// TestRollback covers the undo token the engine uses when a transition it has
// already recorded cannot be published.
func TestRollback(t *testing.T) {
	const key = "twin:PUMP-221:vibration_index"
	rules := NewRuleEngine(testConfig("127.0.0.1:0"))
	breach, _ := rules.Evaluate(reading("PUMP-221", SensorVibrationIndex, 9.0, baseTime))
	healthy, _ := rules.Evaluate(reading("PUMP-221", SensorVibrationIndex, 1.0, baseTime))

	t.Run("removes a channel the evaluation created", func(t *testing.T) {
		tr, _ := newScriptTracker()

		_, emit, undo := tr.Evaluate(key, breach, baseTime)
		if !emit {
			t.Fatal("the first breach did not raise")
		}
		tr.Rollback(undo)

		if total, anomalous := tr.Tracked(); total != 0 || anomalous != 0 {
			t.Fatalf("Tracked() = (%d, %d) after rolling back a channel's first transition, want (0, 0)", total, anomalous)
		}
		if _, emit, _ := tr.Evaluate(key, breach, baseTime.Add(time.Second)); !emit {
			t.Fatal("the redelivery was absorbed, so the alarm would never be published")
		}
	})

	t.Run("restores a channel that already existed", func(t *testing.T) {
		tr, _ := newScriptTracker()
		tr.Evaluate(key, healthy, baseTime)

		_, emit, undo := tr.Evaluate(key, breach, baseTime.Add(time.Second))
		if !emit {
			t.Fatal("the breach did not raise")
		}
		tr.Rollback(undo)

		total, anomalous := tr.Tracked()
		if total != 1 || anomalous != 0 {
			t.Fatalf("Tracked() = (%d, %d), want the channel kept but no longer in alarm", total, anomalous)
		}
		if _, emit, _ := tr.Evaluate(key, breach, baseTime.Add(2*time.Second)); !emit {
			t.Fatal("the redelivery was absorbed after the rollback")
		}
	})

	t.Run("skips a channel another delivery has already advanced", func(t *testing.T) {
		tr, _ := newScriptTracker()

		_, _, undo := tr.Evaluate(key, breach, baseTime)
		// A second worker clears the alarm before the first one's rollback lands.
		tr.Evaluate(key, healthy, baseTime.Add(time.Second))
		tr.Rollback(undo)

		if total, anomalous := tr.Tracked(); total != 1 || anomalous != 0 {
			t.Fatalf("Tracked() = (%d, %d); the rollback discarded a newer delivery's progress", total, anomalous)
		}
	})

	t.Run("a zero token is a no-op", func(t *testing.T) {
		tr, _ := newScriptTracker()
		tr.Evaluate(key, breach, baseTime)

		tr.Rollback(StateUndo{})

		if total, anomalous := tr.Tracked(); total != 1 || anomalous != 1 {
			t.Fatalf("Tracked() = (%d, %d), want the channel untouched", total, anomalous)
		}
	})
}

// ---------------------------------------------------------------------------
// Housekeeping
// ---------------------------------------------------------------------------

func TestPruneEvictsIdleHealthyChannelsOnly(t *testing.T) {
	tr, rules := newScriptTracker()

	feed := func(key string, value float64, at time.Time) {
		ev := reading("PUMP-221", SensorVibrationIndex, value, at)
		eval, _ := rules.Evaluate(ev)
		evaluate(tr, key, eval, at)
	}

	feed("twin:A:vibration_index", 1.0, baseTime)                     // healthy, idle
	feed("twin:B:vibration_index", 9.0, baseTime)                     // anomalous, idle
	feed("twin:C:vibration_index", 1.0, baseTime.Add(59*time.Minute)) // healthy, recent

	if total, anomalous := tr.Tracked(); total != 3 || anomalous != 1 {
		t.Fatalf("Tracked() = (%d, %d), want (3, 1)", total, anomalous)
	}

	removed := tr.Prune(baseTime.Add(time.Hour), 30*time.Minute)
	if removed != 1 {
		t.Fatalf("Prune removed %d channels, want 1 (only the idle healthy one)", removed)
	}

	total, anomalous := tr.Tracked()
	if total != 2 || anomalous != 1 {
		t.Fatalf("after Prune Tracked() = (%d, %d), want (2, 1)", total, anomalous)
	}
	if removed := tr.Prune(baseTime.Add(time.Hour), 30*time.Minute); removed != 0 {
		t.Fatalf("second Prune removed %d channels, want 0", removed)
	}
}

// TestShardingDistributesKeys guards against a shard function that funnels
// every key onto one mutex, which would silently serialise the whole fleet.
func TestShardingDistributesKeys(t *testing.T) {
	tr, _ := newScriptTracker()

	occupied := make(map[*stateShard]int)
	for i := 0; i < 4096; i++ {
		occupied[tr.shard(fmt.Sprintf("twin:ASSET-%04d:vibration_index", i))]++
	}
	if len(occupied) != stateShardCount {
		t.Fatalf("4096 keys landed on %d of %d shards", len(occupied), stateShardCount)
	}
	if got := tr.shard("twin:A:v"); got != tr.shard("twin:A:v") {
		t.Fatal("shard() is not deterministic for a fixed key")
	}
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

// TestEvaluateIsRaceFreeAcrossShards hammers the tracker from many goroutines,
// both contending on one key and spread across every shard. Run with -race.
//
// The correctness assertion is the one that matters operationally: however many
// goroutines observe the same breach, the NORMAL -> ANOMALOUS edge is crossed
// exactly once, so exactly one RAISED reaches ontology.mutations.
func TestEvaluateIsRaceFreeAcrossShards(t *testing.T) {
	const (
		keys           = 64
		goroutinesEach = 16
		readingsEach   = 50
	)

	tr, rules := newScriptTracker()

	breach, _ := rules.Evaluate(reading("PUMP-221", SensorVibrationIndex, 9.0, baseTime))
	healthy, _ := rules.Evaluate(reading("PUMP-221", SensorVibrationIndex, 1.0, baseTime))

	var (
		mu     sync.Mutex
		counts = make(map[string]map[TransitionKind]int)
	)
	record := func(key string, kind TransitionKind) {
		mu.Lock()
		defer mu.Unlock()
		if counts[key] == nil {
			counts[key] = make(map[TransitionKind]int)
		}
		counts[key][kind]++
	}

	// Phase 1: every goroutine drives the same breach into every key. Exactly
	// one RAISED per key must survive the race.
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := 0; g < goroutinesEach; g++ {
		for k := 0; k < keys; k++ {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				<-start
				for i := 0; i < readingsEach; i++ {
					if transition, emit := evaluate(tr, key, breach, baseTime); emit {
						record(key, transition.Kind)
					}
				}
			}(fmt.Sprintf("twin:ASSET-%03d:vibration_index", k))
		}
	}
	close(start)
	wg.Wait()

	if len(counts) != keys {
		t.Fatalf("%d keys raised an alarm, want %d", len(counts), keys)
	}
	for key, kinds := range counts {
		if kinds[TransitionRaised] != 1 {
			t.Fatalf("key %s emitted %d RAISED transitions, want exactly 1", key, kinds[TransitionRaised])
		}
		if len(kinds) != 1 {
			t.Fatalf("key %s emitted unexpected transitions: %v", key, kinds)
		}
	}

	if total, anomalous := tr.Tracked(); total != keys || anomalous != keys {
		t.Fatalf("Tracked() = (%d, %d), want (%d, %d)", total, anomalous, keys, keys)
	}

	// Phase 2: recoveries, reads and prunes all racing at once. This phase has
	// no deterministic outcome to assert — it exists so -race can observe the
	// tracker's mutating, reading and evicting paths overlapping.
	later := baseTime.Add(time.Hour)
	for g := 0; g < goroutinesEach; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for k := 0; k < keys; k++ {
				key := fmt.Sprintf("twin:ASSET-%03d:vibration_index", k)
				switch g % 4 {
				case 0:
					evaluate(tr, key, healthy, later)
				case 1:
					evaluate(tr, key, breach, later)
				case 2:
					tr.Tracked()
				case 3:
					tr.Prune(later.Add(time.Hour), time.Minute)
				}
			}
		}(g)
	}
	wg.Wait()
}
