package main

import (
	"hash/fnv"
	"sync"
	"time"
)

// AlarmState is the twin channel's position in the anomaly state machine.
type AlarmState uint8

const (
	StateNormal AlarmState = iota
	StateAnomalous
)

// TransitionKind labels why a mutation was emitted.
type TransitionKind string

const (
	// TransitionRaised is the NORMAL -> ANOMALOUS edge.
	TransitionRaised TransitionKind = "RAISED"
	// TransitionEscalated fires when severity increases while already anomalous.
	TransitionEscalated TransitionKind = "ESCALATED"
	// TransitionSustained is the periodic re-assertion of an unresolved anomaly.
	TransitionSustained TransitionKind = "SUSTAINED"
	// TransitionCleared is the ANOMALOUS -> NORMAL edge, past the hysteresis band.
	TransitionCleared TransitionKind = "CLEARED"
)

// Transition is the decision returned by the tracker.
type Transition struct {
	Kind        TransitionKind
	Severity    Severity
	ActiveSince time.Time
	BreachCount uint64
}

type sensorState struct {
	state       AlarmState
	severity    Severity
	activeSince time.Time
	lastEmit    time.Time
	lastSeen    time.Time
	breachCount uint64

	// sequence increments on every Evaluate. It lets Rollback tell "nothing has
	// touched this channel since I evaluated it" from "another delivery has
	// moved it on", so a rollback can never discard someone else's progress.
	sequence uint64
}

// StateUndo reverses exactly one Evaluate call.
//
// A transition is only real once its mutation has been published. Until then
// the caller holds this token, and hands it back if the publish fails: without
// that, the redelivery Kafka performs is absorbed by the state machine as a
// duplicate transition, and the alarm is lost with nothing dead-lettered.
type StateUndo struct {
	key      string
	previous sensorState
	existed  bool
	sequence uint64
	captured bool
}

const stateShardCount = 64

type stateShard struct {
	mu      sync.Mutex
	entries map[string]*sensorState
}

// StateTracker owns the per-channel alarm state machine. It is sharded by key
// so that high-cardinality fleets do not serialise on a single mutex, and
// every method is safe for concurrent use by all consumer workers.
type StateTracker struct {
	shards          [stateShardCount]*stateShard
	reAlertInterval time.Duration
	hysteresisRatio float64
}

// NewStateTracker builds an empty tracker.
func NewStateTracker(reAlertInterval time.Duration, hysteresisRatio float64) *StateTracker {
	t := &StateTracker{
		reAlertInterval: reAlertInterval,
		hysteresisRatio: hysteresisRatio,
	}
	for i := range t.shards {
		t.shards[i] = &stateShard{entries: make(map[string]*sensorState)}
	}
	return t
}

func (t *StateTracker) shard(key string) *stateShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return t.shards[h.Sum32()%stateShardCount]
}

// Evaluate advances the state machine for one channel and reports whether a
// mutation must be emitted. Emission policy:
//
//	breached + previously normal      -> RAISED
//	breached + severity increased     -> ESCALATED
//	breached + re-alert interval up   -> SUSTAINED
//	recovered past hysteresis band    -> CLEARED
//
// Anything else is absorbed, which is what keeps a flapping sensor from
// flooding ontology.mutations.
//
// The third return value reverses this call. Callers that fail to publish the
// transition must hand it to Rollback; see StateUndo.
func (t *StateTracker) Evaluate(key string, eval RuleEvaluation, now time.Time) (Transition, bool, StateUndo) {
	sh := t.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	st, existed := sh.entries[key]
	if !existed {
		st = &sensorState{state: StateNormal, severity: SeverityInfo}
		sh.entries[key] = st
	}

	undo := StateUndo{key: key, previous: *st, existed: existed, captured: true}
	st.lastSeen = now
	st.sequence++
	undo.sequence = st.sequence

	if eval.Breached {
		st.breachCount++
		previousState := st.state
		previousSeverity := st.severity
		st.state = StateAnomalous

		// Severity is a high-water mark for the lifetime of one alarm episode.
		// Letting it fall back would re-arm the escalation edge below, so a
		// reading dithering across limit*(1+criticalRatio) would emit ESCALATED
		// on every upswing — the same flood the hysteresis band prevents at the
		// alarm boundary. It is reset when the episode ends, not before.
		if eval.Severity.Rank() > st.severity.Rank() {
			st.severity = eval.Severity
		}

		switch {
		case previousState == StateNormal:
			st.activeSince = now
			st.lastEmit = now
			return Transition{
				Kind:        TransitionRaised,
				Severity:    eval.Severity,
				ActiveSince: st.activeSince,
				BreachCount: st.breachCount,
			}, true, undo

		case eval.Severity.Rank() > previousSeverity.Rank():
			st.lastEmit = now
			return Transition{
				Kind:        TransitionEscalated,
				Severity:    eval.Severity,
				ActiveSince: st.activeSince,
				BreachCount: st.breachCount,
			}, true, undo

		case t.reAlertInterval > 0 && now.Sub(st.lastEmit) >= t.reAlertInterval:
			st.lastEmit = now
			return Transition{
				Kind:        TransitionSustained,
				Severity:    eval.Severity,
				ActiveSince: st.activeSince,
				BreachCount: st.breachCount,
			}, true, undo
		}
		return Transition{}, false, undo
	}

	if st.state == StateNormal {
		return Transition{}, false, undo
	}

	// Hysteresis: hold the alarm until the reading has dropped meaningfully
	// below the limit, otherwise a sensor sitting on the threshold produces a
	// RAISED/CLEARED storm.
	clearAt := eval.Threshold.Limit * (1 - t.hysteresisRatio)
	if eval.Observed > clearAt {
		return Transition{}, false, undo
	}

	activeSince := st.activeSince
	breachCount := st.breachCount
	st.state = StateNormal
	st.severity = SeverityInfo
	st.activeSince = time.Time{}
	st.lastEmit = now

	return Transition{
		Kind:        TransitionCleared,
		Severity:    SeverityInfo,
		ActiveSince: activeSince,
		BreachCount: breachCount,
	}, true, undo
}

// Rollback reverses the Evaluate call that produced undo, restoring the channel
// to the state it held before that evaluation.
//
// It is called when the mutation the transition described could not be
// published. Leaving the transition in place would make the redelivery that
// follows look like a duplicate — an already-ANOMALOUS channel absorbs a second
// RAISED — so the alarm would be dropped silently, without even a dead letter.
//
// A rollback is skipped when another delivery has advanced the channel in the
// meantime: that delivery's progress is more recent than ours and outranks it.
func (t *StateTracker) Rollback(undo StateUndo) {
	if !undo.captured {
		return
	}

	sh := t.shard(undo.key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	st, ok := sh.entries[undo.key]
	if !ok || st.sequence != undo.sequence {
		return
	}
	if !undo.existed {
		delete(sh.entries, undo.key)
		return
	}
	*st = undo.previous
}

// Prune drops channels that have been quiet and healthy for longer than idle.
// Without it, a long-lived process tracking a churning fleet would grow its
// map without bound.
func (t *StateTracker) Prune(now time.Time, idle time.Duration) int {
	removed := 0
	for _, sh := range t.shards {
		sh.mu.Lock()
		for key, st := range sh.entries {
			if st.state == StateNormal && now.Sub(st.lastSeen) > idle {
				delete(sh.entries, key)
				removed++
			}
		}
		sh.mu.Unlock()
	}
	return removed
}

// Tracked reports how many channels currently hold state, split by alarm.
func (t *StateTracker) Tracked() (total, anomalous int) {
	for _, sh := range t.shards {
		sh.mu.Lock()
		for _, st := range sh.entries {
			total++
			if st.state == StateAnomalous {
				anomalous++
			}
		}
		sh.mu.Unlock()
	}
	return total, anomalous
}
