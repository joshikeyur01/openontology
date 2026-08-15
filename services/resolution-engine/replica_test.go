package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/openontology/resolution-engine/internal/crdt"
)

func testReplicaConfig(id string) ReplicaConfig {
	return ReplicaConfig{
		Enabled:         true,
		ID:              id,
		SyncInterval:    time.Second,
		ReconcileBudget: time.Second,
		SyncTimeout:     5 * time.Second,
	}
}

func newTestReplica(t *testing.T, id string) *TopologyReplica {
	t.Helper()
	return NewTopologyReplica(testReplicaConfig(id), discardLogger())
}

// pumpContext is a resolved ontology context with every replicated relationship
// present, so a fold exercises vertices, containment, components, operators and
// both flow directions.
func pumpContext() OntologyContext {
	return OntologyContext{
		AssetID:     "HPP-PUMP-221",
		AssetName:   "Hydraulic Power Pack Pump 221",
		AssetClass:  "industrial.hydraulics.pump",
		ModelNumber: "A11VO-190",
		Site:        "PLANT-ROTTERDAM-L4",
		Criticality: "HIGH",
		ParentSystems: []SystemNode{
			{NodeID: "SYS-HYD-L4", Name: "Line 4 Hydraulic Loop", Type: "Subsystem", Depth: 1},
		},
		Components: []string{"drive_coupling", "thrust_bearing"},
		AssignedOperators: []Operator{
			{OperatorID: "OP-8801", Name: "J. de Vries", Role: "Maintenance Technician", EscalationOrder: 1},
		},
		UpstreamDependencies: []FlowRef{
			{AssetID: "SUCTION-STRAINER-S14", Name: "Suction Strainer S-14", Relation: "SUPPLIES", Hops: 1},
		},
		DownstreamImpacts: []FlowRef{
			{AssetID: "HX-SHELL-TUBE-E220", Name: "Exchanger E-220", Relation: "IMPACTS", Hops: 1},
		},
		BlastRadius: 1,
	}
}

// This is the property the whole tier depends on. AddVertex re-asserts the
// entire property map under a fresh Lamport stamp, so folding an unchanged
// context on every telemetry event would climb the clock forever, make every
// snapshot differ from the last, and turn anti-entropy into a permanent full
// transfer of a graph that never changed.
func TestObservingAnUnchangedContextWritesNothing(t *testing.T) {
	replica := newTestReplica(t, "site-a")

	replica.Observe(pumpContext())
	clockAfterFirst := replica.Clock()
	digestAfterFirst := replica.Digest()
	writesAfterFirst := replica.metrics.VerticesWritten.Load() + replica.metrics.EdgesWritten.Load()

	if writesAfterFirst == 0 {
		t.Fatal("first observation wrote nothing; the fold is not reaching the CRDT")
	}

	for i := 0; i < 25; i++ {
		replica.Observe(pumpContext())
	}

	if got := replica.Clock(); got != clockAfterFirst {
		t.Errorf("Lamport clock climbed from %d to %d across 25 identical observations; "+
			"writes are not content-addressed and anti-entropy will never settle",
			clockAfterFirst, got)
	}
	if got := replica.Digest(); got != digestAfterFirst {
		t.Error("digest changed across identical observations; a peer would re-transfer on every round")
	}
	writes := replica.metrics.VerticesWritten.Load() + replica.metrics.EdgesWritten.Load()
	if writes != writesAfterFirst {
		t.Errorf("writes climbed from %d to %d across identical observations", writesAfterFirst, writes)
	}
	if replica.metrics.WritesSkipped.Load() == 0 {
		t.Error("no writes were reported as skipped; the suppression path is not being taken")
	}
}

// A genuine change must still be written, or the suppression above would be
// indistinguishable from the replica ignoring updates.
func TestObservingAChangedContextWrites(t *testing.T) {
	replica := newTestReplica(t, "site-a")
	replica.Observe(pumpContext())
	before := replica.Digest()

	changed := pumpContext()
	changed.Criticality = "SAFETY_CRITICAL"
	replica.Observe(changed)

	if replica.Digest() == before {
		t.Error("a changed property did not change the replica state")
	}
	vertex, found := replica.crdt.LookupVertex("HPP-PUMP-221")
	if !found {
		t.Fatal("asset vertex missing after observation")
	}
	if got := vertex.Properties["criticality"]; got != "SAFETY_CRITICAL" {
		t.Errorf("criticality = %q, want SAFETY_CRITICAL", got)
	}
}

// Convergence is the reason this tier exists: two sites that mutated their own
// slice while partitioned must agree once they exchange state, regardless of
// which direction the exchange happens in.
func TestTwoReplicasConvergeAfterAPartition(t *testing.T) {
	siteA := newTestReplica(t, "site-a")
	siteB := newTestReplica(t, "site-b")

	// Both sites see the shared asset.
	siteA.Observe(pumpContext())
	siteB.Observe(pumpContext())

	// Then each learns something the other has not, as an isolated site would.
	turbofan := OntologyContext{
		AssetID:    "TURBOFAN-A320-0417",
		AssetName:  "CFM56-5B Turbofan #0417",
		AssetClass: "aero.propulsion.turbofan",
	}
	mill := OntologyContext{
		AssetID:    "CNC-MILL-07",
		AssetName:  "5-Axis CNC Mill 07",
		AssetClass: "industrial.machining.cnc",
	}
	siteA.Observe(turbofan)
	siteB.Observe(mill)

	if siteA.Digest() == siteB.Digest() {
		t.Fatal("replicas agree before exchanging state; the test is not exercising divergence")
	}

	// Anti-entropy, both directions.
	if err := siteA.MergeSnapshot(siteB.Snapshot()); err != nil {
		t.Fatalf("A merging B: %v", err)
	}
	if err := siteB.MergeSnapshot(siteA.Snapshot()); err != nil {
		t.Fatalf("B merging A: %v", err)
	}

	if siteA.Digest() != siteB.Digest() {
		t.Errorf("replicas did not converge:\n  site-a %s\n  site-b %s", siteA.Digest(), siteB.Digest())
	}
	for _, id := range []string{"HPP-PUMP-221", "TURBOFAN-A320-0417", "CNC-MILL-07"} {
		if !siteA.crdt.HasVertex(id) {
			t.Errorf("site-a is missing %s after convergence", id)
		}
		if !siteB.crdt.HasVertex(id) {
			t.Errorf("site-b is missing %s after convergence", id)
		}
	}
}

// The join is idempotent, so an anti-entropy round against an unchanged peer
// must be free. If it were not, a quiet fleet would still churn its clocks.
func TestRepeatedMergeOfTheSameStateIsANoOp(t *testing.T) {
	siteA := newTestReplica(t, "site-a")
	siteB := newTestReplica(t, "site-b")

	siteA.Observe(pumpContext())
	snapshot := siteA.Snapshot()

	if err := siteB.MergeSnapshot(snapshot); err != nil {
		t.Fatalf("first merge: %v", err)
	}
	afterFirst := siteB.Digest()

	for i := 0; i < 10; i++ {
		if err := siteB.MergeSnapshot(snapshot); err != nil {
			t.Fatalf("repeat merge %d: %v", i, err)
		}
	}
	if got := siteB.Digest(); got != afterFirst {
		t.Errorf("digest moved across repeated merges of identical state: %s then %s", afterFirst, got)
	}
}

// Merge order must not matter — that is what makes the join a semilattice and
// what lets sites reconcile in whatever order uplinks come back.
func TestMergeIsOrderIndependent(t *testing.T) {
	build := func(id string) *TopologyReplica {
		r := newTestReplica(t, id)
		r.Observe(OntologyContext{AssetID: "ASSET-" + id, AssetName: id})
		return r
	}

	forward := newTestReplica(t, "collector-1")
	for _, id := range []string{"a", "b", "c"} {
		if err := forward.MergeSnapshot(build(id).Snapshot()); err != nil {
			t.Fatalf("forward merge %s: %v", id, err)
		}
	}

	reverse := newTestReplica(t, "collector-2")
	for _, id := range []string{"c", "b", "a"} {
		if err := reverse.MergeSnapshot(build(id).Snapshot()); err != nil {
			t.Fatalf("reverse merge %s: %v", id, err)
		}
	}

	if forward.Digest() != reverse.Digest() {
		t.Errorf("merge order changed the result:\n  forward %s\n  reverse %s",
			forward.Digest(), reverse.Digest())
	}
}

func TestObservationsReportPerReplicaTimelines(t *testing.T) {
	siteA := newTestReplica(t, "site-a")
	siteB := newTestReplica(t, "site-b")

	siteA.Observe(pumpContext())

	changed := pumpContext()
	changed.Site = "PLANT-ROTTERDAM-L5"
	siteB.Observe(changed)

	if err := siteA.MergeSnapshot(siteB.Snapshot()); err != nil {
		t.Fatalf("merge: %v", err)
	}

	observations := siteA.Observations("HPP-PUMP-221")
	if len(observations) != 2 {
		t.Fatalf("expected a timeline entry per replica, got %d: %+v", len(observations), observations)
	}
	// Sorted by replica id, so the payload is stable across scrapes.
	if observations[0].ReplicaID != "site-a" || observations[1].ReplicaID != "site-b" {
		t.Errorf("observations are not sorted by replica id: %+v", observations)
	}
	for _, observation := range observations {
		if observation.AddStamp <= 0 {
			t.Errorf("replica %s reports no add stamp: %+v", observation.ReplicaID, observation)
		}
	}
}

// A disabled replica must be inert and nil-safe, because that is the
// single-site configuration every existing deployment runs.
func TestDisabledReplicaIsInert(t *testing.T) {
	replica := NewTopologyReplica(ReplicaConfig{Enabled: false}, discardLogger())

	replica.Observe(pumpContext())

	if got := replica.metrics.ContextsObserved.Load(); got != 0 {
		t.Errorf("a disabled replica observed %d contexts", got)
	}
	if got := replica.Observations("HPP-PUMP-221"); got != nil {
		t.Errorf("a disabled replica returned observations: %+v", got)
	}
	if stats := replica.Stats(); stats.Enabled {
		t.Error("a disabled replica reported itself enabled")
	}
	if err := replica.MergeSnapshot(crdt.Snapshot{}); err == nil {
		t.Error("a disabled replica accepted a merge")
	}
}

// A replica id is required rather than defaulted, because a replica that took a
// new identity on every restart would leave timeline entries under identities
// nothing ever writes to again — tombstones that can never be superseded.
func TestReplicaConfigRequiresAnIdentityWhenEnabled(t *testing.T) {
	cfg := testReplicaConfig("")
	if err := cfg.Validate(); err == nil {
		t.Error("an enabled replica with no REPLICA_ID passed validation")
	}

	disabled := ReplicaConfig{Enabled: false}
	if err := disabled.Validate(); err != nil {
		t.Errorf("a disabled replica should need no identity, got %v", err)
	}
}

// The transfer has to fit inside the request alongside the join, or the budget
// can never be the thing that fires and its distinct diagnosis is lost.
func TestReplicaConfigRejectsABudgetLargerThanItsTimeout(t *testing.T) {
	cfg := testReplicaConfig("site-a")
	cfg.ReconcileBudget = 30 * time.Second
	cfg.SyncTimeout = 5 * time.Second

	if err := cfg.Validate(); err == nil {
		t.Error("a reconcile budget exceeding the sync timeout passed validation")
	}
}

// failingPeer stands in for a partitioned peer.
type failingPeer struct{ calls int }

func (p *failingPeer) FetchState(context.Context, string) (crdt.Snapshot, error) {
	p.calls++
	return crdt.Snapshot{}, errors.New("connection refused")
}

// A partition is the condition this tier exists to survive, so an unreachable
// peer must be counted and survived rather than propagated.
func TestSyncSurvivesAnUnreachablePeer(t *testing.T) {
	replica := newTestReplica(t, "site-a")
	replica.Observe(pumpContext())
	before := replica.Digest()

	peer := &failingPeer{}
	replica.syncOnce(context.Background(), peer, "http://unreachable:8081")

	if peer.calls != 1 {
		t.Errorf("expected one fetch attempt, got %d", peer.calls)
	}
	if got := replica.metrics.SyncFailures.Load(); got != 1 {
		t.Errorf("sync failures = %d, want 1", got)
	}
	if replica.Digest() != before {
		t.Error("a failed sync changed local state")
	}
}

// snapshotPeer serves a fixed snapshot, standing in for a reachable peer.
type snapshotPeer struct{ snapshot crdt.Snapshot }

func (p snapshotPeer) FetchState(context.Context, string) (crdt.Snapshot, error) {
	return p.snapshot, nil
}

func TestSyncMergesAReachablePeer(t *testing.T) {
	siteA := newTestReplica(t, "site-a")
	siteB := newTestReplica(t, "site-b")
	siteB.Observe(pumpContext())

	siteA.syncOnce(context.Background(), snapshotPeer{snapshot: siteB.Snapshot()}, "http://peer:8081")

	if got := siteA.metrics.SyncFailures.Load(); got != 0 {
		t.Errorf("sync failures = %d, want 0", got)
	}
	if !siteA.crdt.HasVertex("HPP-PUMP-221") {
		t.Error("site-a did not learn the peer's asset")
	}
	if siteA.Digest() != siteB.Digest() {
		t.Error("a completed sync left the replicas divergent")
	}
}
