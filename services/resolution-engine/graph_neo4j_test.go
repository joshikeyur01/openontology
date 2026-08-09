package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openontology/resolution-engine/internal/graph"
)

// fakeOntologyResolver stands in for internal/graph so the adapter's caching,
// budget and failure behaviour can be exercised without a cluster.
type fakeOntologyResolver struct {
	neighbourhood *graph.OntologyNeighbourhood
	err           error
	delay         time.Duration

	calls  atomic.Uint64
	closed atomic.Uint64
}

func (f *fakeOntologyResolver) ResolveOntologyNeighbourhood(ctx context.Context, assetID string) (*graph.OntologyNeighbourhood, error) {
	f.calls.Add(1)
	if f.delay > 0 {
		timer := time.NewTimer(f.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.neighbourhood, nil
}

func (f *fakeOntologyResolver) Close() error {
	f.closed.Add(1)
	return nil
}

// pumpNeighbourhood mirrors what ops/neo4j/002_fixture_ontology.cypher loads for
// HPP-PUMP-221, in the shape internal/graph hands back.
func pumpNeighbourhood() *graph.OntologyNeighbourhood {
	return &graph.OntologyNeighbourhood{
		Target: graph.AssetNode{
			AssetID:       "HPP-PUMP-221",
			Name:          "Hydraulic Power Pack Pump 221",
			ModelNumber:   "A11VO-190",
			CurrentStatus: "OPERATIONAL",
		},
		AssetClass:        "industrial.hydraulics.pump",
		ModelNumber:       "A11VO-190",
		Site:              "PLANT-ROTTERDAM-L4",
		Criticality:       "HIGH",
		MaintenanceWindow: "2026-08-09T02:00:00Z/2026-08-09T06:00:00Z",
		Upstream: []graph.FlowNode{
			{AssetNode: graph.AssetNode{AssetID: "SUCTION-STRAINER-S14", Name: "Suction Strainer S-14", ModelNumber: "STR-S14-316L", CurrentStatus: "OPERATIONAL"}, Hops: 1},
			{AssetNode: graph.AssetNode{AssetID: "FEED-DRUM-D101", Name: "Feed Drum D-101", ModelNumber: "DRM-D101-CS", CurrentStatus: "OPERATIONAL"}, Hops: 2},
		},
		Downstream: []graph.FlowNode{
			{AssetNode: graph.AssetNode{AssetID: "HX-SHELL-TUBE-E220", Name: "Shell & Tube Exchanger E-220", ModelNumber: "HX-E220-BEM", CurrentStatus: "OPERATIONAL"}, Hops: 1},
			{AssetNode: graph.AssetNode{AssetID: "REACTOR-R310", Name: "Polymerisation Reactor R-310", ModelNumber: "CSTR-R310-GL", CurrentStatus: "OPERATIONAL"}, Hops: 2},
			{AssetNode: graph.AssetNode{AssetID: "PRODUCT-COOLER-C450", Name: "Product Cooler C-450", ModelNumber: "CLR-C450-AIR", CurrentStatus: "OPERATIONAL"}, Hops: 3},
		},
		ParentSystems: []graph.SystemNode{
			{NodeID: "SYS-HYD-L4", Name: "Line 4 Hydraulic Loop", Type: "Subsystem", Depth: 1},
			{NodeID: "LINE-4", Name: "Extrusion Line 4", Type: "System", Depth: 2},
			{NodeID: "PLANT-ROTTERDAM", Name: "Rotterdam Plant", Type: "Site", Depth: 3},
		},
		Components: []graph.ComponentNode{
			{ComponentID: "CMP-HPP-PUMP-221-01", Name: "drive_coupling"},
			{ComponentID: "CMP-HPP-PUMP-221-02", Name: "thrust_bearing"},
		},
		Operators: []graph.OperatorAssignment{
			{
				OperatorNode:    graph.OperatorNode{TechnicianID: "OP-8801", Name: "J. de Vries"},
				Role:            "Maintenance Technician",
				Shift:           "A",
				Contact:         "+31-10-555-8801",
				EscalationOrder: 1,
			},
			{
				OperatorNode:    graph.OperatorNode{TechnicianID: "OP-8815", Name: "M. Okafor"},
				Role:            "Line Supervisor",
				Shift:           "A",
				EscalationOrder: 2,
			},
		},
		ResolvedAt: baseTime,
	}
}

func newTestAdapter(t *testing.T, resolver ontologyContextResolver) *Neo4jGraphAdapter {
	t.Helper()
	cfg := testConfig("127.0.0.1:1")
	adapter := newNeo4jGraphAdapter(func() (ontologyContextResolver, error) { return resolver, nil }, cfg, discardLogger())
	t.Cleanup(func() { _ = adapter.Close() })
	return adapter
}

// TestAdapterProjectsNeighbourhoodOntoWireModel is the reconciliation the whole
// change exists for: internal/graph's containment subtree has to arrive on
// ontology.mutations as an OntologyContext the interceptor already understands.
func TestAdapterProjectsNeighbourhoodOntoWireModel(t *testing.T) {
	adapter := newTestAdapter(t, &fakeOntologyResolver{neighbourhood: pumpNeighbourhood()})

	resolved, err := adapter.ResolveAsset(context.Background(), "HPP-PUMP-221")
	if err != nil {
		t.Fatalf("ResolveAsset: %v", err)
	}

	if resolved.Source != SourceNeo4jLive {
		t.Errorf("source = %q, want %q — a consumer cannot tell live enrichment from the stand-in otherwise",
			resolved.Source, SourceNeo4jLive)
	}
	if resolved.AssetID != "HPP-PUMP-221" || resolved.AssetName != "Hydraulic Power Pack Pump 221" {
		t.Errorf("asset = %q/%q, want HPP-PUMP-221/Hydraulic Power Pack Pump 221", resolved.AssetID, resolved.AssetName)
	}
	if resolved.Site != "PLANT-ROTTERDAM-L4" || resolved.Criticality != "HIGH" {
		t.Errorf("site/criticality = %q/%q, want PLANT-ROTTERDAM-L4/HIGH", resolved.Site, resolved.Criticality)
	}
	if resolved.AssetClass != "industrial.hydraulics.pump" {
		t.Errorf("asset_class = %q, want industrial.hydraulics.pump", resolved.AssetClass)
	}

	wantSystems := []SystemNode{
		{NodeID: "SYS-HYD-L4", Name: "Line 4 Hydraulic Loop", Type: "Subsystem", Depth: 1},
		{NodeID: "LINE-4", Name: "Extrusion Line 4", Type: "System", Depth: 2},
		{NodeID: "PLANT-ROTTERDAM", Name: "Rotterdam Plant", Type: "Site", Depth: 3},
	}
	if len(resolved.ParentSystems) != len(wantSystems) {
		t.Fatalf("parent_systems = %d entries, want %d", len(resolved.ParentSystems), len(wantSystems))
	}
	for i, want := range wantSystems {
		if resolved.ParentSystems[i] != want {
			t.Errorf("parent_systems[%d] = %+v, want %+v", i, resolved.ParentSystems[i], want)
		}
	}

	// The wire model carries component names, not nodes.
	wantComponents := []string{"drive_coupling", "thrust_bearing"}
	for i, want := range wantComponents {
		if i >= len(resolved.Components) || resolved.Components[i] != want {
			t.Errorf("components = %v, want %v", resolved.Components, wantComponents)
			break
		}
	}

	primary, ok := resolved.PrimaryOperator()
	if !ok {
		t.Fatal("no primary operator; the graph named two")
	}
	if primary.OperatorID != "OP-8801" || primary.EscalationOrder != 1 {
		t.Errorf("primary operator = %s (order %d), want OP-8801 (order 1)", primary.OperatorID, primary.EscalationOrder)
	}
	if primary.Contact != "+31-10-555-8801" || primary.Role != "Maintenance Technician" {
		t.Errorf("primary operator contact/role = %q/%q, want the values on the graph node", primary.Contact, primary.Role)
	}
}

// TestAdapterEmptyNeighbourhoodEncodesAsLists guards the JSON contract: an asset
// with no modelled parents must serialise as [] rather than null, or a consumer
// ranging over parent_systems breaks on an asset that is merely unmodelled.
func TestAdapterEmptyNeighbourhoodEncodesAsLists(t *testing.T) {
	bare := &graph.OntologyNeighbourhood{Target: graph.AssetNode{AssetID: "CNC-MILL-07"}}
	adapter := newTestAdapter(t, &fakeOntologyResolver{neighbourhood: bare})

	resolved, err := adapter.ResolveAsset(context.Background(), "CNC-MILL-07")
	if err != nil {
		t.Fatalf("ResolveAsset: %v", err)
	}
	if resolved.ParentSystems == nil || resolved.Components == nil || resolved.AssignedOperators == nil {
		t.Fatalf("nil slice in %+v; every collection must be non-nil", resolved)
	}
}

// TestAdapterServesRepeatLookupsFromCache covers the property the hot path
// depends on: a re-alerting asset must not re-traverse the graph.
func TestAdapterServesRepeatLookupsFromCache(t *testing.T) {
	fake := &fakeOntologyResolver{neighbourhood: pumpNeighbourhood()}
	adapter := newTestAdapter(t, fake)

	for i := 0; i < 5; i++ {
		if _, err := adapter.ResolveAsset(context.Background(), "HPP-PUMP-221"); err != nil {
			t.Fatalf("ResolveAsset #%d: %v", i, err)
		}
	}

	if calls := fake.calls.Load(); calls != 1 {
		t.Errorf("resolver called %d times for 5 lookups, want 1", calls)
	}
	lookups, hits, errs := adapter.Stats()
	if lookups != 5 || hits != 4 || errs != 0 {
		t.Errorf("stats = %d lookups / %d hits / %d errors, want 5/4/0", lookups, hits, errs)
	}
}

// TestAdapterCachedContextIsIsolated proves a caller cannot corrupt the cache
// for every other worker by mutating the slice it was handed.
func TestAdapterCachedContextIsIsolated(t *testing.T) {
	adapter := newTestAdapter(t, &fakeOntologyResolver{neighbourhood: pumpNeighbourhood()})

	first, err := adapter.ResolveAsset(context.Background(), "HPP-PUMP-221")
	if err != nil {
		t.Fatalf("ResolveAsset: %v", err)
	}
	first.ParentSystems[0].Name = "clobbered"
	first.Components[0] = "clobbered"

	second, err := adapter.ResolveAsset(context.Background(), "HPP-PUMP-221")
	if err != nil {
		t.Fatalf("ResolveAsset (cached): %v", err)
	}
	if second.ParentSystems[0].Name != "Line 4 Hydraulic Loop" || second.Components[0] != "drive_coupling" {
		t.Errorf("cache was mutated through a returned value: %+v", second)
	}
	if !second.CacheHit {
		t.Error("second resolution did not report cache_hit")
	}
}

// TestAdapterEnforcesQueryBudget is the ingestion-path guarantee: a traversal
// that outruns its budget fails, it does not hold the worker.
func TestAdapterEnforcesQueryBudget(t *testing.T) {
	cfg := testConfig("127.0.0.1:1")
	cfg.GraphQueryBudget = 40 * time.Millisecond

	fake := &fakeOntologyResolver{neighbourhood: pumpNeighbourhood(), delay: 2 * time.Second}
	adapter := newNeo4jGraphAdapter(func() (ontologyContextResolver, error) { return fake, nil }, cfg, discardLogger())
	t.Cleanup(func() { _ = adapter.Close() })

	started := time.Now()
	_, err := adapter.ResolveAsset(context.Background(), "HPP-PUMP-221")
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("ResolveAsset succeeded past its budget, want a deadline error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want context.DeadlineExceeded", err)
	}
	// Generous slack for a loaded CI box; the point is that it did not wait 2s.
	if elapsed > time.Second {
		t.Errorf("resolution took %s, want it abandoned near the %s budget", elapsed, cfg.GraphQueryBudget)
	}
	if _, _, errs := adapter.Stats(); errs != 1 {
		t.Errorf("error counter = %d, want 1", errs)
	}
}

// TestAdapterDegradesWhenNeverConnected is requirement zero for the graph tier:
// with no cluster to talk to, resolution fails cleanly and the engine is free to
// emit a degraded mutation rather than dropping the alarm.
func TestAdapterDegradesWhenNeverConnected(t *testing.T) {
	dialErr := errors.New("dial tcp 10.0.0.5:7687: connect: connection refused")
	var dials atomic.Uint64

	cfg := testConfig("127.0.0.1:1")
	adapter := newNeo4jGraphAdapter(func() (ontologyContextResolver, error) {
		dials.Add(1)
		return nil, dialErr
	}, cfg, discardLogger())
	t.Cleanup(func() { _ = adapter.Close() })

	for i := 0; i < 3; i++ {
		_, err := adapter.ResolveAsset(context.Background(), "HPP-PUMP-221")
		if !errors.Is(err, ErrGraphDisconnected) {
			t.Fatalf("resolution #%d returned %v, want ErrGraphDisconnected", i, err)
		}
	}

	// One dial, not three: an outage must not cost a handshake per alarm.
	if got := dials.Load(); got != 1 {
		t.Errorf("dialled %d times across 3 resolutions, want 1 (redial interval is %s)", got, neo4jRedialInterval)
	}
	if adapter.Connected() {
		t.Error("adapter reports connected after a failed dial")
	}
	if detail := adapter.Detail(); detail["connected"] != false {
		t.Errorf("Detail()[connected] = %v, want false", detail["connected"])
	}
}

// TestAdapterConnectsLazilyAfterAFailedStart shows the recovery path: an engine
// that started before Neo4j did picks it up without a restart.
func TestAdapterConnectsLazilyAfterAFailedStart(t *testing.T) {
	fake := &fakeOntologyResolver{neighbourhood: pumpNeighbourhood()}
	var down atomic.Bool
	down.Store(true)

	cfg := testConfig("127.0.0.1:1")
	adapter := newNeo4jGraphAdapter(func() (ontologyContextResolver, error) {
		if down.Load() {
			return nil, errors.New("connection refused")
		}
		return fake, nil
	}, cfg, discardLogger())
	t.Cleanup(func() { _ = adapter.Close() })

	if _, err := adapter.ResolveAsset(context.Background(), "HPP-PUMP-221"); !errors.Is(err, ErrGraphDisconnected) {
		t.Fatalf("first resolution returned %v, want ErrGraphDisconnected", err)
	}

	// Bring the cluster back and clear the backoff the failed dial installed.
	down.Store(false)
	adapter.mu.Lock()
	adapter.nextDialAt = time.Time{}
	adapter.mu.Unlock()

	resolved, err := adapter.ResolveAsset(context.Background(), "HPP-PUMP-221")
	if err != nil {
		t.Fatalf("resolution after recovery: %v", err)
	}
	if resolved.Source != SourceNeo4jLive {
		t.Errorf("source = %q, want %q", resolved.Source, SourceNeo4jLive)
	}
	if !adapter.Connected() {
		t.Error("adapter still reports disconnected after a successful dial")
	}
}

// TestAdapterCountsAbsentAssetsSeparately keeps "this asset was never modelled"
// distinguishable from "the cluster is down" on /stats. Both degrade the
// mutation; only one of them is an incident.
func TestAdapterCountsAbsentAssetsSeparately(t *testing.T) {
	fake := &fakeOntologyResolver{err: fmt.Errorf("resolve asset %q: %w", "GHOST-1", graph.ErrAssetNotFound)}
	adapter := newTestAdapter(t, fake)

	_, err := adapter.ResolveAsset(context.Background(), "GHOST-1")
	if !errors.Is(err, graph.ErrAssetNotFound) {
		t.Fatalf("error = %v, want graph.ErrAssetNotFound", err)
	}
	if got := adapter.Detail()["assets_not_in_graph"]; got != uint64(1) {
		t.Errorf("assets_not_in_graph = %v, want 1", got)
	}
}

// TestAdapterCloseIsIdempotent covers main's deferred Close racing an explicit
// one; double-closing the driver pool would return a spurious shutdown error.
func TestAdapterCloseIsIdempotent(t *testing.T) {
	fake := &fakeOntologyResolver{neighbourhood: pumpNeighbourhood()}
	cfg := testConfig("127.0.0.1:1")
	adapter := newNeo4jGraphAdapter(func() (ontologyContextResolver, error) { return fake, nil }, cfg, discardLogger())

	if _, err := adapter.ResolveAsset(context.Background(), "HPP-PUMP-221"); err != nil {
		t.Fatalf("ResolveAsset: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := adapter.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := fake.closed.Load(); got != 1 {
		t.Errorf("underlying resolver closed %d times, want 1", got)
	}
}

// TestMockAndNeo4jAgreeOnTheFixtureOntology is what makes GRAPH_PROVIDER a real
// switch rather than two unrelated code paths: the same asset resolves to the
// same context either way, and only the source differs. It is also the guard
// that catches ops/neo4j/002_fixture_ontology.cypher drifting from
// defaultFixtures().
func TestMockAndNeo4jAgreeOnTheFixtureOntology(t *testing.T) {
	cfg := testConfig("127.0.0.1:1")
	mock := NewMockNeo4jResolver(cfg, discardLogger())
	t.Cleanup(func() { _ = mock.Close() })

	fromMock, err := mock.ResolveAsset(context.Background(), "HPP-PUMP-221")
	if err != nil {
		t.Fatalf("mock ResolveAsset: %v", err)
	}

	adapter := newTestAdapter(t, &fakeOntologyResolver{neighbourhood: pumpNeighbourhood()})
	fromGraph, err := adapter.ResolveAsset(context.Background(), "HPP-PUMP-221")
	if err != nil {
		t.Fatalf("adapter ResolveAsset: %v", err)
	}

	if fromMock.AssetName != fromGraph.AssetName {
		t.Errorf("asset_name: mock %q vs graph %q", fromMock.AssetName, fromGraph.AssetName)
	}
	if fromMock.Site != fromGraph.Site || fromMock.Criticality != fromGraph.Criticality {
		t.Errorf("site/criticality: mock %s/%s vs graph %s/%s",
			fromMock.Site, fromMock.Criticality, fromGraph.Site, fromGraph.Criticality)
	}
	if len(fromMock.ParentSystems) != len(fromGraph.ParentSystems) {
		t.Fatalf("parent_systems: mock has %d, graph has %d", len(fromMock.ParentSystems), len(fromGraph.ParentSystems))
	}
	for i := range fromMock.ParentSystems {
		if fromMock.ParentSystems[i] != fromGraph.ParentSystems[i] {
			t.Errorf("parent_systems[%d]: mock %+v vs graph %+v", i, fromMock.ParentSystems[i], fromGraph.ParentSystems[i])
		}
	}

	mockPrimary, _ := fromMock.PrimaryOperator()
	graphPrimary, _ := fromGraph.PrimaryOperator()
	if mockPrimary != graphPrimary {
		t.Errorf("primary operator: mock %+v vs graph %+v", mockPrimary, graphPrimary)
	}

	if fromMock.Source == fromGraph.Source {
		t.Errorf("both providers reported source %q; they must be distinguishable", fromMock.Source)
	}

	// The flow network is the half added in mutation.v2, and it is the half a
	// containment decision reads. Comparing it here is what keeps
	// ops/neo4j/003_flow_network.cypher and defaultFixtures() in step.
	if fromMock.ModelNumber != fromGraph.ModelNumber {
		t.Errorf("model_number: mock %q vs graph %q", fromMock.ModelNumber, fromGraph.ModelNumber)
	}
	if fromMock.BlastRadius != fromGraph.BlastRadius {
		t.Errorf("blast_radius: mock %d vs graph %d", fromMock.BlastRadius, fromGraph.BlastRadius)
	}

	compareFlow(t, "upstream_dependencies", fromMock.UpstreamDependencies, fromGraph.UpstreamDependencies)
	compareFlow(t, "downstream_impacts", fromMock.DownstreamImpacts, fromGraph.DownstreamImpacts)
}

// compareFlow asserts two flow projections are identical, ordering included.
// Order is part of the contract: index 0 is one hop away, and a consumer walking
// a cascade back to its origin reads that.
func compareFlow(t *testing.T, field string, mock, live []FlowRef) {
	t.Helper()

	if len(mock) != len(live) {
		t.Fatalf("%s: mock has %d entries, graph has %d\n  mock: %+v\n  graph: %+v",
			field, len(mock), len(live), mock, live)
	}
	for i := range mock {
		if mock[i] != live[i] {
			t.Errorf("%s[%d]: mock %+v vs graph %+v", field, i, mock[i], live[i])
		}
	}
}
