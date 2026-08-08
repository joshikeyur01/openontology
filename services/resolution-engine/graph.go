package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"
)

// The statement this resolver stands in for now runs for real: see
// graph.CypherResolveOntologyNeighbourhood in internal/graph, reached through
// the Neo4jGraphAdapter that GRAPH_PROVIDER=neo4j selects. This file remains
// the offline path — it needs no server, so the pipeline stays exercisable on a
// laptop and in CI.

// GraphResolver resolves an asset's ontology neighbourhood. Every provider
// selected by GRAPH_PROVIDER satisfies it, so the engine and the admin
// endpoints never learn which tier is live.
type GraphResolver interface {
	ResolveAsset(ctx context.Context, assetID string) (OntologyContext, error)
	Stats() (lookups, hits, errs uint64)

	// Provider names the live backing store, for the admin endpoints.
	Provider() string

	// Close releases whatever the provider holds. It is idempotent.
	Close() error
}

// Compile-time proof that the stand-in still honours the contract.
var _ GraphResolver = (*MockNeo4jResolver)(nil)

// MockNeo4jResolver simulates the graph tier: fixture-backed for known assets,
// deterministically synthesised for anything else, with a TTL cache and a
// configurable latency so the pipeline's timing behaviour matches a real
// round trip to a graph database.
type MockNeo4jResolver struct {
	log      *slog.Logger
	latency  time.Duration
	fixtures map[string]OntologyContext
	cache    *graphContextCache

	lookups atomic.Uint64
	hits    atomic.Uint64
	errs    atomic.Uint64
}

// NewMockNeo4jResolver builds the stand-in graph client.
func NewMockNeo4jResolver(cfg Config, log *slog.Logger) *MockNeo4jResolver {
	return &MockNeo4jResolver{
		log:      log.With("component", "graph", "provider", GraphProviderMock),
		latency:  cfg.GraphLatency,
		fixtures: defaultFixtures(),
		cache:    newGraphContextCache(cfg.GraphCacheTTL),
	}
}

// Provider names the backing store.
func (r *MockNeo4jResolver) Provider() string { return GraphProviderMock }

// Close satisfies GraphResolver. The stand-in holds no external resources.
func (r *MockNeo4jResolver) Close() error { return nil }

// ResolveAsset returns the parent systems and assigned operators for an asset.
func (r *MockNeo4jResolver) ResolveAsset(ctx context.Context, assetID string) (OntologyContext, error) {
	r.lookups.Add(1)

	if cached, ok := r.cache.Lookup(assetID); ok {
		r.hits.Add(1)
		cached.CacheHit = true
		return cached, nil
	}

	// Simulate the network round trip while staying cancellable, so a shutdown
	// or an expired operation deadline is honoured immediately.
	if r.latency > 0 {
		timer := time.NewTimer(r.latency)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			r.errs.Add(1)
			return OntologyContext{}, fmt.Errorf("graph lookup for %s: %w", assetID, ctx.Err())
		case <-timer.C:
		}
	}

	resolved, fromFixture := r.fixtures[strings.ToLower(assetID)]
	if fromFixture {
		resolved = resolved.Clone()
		resolved.Source = "neo4j-mock:fixture"
	} else {
		resolved = synthesizeContext(assetID)
		r.log.Debug("asset absent from graph fixtures, synthesising context", "asset_id", assetID)
	}

	resolved.AssetID = assetID
	resolved.ResolvedAt = time.Now().UTC()
	resolved.CacheHit = false

	r.cache.Store(assetID, resolved)
	return resolved.Clone(), nil
}

// Stats exposes counters for the metrics endpoint.
func (r *MockNeo4jResolver) Stats() (lookups, hits, errs uint64) {
	return r.lookups.Load(), r.hits.Load(), r.errs.Load()
}

// defaultFixtures mirrors a small industrial/aerospace ontology. Keys are
// lower-cased asset identifiers.
func defaultFixtures() map[string]OntologyContext {
	return map[string]OntologyContext{
		"turbofan-a320-0417": {
			AssetName:   "CFM56-5B Turbofan #0417",
			AssetClass:  "aero.propulsion.turbofan",
			ModelNumber: "CFM56-5B4/P",
			Site:        "MRO-TOULOUSE-B2",
			Criticality: "SAFETY_CRITICAL",
			ParentSystems: []SystemNode{
				{NodeID: "SYS-PROP-A320-0417", Name: "Propulsion Subsystem (Engine 1)", Type: "Subsystem", Depth: 1},
				{NodeID: "AIRFRAME-A320-MSN4412", Name: "Airframe A320 MSN4412", Type: "System", Depth: 2},
				{NodeID: "FLEET-EU-SHORTHAUL", Name: "EU Short-Haul Fleet", Type: "Fleet", Depth: 3},
			},
			Components: []string{
				"hpt_bearing_no3", "lpc_fan_module", "egt_harness", "fadec_channel_a",
			},
			AssignedOperators: []Operator{
				{OperatorID: "OP-4471", Name: "L. Moreau", Role: "Lead Powerplant Engineer", Shift: "B", Contact: "+33-5-6100-4471", EscalationOrder: 1},
				{OperatorID: "OP-2210", Name: "S. Kaur", Role: "Reliability Engineer", Shift: "B", Contact: "+33-5-6100-2210", EscalationOrder: 2},
				{OperatorID: "OPS-DUTY", Name: "MRO Duty Manager", Role: "Operations Duty", Shift: "24x7", Contact: "+33-5-6100-0000", EscalationOrder: 3},
			},
			MaintenanceWindow: "2026-08-12T22:00:00Z/2026-08-13T04:00:00Z",
			UpstreamDependencies: []FlowRef{
				{AssetID: "FUEL-PUMP-HP-0417", Name: "HP Fuel Pump #0417", Model: "FP-HP-0417", Status: "OPERATIONAL", Relation: "SUPPLIES", Hops: 1},
			},
			DownstreamImpacts: []FlowRef{
				{AssetID: "BLEED-AIR-MANIFOLD-1", Name: "Bleed Air Manifold 1", Model: "BAM-1-A320", Status: "OPERATIONAL", Relation: "IMPACTS", Hops: 1},
				{AssetID: "HYD-PUMP-GREEN-1", Name: "Green Hydraulic Pump 1", Model: "HYD-G1-A320", Status: "OPERATIONAL", Relation: "IMPACTS", Hops: 1},
			},
			BlastRadius: 2,
		},
		"hpp-pump-221": {
			AssetName:   "Hydraulic Power Pack Pump 221",
			AssetClass:  "industrial.hydraulics.pump",
			ModelNumber: "A11VO-190",
			Site:        "PLANT-ROTTERDAM-L4",
			Criticality: "HIGH",
			ParentSystems: []SystemNode{
				{NodeID: "SYS-HYD-L4", Name: "Line 4 Hydraulic Loop", Type: "Subsystem", Depth: 1},
				{NodeID: "LINE-4", Name: "Extrusion Line 4", Type: "System", Depth: 2},
				{NodeID: "PLANT-ROTTERDAM", Name: "Rotterdam Plant", Type: "Site", Depth: 3},
			},
			Components: []string{"drive_coupling", "thrust_bearing", "seal_pack", "vfd_inverter"},
			AssignedOperators: []Operator{
				{OperatorID: "OP-8801", Name: "J. de Vries", Role: "Maintenance Technician", Shift: "A", Contact: "+31-10-555-8801", EscalationOrder: 1},
				{OperatorID: "OP-8815", Name: "M. Okafor", Role: "Line Supervisor", Shift: "A", Contact: "+31-10-555-8815", EscalationOrder: 2},
			},
			MaintenanceWindow: "2026-08-09T02:00:00Z/2026-08-09T06:00:00Z",
			UpstreamDependencies: []FlowRef{
				{AssetID: "SUCTION-STRAINER-S14", Name: "Suction Strainer S-14", Model: "STR-S14-316L", Status: "OPERATIONAL", Relation: "SUPPLIES", Hops: 1},
				{AssetID: "FEED-DRUM-D101", Name: "Feed Drum D-101", Model: "DRM-D101-CS", Status: "OPERATIONAL", Relation: "SUPPLIES", Hops: 2},
			},
			DownstreamImpacts: []FlowRef{
				{AssetID: "HX-SHELL-TUBE-E220", Name: "Shell & Tube Exchanger E-220", Model: "HX-E220-BEM", Status: "OPERATIONAL", Relation: "IMPACTS", Hops: 1},
				{AssetID: "REACTOR-R310", Name: "Polymerisation Reactor R-310", Model: "CSTR-R310-GL", Status: "OPERATIONAL", Relation: "IMPACTS", Hops: 2},
				{AssetID: "PRODUCT-COOLER-C450", Name: "Product Cooler C-450", Model: "CLR-C450-AIR", Status: "OPERATIONAL", Relation: "IMPACTS", Hops: 3},
			},
			BlastRadius: 3,
		},
		"cnc-mill-07": {
			AssetName:   "5-Axis CNC Mill 07",
			AssetClass:  "industrial.machining.cnc",
			ModelNumber: "DMU-50",
			Site:        "PLANT-GREENVILLE-C1",
			Criticality: "MEDIUM",
			ParentSystems: []SystemNode{
				{NodeID: "CELL-C1-03", Name: "Machining Cell C1-03", Type: "Subsystem", Depth: 1},
				{NodeID: "SHOP-GREENVILLE", Name: "Greenville Machine Shop", Type: "System", Depth: 2},
			},
			Components: []string{"spindle_head", "tool_changer", "x_axis_ballscrew", "coolant_manifold"},
			AssignedOperators: []Operator{
				{OperatorID: "OP-1042", Name: "R. Alvarez", Role: "CNC Operator", Shift: "C", Contact: "+1-864-555-1042", EscalationOrder: 1},
			},
			MaintenanceWindow: "2026-08-15T05:00:00Z/2026-08-15T09:00:00Z",
			UpstreamDependencies: []FlowRef{
				{AssetID: "COOLANT-SKID-K3", Name: "Coolant Skid K3", Model: "CSK-K3", Status: "OPERATIONAL", Relation: "SUPPLIES", Hops: 1},
			},
			DownstreamImpacts: []FlowRef{
				{AssetID: "CONVEYOR-CV12", Name: "Outfeed Conveyor CV-12", Model: "CV-12-BELT", Status: "OPERATIONAL", Relation: "IMPACTS", Hops: 1},
			},
			BlastRadius: 1,
		},
	}
}

// synthesizeContext deterministically fabricates a plausible neighbourhood for
// assets that are not in the fixture set, so an operator can exercise the full
// pipeline with arbitrary identifiers. The same asset always yields the same
// context, which keeps demos and tests reproducible.
func synthesizeContext(assetID string) OntologyContext {
	h := fnv.New64a()
	_, _ = h.Write([]byte(strings.ToLower(assetID)))
	seed := h.Sum64()

	sites := []string{"PLANT-ROTTERDAM-L4", "MRO-TOULOUSE-B2", "PLANT-GREENVILLE-C1", "RIG-NORTHSEA-07"}
	criticalities := []string{"MEDIUM", "HIGH", "SAFETY_CRITICAL"}
	classes := []string{"industrial.rotating.generic", "industrial.thermal.generic", "aero.auxiliary.generic"}
	roles := []string{"Maintenance Technician", "Reliability Engineer", "Shift Supervisor"}

	site := sites[seed%uint64(len(sites))]
	criticality := criticalities[(seed>>8)%uint64(len(criticalities))]
	class := classes[(seed>>16)%uint64(len(classes))]

	return OntologyContext{
		AssetName:   "Unregistered Asset " + assetID,
		AssetClass:  class,
		Site:        site,
		Criticality: criticality,
		ParentSystems: []SystemNode{
			{NodeID: fmt.Sprintf("SYS-%04d", seed%10000), Name: "Unmapped Subsystem", Type: "Subsystem", Depth: 1},
			{NodeID: site, Name: site, Type: "Site", Depth: 2},
		},
		Components: []string{"primary_drive", "instrumentation_loop"},
		AssignedOperators: []Operator{
			{
				OperatorID:      fmt.Sprintf("OP-%04d", (seed>>24)%10000),
				Name:            "On-Call Duty Engineer",
				Role:            roles[(seed>>32)%uint64(len(roles))],
				Shift:           "24x7",
				Contact:         "duty-desk@openontology.local",
				EscalationOrder: 1,
			},
		},
		Source: "neo4j-mock:synthesized",
	}
}
