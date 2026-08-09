package graph

// This file adds a second projection over the same topology graph, alongside
// the blast-radius read in graph_resolver.go.
//
// The two answer different questions. ResolveAssetContext walks the physical
// flow network (:FEEDS, :CONTROLS) to answer "what breaks if this breaks".
// ResolveOntologyNeighbourhood walks the containment hierarchy (:PART_OF,
// :HAS_COMPONENT, :RESPONSIBLE_FOR) to answer "what is this thing, where does
// it sit, and who is accountable for it" — which is what an enriched mutation
// carries to an operator or an AI agent.
//
// Both run on the same driver, pool, budget and error classification, so
// putting one on the hot path puts all of that machinery on the hot path.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// OntologyContextResolver is the contract the ingestion tier depends on. It is
// deliberately narrower than GraphResolver: an adapter only needs the one read
// plus the ability to release the driver.
type OntologyContextResolver interface {
	ResolveOntologyNeighbourhood(ctx context.Context, assetID string) (*OntologyNeighbourhood, error)
	Close() error
}

// Compile-time proof that the live resolver serves both projections.
var _ OntologyContextResolver = (*Neo4jGraphResolver)(nil)

// ---------------------------------------------------------------------------
// Domain model
// ---------------------------------------------------------------------------

// SystemNode is one ancestor in an asset's containment hierarchy. Depth is the
// number of :PART_OF hops from the asset, so 1 is the immediate parent.
type SystemNode struct {
	NodeID string `json:"node_id"`
	Name   string `json:"name,omitempty"`
	Type   string `json:"type,omitempty"`
	Depth  int    `json:"depth"`

	ElementID string `json:"element_id,omitempty"`
}

// ComponentNode is a serviceable part of an asset — the granularity a work
// order is written against.
type ComponentNode struct {
	ComponentID string `json:"component_id"`
	Name        string `json:"name,omitempty"`

	ElementID string `json:"element_id,omitempty"`
}

// OperatorAssignment is an operator together with the terms of their
// accountability for this particular asset. EscalationOrder is a property of
// the :RESPONSIBLE_FOR edge rather than of the person: the same engineer can be
// first call for one machine and third for another.
type OperatorAssignment struct {
	OperatorNode

	Role            string `json:"role,omitempty"`
	Shift           string `json:"shift,omitempty"`
	Contact         string `json:"contact,omitempty"`
	EscalationOrder int    `json:"escalation_order"`
}

// OntologyNeighbourhood is the containment subtree around a target asset.
//
// ParentSystems, Components and Operators are always non-nil so consumers can
// range over them unconditionally and JSON encodes them as [] rather than null.
type OntologyNeighbourhood struct {
	// Target is the asset the resolution was requested for.
	Target AssetNode `json:"target"`

	// Descriptive properties carried on the asset node itself. They are
	// separate from AssetNode because they describe where the asset sits in the
	// business, not what state the equipment is in.
	AssetClass        string `json:"asset_class,omitempty"`
	ModelNumber       string `json:"model_number,omitempty"`
	Site              string `json:"site,omitempty"`
	Criticality       string `json:"criticality,omitempty"`
	MaintenanceWindow string `json:"maintenance_window,omitempty"`

	// ParentSystems is ordered from the immediate parent outwards.
	ParentSystems []SystemNode `json:"parent_systems"`

	// Components are the asset's serviceable parts, ordered by identifier.
	Components []ComponentNode `json:"components"`

	// Operators are ordered by escalation order, so the first entry is who to
	// call.
	Operators []OperatorAssignment `json:"operators"`

	// Upstream is what supplies this asset, nearest first: index 0 feeds it
	// directly, index 1 feeds index 0. The ordering is the contract — a
	// consumer walking a cascade back to its origin relies on it.
	Upstream []FlowNode `json:"upstream_dependencies"`

	// Downstream is what this asset supplies or controls, nearest first. This
	// is the blast radius: what stops if the asset is isolated.
	Downstream []FlowNode `json:"downstream_impacts"`

	ResolvedAt        time.Time     `json:"resolved_at"`
	ResolutionLatency time.Duration `json:"resolution_latency_ns"`
}

// FlowNode is one asset in the process flow around the target, with how many
// hops away it sits. Hops is what lets a consumer distinguish "this pump feeds
// the exchanger directly" from "this pump eventually feeds the cooler four
// stages downstream", which is the difference between an immediate consequence
// and a knock-on one.
type FlowNode struct {
	AssetNode
	Hops int `json:"hops"`
}

// BlastRadius is the number of distinct assets downstream of the target.
func (n *OntologyNeighbourhood) BlastRadius() int {
	if n == nil {
		return 0
	}
	return len(n.Downstream)
}

// PrimaryOperator returns the lowest escalation-order assignment.
func (n *OntologyNeighbourhood) PrimaryOperator() (OperatorAssignment, bool) {
	if n == nil || len(n.Operators) == 0 {
		return OperatorAssignment{}, false
	}
	// Operators is kept sorted by escalation order at mapping time.
	return n.Operators[0], true
}

// LogValue renders the neighbourhood compactly, so a resolved subtree never
// dumps every component into the log stream.
func (n *OntologyNeighbourhood) LogValue() slog.Value {
	if n == nil {
		return slog.StringValue("<nil>")
	}
	operator := "unassigned"
	if primary, ok := n.PrimaryOperator(); ok {
		operator = primary.TechnicianID
	}
	return slog.GroupValue(
		slog.String("asset_id", n.Target.AssetID),
		slog.String("site", n.Site),
		slog.String("criticality", n.Criticality),
		slog.Int("parent_systems", len(n.ParentSystems)),
		slog.Int("components", len(n.Components)),
		slog.String("operator", operator),
		slog.Duration("latency", n.ResolutionLatency),
	)
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

// CypherResolveOntologyNeighbourhood collects the containment subtree in one
// round trip.
//
// Each branch runs in its own CALL subquery rather than as sibling OPTIONAL
// MATCHes: siblings multiply into a cartesian product (systems x components x
// operators), which for a modest asset is already hundreds of rows the server
// has to build and DISTINCT back down. Subqueries aggregate independently, so
// the cost stays linear — which matters because this runs inline with
// telemetry ingestion.
//
// Ancestry is bounded at four :PART_OF hops. Real hierarchies are three or four
// deep (component -> subsystem -> system -> site -> fleet) and an unbounded
// walk on a mis-modelled cyclic graph is exactly the kind of traversal that
// must never reach the ingestion path.
const CypherResolveOntologyNeighbourhood = `
MATCH (a:Asset {id: $assetId})
CALL {
  WITH a
  OPTIONAL MATCH path = (a)-[:PART_OF*1..4]->(s:System)
  WITH s, min(length(path)) AS depth
  WHERE s IS NOT NULL
  RETURN collect({node: s, depth: depth}) AS parent_systems
}
CALL {
  WITH a
  OPTIONAL MATCH (a)-[:HAS_COMPONENT]->(c:Component)
  WITH c WHERE c IS NOT NULL
  RETURN collect(DISTINCT c) AS components
}
CALL {
  WITH a
  OPTIONAL MATCH (op:Operator)-[r:RESPONSIBLE_FOR]->(a)
  WITH op, r WHERE op IS NOT NULL
  RETURN collect({node: op, escalation_order: r.escalation_order}) AS operators
}
CALL {
  // Upstream supply: what this asset depends on. :FEEDS only — a controller
  // upstream is not a supplier, and treating it as one would say an asset is
  // starved when its PLC fails.
  WITH a
  OPTIONAL MATCH path = (u:Asset)-[:FEEDS*1..3]->(a)
  WITH u, min(length(path)) AS hops
  WHERE u IS NOT NULL
  RETURN collect({node: u, hops: hops}) AS upstream
}
CALL {
  // Downstream blast radius: what stops, or runs unsupervised, if this asset
  // is isolated. :FEEDS|CONTROLS, because both are consequences of losing it.
  WITH a
  OPTIONAL MATCH path = (a)-[:FEEDS|CONTROLS*1..3]->(d:Asset)
  WITH d, min(length(path)) AS hops
  WHERE d IS NOT NULL
  RETURN collect({node: d, hops: hops}) AS downstream
}
RETURN a AS target, parent_systems, components, operators, upstream, downstream
`

// Result columns, named once so the query and the mapper cannot drift apart.
const (
	columnParentSystems = "parent_systems"
	columnComponents    = "components"
	columnOperators     = "operators"

	// Keys inside the maps the subqueries collect.
	keyNode            = "node"
	keyDepth           = "depth"
	keyEscalationOrder = "escalation_order"
	keyHops            = "hops"
)

// Property keys, canonical form first, mirroring the tolerance the blast-radius
// mapper already applies to asset nodes.
var (
	systemIDKeys   = []string{"id", "node_id", "nodeId"}
	systemNameKeys = []string{"name", "display_name"}
	systemTypeKeys = []string{"type", "system_type", "systemType"}

	componentIDKeys   = []string{"id", "component_id", "componentId"}
	componentNameKeys = []string{"name", "display_name"}

	// operatorShiftLabelKeys names the shift an operator works. It is distinct
	// from operatorShiftKeys in graph_resolver.go, which is the boolean "is this
	// person on shift right now".
	operatorShiftLabelKeys = []string{"shift", "shift_code", "shiftCode"}
	operatorRoleKeys       = []string{"role", "job_role", "jobRole"}
	operatorContactKeys    = []string{"contact", "phone", "email"}

	assetClassKeys       = []string{"asset_class", "assetClass", "class"}
	assetSiteKeys        = []string{"site", "site_id", "siteId", "location"}
	assetCriticalityKeys = []string{"criticality", "criticality_level", "criticalityLevel"}
	assetWindowKeys      = []string{"maintenance_window", "maintenanceWindow"}
)

// ---------------------------------------------------------------------------
// Resolution
// ---------------------------------------------------------------------------

// ResolveOntologyNeighbourhood returns the containment subtree around assetID.
//
// It shares every guarantee ResolveAssetContext makes: the caller's context is
// narrowed to the resolver's budget rather than replaced, so an upstream
// cancellation still works but a slow traversal can never pin an ingestion
// worker; ErrAssetNotFound is returned and counted separately when the asset is
// simply absent from the topology.
func (r *Neo4jGraphResolver) ResolveOntologyNeighbourhood(ctx context.Context, assetID string) (*OntologyNeighbourhood, error) {
	assetID = strings.TrimSpace(assetID)
	if assetID == "" {
		return nil, errors.New("asset id must not be empty")
	}

	r.mu.RLock()
	closed := r.closed
	r.mu.RUnlock()
	if closed {
		return nil, fmt.Errorf("resolve asset %q: %w", assetID, ErrResolverClosed)
	}

	r.resolutions.Add(1)
	started := time.Now()
	log := r.log.With("asset_id", assetID, "projection", "containment")

	resolved, err := executeRead(ctx, r, assetID, started, log, func(txCtx context.Context, tx neo4j.ManagedTransaction) (*OntologyNeighbourhood, error) {
		return readOntologyNeighbourhood(txCtx, tx, assetID, log)
	})
	if err != nil {
		return nil, err
	}

	resolved.ResolvedAt = time.Now().UTC()
	resolved.ResolutionLatency = time.Since(started)

	log.Debug("resolved asset ontology neighbourhood", "neighbourhood", resolved)
	return resolved, nil
}

// executeRead runs read inside a managed read transaction under the resolver's
// budget, tearing the session down on an independent deadline and routing every
// failure through the shared classifier.
func executeRead[T any](
	ctx context.Context,
	r *Neo4jGraphResolver,
	assetID string,
	started time.Time,
	log *slog.Logger,
	read func(context.Context, neo4j.ManagedTransaction) (*T, error),
) (*T, error) {
	queryCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	session := r.driver.NewSession(queryCtx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeRead,
		DatabaseName: r.database,
	})
	defer func() {
		// The query context is very likely already expired by now, so the
		// session is torn down under a fresh, independent deadline.
		closeCtx, closeCancel := context.WithTimeout(context.WithoutCancel(ctx), sessionCloseTimeout)
		defer closeCancel()
		if err := session.Close(closeCtx); err != nil {
			log.Warn("closing neo4j session failed", "error", err)
		}
	}()

	result, err := session.ExecuteRead(queryCtx, func(tx neo4j.ManagedTransaction) (any, error) {
		return read(queryCtx, tx)
	}, neo4j.WithTxTimeout(r.timeout))
	if err != nil {
		return nil, r.classify(ctx, queryCtx, err, assetID, started, log)
	}

	typed, ok := result.(*T)
	if !ok || typed == nil {
		r.failures.Add(1)
		log.Error("read transaction returned an unexpected value", "type", fmt.Sprintf("%T", result))
		var want *T
		return nil, fmt.Errorf("resolve asset %q: read transaction returned %T, want %T", assetID, result, want)
	}
	return typed, nil
}

// readOntologyNeighbourhood runs the containment query and materialises the
// result inside the managed transaction, so nothing streams back out of a
// closed scope.
func readOntologyNeighbourhood(
	ctx context.Context,
	tx neo4j.ManagedTransaction,
	assetID string,
	log *slog.Logger,
) (*OntologyNeighbourhood, error) {
	result, err := tx.Run(ctx, CypherResolveOntologyNeighbourhood, map[string]any{"assetId": assetID})
	if err != nil {
		return nil, fmt.Errorf("run ontology query: %w", err)
	}

	if !result.Next(ctx) {
		if err := result.Err(); err != nil {
			return nil, fmt.Errorf("read ontology record: %w", err)
		}
		// The leading MATCH is not optional, so zero rows means zero assets.
		return nil, ErrAssetNotFound
	}

	resolved, err := mapNeighbourhood(result.Record(), log)
	if err != nil {
		return nil, err
	}

	// Every branch aggregates, so a second row is not reachable through this
	// query. Draining anyway returns the connection to the pool cleanly if that
	// ever stops being true.
	for result.Next(ctx) {
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("drain ontology result: %w", err)
	}

	return resolved, nil
}

// ---------------------------------------------------------------------------
// Record mapping
// ---------------------------------------------------------------------------

// mapNeighbourhood unwraps one driver record into the domain model. A malformed
// neighbour must never take down an otherwise usable resolution: the target is
// the only mandatory part.
func mapNeighbourhood(record *neo4j.Record, log *slog.Logger) (*OntologyNeighbourhood, error) {
	if record == nil {
		return nil, errors.New("resolution returned a nil record")
	}

	targetRaw, ok := record.Get(columnTarget)
	if !ok {
		return nil, fmt.Errorf("resolution record has no %q column", columnTarget)
	}
	if targetRaw == nil {
		return nil, ErrAssetNotFound
	}
	targetNode, ok := asNode(targetRaw)
	if !ok {
		return nil, fmt.Errorf("column %q holds %T, want a graph node", columnTarget, targetRaw)
	}
	target, err := mapAssetNode(targetNode)
	if err != nil {
		return nil, fmt.Errorf("map target asset: %w", err)
	}

	resolved := &OntologyNeighbourhood{Target: target}
	resolved.AssetClass, _ = stringProp(targetNode.Props, assetClassKeys...)
	resolved.Site, _ = stringProp(targetNode.Props, assetSiteKeys...)
	resolved.Criticality, _ = stringProp(targetNode.Props, assetCriticalityKeys...)
	resolved.MaintenanceWindow, _ = stringProp(targetNode.Props, assetWindowKeys...)

	resolved.ModelNumber, _ = stringProp(targetNode.Props, assetModelKeys...)

	resolved.ParentSystems = mapParentSystems(record, log)
	resolved.Components = mapComponents(record, log)
	resolved.Operators = mapOperators(record, log)
	resolved.Upstream = mapFlowNodes(record, columnUpstream, log)
	resolved.Downstream = mapFlowNodes(record, columnDownstream, log)

	return resolved, nil
}

// mapFlowNodes unwraps an upstream or downstream column into assets ordered
// nearest-first.
//
// The ordering is the contract, not a convenience. Agent and operator logic
// walks upstream to find the origin of a cascade and downstream to size a
// containment decision; both read index 0 as "one hop away". Ties are broken by
// asset id so two resolutions of an unchanged graph are identical, which is
// what makes the fixture-vs-live equivalence test meaningful.
func mapFlowNodes(record *neo4j.Record, column string, log *slog.Logger) []FlowNode {
	entries := collectedList(record, column, log)
	nodes := make([]FlowNode, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))

	for _, entry := range entries {
		fields, ok := entry.(map[string]any)
		if !ok {
			log.Warn("flow entry is not a map; skipping",
				"column", column, "type", fmt.Sprintf("%T", entry))
			continue
		}

		node, ok := asNode(fields[keyNode])
		if !ok {
			continue
		}
		asset, err := mapAssetNode(node)
		if err != nil {
			log.Warn("skipping unmappable flow asset", "column", column, "error", err)
			continue
		}
		// A diamond in the flow network reaches the same asset by two paths.
		// The query already takes min(length(path)), but a duplicate would
		// still double-count the blast radius.
		if _, duplicate := seen[asset.AssetID]; duplicate {
			continue
		}
		seen[asset.AssetID] = struct{}{}

		hops, _ := intValue(fields[keyHops])
		nodes = append(nodes, FlowNode{AssetNode: asset, Hops: hops})
	}

	sort.SliceStable(nodes, func(i, j int) bool {
		if nodes[i].Hops != nodes[j].Hops {
			return nodes[i].Hops < nodes[j].Hops
		}
		return nodes[i].AssetID < nodes[j].AssetID
	})
	return nodes
}

// mapParentSystems unwraps the ancestry column, ordered from the immediate
// parent outwards so a consumer rendering "Pump 221, Line 4 Hydraulic Loop,
// Rotterdam Plant" gets it right without sorting.
func mapParentSystems(record *neo4j.Record, log *slog.Logger) []SystemNode {
	entries := collectedList(record, columnParentSystems, log)
	systems := make([]SystemNode, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	skipped := 0

	for _, entry := range entries {
		wrapper, ok := entry.(map[string]any)
		if !ok {
			skipped++
			continue
		}
		node, ok := asNode(wrapper[keyNode])
		if !ok {
			skipped++
			continue
		}
		if node.ElementId != "" {
			if _, duplicate := seen[node.ElementId]; duplicate {
				continue
			}
		}

		id, ok := stringProp(node.Props, systemIDKeys...)
		if !ok {
			skipped++
			log.Warn("skipping unidentifiable system node",
				"element_id", node.ElementId, "labels", node.Labels)
			continue
		}

		system := SystemNode{NodeID: id, ElementID: node.ElementId}
		system.Name, _ = stringProp(node.Props, systemNameKeys...)
		if kind, ok := stringProp(node.Props, systemTypeKeys...); ok {
			system.Type = kind
		} else {
			system.Type = specificLabel(node.Labels, "System")
		}
		if depth, ok := intValue(wrapper[keyDepth]); ok {
			system.Depth = depth
		}

		if node.ElementId != "" {
			seen[node.ElementId] = struct{}{}
		}
		systems = append(systems, system)
	}

	if skipped > 0 {
		log.Warn("ancestor nodes dropped during mapping", "column", columnParentSystems, "skipped", skipped)
	}

	// Ties keep their traversal order, which is stable for a given graph.
	sort.SliceStable(systems, func(i, j int) bool { return systems[i].Depth < systems[j].Depth })
	return systems
}

// mapComponents unwraps the component column, ordered by identifier so two
// resolutions of the same asset produce byte-identical payloads.
func mapComponents(record *neo4j.Record, log *slog.Logger) []ComponentNode {
	entries := collectedList(record, columnComponents, log)
	components := make([]ComponentNode, 0, len(entries))
	skipped := 0

	for _, entry := range entries {
		node, ok := asNode(entry)
		if !ok {
			skipped++
			continue
		}
		id, ok := stringProp(node.Props, componentIDKeys...)
		if !ok {
			skipped++
			log.Warn("skipping unidentifiable component node",
				"element_id", node.ElementId, "labels", node.Labels)
			continue
		}
		component := ComponentNode{ComponentID: id, ElementID: node.ElementId}
		component.Name, _ = stringProp(node.Props, componentNameKeys...)
		components = append(components, component)
	}

	if skipped > 0 {
		log.Warn("component nodes dropped during mapping", "column", columnComponents, "skipped", skipped)
	}

	sort.SliceStable(components, func(i, j int) bool { return components[i].ComponentID < components[j].ComponentID })
	return components
}

// mapOperators unwraps the accountability column, ordered by escalation order
// so the first entry is who to call. An operator whose edge carries no
// escalation order sorts last rather than first, since an unspecified position
// is not a claim to be primary.
func mapOperators(record *neo4j.Record, log *slog.Logger) []OperatorAssignment {
	entries := collectedList(record, columnOperators, log)
	operators := make([]OperatorAssignment, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	skipped := 0

	for _, entry := range entries {
		wrapper, ok := entry.(map[string]any)
		if !ok {
			skipped++
			continue
		}
		node, ok := asNode(wrapper[keyNode])
		if !ok {
			skipped++
			continue
		}
		if node.ElementId != "" {
			if _, duplicate := seen[node.ElementId]; duplicate {
				continue
			}
		}
		operator, err := mapOperatorNode(node)
		if err != nil {
			skipped++
			log.Warn("skipping unmappable operator node",
				"element_id", node.ElementId, "error", err)
			continue
		}

		assignment := OperatorAssignment{OperatorNode: operator}
		assignment.Role, _ = stringProp(node.Props, operatorRoleKeys...)
		assignment.Shift, _ = stringProp(node.Props, operatorShiftLabelKeys...)
		assignment.Contact, _ = stringProp(node.Props, operatorContactKeys...)
		if order, ok := intValue(wrapper[keyEscalationOrder]); ok && order > 0 {
			assignment.EscalationOrder = order
		}

		if node.ElementId != "" {
			seen[node.ElementId] = struct{}{}
		}
		operators = append(operators, assignment)
	}

	if skipped > 0 {
		log.Warn("operator nodes dropped during mapping", "column", columnOperators, "skipped", skipped)
	}

	sort.SliceStable(operators, func(i, j int) bool {
		left, right := operators[i].EscalationOrder, operators[j].EscalationOrder
		switch {
		case left == right:
			return operators[i].TechnicianID < operators[j].TechnicianID
		case left == 0:
			return false
		case right == 0:
			return true
		default:
			return left < right
		}
	})
	return operators
}

// collectedList narrows a collect() column to a slice, treating anything
// unexpected as empty rather than failing the whole resolution.
func collectedList(record *neo4j.Record, column string, log *slog.Logger) []any {
	raw, ok := record.Get(column)
	if !ok || raw == nil {
		return nil
	}
	list, ok := raw.([]any)
	if !ok {
		log.Warn("collected column is not a list; treating it as empty",
			"column", column, "type", fmt.Sprintf("%T", raw))
		return nil
	}
	return list
}

// specificLabel picks the most informative label on a node, preferring anything
// other than the generic one the query already matched on.
func specificLabel(labels []string, generic string) string {
	for _, label := range labels {
		if label != generic {
			return label
		}
	}
	if len(labels) > 0 {
		return labels[0]
	}
	return ""
}

// intValue coerces the numeric shapes Bolt can deliver for a small integer.
// Cypher integers arrive as int64; a property written by a JSON loader can
// arrive as float64 or even as a string.
func intValue(raw any) (int, bool) {
	switch v := raw.(type) {
	case int64:
		return int(v), true
	case int:
		return v, true
	case float64:
		return int(v), true
	case string:
		if parsed, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return parsed, true
		}
	}
	return 0, false
}
