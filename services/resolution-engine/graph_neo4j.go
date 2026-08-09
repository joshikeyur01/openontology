package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openontology/resolution-engine/internal/graph"
)

// SourceNeo4jLive marks an ontology context that came out of the live topology
// graph. It is deliberately distinct from the stand-in's "neo4j-mock:*" sources
// so a consumer — or an operator reading a mutation off the topic — can tell at
// a glance whether the enrichment was real.
const SourceNeo4jLive = "neo4j:live"

// neo4jRedialInterval bounds how often a disconnected adapter retries the
// handshake. The graph is a degradable dependency, so a cluster that is down
// must cost one failed dial per interval, not one per telemetry event.
const neo4jRedialInterval = 15 * time.Second

// ErrGraphDisconnected reports that the resolver has never reached the cluster
// and is waiting out its redial interval. It is returned instead of dialling on
// every call, so an outage degrades mutations at full ingestion speed.
var ErrGraphDisconnected = errors.New("neo4j graph tier is not connected")

// ontologyContextResolver is the slice of internal/graph this adapter needs.
// Declaring it here rather than depending on the concrete resolver keeps the
// mapping testable without a cluster.
type ontologyContextResolver interface {
	ResolveOntologyNeighbourhood(ctx context.Context, assetID string) (*graph.OntologyNeighbourhood, error)
	Close() error
}

// Neo4jGraphAdapter reconciles two interfaces that were written against each
// other's absence: internal/graph resolves an OntologyNeighbourhood out of
// Neo4j, and the engine consumes an OntologyContext. Neither side moves; the
// translation lives here.
//
// It owns three things the raw resolver deliberately does not:
//
//   - the TTL cache, so a re-alerting asset does not re-traverse the graph
//     every time it breaches;
//   - a hard query budget at the call boundary, enforced independently of the
//     driver's own transaction timeout;
//   - failure semantics identical to the stand-in's, so a graph outage degrades
//     a mutation instead of dropping an alarm.
//
// The connection is established eagerly at construction and re-established
// lazily afterwards. Booting into an unreachable cluster is a degraded state,
// not a fatal one: an engine that refuses to start because the topology store
// is down drops every alarm, which is the exact failure this tier is supposed
// to prevent.
type Neo4jGraphAdapter struct {
	dial   func() (ontologyContextResolver, error)
	cache  *graphContextCache
	budget time.Duration
	log    *slog.Logger

	// mu guards the connection state below.
	mu         sync.RWMutex
	resolver   ontologyContextResolver
	dialErr    error
	nextDialAt time.Time

	// dialing admits one redial at a time. Callers that lose the race degrade
	// immediately rather than queueing behind a handshake.
	dialing atomic.Bool

	lookups atomic.Uint64
	hits    atomic.Uint64
	errs    atomic.Uint64
	absent  atomic.Uint64

	closeOnce sync.Once
	closeErr  error
}

// Compile-time proof that the adapter satisfies the engine's contract.
var (
	_ GraphResolver = (*Neo4jGraphAdapter)(nil)
	_ graphDetailer = (*Neo4jGraphAdapter)(nil)
)

// NewNeo4jGraphAdapter builds the adapter and attempts the first connection.
// A failed handshake is logged loudly and left to the redial path; it never
// stops the engine from starting.
func NewNeo4jGraphAdapter(cfg Config, log *slog.Logger) *Neo4jGraphAdapter {
	adapter := newNeo4jGraphAdapter(func() (ontologyContextResolver, error) {
		return graph.NewNeo4jGraphResolver(
			cfg.Neo4jURI,
			cfg.Neo4jUsername,
			cfg.Neo4jPassword,
			graph.WithLogger(log),
			graph.WithDatabase(cfg.Neo4jDatabase),
			graph.WithQueryTimeout(cfg.GraphQueryBudget),
			graph.WithMaxConnectionPoolSize(cfg.Neo4jMaxPoolSize),
			graph.WithConnectivityTimeout(cfg.Neo4jConnectTimeout),
		)
	}, cfg, log)

	if _, err := adapter.connect(); err != nil {
		adapter.log.Error("neo4j unreachable at startup; mutations will be degraded until it answers",
			"uri", cfg.Neo4jURI, "retry_interval", neo4jRedialInterval, "error", err)
	}
	return adapter
}

// newNeo4jGraphAdapter builds an adapter over an arbitrary dialler. Tests supply
// a fake here; NewNeo4jGraphAdapter supplies the live driver.
func newNeo4jGraphAdapter(dial func() (ontologyContextResolver, error), cfg Config, log *slog.Logger) *Neo4jGraphAdapter {
	return &Neo4jGraphAdapter{
		dial:   dial,
		cache:  newGraphContextCache(cfg.GraphCacheTTL),
		budget: cfg.GraphQueryBudget,
		log:    log.With("component", "graph", "provider", GraphProviderNeo4j),
	}
}

// Provider names the backing store.
func (a *Neo4jGraphAdapter) Provider() string { return GraphProviderNeo4j }

// ResolveAsset returns the asset's ontology context, from the TTL cache when it
// can and from Neo4j when it must.
//
// Every failure path returns an error rather than a thin context: the engine
// turns that into degraded=true and still emits the mutation. Nothing here is
// allowed to swallow an alarm.
func (a *Neo4jGraphAdapter) ResolveAsset(ctx context.Context, assetID string) (OntologyContext, error) {
	a.lookups.Add(1)

	if cached, ok := a.cache.Lookup(assetID); ok {
		a.hits.Add(1)
		cached.CacheHit = true
		return cached, nil
	}

	resolver, err := a.connect()
	if err != nil {
		a.errs.Add(1)
		return OntologyContext{}, err
	}

	// The budget is enforced here as well as inside the resolver. The resolver's
	// own timeout covers the transaction; this one also covers connection
	// acquisition and any retry the driver performs, which is what actually
	// bounds an ingestion worker's exposure to a sick cluster.
	queryCtx, cancel := context.WithTimeout(ctx, a.budget)
	defer cancel()

	// A miss is not cached. The engine only resolves when it is about to emit a
	// mutation — a state transition or a re-alert, not every sample — so an
	// unregistered asset costs one traversal per alarm, and an asset seeded
	// after the engine started resolves on its next breach rather than after a
	// cache TTL.
	neighbourhood, err := resolver.ResolveOntologyNeighbourhood(queryCtx, assetID)
	if err != nil {
		a.errs.Add(1)
		if errors.Is(err, graph.ErrAssetNotFound) {
			a.absent.Add(1)
		}
		return OntologyContext{}, err
	}

	resolved := ontologyContextFrom(assetID, neighbourhood)
	a.cache.Store(assetID, resolved)
	return resolved.Clone(), nil
}

// connect returns the live resolver, dialling at most once per redial interval.
//
// Once a connection has been made it is kept: the Bolt driver owns its own pool
// and re-opens connections when a cluster comes back, so a mid-run outage needs
// no help from here — it surfaces as failed resolutions that degrade mutations
// and then simply stops happening.
func (a *Neo4jGraphAdapter) connect() (ontologyContextResolver, error) {
	a.mu.RLock()
	resolver := a.resolver
	a.mu.RUnlock()
	if resolver != nil {
		return resolver, nil
	}

	if !a.dialing.CompareAndSwap(false, true) {
		return nil, a.disconnectedError()
	}
	defer a.dialing.Store(false)

	a.mu.RLock()
	resolver, notBefore := a.resolver, a.nextDialAt
	a.mu.RUnlock()
	if resolver != nil {
		return resolver, nil
	}
	if time.Now().Before(notBefore) {
		return nil, a.disconnectedError()
	}

	dialed, err := a.dial()

	a.mu.Lock()
	defer a.mu.Unlock()
	if err != nil {
		a.dialErr = err
		a.nextDialAt = time.Now().Add(neo4jRedialInterval)
		return nil, fmt.Errorf("%w: %w", ErrGraphDisconnected, err)
	}
	a.resolver = dialed
	a.dialErr = nil
	a.log.Info("neo4j graph tier connected")
	return dialed, nil
}

// disconnectedError reports the outage without re-dialling, carrying the reason
// the last handshake failed so a degraded mutation says something useful.
func (a *Neo4jGraphAdapter) disconnectedError() error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.dialErr != nil {
		return fmt.Errorf("%w: %w", ErrGraphDisconnected, a.dialErr)
	}
	return ErrGraphDisconnected
}

// Connected reports whether a handshake has succeeded.
func (a *Neo4jGraphAdapter) Connected() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.resolver != nil
}

// Stats exposes the counters the engine's metrics endpoint already renders.
func (a *Neo4jGraphAdapter) Stats() (lookups, hits, errs uint64) {
	return a.lookups.Load(), a.hits.Load(), a.errs.Load()
}

// Detail adds the provider-specific counters to /stats — the ones that separate
// "the cluster is down" from "this asset was never modelled".
func (a *Neo4jGraphAdapter) Detail() map[string]any {
	a.mu.RLock()
	resolver, dialErr := a.resolver, a.dialErr
	a.mu.RUnlock()

	detail := map[string]any{
		"connected":           resolver != nil,
		"cached_assets":       a.cache.Len(),
		"assets_not_in_graph": a.absent.Load(),
		"query_budget":        a.budget.String(),
	}
	if dialErr != nil {
		detail["last_connect_error"] = truncate(dialErr.Error(), 300)
	}
	if live, ok := resolver.(interface{ Stats() graph.Stats }); ok {
		driver := live.Stats()
		detail["driver_resolutions"] = driver.Resolutions
		detail["driver_not_found"] = driver.NotFound
		detail["driver_failures"] = driver.Failures
		detail["driver_timeouts"] = driver.Timeouts
	}
	return detail
}

// Close releases the driver. It is idempotent, so a deferred Close in main and
// an explicit one in a test cannot double-close the pool.
func (a *Neo4jGraphAdapter) Close() error {
	a.closeOnce.Do(func() {
		a.mu.Lock()
		resolver := a.resolver
		a.resolver = nil
		a.mu.Unlock()

		if resolver != nil {
			a.closeErr = resolver.Close()
		}
	})
	return a.closeErr
}

// ontologyContextFrom projects the graph package's containment subtree onto the
// wire model that ontology.mutations carries.
//
// assetID is the identifier the telemetry arrived under. The graph's own
// spelling wins when it has one, so downstream consumers key on the canonical
// identifier rather than on whatever a field gateway happened to send.
func ontologyContextFrom(assetID string, n *graph.OntologyNeighbourhood) OntologyContext {
	out := OntologyContext{
		AssetID:           assetID,
		Source:            SourceNeo4jLive,
		ResolvedAt:        time.Now().UTC(),
		ParentSystems:     []SystemNode{},
		Components:        []string{},
		AssignedOperators: []Operator{},

		UpstreamDependencies: []FlowRef{},
		DownstreamImpacts:    []FlowRef{},
	}
	if n == nil {
		return out
	}

	if n.Target.AssetID != "" {
		out.AssetID = n.Target.AssetID
	}
	out.AssetName = n.Target.Name
	out.AssetClass = n.AssetClass
	out.ModelNumber = n.ModelNumber
	out.Site = n.Site
	out.Criticality = n.Criticality
	out.MaintenanceWindow = n.MaintenanceWindow
	if !n.ResolvedAt.IsZero() {
		out.ResolvedAt = n.ResolvedAt
	}

	// Relation is stamped by direction rather than read from the edge, because
	// the query already encodes it: the upstream subquery follows :FEEDS only,
	// while the downstream one follows :FEEDS|CONTROLS. Carrying the literal
	// relationship type per edge would need a third traversal to recover it,
	// and "supplies" versus "impacts" is the distinction a consumer acts on.
	out.UpstreamDependencies = flowRefsFrom(n.Upstream, "SUPPLIES")
	out.DownstreamImpacts = flowRefsFrom(n.Downstream, "IMPACTS")
	out.BlastRadius = len(out.DownstreamImpacts)

	out.ParentSystems = make([]SystemNode, 0, len(n.ParentSystems))
	for _, system := range n.ParentSystems {
		out.ParentSystems = append(out.ParentSystems, SystemNode{
			NodeID: system.NodeID,
			Name:   system.Name,
			Type:   system.Type,
			Depth:  system.Depth,
		})
	}

	// The wire model carries component names, not nodes. The identifier is the
	// stable thing a work order references, so it is the fallback when a
	// component node carries no display name.
	out.Components = make([]string, 0, len(n.Components))
	for _, component := range n.Components {
		out.Components = append(out.Components, firstNonEmpty(component.Name, component.ComponentID))
	}

	out.AssignedOperators = make([]Operator, 0, len(n.Operators))
	for i, assignment := range n.Operators {
		operator := Operator{
			OperatorID:      assignment.TechnicianID,
			Name:            assignment.Name,
			Role:            firstNonEmpty(assignment.Role, assignment.CertificationLevel),
			Shift:           assignment.Shift,
			Contact:         assignment.Contact,
			EscalationOrder: assignment.EscalationOrder,
		}
		// Operators arrive sorted by escalation order. An edge that carries none
		// still needs a position, or PrimaryOperator would pick arbitrarily.
		if operator.EscalationOrder == 0 {
			operator.EscalationOrder = i + 1
		}
		out.AssignedOperators = append(out.AssignedOperators, operator)
	}

	return out
}

// graphProviderMetric renders the live provider as a Prometheus info gauge, so
// a dashboard can tell a degraded real deployment apart from one that is
// happily running on fixtures.
func graphProviderMetric(provider string) string {
	return fmt.Sprintf(
		"# HELP openontology_graph_provider_info The graph tier currently backing ontology resolution.\n"+
			"# TYPE openontology_graph_provider_info gauge\n"+
			"openontology_graph_provider_info{provider=%q} 1\n",
		provider,
	)
}

// flowRefsFrom projects resolved flow nodes onto the wire model, preserving the
// nearest-first ordering the resolver established.
func flowRefsFrom(nodes []graph.FlowNode, relation string) []FlowRef {
	refs := make([]FlowRef, 0, len(nodes))
	for _, node := range nodes {
		refs = append(refs, FlowRef{
			AssetID:  node.AssetID,
			Name:     node.Name,
			Model:    node.ModelNumber,
			Status:   node.CurrentStatus,
			Relation: relation,
			Hops:     node.Hops,
		})
	}
	return refs
}
