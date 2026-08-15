package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/openontology/resolution-engine/internal/crdt"
)

// The topology replica: the engine's local, convergent view of the asset graph.
//
// internal/crdt implements a state-based OR-Set over vertices and edges. This
// file is what gives it a producer. Every ontology context the engine resolves
// is folded into the replica, so the topology an engine has actually seen
// becomes replicated state rather than a per-request lookup that evaporates.
//
// The property it buys is the one the CRDT was written for: an edge site — an
// aircraft between uplinks, an isolated plant network — keeps resolving and
// mutating its slice of the topology through an outage of any length, and folds
// those mutations back afterwards without ever taking a distributed lock.
//
// Two things here are load-bearing and easy to get wrong:
//
//   - Writes are content-addressed. AddVertex re-asserts the whole property map
//     under a fresh Lamport stamp, so calling it once per mutation would climb
//     the clock forever, make every snapshot differ, and turn anti-entropy into
//     a permanent full transfer. Nothing is written unless the content actually
//     changed.
//
//   - Observation is off the critical path. A failure to fold a context into the
//     replica must never fail the mutation: the alarm matters, the replica is
//     bookkeeping. Errors are counted and logged, never returned upward.

// Relationship types replicated into the graph. They mirror the edges the
// resolver traverses, so the replicated graph is the same shape as the one
// Neo4j holds rather than a private encoding of it.
const (
	RelPartOf         = "PART_OF"
	RelHasComponent   = "HAS_COMPONENT"
	RelResponsibleFor = "RESPONSIBLE_FOR"
	RelFeeds          = "FEEDS"
	RelControls       = "CONTROLS"
)

// ReplicaConfig is the replication tier's configuration.
type ReplicaConfig struct {
	// Enabled turns the whole tier off. A single-site deployment has nothing to
	// converge with and pays nothing for it.
	Enabled bool

	// ID identifies this replica in every Lamport timeline. It must be stable
	// across restarts and unique across sites: two replicas sharing an id would
	// overwrite each other's timeline entries and lose removals.
	ID string

	// Peers are the replicas to exchange state with, as base URLs.
	Peers []string

	// SyncInterval is the anti-entropy period.
	SyncInterval time.Duration

	// ReconcileBudget bounds one join. A merge that outruns it leaves a valid,
	// partially joined state — every element's join is independently monotonic
	// — and the remainder arrives on the next round.
	ReconcileBudget time.Duration

	// SyncTimeout bounds one peer exchange, including the transfer.
	SyncTimeout time.Duration
}

// LoadReplicaConfig reads the replication tier from the environment.
func LoadReplicaConfig() (ReplicaConfig, error) {
	env := &envReader{}

	cfg := ReplicaConfig{
		Enabled:         env.boolean("REPLICA_ENABLED", false),
		ID:              env.str("REPLICA_ID", ""),
		Peers:           env.csv("REPLICA_PEERS", nil),
		SyncInterval:    env.duration("REPLICA_SYNC_INTERVAL", 15*time.Second),
		ReconcileBudget: env.duration("REPLICA_RECONCILE_BUDGET", 5*time.Second),
		SyncTimeout:     env.duration("REPLICA_SYNC_TIMEOUT", 10*time.Second),
	}
	if err := env.err(); err != nil {
		return cfg, err
	}
	return cfg, cfg.Validate()
}

// Validate rejects a configuration that would converge incorrectly.
func (c ReplicaConfig) Validate() error {
	if !c.Enabled {
		return nil
	}

	var errs []error
	if c.ID == "" {
		// Defaulting to a random id would be worse than failing: the replica
		// would take a new identity on every restart, so its old timeline
		// entries would never be superseded and tombstones would accumulate
		// under identities nothing writes to again.
		errs = append(errs, errors.New("REPLICA_ID is required when REPLICA_ENABLED is true"))
	}
	if c.SyncInterval <= 0 {
		errs = append(errs, fmt.Errorf("REPLICA_SYNC_INTERVAL must be positive, got %s", c.SyncInterval))
	}
	if c.ReconcileBudget <= 0 {
		errs = append(errs, fmt.Errorf("REPLICA_RECONCILE_BUDGET must be positive, got %s", c.ReconcileBudget))
	}
	if c.SyncTimeout <= c.ReconcileBudget {
		// The transfer has to fit inside the request alongside the join, or the
		// budget can never be the thing that fires and its distinct diagnosis
		// is lost.
		errs = append(errs, fmt.Errorf(
			"REPLICA_SYNC_TIMEOUT (%s) must exceed REPLICA_RECONCILE_BUDGET (%s)",
			c.SyncTimeout, c.ReconcileBudget))
	}
	return errors.Join(errs...)
}

// ReplicaMetrics counts what the replication tier did.
type ReplicaMetrics struct {
	ContextsObserved atomic.Uint64
	VerticesWritten  atomic.Uint64
	EdgesWritten     atomic.Uint64
	WritesSkipped    atomic.Uint64
	SyncAttempts     atomic.Uint64
	SyncFailures     atomic.Uint64
	MergesApplied    atomic.Uint64
	MergesReceived   atomic.Uint64
	BudgetExceeded   atomic.Uint64
}

// TopologyReplica owns this engine's CRDT view of the asset graph.
type TopologyReplica struct {
	cfg     ReplicaConfig
	crdt    *crdt.GraphCRDT
	log     *slog.Logger
	metrics ReplicaMetrics
}

// NewTopologyReplica constructs the replica. It is safe for concurrent use;
// GraphCRDT does its own locking.
func NewTopologyReplica(cfg ReplicaConfig, log *slog.Logger) *TopologyReplica {
	return &TopologyReplica{
		cfg: cfg,
		crdt: crdt.NewGraphCRDT(cfg.ID,
			crdt.WithLogger(log.With("component", "crdt")),
			crdt.WithReconcileBudget(cfg.ReconcileBudget),
		),
		log: log.With("component", "replica", "replica_id", cfg.ID),
	}
}

// ID returns this replica's identity.
func (r *TopologyReplica) ID() string { return r.cfg.ID }

// Clock returns the current Lamport clock.
func (r *TopologyReplica) Clock() int64 {
	if r == nil {
		return 0
	}
	return r.crdt.Clock()
}

// Digest is the content hash of the replicated state. Two converged replicas
// report the same digest, which is what makes convergence observable rather
// than asserted.
func (r *TopologyReplica) Digest() string {
	if r == nil {
		return ""
	}
	digest, err := r.crdt.Digest()
	if err != nil {
		r.log.Warn("could not compute replica digest", "error", err)
		return ""
	}
	return digest
}

// Snapshot returns the transferable replica state.
func (r *TopologyReplica) Snapshot() crdt.Snapshot { return r.crdt.Snapshot() }

// Observe folds one resolved ontology context into the replica.
//
// Deliberately returns nothing. This runs inside message processing, and a
// replication problem must not dead-letter an alarm — the mutation is the
// product, the replica is bookkeeping that catches up on the next observation.
func (r *TopologyReplica) Observe(ctx OntologyContext) {
	if r == nil || !r.cfg.Enabled || ctx.AssetID == "" {
		return
	}
	r.metrics.ContextsObserved.Add(1)

	r.upsertVertex(ctx.AssetID, map[string]string{
		"kind":         "asset",
		"name":         ctx.AssetName,
		"asset_class":  ctx.AssetClass,
		"model_number": ctx.ModelNumber,
		"site":         ctx.Site,
		"criticality":  ctx.Criticality,
	})

	for _, system := range ctx.ParentSystems {
		if system.NodeID == "" {
			continue
		}
		r.upsertVertex(system.NodeID, map[string]string{
			"kind":  "system",
			"name":  system.Name,
			"type":  system.Type,
			"depth": strconv.Itoa(system.Depth),
		})
		r.upsertEdge(ctx.AssetID, system.NodeID, RelPartOf)
	}

	for _, component := range ctx.Components {
		if component == "" {
			continue
		}
		// Components arrive as names rather than node ids on the wire, so the
		// replicated identity is scoped to the asset. Two assets with a part
		// called "seal_pack" are two different physical parts, and merging them
		// into one vertex would let a removal on one asset tombstone the other.
		id := ctx.AssetID + "/" + component
		r.upsertVertex(id, map[string]string{
			"kind":  "component",
			"name":  component,
			"asset": ctx.AssetID,
		})
		r.upsertEdge(ctx.AssetID, id, RelHasComponent)
	}

	for _, operator := range ctx.AssignedOperators {
		if operator.OperatorID == "" {
			continue
		}
		r.upsertVertex(operator.OperatorID, map[string]string{
			"kind": "operator",
			"name": operator.Name,
			"role": operator.Role,
		})
		// Direction matches the graph: the operator is responsible for the
		// asset, not the other way round.
		r.upsertEdge(operator.OperatorID, ctx.AssetID, RelResponsibleFor)
	}

	// Flow edges carry the direction the physical process runs in: an upstream
	// node feeds this asset, this asset feeds a downstream one.
	for _, node := range ctx.UpstreamDependencies {
		r.observeFlow(node, node.AssetID, ctx.AssetID)
	}
	for _, node := range ctx.DownstreamImpacts {
		r.observeFlow(node, ctx.AssetID, node.AssetID)
	}
}

func (r *TopologyReplica) observeFlow(node FlowRef, source, target string) {
	if node.AssetID == "" || source == "" || target == "" {
		return
	}
	r.upsertVertex(node.AssetID, map[string]string{
		"kind":         "asset",
		"name":         node.Name,
		"model_number": node.Model,
		"status":       node.Status,
	})
	// The payload distinguishes SUPPLIES from IMPACTS by direction, not by
	// relationship type, and both are :FEEDS in the graph. A :CONTROLS edge is
	// only ever downstream, and is not separable here without carrying the type
	// on the wire; FEEDS is the conservative choice because it is the one that
	// implies material dependency.
	r.upsertEdge(source, target, RelFeeds)
}

// upsertVertex writes a vertex only when its content differs from what the
// replica already holds.
//
// This is what keeps the Lamport clock from climbing on every telemetry event.
// AddVertex re-asserts the entire property map under a fresh stamp, so an
// unconditional write would make every snapshot differ from the last, every
// digest unique, and every anti-entropy round a full transfer of a graph that
// had not changed.
func (r *TopologyReplica) upsertVertex(id string, props map[string]string) {
	props = pruneEmpty(props)

	if existing, found := r.crdt.LookupVertex(id); found && existing.Live() {
		if sameProperties(existing.Properties, props) {
			r.metrics.WritesSkipped.Add(1)
			return
		}
	}
	r.crdt.AddVertex(id, props)
	r.metrics.VerticesWritten.Add(1)
}

// upsertEdge writes an edge only when it is not already live.
//
// Edges carry no payload, so an existing live edge is already exactly what a
// re-assert would produce — the only effect would be to advance the clock.
func (r *TopologyReplica) upsertEdge(source, target, relationship string) {
	for _, edge := range r.crdt.LiveEdges() {
		if edge.SourceUUID == source && edge.TargetUUID == target && edge.RelationshipType == relationship {
			r.metrics.WritesSkipped.Add(1)
			return
		}
	}
	r.crdt.AddEdge(source, target, relationship)
	r.metrics.EdgesWritten.Add(1)
}

// pruneEmpty drops blank values so that an absent property and an empty one are
// the same state. Without this a context resolved before a field was populated
// would differ from one resolved after, and the two would fight forever.
func pruneEmpty(props map[string]string) map[string]string {
	out := make(map[string]string, len(props))
	for key, value := range props {
		if value != "" {
			out[key] = value
		}
	}
	return out
}

func sameProperties(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if other, ok := b[key]; !ok || other != value {
			return false
		}
	}
	return true
}

// ReplicaObservation is one replica's Lamport timeline for a vertex, as carried
// on the mutation payload.
//
// This is what lets a downstream consumer decide whether the topology it is
// being handed is trustworthy enough to act irreversibly on. A vertex whose
// highest add stamp barely leads its highest remove stamp, or which several
// replicas disagree about, is a graph in the middle of converging — not a basis
// for isolating an asset.
type ReplicaObservation struct {
	ReplicaID   string `json:"replica_id"`
	AddStamp    int64  `json:"add_stamp"`
	RemoveStamp int64  `json:"remove_stamp"`
}

// Observations returns the per-replica timeline for one asset vertex, sorted by
// replica id so the payload is stable across scrapes and diffs.
func (r *TopologyReplica) Observations(assetID string) []ReplicaObservation {
	if r == nil || !r.cfg.Enabled || assetID == "" {
		return nil
	}

	vertex, found := r.crdt.LookupVertex(assetID)
	if !found {
		return nil
	}

	byReplica := make(map[string]*ReplicaObservation)
	for replica, stamp := range vertex.AddTimeline {
		byReplica[replica] = &ReplicaObservation{ReplicaID: replica, AddStamp: stamp}
	}
	for replica, stamp := range vertex.RemoveTimeline {
		if existing, ok := byReplica[replica]; ok {
			existing.RemoveStamp = stamp
			continue
		}
		byReplica[replica] = &ReplicaObservation{ReplicaID: replica, RemoveStamp: stamp}
	}

	out := make([]ReplicaObservation, 0, len(byReplica))
	for _, observation := range byReplica {
		out = append(out, *observation)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ReplicaID < out[j].ReplicaID })
	return out
}

// MergeSnapshot folds a peer's state into this replica.
func (r *TopologyReplica) MergeSnapshot(snap crdt.Snapshot) error {
	if r == nil || !r.cfg.Enabled {
		return errors.New("replication is disabled on this engine")
	}
	r.metrics.MergesReceived.Add(1)

	if err := r.crdt.MergeSnapshot(snap); err != nil {
		if errors.Is(err, crdt.ErrReconcileBudgetExceeded) {
			r.metrics.BudgetExceeded.Add(1)
		}
		return err
	}
	r.metrics.MergesApplied.Add(1)
	return nil
}

// ReplicaStats is the replication tier's view for /stats.
type ReplicaStats struct {
	Enabled        bool     `json:"enabled"`
	ReplicaID      string   `json:"replica_id,omitempty"`
	Peers          []string `json:"peers,omitempty"`
	SyncInterval   string   `json:"sync_interval,omitempty"`
	LamportClock   int64    `json:"lamport_clock"`
	Digest         string   `json:"graph_revision,omitempty"`
	LiveVertices   int      `json:"live_vertices"`
	LiveEdges      int      `json:"live_edges"`
	DanglingEdges  int      `json:"dangling_edges"`
	Observed       uint64   `json:"contexts_observed"`
	VertexWrites   uint64   `json:"vertex_writes"`
	EdgeWrites     uint64   `json:"edge_writes"`
	WritesSkipped  uint64   `json:"writes_skipped"`
	SyncAttempts   uint64   `json:"sync_attempts"`
	SyncFailures   uint64   `json:"sync_failures"`
	MergesApplied  uint64   `json:"merges_applied"`
	MergesReceived uint64   `json:"merges_received"`
	BudgetExceeded uint64   `json:"budget_exceeded"`
}

// Stats renders the replication tier.
func (r *TopologyReplica) Stats() ReplicaStats {
	if r == nil || !r.cfg.Enabled {
		return ReplicaStats{Enabled: false}
	}
	return ReplicaStats{
		Enabled:        true,
		ReplicaID:      r.cfg.ID,
		Peers:          r.cfg.Peers,
		SyncInterval:   r.cfg.SyncInterval.String(),
		LamportClock:   r.crdt.Clock(),
		Digest:         r.Digest(),
		LiveVertices:   len(r.crdt.LiveVertices()),
		LiveEdges:      len(r.crdt.LiveEdges()),
		DanglingEdges:  len(r.crdt.DanglingEdges()),
		Observed:       r.metrics.ContextsObserved.Load(),
		VertexWrites:   r.metrics.VerticesWritten.Load(),
		EdgeWrites:     r.metrics.EdgesWritten.Load(),
		WritesSkipped:  r.metrics.WritesSkipped.Load(),
		SyncAttempts:   r.metrics.SyncAttempts.Load(),
		SyncFailures:   r.metrics.SyncFailures.Load(),
		MergesApplied:  r.metrics.MergesApplied.Load(),
		MergesReceived: r.metrics.MergesReceived.Load(),
		BudgetExceeded: r.metrics.BudgetExceeded.Load(),
	}
}

// Prometheus renders the replication tier for /metrics.
func (r *TopologyReplica) Prometheus() string {
	if r == nil || !r.cfg.Enabled {
		return renderExposition([]metricSample{
			{"openontology_replica_enabled", "1 when CRDT topology replication is active.", "gauge", 0, nil},
		})
	}
	stats := r.Stats()
	return renderExposition([]metricSample{
		{"openontology_replica_enabled", "1 when CRDT topology replication is active.", "gauge", 1, nil},
		{"openontology_replica_lamport_clock", "Current Lamport clock of the local replica.", "gauge", float64(stats.LamportClock), nil},
		{"openontology_replica_live_vertices", "Vertices currently live in the replicated topology.", "gauge", float64(stats.LiveVertices), nil},
		{"openontology_replica_live_edges", "Edges currently live in the replicated topology.", "gauge", float64(stats.LiveEdges), nil},
		{"openontology_replica_dangling_edges", "Live edges whose endpoints are not both live.", "gauge", float64(stats.DanglingEdges), nil},
		{"openontology_replica_contexts_observed_total", "Ontology contexts folded into the replica.", "counter", float64(stats.Observed), nil},
		{"openontology_replica_vertex_writes_total", "Vertex writes applied because content changed.", "counter", float64(stats.VertexWrites), nil},
		{"openontology_replica_edge_writes_total", "Edge writes applied because the edge was not already live.", "counter", float64(stats.EdgeWrites), nil},
		{"openontology_replica_writes_skipped_total", "Writes suppressed because the replica already held the content.", "counter", float64(stats.WritesSkipped), nil},
		{"openontology_replica_sync_attempts_total", "Anti-entropy rounds attempted against a peer.", "counter", float64(stats.SyncAttempts), nil},
		{"openontology_replica_sync_failures_total", "Anti-entropy rounds that failed to reach or parse a peer.", "counter", float64(stats.SyncFailures), nil},
		{"openontology_replica_merges_applied_total", "Peer snapshots successfully joined.", "counter", float64(stats.MergesApplied), nil},
		{"openontology_replica_merges_received_total", "Peer snapshots offered for joining.", "counter", float64(stats.MergesReceived), nil},
		{"openontology_replica_budget_exceeded_total", "Joins that outran the reconcile budget.", "counter", float64(stats.BudgetExceeded), nil},
	})
}

// RunSync drives anti-entropy until the context is cancelled.
//
// Pull rather than push: a replica asks its peers for their state and joins it
// locally. That keeps the direction of trust simple — nothing can write into
// this replica except through a join it initiated or an explicit POST — and it
// means a peer that is down costs one failed request rather than a queue of
// undeliverable pushes.
func (r *TopologyReplica) RunSync(ctx context.Context, client PeerClient) {
	if r == nil || !r.cfg.Enabled || len(r.cfg.Peers) == 0 {
		return
	}

	ticker := time.NewTicker(r.cfg.SyncInterval)
	defer ticker.Stop()

	r.log.Info("anti-entropy started", "peers", r.cfg.Peers, "interval", r.cfg.SyncInterval)

	for {
		select {
		case <-ctx.Done():
			r.log.Info("anti-entropy stopping")
			return
		case <-ticker.C:
			for _, peer := range r.cfg.Peers {
				r.syncOnce(ctx, client, peer)
			}
		}
	}
}

// PeerClient fetches a peer's replica state. An interface rather than a
// concrete HTTP client so the sync loop is testable without a listener.
type PeerClient interface {
	FetchState(ctx context.Context, peer string) (crdt.Snapshot, error)
}

func (r *TopologyReplica) syncOnce(ctx context.Context, client PeerClient, peer string) {
	r.metrics.SyncAttempts.Add(1)

	callCtx, cancel := context.WithTimeout(ctx, r.cfg.SyncTimeout)
	defer cancel()

	snap, err := client.FetchState(callCtx, peer)
	if err != nil {
		r.metrics.SyncFailures.Add(1)
		// Expected during a partition, which is the condition this whole tier
		// exists to survive. Logged at warn rather than error so a deliberate
		// split does not read as a fault.
		r.log.Warn("peer state unavailable", "peer", peer, "error", err)
		return
	}

	before := r.Digest()
	if err := r.MergeSnapshot(snap); err != nil {
		r.metrics.SyncFailures.Add(1)
		r.log.Error("merging peer state failed", "peer", peer, "error", err)
		return
	}

	if after := r.Digest(); after != before {
		r.log.Info("replica converged on new state",
			"peer", peer,
			"peer_replica", snap.ClientID,
			"vertices", len(snap.Vertices),
			"edges", len(snap.Edges),
			"graph_revision", after)
	}
}
