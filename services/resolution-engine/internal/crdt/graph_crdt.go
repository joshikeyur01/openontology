// Package crdt implements the OpenOntology state-based graph CRDT: the
// replication core that lets a disconnected edge site — an aircraft between
// uplinks, an isolated plant network, a survey vessel — keep mutating its slice
// of the physical asset topology and later fold those mutations back into the
// fleet state without ever taking a distributed lock.
//
// The model is an Observed-Removed Set (OR-Set) over both vertices and edges.
// Every element carries two timelines keyed by replica id — when that replica
// added it, when that replica removed it — stamped with monotonic Lamport
// timestamps. The join (⊔) is the pointwise maximum of those maps, and an
// element is live only when its highest add stamp is strictly greater than its
// highest remove stamp. That makes the merge commutative, associative and
// idempotent: replicas that have observed the same set of mutations converge on
// the same graph no matter what order the states arrived in, and folding the
// same state in twice changes nothing. There is no coordination on the write
// path, so an edge site stays writable through an outage of any length.
//
// A GraphCRDT is safe for concurrent use. Mutators take the write lock for the
// duration of one operation; a merge takes it for the duration of the join,
// which is why ReconcileRemoteDelta runs under an execution budget.
//
// Two properties are worth stating up front because they shape every caller:
//
//   - Ties resolve to removed. An add and a remove that land on the same
//     logical instant leave the element tombstoned. Resurrection is always
//     available — a later add carries a strictly greater stamp — so this costs
//     an operator a retry rather than a permanently unreachable asset.
//
//   - A vertex payload is one last-writer-wins register, not a per-key map.
//     AddVertex re-asserts the whole property map, and the winner is the
//     replica holding the greatest (stamp, replica id) add. Per-key merging
//     would need a timeline per key, which the module contract deliberately
//     does not carry.
package crdt

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Logging contract
// ---------------------------------------------------------------------------

// Logger is the minimal structured-logging surface this engine needs. It is an
// interface rather than *slog.Logger so an edge deployment can route records
// into whatever the site already runs, and so tests can inject a recorder
// without a global handler swap. *slog.Logger satisfies it as-is.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// Compile-time proof that the standard logger is a drop-in.
var _ Logger = (*slog.Logger)(nil)

type discardLogger struct{}

func (discardLogger) Debug(string, ...any) {}
func (discardLogger) Info(string, ...any)  {}
func (discardLogger) Warn(string, ...any)  {}
func (discardLogger) Error(string, ...any) {}

// DiscardLogger returns a Logger that drops every record. Convergence tests
// merge thousands of states; without this they are unreadable.
func DiscardLogger() Logger { return discardLogger{} }

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

var (
	// ErrNilRemoteState reports a merge against a nil peer. It is a programming
	// error rather than a replication condition, so it is never retried.
	ErrNilRemoteState = errors.New("remote crdt state is nil")

	// ErrReconcileBudgetExceeded marks a join that outran the replica's own
	// execution budget. It is joined with context.DeadlineExceeded, so either
	// sentinel matches, and it is deliberately distinct from a caller
	// cancellation: this one means our state is too large for the budget, and
	// the operator response is to raise the budget or compact, not to retry.
	ErrReconcileBudgetExceeded = errors.New("delta reconciliation exceeded its execution budget")
)

// TransportError is the shape the replication transport hands back when a peer
// exchange fails. It carries the peer and operation for triage and wraps the
// driver-level cause.
type TransportError struct {
	Peer  string
	Op    string
	Inner error
}

func (e *TransportError) Error() string {
	if e == nil {
		return "<nil transport error>"
	}
	if e.Inner == nil {
		return fmt.Sprintf("replication transport: %s to peer %q failed", e.Op, e.Peer)
	}
	return fmt.Sprintf("replication transport: %s to peer %q failed: %v", e.Op, e.Peer, e.Inner)
}

// Unwrap keeps errors.Is and errors.As working through this type. The peeler
// below exists for the third-party types that do not do this.
func (e *TransportError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Inner
}

// ReplicaSyncLimit reports that an anti-entropy round exhausted its retries
// against one peer. Every attempt is retained: the failures usually differ
// (refused, then TLS, then deadline) and the sequence is the diagnosis.
type ReplicaSyncLimit struct {
	Peer     string
	Attempts []error
}

func (e *ReplicaSyncLimit) Error() string {
	if e == nil {
		return "<nil replica sync limit>"
	}
	if len(e.Attempts) == 0 {
		return fmt.Sprintf("replica sync to peer %q exhausted its retries", e.Peer)
	}
	return fmt.Sprintf("replica sync to peer %q exhausted its retries after %d attempts: %v",
		e.Peer, len(e.Attempts), e.Attempts[len(e.Attempts)-1])
}

// Unwrap exposes every attempt to errors.Is and errors.As.
func (e *ReplicaSyncLimit) Unwrap() []error {
	if e == nil {
		return nil
	}
	return e.Attempts
}

// ---------------------------------------------------------------------------
// Domain model
// ---------------------------------------------------------------------------

// Relationship types recognised by the topology tier. Anything else is carried
// through the CRDT untouched — replication must never be the layer that decides
// a site's vocabulary is invalid — but it is counted and logged, because an
// unrecognised type is invisible to the blast-radius traversal downstream.
const (
	RelationshipFeeds    = "FEEDS"
	RelationshipControls = "CONTROLS"
)

var knownRelationships = map[string]struct{}{
	RelationshipFeeds:    {},
	RelationshipControls: {},
}

// keySeparator is a unit separator, chosen because it cannot appear in an asset
// identifier emitted by any of the ingest paths. A printable separator would
// let ("a|b", "c") and ("a", "b|c") collide into one edge.
const keySeparator = "\x1f"

// Vertex is one physical asset under replication.
//
// AddTimeline and RemoveTimeline are keyed by replica id and hold that
// replica's highest observed Lamport stamp for each operation. Keeping the full
// map rather than a single scalar is what makes the join lossless: two replicas
// that both removed the asset, and a third that re-added it, all survive the
// merge and arbitrate by stamp.
type Vertex struct {
	UUID           string            `json:"uuid"`
	Properties     map[string]string `json:"properties,omitempty"`
	AddTimeline    map[string]int64  `json:"add_timeline,omitempty"`
	RemoveTimeline map[string]int64  `json:"remove_timeline,omitempty"`
}

// Live reports whether the vertex survives the OR-Set presence rule: its
// highest add stamp must be strictly greater than its highest remove stamp.
func (v Vertex) Live() bool {
	return highestStamp(v.AddTimeline) > highestStamp(v.RemoveTimeline)
}

// Observed reports whether any replica has ever touched this vertex. An element
// with two empty timelines carries no information and is dropped on ingest.
func (v Vertex) Observed() bool {
	return len(v.AddTimeline) > 0 || len(v.RemoveTimeline) > 0
}

// Clone deep-copies the vertex. Every value handed across the package boundary
// goes through here, so a caller can never reach into replicated state through
// an aliased map.
func (v Vertex) Clone() Vertex {
	return Vertex{
		UUID:           v.UUID,
		Properties:     cloneProperties(v.Properties),
		AddTimeline:    cloneTimeline(v.AddTimeline),
		RemoveTimeline: cloneTimeline(v.RemoveTimeline),
	}
}

// LogValue renders a vertex compactly, so a debug line never dumps an entire
// property map into the log stream.
func (v Vertex) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("uuid", v.UUID),
		slog.Bool("live", v.Live()),
		slog.Int("properties", len(v.Properties)),
		slog.Int64("add_stamp", highestStamp(v.AddTimeline)),
		slog.Int64("remove_stamp", highestStamp(v.RemoveTimeline)),
	)
}

// Edge is a directed dependency between two assets — what feeds what, what
// controls what. It carries its own timelines and is replicated independently
// of its endpoints, because two sites can legitimately learn about an edge and
// its vertices in either order.
type Edge struct {
	SourceUUID       string           `json:"source_uuid"`
	TargetUUID       string           `json:"target_uuid"`
	RelationshipType string           `json:"relationship_type"`
	AddTimeline      map[string]int64 `json:"add_timeline,omitempty"`
	RemoveTimeline   map[string]int64 `json:"remove_timeline,omitempty"`
}

// Key is the edge's identity in the replicated map. Direction is part of it:
// (pump -FEEDS-> manifold) and (manifold -FEEDS-> pump) are different claims
// about the plant and must not merge into one.
func (e Edge) Key() string {
	return e.SourceUUID + keySeparator + e.RelationshipType + keySeparator + e.TargetUUID
}

// Live applies the same presence rule as Vertex.Live. It says nothing about the
// endpoints; see GraphCRDT.LiveEdges for the dangling-edge filter.
func (e Edge) Live() bool {
	return highestStamp(e.AddTimeline) > highestStamp(e.RemoveTimeline)
}

// Observed reports whether any replica has ever touched this edge.
func (e Edge) Observed() bool {
	return len(e.AddTimeline) > 0 || len(e.RemoveTimeline) > 0
}

// Clone deep-copies the edge.
func (e Edge) Clone() Edge {
	return Edge{
		SourceUUID:       e.SourceUUID,
		TargetUUID:       e.TargetUUID,
		RelationshipType: e.RelationshipType,
		AddTimeline:      cloneTimeline(e.AddTimeline),
		RemoveTimeline:   cloneTimeline(e.RemoveTimeline),
	}
}

// LogValue renders an edge compactly for structured logging.
func (e Edge) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("source", e.SourceUUID),
		slog.String("relationship", e.RelationshipType),
		slog.String("target", e.TargetUUID),
		slog.Bool("live", e.Live()),
	)
}

// writeStamp identifies the write that most recently defined an element's
// payload: the greatest (Lamport stamp, replica id) pair in its add timeline.
// The replica id breaks stamp ties, which is what keeps the payload merge a
// total order and therefore associative.
type writeStamp struct {
	Timestamp int64
	ClientID  string
}

// after reports whether s dominates other in that total order.
func (s writeStamp) after(other writeStamp) bool {
	if s.Timestamp != other.Timestamp {
		return s.Timestamp > other.Timestamp
	}
	return s.ClientID > other.ClientID
}

// observed reports whether any add write exists. Lamport stamps start at 1, so
// a zero timestamp means the timeline was empty.
func (s writeStamp) observed() bool { return s.Timestamp > 0 }

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

const (
	// DefaultReconcileBudget bounds one ReconcileRemoteDelta. A join holds the
	// write lock for its whole duration, so this is really a bound on how long
	// local mutation can be stalled by an inbound state — and on an edge site
	// the local operator is the one who cannot wait.
	DefaultReconcileBudget = 2 * time.Second

	// defaultPreallocatedSize sizes the element maps for a typical single-site
	// topology. WithPreallocatedSize covers the fleet-aggregator case, where
	// growing from this default costs a run of rehashes on the merge path.
	defaultPreallocatedSize = 1024

	// reconcileCheckStride is how many elements are joined between context
	// checks. Checking every element makes ctx.Err() a measurable fraction of
	// the merge; checking too rarely blunts the budget on large states.
	reconcileCheckStride = 256
)

type crdtSettings struct {
	logger       Logger
	preallocate  int
	budget       time.Duration
	initialClock int64
}

// CRDTOption customises a replica at construction time. NewGraphCRDT with no
// options yields the production defaults above.
type CRDTOption func(*crdtSettings)

// WithLogger installs the structured logger. A nil logger is ignored rather
// than installed, so a mis-wired test cannot turn every log call into a panic
// on the replication path. Defaults to slog.Default().
func WithLogger(logger Logger) CRDTOption {
	return func(s *crdtSettings) {
		if logger != nil {
			s.logger = logger
		}
	}
}

// WithPreallocatedSize sizes the vertex and edge maps up front. Set it to the
// expected element count when a replica is about to absorb a full fleet
// snapshot; the merge path is where rehashing hurts, because it happens under
// the write lock.
func WithPreallocatedSize(size int) CRDTOption {
	return func(s *crdtSettings) {
		if size > 0 {
			s.preallocate = size
		}
	}
}

// WithReconcileBudget overrides the per-reconciliation execution budget.
func WithReconcileBudget(d time.Duration) CRDTOption {
	return func(s *crdtSettings) {
		if d > 0 {
			s.budget = d
		}
	}
}

// WithInitialLamportClock restores the clock from persisted state.
//
// This is not a convenience. A replica that restarts at zero re-issues stamps
// it has already used, and those writes silently lose every merge against a
// peer that remembers the higher value. Any site that persists its graph across
// a power cycle must also persist and restore this.
func WithInitialLamportClock(stamp int64) CRDTOption {
	return func(s *crdtSettings) {
		if stamp > 0 {
			s.initialClock = stamp
		}
	}
}

// GraphCRDT is one replica of the asset topology.
//
// It must not be copied after first use: the mutex and the counters make a copy
// both racy and wrong. Pass it by pointer, and use Snapshot or Clone to hand
// state to something else.
type GraphCRDT struct {
	// ClientID is this replica's identity and the key it writes into every
	// timeline. It is immutable after construction and safe to read without the
	// lock. Two live replicas sharing one id break the OR-Set invariant; merges
	// detect that and count it.
	ClientID string

	// LamportClock is the local logical clock. It is guarded by mu — read it
	// through Clock() rather than touching the field directly. It is exported
	// because an edge site has to persist it across restarts (see
	// WithInitialLamportClock).
	LamportClock int64

	// mu guards LamportClock, vertices, edges and the last-merge fields.
	mu       sync.RWMutex
	vertices map[string]*Vertex
	edges    map[string]*Edge

	lastMergeAt       time.Time
	lastMergeDuration time.Duration

	log       Logger
	budget    time.Duration
	startedAt time.Time

	// Counters are atomics rather than mutex-guarded fields so that Stats can be
	// scraped without contending with the merge path.
	mutations           atomic.Uint64
	rejectedMutations   atomic.Uint64
	merges              atomic.Uint64
	mergeConflicts      atomic.Uint64
	elementsJoined      atomic.Uint64
	rejectedElements    atomic.Uint64
	callerCancellations atomic.Uint64
	budgetExceeded      atomic.Uint64
	identityCollisions  atomic.Uint64
	unknownRelations    atomic.Uint64
}

// NewGraphCRDT builds an empty replica identified by clientID.
//
// An empty clientID is repaired rather than rejected, because the signature has
// no error to return and the failure mode is severe: every anonymous replica
// would write into the same "" timeline key and silently clobber the others'
// history. A random id is substituted and the substitution is logged at error
// level, which turns a convergence bug into a startup log line.
func NewGraphCRDT(clientID string, opts ...CRDTOption) *GraphCRDT {
	cfg := crdtSettings{
		logger:      slog.Default(),
		preallocate: defaultPreallocatedSize,
		budget:      DefaultReconcileBudget,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	trimmed := strings.TrimSpace(clientID)
	generated := trimmed == ""
	if generated {
		trimmed = generateReplicaID()
	}

	c := &GraphCRDT{
		ClientID:     trimmed,
		LamportClock: cfg.initialClock,
		vertices:     make(map[string]*Vertex, cfg.preallocate),
		edges:        make(map[string]*Edge, cfg.preallocate),
		log:          cfg.logger,
		budget:       cfg.budget,
		startedAt:    time.Now().UTC(),
	}

	if generated {
		c.log.Error("crdt replica constructed without a client id; a random one was generated",
			"generated_client_id", trimmed,
			"impact", "state persisted under this id cannot be reclaimed after a restart")
	}
	c.log.Info("crdt replica ready",
		"client_id", c.ClientID,
		"initial_lamport_clock", c.LamportClock,
		"reconcile_budget", c.budget,
		"preallocated", cfg.preallocate)

	return c
}

// generateReplicaID produces a collision-resistant fallback identity. The
// timestamp path is a last resort for a machine whose entropy source is not yet
// seeded at boot — common enough on embedded edge hardware to be worth handling.
func generateReplicaID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("replica-%d", time.Now().UnixNano())
	}
	return "replica-" + hex.EncodeToString(buf)
}

// ---------------------------------------------------------------------------
// Local mutation
// ---------------------------------------------------------------------------

// tickLocked advances the logical clock and returns the new stamp. Caller holds
// the write lock.
func (c *GraphCRDT) tickLocked() int64 {
	c.LamportClock++
	return c.LamportClock
}

// raiseClockLocked applies the Lamport receive rule: after observing a peer's
// state, every stamp this replica issues must exceed everything it has seen.
// Skipping this is the classic way to make a merge look idempotent in a test
// and diverge in the field. Caller holds the write lock.
func (c *GraphCRDT) raiseClockLocked(observed int64) {
	if observed > c.LamportClock {
		c.LamportClock = observed
	}
}

// Clock returns the current Lamport stamp. Persist it on shutdown and restore
// it with WithInitialLamportClock.
func (c *GraphCRDT) Clock() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.LamportClock
}

// AddVertex adds or re-asserts an asset.
//
// The property map is replaced wholesale, not merged: the payload is a single
// last-writer-wins register keyed by this write's stamp. Passing nil clears the
// properties, which is the honest reading of "this is the asset now".
//
// Re-adding a tombstoned asset resurrects it, because the new stamp is strictly
// greater than the remove that killed it. That is the intended way to undo an
// erroneous removal at a disconnected site.
func (c *GraphCRDT) AddVertex(id string, props map[string]string) {
	trimmed := strings.TrimSpace(id)
	if trimmed == "" {
		c.rejectedMutations.Add(1)
		c.log.Error("rejecting AddVertex with an empty asset id")
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	stamp := c.tickLocked()
	vertex, ok := c.vertices[trimmed]
	if !ok {
		vertex = &Vertex{
			UUID:           trimmed,
			Properties:     make(map[string]string, len(props)),
			AddTimeline:    make(map[string]int64, 1),
			RemoveTimeline: make(map[string]int64),
		}
		c.vertices[trimmed] = vertex
	}
	vertex.AddTimeline[c.ClientID] = stamp
	vertex.Properties = cloneProperties(props)

	c.mutations.Add(1)
	c.log.Debug("vertex added", "vertex", *vertex, "stamp", stamp, "client_id", c.ClientID)
}

// RemoveVertex tombstones an asset and every edge currently incident to it.
//
// The cascade is what keeps the topology honest: a merge that revived the
// vertex's edges but not the vertex would leave the blast-radius traversal
// walking through equipment that no longer exists. Because the cascade only
// touches edges this replica has actually observed, an edge added concurrently
// elsewhere survives — observed-remove semantics, applied to the incidence set.
//
// Removing an unknown id records a tombstone-only entry rather than doing
// nothing. A disconnected site frequently learns of a decommissioning before it
// receives the asset itself, and discarding that would resurrect the asset on
// the next merge.
func (c *GraphCRDT) RemoveVertex(id string) {
	trimmed := strings.TrimSpace(id)
	if trimmed == "" {
		c.rejectedMutations.Add(1)
		c.log.Error("rejecting RemoveVertex with an empty asset id")
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// One operation, one logical instant: the vertex tombstone and its cascade
	// share a stamp so no peer can interleave a merge between them.
	stamp := c.tickLocked()

	vertex, ok := c.vertices[trimmed]
	if !ok {
		vertex = &Vertex{
			UUID:           trimmed,
			Properties:     make(map[string]string),
			AddTimeline:    make(map[string]int64),
			RemoveTimeline: make(map[string]int64, 1),
		}
		c.vertices[trimmed] = vertex
		c.log.Debug("tombstoning an unobserved asset", "uuid", trimmed, "stamp", stamp)
	}
	vertex.RemoveTimeline[c.ClientID] = stamp

	// O(E) per removal. An incidence index would make this O(deg) but would need
	// its own tombstone bookkeeping to stay mergeable, and removals are rare
	// next to reads on every deployment this runs on.
	cascaded := 0
	for _, edge := range c.edges {
		if edge.SourceUUID != trimmed && edge.TargetUUID != trimmed {
			continue
		}
		if !edge.Live() {
			continue
		}
		edge.RemoveTimeline[c.ClientID] = stamp
		cascaded++
	}

	c.mutations.Add(1)
	c.log.Debug("vertex removed", "uuid", trimmed, "stamp", stamp, "cascaded_edges", cascaded)
}

// AddEdge adds or re-asserts a dependency between two assets.
//
// The relationship type is trimmed and upper-cased so that "feeds" and "FEEDS"
// cannot become two edges that never merge. Endpoints are not required to exist
// locally: on a partitioned site the edge routinely arrives before the asset,
// and rejecting it would drop a real topology fact. The condition is counted so
// a site that is permanently missing its endpoints is visible in /metrics.
func (c *GraphCRDT) AddEdge(source, target, relationship string) {
	src, dst, rel, ok := c.normalizeEdge(source, target, relationship, "AddEdge")
	if !ok {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	stamp := c.tickLocked()
	key := src + keySeparator + rel + keySeparator + dst
	edge, exists := c.edges[key]
	if !exists {
		edge = &Edge{
			SourceUUID:       src,
			TargetUUID:       dst,
			RelationshipType: rel,
			AddTimeline:      make(map[string]int64, 1),
			RemoveTimeline:   make(map[string]int64),
		}
		c.edges[key] = edge
	}
	edge.AddTimeline[c.ClientID] = stamp

	if !c.vertexLiveLocked(src) || !c.vertexLiveLocked(dst) {
		c.log.Warn("edge added ahead of one of its endpoints",
			"edge", *edge,
			"source_live", c.vertexLiveLocked(src),
			"target_live", c.vertexLiveLocked(dst))
	}

	c.mutations.Add(1)
	c.log.Debug("edge added", "edge", *edge, "stamp", stamp)
}

// RemoveEdge tombstones a dependency. As with RemoveVertex, an unobserved edge
// gets a tombstone-only entry so a removal cannot be undone by a later delivery
// of the add.
func (c *GraphCRDT) RemoveEdge(source, target, relationship string) {
	src, dst, rel, ok := c.normalizeEdge(source, target, relationship, "RemoveEdge")
	if !ok {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	stamp := c.tickLocked()
	key := src + keySeparator + rel + keySeparator + dst
	edge, exists := c.edges[key]
	if !exists {
		edge = &Edge{
			SourceUUID:       src,
			TargetUUID:       dst,
			RelationshipType: rel,
			AddTimeline:      make(map[string]int64),
			RemoveTimeline:   make(map[string]int64, 1),
		}
		c.edges[key] = edge
	}
	edge.RemoveTimeline[c.ClientID] = stamp

	c.mutations.Add(1)
	c.log.Debug("edge removed", "edge", *edge, "stamp", stamp)
}

// normalizeEdge validates and canonicalises the three edge components, so a
// caller's whitespace or casing can never fork one dependency into two.
func (c *GraphCRDT) normalizeEdge(source, target, relationship, op string) (string, string, string, bool) {
	src := strings.TrimSpace(source)
	dst := strings.TrimSpace(target)
	rel := strings.ToUpper(strings.TrimSpace(relationship))

	switch {
	case src == "":
		c.rejectedMutations.Add(1)
		c.log.Error("rejecting edge mutation with an empty source", "op", op, "target", dst, "relationship", rel)
		return "", "", "", false
	case dst == "":
		c.rejectedMutations.Add(1)
		c.log.Error("rejecting edge mutation with an empty target", "op", op, "source", src, "relationship", rel)
		return "", "", "", false
	case rel == "":
		c.rejectedMutations.Add(1)
		c.log.Error("rejecting edge mutation with an empty relationship type", "op", op, "source", src, "target", dst)
		return "", "", "", false
	}

	if _, known := knownRelationships[rel]; !known {
		c.unknownRelations.Add(1)
		c.log.Warn("edge uses a relationship type the topology tier does not traverse",
			"op", op, "relationship", rel, "known", []string{RelationshipFeeds, RelationshipControls})
	}
	return src, dst, rel, true
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

// vertexLiveLocked answers the presence question without copying. Caller holds
// either lock.
func (c *GraphCRDT) vertexLiveLocked(id string) bool {
	vertex, ok := c.vertices[id]
	return ok && vertex.Live()
}

// HasVertex reports whether the asset is currently live on this replica.
func (c *GraphCRDT) HasVertex(id string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.vertexLiveLocked(strings.TrimSpace(id))
}

// LookupVertex returns a deep copy of a live asset. A tombstoned asset reports
// false: callers want the topology, not its history.
func (c *GraphCRDT) LookupVertex(id string) (Vertex, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	vertex, ok := c.vertices[strings.TrimSpace(id)]
	if !ok || !vertex.Live() {
		return Vertex{}, false
	}
	return vertex.Clone(), true
}

// LiveVertices returns every live asset, sorted by UUID so that two replicas
// which have converged also render identically.
func (c *GraphCRDT) LiveVertices() []Vertex {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make([]Vertex, 0, len(c.vertices))
	for _, vertex := range c.vertices {
		if vertex.Live() {
			out = append(out, vertex.Clone())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UUID < out[j].UUID })
	return out
}

// LiveEdges returns every live dependency whose endpoints are both live,
// sorted by edge key.
//
// A dangling edge — live itself, pointing at a tombstoned or not-yet-replicated
// asset — is filtered from the view but kept in the state. Discarding it would
// break convergence: the endpoint may be resurrected by the very next merge,
// and the edge has to come back with it.
func (c *GraphCRDT) LiveEdges() []Edge {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make([]Edge, 0, len(c.edges))
	for _, edge := range c.edges {
		if !edge.Live() {
			continue
		}
		if !c.vertexLiveLocked(edge.SourceUUID) || !c.vertexLiveLocked(edge.TargetUUID) {
			continue
		}
		out = append(out, edge.Clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key() < out[j].Key() })
	return out
}

// DanglingEdges returns live edges with at least one endpoint that is not live.
// On a healthy fleet this drains to empty after every anti-entropy round; a
// count that stays high means a site is missing state it will never receive.
func (c *GraphCRDT) DanglingEdges() []Edge {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make([]Edge, 0)
	for _, edge := range c.edges {
		if !edge.Live() {
			continue
		}
		if c.vertexLiveLocked(edge.SourceUUID) && c.vertexLiveLocked(edge.TargetUUID) {
			continue
		}
		out = append(out, edge.Clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key() < out[j].Key() })
	return out
}

// ---------------------------------------------------------------------------
// Snapshots and the wire format
// ---------------------------------------------------------------------------

// Snapshot is the full replica state in transferable form — what an edge site
// ships when the uplink comes back. It is a state-based CRDT, so this is the
// unit of replication: there is no operation log to replay and no requirement
// that snapshots arrive in order, or at all.
type Snapshot struct {
	ClientID     string    `json:"client_id"`
	LamportClock int64     `json:"lamport_clock"`
	CapturedAt   time.Time `json:"captured_at"`
	Vertices     []Vertex  `json:"vertices"`
	Edges        []Edge    `json:"edges"`
}

// Snapshot captures the replica, including tombstones, which are part of the
// state and cannot be omitted without losing removals.
func (c *GraphCRDT) Snapshot() Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.snapshotLocked()
}

// snapshotLocked builds the deterministic, deep-copied snapshot. Ordering is
// stable so that Digest is stable. Caller holds at least the read lock.
func (c *GraphCRDT) snapshotLocked() Snapshot {
	snap := Snapshot{
		ClientID:     c.ClientID,
		LamportClock: c.LamportClock,
		CapturedAt:   time.Now().UTC(),
		Vertices:     make([]Vertex, 0, len(c.vertices)),
		Edges:        make([]Edge, 0, len(c.edges)),
	}
	for _, vertex := range c.vertices {
		snap.Vertices = append(snap.Vertices, vertex.Clone())
	}
	for _, edge := range c.edges {
		snap.Edges = append(snap.Edges, edge.Clone())
	}
	sort.Slice(snap.Vertices, func(i, j int) bool { return snap.Vertices[i].UUID < snap.Vertices[j].UUID })
	sort.Slice(snap.Edges, func(i, j int) bool { return snap.Edges[i].Key() < snap.Edges[j].Key() })
	return snap
}

// Clone returns an independent replica holding the same state under the same
// identity. Use it to hand a consistent view to a long-running export without
// pinning the write lock for its duration.
func (c *GraphCRDT) Clone() *GraphCRDT {
	c.mu.RLock()
	snap := c.snapshotLocked()
	logger, budget := c.log, c.budget
	c.mu.RUnlock()

	clone := NewGraphCRDT(snap.ClientID,
		WithLogger(logger),
		WithReconcileBudget(budget),
		WithPreallocatedSize(len(snap.Vertices)+len(snap.Edges)),
		WithInitialLamportClock(snap.LamportClock))

	// Joining into an empty replica is a load: every element is an insert and
	// no conflict is possible.
	clone.mu.Lock()
	defer clone.mu.Unlock()
	var report mergeReport
	_ = clone.applyLocked(context.Background(), snap, &report)
	return clone
}

// MarshalJSON renders the replica in the wire format. It takes the read lock,
// so it is safe to call on a live replica, and it emits elements in a stable
// order so equal states produce equal bytes.
func (c *GraphCRDT) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.Snapshot())
}

// UnmarshalJSON replaces this replica's state with the decoded one. It is a
// load, not a merge — decoding into a populated replica discards what was
// there. Use MergeSnapshot to fold a peer's state in.
func (c *GraphCRDT) UnmarshalJSON(data []byte) error {
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("decode crdt state: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// A zero-value GraphCRDT can arrive here straight from json.Unmarshal, so
	// every field the constructor would have set has to be repaired.
	if c.log == nil {
		c.log = slog.Default()
	}
	if c.budget <= 0 {
		c.budget = DefaultReconcileBudget
	}
	if c.startedAt.IsZero() {
		c.startedAt = time.Now().UTC()
	}
	if strings.TrimSpace(c.ClientID) == "" {
		c.ClientID = strings.TrimSpace(snap.ClientID)
		if c.ClientID == "" {
			c.ClientID = generateReplicaID()
			c.log.Error("decoded crdt state carries no client id; a random one was generated",
				"generated_client_id", c.ClientID)
		}
	}
	c.vertices = make(map[string]*Vertex, len(snap.Vertices))
	c.edges = make(map[string]*Edge, len(snap.Edges))
	c.LamportClock = 0

	var report mergeReport
	if err := c.applyLocked(context.Background(), snap, &report); err != nil {
		return fmt.Errorf("load decoded crdt state: %w", err)
	}
	return nil
}

// Digest is a content hash of the replica state, stable across replicas that
// have converged. Comparing digests before shipping a snapshot is what keeps an
// anti-entropy round from transferring megabytes to discover nothing changed.
func (c *GraphCRDT) Digest() (string, error) {
	snap := c.Snapshot()
	// The capture time is wall-clock and would make every digest unique.
	snap.CapturedAt = time.Time{}
	// So would the local clock, which is replica-local metadata rather than
	// replicated content.
	snap.LamportClock = 0
	snap.ClientID = ""

	encoded, err := json.Marshal(snap)
	if err != nil {
		return "", fmt.Errorf("encode crdt state for digest: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// ---------------------------------------------------------------------------
// The join
// ---------------------------------------------------------------------------

// mergeReport accumulates what one join did, so the caller can log a single
// meaningful line instead of one per element.
type mergeReport struct {
	VerticesJoined int
	EdgesJoined    int
	Conflicts      int
	Skipped        int
	Inspected      int
	HighestStamp   int64
}

// Merge folds a peer replica into this one — the join (⊔) of the semilattice.
//
// It is commutative, associative and idempotent: a.Merge(b) and b.Merge(a)
// leave both replicas describing the same graph, merging in any order converges,
// and merging the same state twice is a no-op. There is no lock held on both
// replicas at once — the peer is snapshotted first — so two replicas
// reconciling each other concurrently cannot deadlock.
//
// Merge is unbudgeted and will run to completion. Anything on a request path
// should call ReconcileRemoteDelta instead.
func (c *GraphCRDT) Merge(remote *GraphCRDT) error {
	if remote == nil {
		return fmt.Errorf("merge remote state: %w", ErrNilRemoteState)
	}
	if remote == c {
		// Idempotence makes this a no-op, and short-circuiting avoids both the
		// pointless copy and a self-inflicted identity-collision warning.
		return nil
	}
	return c.MergeSnapshot(remote.Snapshot())
}

// MergeSnapshot folds a snapshot received over the wire into this replica. It
// is the merge entry point for a site that reconnects and uploads its state.
func (c *GraphCRDT) MergeSnapshot(snap Snapshot) error {
	_, err := c.reconcile(context.Background(), snap)
	return err
}

// ReconcileRemoteDelta merges a peer under an execution budget and reports
// precisely why it stopped when it does not finish.
//
// The distinction that matters operationally is between a caller that gave up
// and a budget that fired. The first means the ingestion worker was shut down
// or rebalanced and the merge should simply be retried; the second means this
// replica's state has outgrown its budget, and retrying will fail identically
// until the budget is raised or Compact is run. The contexts are the authority
// for telling those apart — the returned error is not, because a cancelled
// caller and an expired budget both surface as context errors from the same
// call site.
//
// A partially applied join is safe to leave in place. Every element's join is
// independently monotonic, so an aborted merge yields a valid state that is
// still mergeable; the remainder arrives on the next round.
func (c *GraphCRDT) ReconcileRemoteDelta(ctx context.Context, remote *GraphCRDT) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if remote == nil {
		return fmt.Errorf("reconcile remote delta: %w", ErrNilRemoteState)
	}
	if remote == c {
		return nil
	}

	peer := remote.ClientID

	// Check before doing any work: a caller that is already cancelled should not
	// pay for a snapshot copy, and the replica should not take the write lock.
	if err := ctx.Err(); err != nil {
		c.callerCancellations.Add(1)
		c.log.Warn("reconciliation abandoned before it started", "peer", peer, "cause", err)
		return fmt.Errorf("reconcile remote delta from %q: %w", peer, err)
	}

	// Snapshotting the peer outside our own lock is what makes concurrent
	// mutual reconciliation deadlock-free, and it keeps the peer writable while
	// we spend the budget on the join.
	snap := remote.Snapshot()

	started := time.Now()
	execCtx, cancel := context.WithTimeout(ctx, c.budget)
	defer cancel()

	report, err := c.reconcile(execCtx, snap)
	if err != nil {
		return c.classifyReconcile(ctx, execCtx, err, peer, report, started)
	}

	c.log.Debug("reconciled remote delta",
		"peer", peer,
		"vertices_joined", report.VerticesJoined,
		"edges_joined", report.EdgesJoined,
		"conflicts", report.Conflicts,
		"skipped", report.Skipped,
		"elapsed", time.Since(started))
	return nil
}

// classifyReconcile turns an aborted join into a typed, counted, logged error.
//
// callerCtx is the context the caller handed in and execCtx is the budgeted one
// the join actually ran under. Comparing the two is authoritative where
// inspecting the error is not, and the order matters: the caller is checked
// first, because a caller that cancels inside the budget window expires both
// contexts at once and the caller's intent is the one that explains the outcome.
func (c *GraphCRDT) classifyReconcile(
	callerCtx, execCtx context.Context,
	err error,
	peer string,
	report mergeReport,
	started time.Time,
) error {
	elapsed := time.Since(started)
	cause := peelRootCause(err)

	switch {
	case callerCtx.Err() != nil:
		// Shutdown, consumer rebalance, or a caller deadline tighter than our
		// budget. Not a replica fault, so it stays at warn and is not counted
		// against the budget.
		c.callerCancellations.Add(1)
		c.log.Warn("reconciliation abandoned by the caller",
			"peer", peer,
			"cause", callerCtx.Err(),
			"elapsed", elapsed,
			"elements_applied", report.VerticesJoined+report.EdgesJoined,
			"note", "the partial join is valid state; the remainder converges on the next round")
		return fmt.Errorf("reconcile remote delta from %q: %w", peer, callerCtx.Err())

	case errors.Is(execCtx.Err(), context.DeadlineExceeded) || errors.Is(cause, context.DeadlineExceeded):
		c.budgetExceeded.Add(1)
		c.log.Error("reconciliation exceeded its execution budget",
			"peer", peer,
			"budget", c.budget,
			"elapsed", elapsed,
			"elements_inspected", report.Inspected,
			"elements_applied", report.VerticesJoined+report.EdgesJoined,
			"remedy", "raise the reconcile budget or compact tombstones below the causal watermark")
		// Both sentinels stay matchable: callers keying on the generic timeout
		// keep working, and callers that need to tell this apart from a caller
		// cancellation can match the specific one.
		return fmt.Errorf("reconcile remote delta from %q: %w of %s: %w",
			peer, ErrReconcileBudgetExceeded, c.budget, context.DeadlineExceeded)

	case errors.Is(cause, context.Canceled):
		c.callerCancellations.Add(1)
		c.log.Warn("reconciliation cancelled", "peer", peer, "elapsed", elapsed)
		return fmt.Errorf("reconcile remote delta from %q: %w", peer, context.Canceled)
	}

	// Anything else is unexpected for an in-memory join, so it is logged with
	// the peeled cause alongside the original — the wrapper chain is usually
	// what identifies the transport that handed us the state.
	c.log.Error("reconciliation failed",
		"peer", peer, "elapsed", elapsed, "error", err, "root_cause", cause)
	return fmt.Errorf("reconcile remote delta from %q: %w", peer, err)
}

// reconcile takes the write lock and applies one snapshot, updating the
// replica's counters from the resulting report.
func (c *GraphCRDT) reconcile(ctx context.Context, snap Snapshot) (mergeReport, error) {
	var report mergeReport

	c.mu.Lock()
	defer c.mu.Unlock()

	if snap.ClientID != "" && snap.ClientID == c.ClientID {
		// Two live replicas writing under one identity break the per-replica
		// monotonicity the OR-Set depends on: their stamps interleave and one
		// replica's history silently overwrites the other's in the timelines.
		c.identityCollisions.Add(1)
		c.log.Error("merging state that claims this replica's own client id",
			"client_id", c.ClientID,
			"impact", "per-replica lamport monotonicity is not guaranteed across the two writers")
	}

	err := c.applyLocked(ctx, snap, &report)

	c.merges.Add(1)
	c.elementsJoined.Add(uint64(report.VerticesJoined + report.EdgesJoined))
	c.mergeConflicts.Add(uint64(report.Conflicts))
	c.rejectedElements.Add(uint64(report.Skipped))
	c.lastMergeAt = time.Now().UTC()

	return report, err
}

// applyLocked performs the join itself. Caller holds the write lock.
//
// The clock is raised on the way out even when the join aborts, so the stamps
// this replica issues after a partial merge still dominate everything it saw.
func (c *GraphCRDT) applyLocked(ctx context.Context, snap Snapshot, report *mergeReport) error {
	started := time.Now()
	defer func() {
		// Runs on the way out of applyLocked, while the caller still holds the
		// write lock — including on the abort path, so a partial join still
		// leaves the clock dominating everything this replica has observed.
		c.raiseClockLocked(snap.LamportClock)
		c.raiseClockLocked(report.HighestStamp)
		c.lastMergeDuration = time.Since(started)
	}()

	for i := range snap.Vertices {
		if i%reconcileCheckStride == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		remote := snap.Vertices[i]
		remote.UUID = strings.TrimSpace(remote.UUID)
		report.Inspected++

		if remote.UUID == "" || !remote.Observed() {
			// An unidentified or untouched element carries no information and
			// would only grow the state. Wire input is not trusted.
			report.Skipped++
			continue
		}
		report.HighestStamp = maxStamp(report.HighestStamp, highestStamp(remote.AddTimeline), highestStamp(remote.RemoveTimeline))

		changed, conflict := c.joinVertexLocked(remote)
		if changed {
			report.VerticesJoined++
		}
		if conflict {
			report.Conflicts++
		}
	}

	for i := range snap.Edges {
		if i%reconcileCheckStride == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		remote := snap.Edges[i]
		remote.SourceUUID = strings.TrimSpace(remote.SourceUUID)
		remote.TargetUUID = strings.TrimSpace(remote.TargetUUID)
		remote.RelationshipType = strings.ToUpper(strings.TrimSpace(remote.RelationshipType))
		report.Inspected++

		if remote.SourceUUID == "" || remote.TargetUUID == "" || remote.RelationshipType == "" || !remote.Observed() {
			report.Skipped++
			continue
		}
		report.HighestStamp = maxStamp(report.HighestStamp, highestStamp(remote.AddTimeline), highestStamp(remote.RemoveTimeline))

		changed, conflict := c.joinEdgeLocked(remote)
		if changed {
			report.EdgesJoined++
		}
		if conflict {
			report.Conflicts++
		}
	}

	return nil
}

// joinVertexLocked merges one remote vertex into local state and reports
// whether anything changed and whether the two replicas had genuinely diverged.
//
// A conflict is not an error — the join always has an answer. It is counted
// because a rising conflict rate is the signal that two sites are editing the
// same equipment, which is an operational question rather than a software one.
func (c *GraphCRDT) joinVertexLocked(remote Vertex) (changed, conflict bool) {
	local, exists := c.vertices[remote.UUID]
	if !exists {
		fresh := remote.Clone()
		ensureVertexMaps(&fresh)
		c.vertices[remote.UUID] = &fresh
		return true, false
	}

	// The dominant writes have to be read before the timelines are joined,
	// because afterwards both sides share one timeline and the question "who
	// wrote the payload we are holding" no longer has an answer.
	localDominant := dominantWrite(local.AddTimeline)
	remoteDominant := dominantWrite(remote.AddTimeline)
	localLive := local.Live()
	remoteLive := remote.Live()
	payloadDiffers := !maps.Equal(local.Properties, remote.Properties)

	if mergeTimelineInto(local.AddTimeline, remote.AddTimeline) {
		changed = true
	}
	if mergeTimelineInto(local.RemoveTimeline, remote.RemoveTimeline) {
		changed = true
	}

	switch {
	case remoteDominant.after(localDominant):
		if payloadDiffers {
			local.Properties = cloneProperties(remote.Properties)
			changed = true
		}
	case localDominant.after(remoteDominant):
		// Local write wins; nothing to copy.
	default:
		// Identical dominant writes with different payloads means one replica id
		// issued the same stamp twice — a duplicated identity or a clock that
		// regressed across a restart. The join still has to converge, so the tie
		// breaks on the canonical encoding, which every replica computes the
		// same way.
		if payloadDiffers {
			if canonicalProperties(remote.Properties) > canonicalProperties(local.Properties) {
				local.Properties = cloneProperties(remote.Properties)
				changed = true
			}
			conflict = true
			c.log.Warn("divergent vertex payload at an identical write stamp",
				"uuid", remote.UUID,
				"stamp", localDominant.Timestamp,
				"writer", localDominant.ClientID,
				"likely_cause", "a reused client id or a lamport clock that regressed across a restart")
		}
	}

	switch {
	case localLive != remoteLive:
		// One replica held the asset while the other had already retired it —
		// the concurrent add/remove the join exists to arbitrate.
		conflict = true
	case payloadDiffers && localDominant.observed() && remoteDominant.observed():
		// Both sides wrote a payload and they disagreed; last-writer-wins picked
		// one and discarded the other.
		conflict = true
	}

	return changed, conflict
}

// joinEdgeLocked merges one remote edge. Edges carry no payload, so the join is
// purely the two timeline maxima.
func (c *GraphCRDT) joinEdgeLocked(remote Edge) (changed, conflict bool) {
	key := remote.Key()
	local, exists := c.edges[key]
	if !exists {
		fresh := remote.Clone()
		ensureEdgeMaps(&fresh)
		c.edges[key] = &fresh
		return true, false
	}

	localLive := local.Live()
	remoteLive := remote.Live()

	if mergeTimelineInto(local.AddTimeline, remote.AddTimeline) {
		changed = true
	}
	if mergeTimelineInto(local.RemoveTimeline, remote.RemoveTimeline) {
		changed = true
	}

	return changed, localLive != remoteLive
}

// ---------------------------------------------------------------------------
// Root-cause peeling
// ---------------------------------------------------------------------------

// maxPeelDepth bounds peelRootCause. A wrapper chain that references itself —
// which a retry aggregator can produce by appending its own error to its attempt
// list — would otherwise spin forever on the replication path.
const maxPeelDepth = 8

var errorInterface = reflect.TypeOf((*error)(nil)).Elem()

// causeFieldNames are the field names that carry a nested error in the driver
// and transport types this engine sits behind, most specific first.
var causeFieldNames = []string{"Inner", "Cause", "Err", "Reason", "Wrapped", "Errors", "Attempts"}

// peelRootCause reduces a wrapped error to the failure that actually happened.
//
// The standard chain is tried first. The reflective arm exists because several
// types on this path box their cause in an exported field and never implement
// Unwrap — the Bolt driver's ConnectivityError (Inner) and
// TransactionExecutionLimit (Errors) are the two that bite in production.
// Without it, a context deadline arriving inside one of those is invisible to
// errors.Is, and every timeout gets misfiled as a generic replication failure,
// which is exactly the misdiagnosis this module is instrumented to prevent.
//
// It never returns nil for a non-nil input: an unpeelable error is its own root
// cause.
func peelRootCause(err error) error {
	for depth := 0; err != nil && depth < maxPeelDepth; depth++ {
		var inner error

		switch unwrapper := err.(type) {
		case interface{ Unwrap() error }:
			inner = unwrapper.Unwrap()
		case interface{ Unwrap() []error }:
			// The last attempt is the one that decided the outcome.
			inner = lastError(unwrapper.Unwrap())
		default:
			inner = unboxCause(err)
		}

		if inner == nil || inner == err {
			return err
		}
		err = inner
	}
	return err
}

// unboxCause reaches into a struct error that does not implement Unwrap and
// returns the first nested error it finds among the conventional field names.
// It reads exported fields only and never panics on an unreadable one.
func unboxCause(err error) error {
	value := reflect.ValueOf(err)
	for value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return nil
	}

	for _, name := range causeFieldNames {
		field := value.FieldByName(name)
		if !field.IsValid() || !field.CanInterface() {
			continue
		}
		if field.Kind() == reflect.Slice {
			for i := field.Len() - 1; i >= 0; i-- {
				if cause := errorFromValue(field.Index(i)); cause != nil {
					return cause
				}
			}
			continue
		}
		if cause := errorFromValue(field); cause != nil {
			return cause
		}
	}
	return nil
}

// errorFromValue extracts an error from a reflected field, guarding every case
// where Interface or IsNil would panic.
func errorFromValue(value reflect.Value) error {
	if !value.IsValid() || !value.CanInterface() {
		return nil
	}
	switch value.Kind() {
	case reflect.Interface, reflect.Pointer, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		if value.IsNil() {
			return nil
		}
	}
	if !value.Type().Implements(errorInterface) {
		return nil
	}
	cause, _ := value.Interface().(error)
	return cause
}

// lastError returns the final non-nil error in a slice.
func lastError(errs []error) error {
	for i := len(errs) - 1; i >= 0; i-- {
		if errs[i] != nil {
			return errs[i]
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Tombstone compaction
// ---------------------------------------------------------------------------

// Compact drops tombstoned elements whose highest remove stamp is strictly
// below stableBefore, and returns how many were dropped.
//
// stableBefore must be a causal stability watermark: a stamp that every replica
// in the fleet is known to have observed, which in practice comes out of a
// completed anti-entropy round rather than from a local clock. Passing a
// watermark that is too high resurrects deleted equipment, because a peer that
// still holds the original add will re-introduce it on the next merge with
// nothing left to say it was removed. This is the one operation in the module
// that can lose information, which is why it is explicit and never automatic.
func (c *GraphCRDT) Compact(stableBefore int64) int {
	if stableBefore <= 0 {
		return 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	removed := 0
	for key, vertex := range c.vertices {
		if vertex.Live() {
			continue
		}
		if highestStamp(vertex.RemoveTimeline) >= stableBefore {
			continue
		}
		delete(c.vertices, key)
		removed++
	}
	for key, edge := range c.edges {
		if edge.Live() {
			continue
		}
		if highestStamp(edge.RemoveTimeline) >= stableBefore {
			continue
		}
		delete(c.edges, key)
		removed++
	}

	if removed > 0 {
		c.log.Info("compacted tombstones below the causal watermark",
			"watermark", stableBefore,
			"removed", removed,
			"vertices_remaining", len(c.vertices),
			"edges_remaining", len(c.edges))
	}
	return removed
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

// CRDTMetrics is a coherent snapshot of one replica's counters and gauges, in
// the shape the /metrics and /stats endpoints expect.
type CRDTMetrics struct {
	ClientID     string `json:"client_id"`
	LamportClock int64  `json:"lamport_clock"`

	LiveVertices       int `json:"live_vertices"`
	TombstonedVertices int `json:"tombstoned_vertices"`
	LiveEdges          int `json:"live_edges"`
	TombstonedEdges    int `json:"tombstoned_edges"`
	DanglingEdges      int `json:"dangling_edges"`

	Mutations           uint64 `json:"mutations_total"`
	RejectedMutations   uint64 `json:"mutations_rejected_total"`
	Merges              uint64 `json:"merges_total"`
	MergeConflicts      uint64 `json:"merge_conflicts_total"`
	ElementsJoined      uint64 `json:"elements_joined_total"`
	RejectedElements    uint64 `json:"elements_rejected_total"`
	CallerCancellations uint64 `json:"reconcile_cancelled_total"`
	BudgetExceeded      uint64 `json:"reconcile_budget_exceeded_total"`
	IdentityCollisions  uint64 `json:"identity_collisions_total"`
	UnknownRelations    uint64 `json:"unknown_relationships_total"`

	LastMergeAt       time.Time     `json:"last_merge_at"`
	LastMergeDuration time.Duration `json:"last_merge_duration_ns"`
	Uptime            time.Duration `json:"uptime_ns"`
}

// Stats returns the current metrics.
//
// The gauges are counted by walking the element maps under the read lock, so a
// scrape costs O(V+E) and is consistent by construction — incrementally
// maintained counts drift the first time a merge revives a tombstone, and a
// wrong asset count is worse than a slightly expensive scrape. Size the scrape
// interval accordingly on a fleet aggregator.
func (c *GraphCRDT) Stats() CRDTMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()

	metrics := CRDTMetrics{
		ClientID:            c.ClientID,
		LamportClock:        c.LamportClock,
		Mutations:           c.mutations.Load(),
		RejectedMutations:   c.rejectedMutations.Load(),
		Merges:              c.merges.Load(),
		MergeConflicts:      c.mergeConflicts.Load(),
		ElementsJoined:      c.elementsJoined.Load(),
		RejectedElements:    c.rejectedElements.Load(),
		CallerCancellations: c.callerCancellations.Load(),
		BudgetExceeded:      c.budgetExceeded.Load(),
		IdentityCollisions:  c.identityCollisions.Load(),
		UnknownRelations:    c.unknownRelations.Load(),
		LastMergeAt:         c.lastMergeAt,
		LastMergeDuration:   c.lastMergeDuration,
		Uptime:              time.Since(c.startedAt),
	}

	for _, vertex := range c.vertices {
		if vertex.Live() {
			metrics.LiveVertices++
		} else {
			metrics.TombstonedVertices++
		}
	}
	for _, edge := range c.edges {
		if !edge.Live() {
			metrics.TombstonedEdges++
			continue
		}
		metrics.LiveEdges++
		if !c.vertexLiveLocked(edge.SourceUUID) || !c.vertexLiveLocked(edge.TargetUUID) {
			metrics.DanglingEdges++
		}
	}

	return metrics
}

// LogValue renders the metrics compactly for a periodic status line.
func (m CRDTMetrics) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("client_id", m.ClientID),
		slog.Int64("lamport_clock", m.LamportClock),
		slog.Int("live_vertices", m.LiveVertices),
		slog.Int("live_edges", m.LiveEdges),
		slog.Int("tombstones", m.TombstonedVertices+m.TombstonedEdges),
		slog.Uint64("merges", m.Merges),
		slog.Uint64("conflicts", m.MergeConflicts),
	)
}

type metricSample struct {
	Name  string
	Help  string
	Type  string
	Value float64
}

func (m CRDTMetrics) samples() []metricSample {
	return []metricSample{
		{"openontology_crdt_lamport_clock", "Current local Lamport clock.", "gauge", float64(m.LamportClock)},
		{"openontology_crdt_vertices_live", "Assets currently present in the replicated topology.", "gauge", float64(m.LiveVertices)},
		{"openontology_crdt_vertices_tombstoned", "Assets retained as tombstones.", "gauge", float64(m.TombstonedVertices)},
		{"openontology_crdt_edges_live", "Dependencies currently present in the replicated topology.", "gauge", float64(m.LiveEdges)},
		{"openontology_crdt_edges_tombstoned", "Dependencies retained as tombstones.", "gauge", float64(m.TombstonedEdges)},
		{"openontology_crdt_edges_dangling", "Live dependencies with at least one endpoint that is not live.", "gauge", float64(m.DanglingEdges)},
		{"openontology_crdt_mutations_total", "Local mutations applied.", "counter", float64(m.Mutations)},
		{"openontology_crdt_mutations_rejected_total", "Local mutations rejected as malformed.", "counter", float64(m.RejectedMutations)},
		{"openontology_crdt_merges_total", "State merges attempted.", "counter", float64(m.Merges)},
		{"openontology_crdt_merge_conflicts_total", "Elements whose merge arbitrated a genuine divergence.", "counter", float64(m.MergeConflicts)},
		{"openontology_crdt_elements_joined_total", "Elements changed by a merge.", "counter", float64(m.ElementsJoined)},
		{"openontology_crdt_elements_rejected_total", "Remote elements discarded as malformed.", "counter", float64(m.RejectedElements)},
		{"openontology_crdt_reconcile_cancelled_total", "Reconciliations abandoned by the caller.", "counter", float64(m.CallerCancellations)},
		{"openontology_crdt_reconcile_budget_exceeded_total", "Reconciliations that outran the local execution budget.", "counter", float64(m.BudgetExceeded)},
		{"openontology_crdt_identity_collisions_total", "Merges against a state claiming this replica's own client id.", "counter", float64(m.IdentityCollisions)},
		{"openontology_crdt_unknown_relationships_total", "Edge mutations using a relationship type the topology tier does not traverse.", "counter", float64(m.UnknownRelations)},
		{"openontology_crdt_last_merge_duration_seconds", "Duration of the most recent merge.", "gauge", m.LastMergeDuration.Seconds()},
		{"openontology_crdt_last_merge_timestamp_seconds", "Unix time of the most recent merge.", "gauge", lastMergeUnix(m.LastMergeAt)},
		{"openontology_crdt_uptime_seconds", "Replica uptime in seconds.", "gauge", m.Uptime.Seconds()},
	}
}

// JSON renders a flat map for the /stats endpoint, matching the shape the rest
// of the service exposes.
func (m CRDTMetrics) JSON() map[string]float64 {
	out := make(map[string]float64, 20)
	for _, sample := range m.samples() {
		out[strings.TrimPrefix(sample.Name, "openontology_crdt_")] = sample.Value
	}
	return out
}

// Prometheus renders the text exposition format without a client library, the
// same way the rest of the service does — the counter set is small and stable
// enough not to warrant one. Every series is labelled with the replica id so a
// process hosting several sites' states does not collapse them into one line.
func (m CRDTMetrics) Prometheus() string {
	samples := m.samples()
	sort.Slice(samples, func(i, j int) bool { return samples[i].Name < samples[j].Name })

	// The quotes are written by hand rather than with %q, which would escape the
	// already-escaped value a second time.
	label := `{replica="` + escapeLabelValue(m.ClientID) + `"}`

	var b strings.Builder
	for _, sample := range samples {
		fmt.Fprintf(&b, "# HELP %s %s\n", sample.Name, sample.Help)
		fmt.Fprintf(&b, "# TYPE %s %s\n", sample.Name, sample.Type)
		fmt.Fprintf(&b, "%s%s %g\n", sample.Name, label, sample.Value)
	}
	return b.String()
}

// lastMergeUnix reports 0 rather than a negative epoch for a replica that has
// never merged, so the series reads as "never" instead of "1970".
func lastMergeUnix(at time.Time) float64 {
	if at.IsZero() {
		return 0
	}
	return float64(at.UnixNano()) / float64(time.Second)
}

// escapeLabelValue escapes the three characters the exposition format reserves
// inside a label value. A client id is operator-supplied, so it cannot be
// trusted to be clean.
func escapeLabelValue(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return replacer.Replace(value)
}

// ---------------------------------------------------------------------------
// Timeline and payload primitives
// ---------------------------------------------------------------------------

// highestStamp returns the greatest stamp in a timeline, or 0 for an empty one.
func highestStamp(timeline map[string]int64) int64 {
	var highest int64
	for _, stamp := range timeline {
		if stamp > highest {
			highest = stamp
		}
	}
	return highest
}

// dominantWrite returns the greatest (stamp, replica id) pair in a timeline.
// The replica id is the tiebreak that makes the payload merge deterministic
// when two replicas wrote at the same logical instant.
func dominantWrite(timeline map[string]int64) writeStamp {
	var dominant writeStamp
	for clientID, stamp := range timeline {
		candidate := writeStamp{Timestamp: stamp, ClientID: clientID}
		if candidate.after(dominant) {
			dominant = candidate
		}
	}
	return dominant
}

// mergeTimelineInto applies the pointwise maximum of src onto dst and reports
// whether dst changed. This is the whole of the semilattice join: max is
// commutative, associative and idempotent, and every convergence property of
// the module follows from it.
func mergeTimelineInto(dst, src map[string]int64) bool {
	changed := false
	for clientID, stamp := range src {
		if existing, ok := dst[clientID]; !ok || stamp > existing {
			dst[clientID] = stamp
			changed = true
		}
	}
	return changed
}

// cloneTimeline copies a timeline, always returning a usable map so that a
// decoded element with an absent timeline is still writable.
func cloneTimeline(timeline map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(timeline))
	for clientID, stamp := range timeline {
		out[clientID] = stamp
	}
	return out
}

// cloneProperties copies a property map, never returning nil.
func cloneProperties(props map[string]string) map[string]string {
	out := make(map[string]string, len(props))
	for key, value := range props {
		out[key] = value
	}
	return out
}

// canonicalProperties renders a property map in a form every replica computes
// identically. It is the last-resort tiebreak in the payload merge, so it must
// be a total function of the map's contents and nothing else.
func canonicalProperties(props map[string]string) string {
	keys := make([]string, 0, len(props))
	for key := range props {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var b strings.Builder
	for _, key := range keys {
		b.WriteString(key)
		b.WriteString(keySeparator)
		b.WriteString(props[key])
		b.WriteString(keySeparator)
	}
	return b.String()
}

// ensureVertexMaps repairs a vertex decoded from the wire, where omitempty and
// a hand-built payload both produce nil maps that would panic on write.
func ensureVertexMaps(v *Vertex) {
	if v.Properties == nil {
		v.Properties = make(map[string]string)
	}
	if v.AddTimeline == nil {
		v.AddTimeline = make(map[string]int64)
	}
	if v.RemoveTimeline == nil {
		v.RemoveTimeline = make(map[string]int64)
	}
}

// ensureEdgeMaps does the same for an edge.
func ensureEdgeMaps(e *Edge) {
	if e.AddTimeline == nil {
		e.AddTimeline = make(map[string]int64)
	}
	if e.RemoveTimeline == nil {
		e.RemoveTimeline = make(map[string]int64)
	}
}

// maxStamp returns the greatest of the supplied stamps.
func maxStamp(stamps ...int64) int64 {
	var highest int64
	for _, stamp := range stamps {
		if stamp > highest {
			highest = stamp
		}
	}
	return highest
}
