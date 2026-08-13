package crdt

import (
	"context"
	"errors"
	"testing"
	"time"
)

func replica(t *testing.T, id string) *GraphCRDT {
	t.Helper()
	return NewGraphCRDT(id, WithLogger(DiscardLogger()))
}

func digest(t *testing.T, c *GraphCRDT) string {
	t.Helper()
	d, err := c.Digest()
	if err != nil {
		t.Fatalf("Digest: %v", err)
	}
	return d
}

// diverged builds two replicas that made conflicting offline edits.
func diverged(t *testing.T) (*GraphCRDT, *GraphCRDT) {
	t.Helper()
	a := replica(t, "aircraft-A")
	b := replica(t, "ground-B")

	for _, r := range []*GraphCRDT{a, b} {
		r.AddVertex("pump-1", map[string]string{"status": "RUNNING"})
		r.AddVertex("manifold-2", map[string]string{"status": "RUNNING"})
		r.AddEdge("pump-1", "manifold-2", "feeds")
	}
	// A retires the pump while offline; B re-labels it and adds a controller.
	a.RemoveVertex("pump-1")
	b.AddVertex("pump-1", map[string]string{"status": "DEGRADED"})
	b.AddVertex("fadec-3", map[string]string{"status": "RUNNING"})
	b.AddEdge("fadec-3", "pump-1", "CONTROLS")
	return a, b
}

func TestMergeConverges(t *testing.T) {
	a, b := diverged(t)
	if err := a.Merge(b); err != nil {
		t.Fatalf("a.Merge(b): %v", err)
	}
	if err := b.Merge(a); err != nil {
		t.Fatalf("b.Merge(a): %v", err)
	}
	if da, db := digest(t, a), digest(t, b); da != db {
		t.Fatalf("replicas diverged:\n a=%s\n b=%s\n a=%+v\n b=%+v", da, db, a.LiveVertices(), b.LiveVertices())
	}

	// Both replicas had issued the same number of operations while partitioned,
	// so A's removal and B's re-add carry the same Lamport stamp. That is the
	// documented tie, and the presence rule resolves it to removed on both
	// sides — which is the property that matters: they agree.
	if a.HasVertex("pump-1") != b.HasVertex("pump-1") {
		t.Fatal("replicas disagree about the contested asset")
	}
	if a.HasVertex("pump-1") {
		t.Fatal("a stamp tie between add and remove must resolve to removed")
	}

	// Resurrection after the merge: the clock has absorbed everything both
	// sides saw, so this add is strictly greater than the removal that killed
	// it and the asset comes back with its new payload.
	b.AddVertex("pump-1", map[string]string{"status": "REPLACED"})
	if err := a.Merge(b); err != nil {
		t.Fatalf("merge after resurrection: %v", err)
	}
	if !a.HasVertex("pump-1") {
		t.Fatal("a re-add above the removal stamp did not resurrect the asset")
	}
	if v, _ := a.LookupVertex("pump-1"); v.Properties["status"] != "REPLACED" {
		t.Fatalf("payload lost the last-writer-wins arbitration: %+v", v.Properties)
	}
}

func TestMergeIsIdempotent(t *testing.T) {
	a, b := diverged(t)
	if err := a.Merge(b); err != nil {
		t.Fatalf("first merge: %v", err)
	}
	once := digest(t, a)
	for i := 0; i < 3; i++ {
		if err := a.Merge(b); err != nil {
			t.Fatalf("repeat merge %d: %v", i, err)
		}
	}
	if again := digest(t, a); again != once {
		t.Fatalf("merge is not idempotent: %s then %s", once, again)
	}
	if err := a.Merge(a); err != nil {
		t.Fatalf("self merge: %v", err)
	}
	if self := digest(t, a); self != once {
		t.Fatalf("self merge mutated state: %s then %s", once, self)
	}
}

func TestMergeIsAssociativeAndCommutative(t *testing.T) {
	build := func() (*GraphCRDT, *GraphCRDT, *GraphCRDT) {
		a, b := diverged(t)
		c := replica(t, "vessel-C")
		c.AddVertex("manifold-2", map[string]string{"status": "OFFLINE"})
		c.RemoveVertex("fadec-3")
		c.AddEdge("manifold-2", "turbine-4", "FEEDS")
		return a, b, c
	}

	// (a ⊔ b) ⊔ c
	a1, b1, c1 := build()
	if err := a1.Merge(b1); err != nil {
		t.Fatal(err)
	}
	if err := a1.Merge(c1); err != nil {
		t.Fatal(err)
	}

	// a ⊔ (b ⊔ c), in the opposite order at every step
	a2, b2, c2 := build()
	if err := c2.Merge(b2); err != nil {
		t.Fatal(err)
	}
	if err := a2.Merge(c2); err != nil {
		t.Fatal(err)
	}

	if left, right := digest(t, a1), digest(t, a2); left != right {
		t.Fatalf("join is not associative:\n left=%s\n right=%s\n left=%+v\n right=%+v",
			left, right, a1.LiveVertices(), a2.LiveVertices())
	}
}

func TestRemoveWinsTiesAndCascadesToEdges(t *testing.T) {
	a := replica(t, "site-A")
	a.AddVertex("pump-1", nil)
	a.AddVertex("manifold-2", nil)
	a.AddEdge("pump-1", "manifold-2", "FEEDS")
	if len(a.LiveEdges()) != 1 {
		t.Fatalf("expected one live edge, got %d", len(a.LiveEdges()))
	}

	a.RemoveVertex("pump-1")
	if a.HasVertex("pump-1") {
		t.Fatal("removed vertex is still live")
	}
	if got := len(a.LiveEdges()); got != 0 {
		t.Fatalf("edge survived its endpoint's removal: %d live", got)
	}

	// Hand-built tie: equal highest add and remove stamps must resolve to removed.
	tie := Vertex{
		UUID:           "tie-1",
		AddTimeline:    map[string]int64{"x": 7},
		RemoveTimeline: map[string]int64{"y": 7},
	}
	if tie.Live() {
		t.Fatal("a stamp tie must resolve to removed")
	}

	// Re-adding resurrects, and the edge comes back with the endpoint.
	a.AddVertex("pump-1", map[string]string{"status": "REPLACED"})
	if !a.HasVertex("pump-1") {
		t.Fatal("re-add did not resurrect the asset")
	}
	a.AddEdge("pump-1", "manifold-2", "FEEDS")
	if got := len(a.LiveEdges()); got != 1 {
		t.Fatalf("expected the re-added edge to be live, got %d", got)
	}
}

func TestDanglingEdgesAreHiddenButRetained(t *testing.T) {
	a := replica(t, "site-A")
	a.AddEdge("pump-1", "manifold-2", "FEEDS") // endpoints not yet replicated
	if got := len(a.LiveEdges()); got != 0 {
		t.Fatalf("dangling edge should not appear in the view, got %d", got)
	}
	if got := len(a.DanglingEdges()); got != 1 {
		t.Fatalf("dangling edge should be retained, got %d", got)
	}
	a.AddVertex("pump-1", nil)
	a.AddVertex("manifold-2", nil)
	if got := len(a.LiveEdges()); got != 1 {
		t.Fatalf("edge should surface once its endpoints arrive, got %d", got)
	}
}

func TestWireRoundTripAndSnapshotMerge(t *testing.T) {
	a, b := diverged(t)
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}

	encoded, err := a.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	decoded := &GraphCRDT{}
	if err := decoded.UnmarshalJSON(encoded); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	if want, got := digest(t, a), digest(t, decoded); want != got {
		t.Fatalf("wire round trip lost state: %s vs %s", want, got)
	}
	if decoded.Clock() != a.Clock() {
		t.Fatalf("wire round trip lost the clock: %d vs %d", decoded.Clock(), a.Clock())
	}

	fresh := replica(t, "site-D")
	if err := fresh.MergeSnapshot(a.Snapshot()); err != nil {
		t.Fatalf("MergeSnapshot: %v", err)
	}
	if want, got := digest(t, a), digest(t, fresh); want != got {
		t.Fatalf("snapshot merge did not converge: %s vs %s", want, got)
	}
	if clone := a.Clone(); digest(t, clone) != digest(t, a) {
		t.Fatal("Clone diverged from its source")
	}
}

func TestReconcileDistinguishesCancellationFromBudget(t *testing.T) {
	a, b := diverged(t)

	// Budget exhausted: the caller is healthy, our own execution budget fired.
	tight := NewGraphCRDT("site-tight", WithLogger(DiscardLogger()), WithReconcileBudget(time.Nanosecond))
	err := tight.ReconcileRemoteDelta(context.Background(), b)
	if !errors.Is(err, ErrReconcileBudgetExceeded) {
		t.Fatalf("want ErrReconcileBudgetExceeded, got %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("budget error must also match context.DeadlineExceeded, got %v", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("budget error must not read as a caller cancellation: %v", err)
	}
	if s := tight.Stats(); s.BudgetExceeded != 1 || s.CallerCancellations != 0 {
		t.Fatalf("budget exhaustion mis-counted: %+v", s)
	}

	// Caller gave up: same context error class, different attribution.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = a.ReconcileRemoteDelta(ctx, b)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if errors.Is(err, ErrReconcileBudgetExceeded) {
		t.Fatalf("caller cancellation must not read as a budget failure: %v", err)
	}
	if s := a.Stats(); s.CallerCancellations != 1 || s.BudgetExceeded != 0 {
		t.Fatalf("caller cancellation mis-counted: %+v", s)
	}

	if err := a.ReconcileRemoteDelta(context.Background(), nil); !errors.Is(err, ErrNilRemoteState) {
		t.Fatalf("want ErrNilRemoteState, got %v", err)
	}
}

// driverConnError mirrors a driver type that boxes its cause without Unwrap.
type driverConnError struct{ Inner error }

func (e *driverConnError) Error() string { return "connectivity: " + e.Inner.Error() }

// driverRetryLimit mirrors a retry aggregator that exposes no Unwrap either.
type driverRetryLimit struct{ Errors []error }

func (e *driverRetryLimit) Error() string { return "retries exhausted" }

func TestPeelRootCause(t *testing.T) {
	sentinel := errors.New("socket closed")

	cases := map[string]struct {
		err  error
		want error
	}{
		"plain":                {sentinel, sentinel},
		"standard wrap":        {&TransportError{Peer: "p", Op: "push", Inner: sentinel}, sentinel},
		"multi unwrap":         {&ReplicaSyncLimit{Peer: "p", Attempts: []error{errors.New("first"), sentinel}}, sentinel},
		"unboxed inner":        {&driverConnError{Inner: sentinel}, sentinel},
		"unboxed slice":        {&driverRetryLimit{Errors: []error{errors.New("first"), sentinel}}, sentinel},
		"deadline in a driver": {&driverConnError{Inner: context.DeadlineExceeded}, context.DeadlineExceeded},
		"nested":               {&TransportError{Inner: &driverRetryLimit{Errors: []error{&driverConnError{Inner: sentinel}}}}, sentinel},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := peelRootCause(tc.err); got != tc.want {
				t.Fatalf("peelRootCause = %v, want %v", got, tc.want)
			}
		})
	}

	if got := peelRootCause(nil); got != nil {
		t.Fatalf("peelRootCause(nil) = %v, want nil", got)
	}

	// A self-referential chain must terminate rather than spin.
	loop := &driverConnError{}
	loop.Inner = loop
	done := make(chan error, 1)
	go func() { done <- peelRootCause(loop) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("peelRootCause did not terminate on a self-referential chain")
	}

	// A driver deadline hidden behind two unboxed types must still classify as
	// a budget failure rather than a generic error.
	buried := &TransportError{Inner: &driverConnError{Inner: context.DeadlineExceeded}}
	if !errors.Is(peelRootCause(buried), context.DeadlineExceeded) {
		t.Fatal("a buried deadline stayed invisible to errors.Is")
	}
}

func TestStatsAndCompaction(t *testing.T) {
	a, b := diverged(t)
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	a.AddVertex("", nil)                        // rejected
	a.AddEdge("x", "y", "  ")                   // rejected
	a.AddEdge("pump-1", "manifold-2", "POWERS") // unknown relationship

	s := a.Stats()
	if s.LiveVertices == 0 || s.Merges != 1 {
		t.Fatalf("unexpected stats: %+v", s)
	}
	if s.RejectedMutations != 2 {
		t.Fatalf("want 2 rejected mutations, got %d", s.RejectedMutations)
	}
	if s.UnknownRelations != 1 {
		t.Fatalf("want 1 unknown relationship, got %d", s.UnknownRelations)
	}
	if s.MergeConflicts == 0 {
		t.Fatalf("a divergent add/remove should have been counted as a conflict: %+v", s)
	}
	if s.ClientID != "aircraft-A" || s.LamportClock <= 0 {
		t.Fatalf("identity or clock missing from stats: %+v", s)
	}
	if prom := s.Prometheus(); len(prom) == 0 ||
		!contains(prom, `openontology_crdt_vertices_live{replica="aircraft-A"}`) {
		t.Fatalf("prometheus rendering is wrong:\n%s", prom)
	}
	if js := s.JSON(); js["vertices_live"] != float64(s.LiveVertices) {
		t.Fatalf("json rendering is wrong: %+v", js)
	}

	// Nothing is compacted below the watermark; everything tombstoned above it.
	if got := a.Compact(1); got != 0 {
		t.Fatalf("compaction below the watermark removed %d elements", got)
	}
	before := a.Stats()
	if removed := a.Compact(a.Clock() + 1); removed != before.TombstonedVertices+before.TombstonedEdges {
		t.Fatalf("compaction removed %d, want %d", removed, before.TombstonedVertices+before.TombstonedEdges)
	}
	after := a.Stats()
	if after.TombstonedVertices != 0 || after.TombstonedEdges != 0 {
		t.Fatalf("tombstones survived compaction: %+v", after)
	}
	if after.LiveVertices != before.LiveVertices {
		t.Fatalf("compaction dropped live assets: %d then %d", before.LiveVertices, after.LiveVertices)
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// TestConcurrentMutualReconciliation is the deadlock regression: two replicas
// reconciling each other while both are being mutated.
func TestConcurrentMutualReconciliation(t *testing.T) {
	a := replica(t, "site-A")
	b := replica(t, "site-B")
	for i := 0; i < 200; i++ {
		a.AddVertex(string(rune('a'+i%26))+"-asset", map[string]string{"n": "1"})
		b.AddVertex(string(rune('a'+i%26))+"-asset", map[string]string{"n": "2"})
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			_ = a.ReconcileRemoteDelta(context.Background(), b)
			a.AddVertex("hot-asset", map[string]string{"n": "a"})
		}
	}()
	for i := 0; i < 50; i++ {
		_ = b.ReconcileRemoteDelta(context.Background(), a)
		b.RemoveVertex("hot-asset")
	}
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("mutual reconciliation deadlocked")
	}

	// Drain to a fixed point, then both replicas must agree.
	for i := 0; i < 3; i++ {
		if err := a.Merge(b); err != nil {
			t.Fatal(err)
		}
		if err := b.Merge(a); err != nil {
			t.Fatal(err)
		}
	}
	if da, db := digest(t, a), digest(t, b); da != db {
		t.Fatalf("replicas did not converge: %s vs %s", da, db)
	}
}
