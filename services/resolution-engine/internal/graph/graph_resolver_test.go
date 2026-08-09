package graph

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func assetNode(elementID string, props map[string]any) neo4j.Node {
	return neo4j.Node{ElementId: elementID, Labels: []string{"Asset"}, Props: props}
}

func operatorNode(elementID string, props map[string]any) neo4j.Node {
	return neo4j.Node{ElementId: elementID, Labels: []string{"Operator"}, Props: props}
}

// resolutionRecord mirrors the shape the driver hands back for
// CypherResolveAssetContext, so the mapper can be exercised without a cluster.
func resolutionRecord(target, upstream, downstream, technician any) *neo4j.Record {
	return &neo4j.Record{
		Keys:   []string{columnTarget, columnUpstream, columnDownstream, columnTechnician},
		Values: []any{target, upstream, downstream, technician},
	}
}

// TestNewNeo4jGraphResolverRejectsBadURI covers the validation that runs before
// any socket is opened.
func TestNewNeo4jGraphResolverRejectsBadURI(t *testing.T) {
	for name, uri := range map[string]string{
		"empty":              "",
		"whitespace":         "   ",
		"unsupported scheme": "http://neo4j.internal:7687",
		"no host":            "neo4j://",
	} {
		t.Run(name, func(t *testing.T) {
			resolver, err := NewNeo4jGraphResolver(uri, "neo4j", "secret", WithLogger(discardLogger()))
			if err == nil {
				_ = resolver.Close()
				t.Fatalf("NewNeo4jGraphResolver(%q) succeeded, want an error", uri)
			}
			if resolver != nil {
				t.Fatalf("NewNeo4jGraphResolver(%q) returned a resolver alongside an error", uri)
			}
		})
	}
}

// TestNewNeo4jGraphResolverFailsFastWhenUnreachable shows the intended
// construction call and proves the startup handshake is enforced: a resolver is
// never handed back pointing at a cluster it has not reached.
func TestNewNeo4jGraphResolverFailsFastWhenUnreachable(t *testing.T) {
	started := time.Now()

	resolver, err := NewNeo4jGraphResolver(
		"bolt://127.0.0.1:1", // reserved port, nothing listens here
		"neo4j",
		"secret",
		WithLogger(discardLogger()),
		WithDatabase("neo4j"),
		WithQueryTimeout(DefaultQueryTimeout),
		WithMaxConnectionPoolSize(16),
		WithConnectivityTimeout(2*time.Second),
	)
	if err == nil {
		_ = resolver.Close()
		t.Fatal("NewNeo4jGraphResolver succeeded against a dead endpoint, want a connectivity error")
	}
	if resolver != nil {
		t.Fatal("NewNeo4jGraphResolver returned a resolver alongside a connectivity error")
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("connectivity check took %s, want it bounded by the 2s budget", elapsed)
	}
}

func TestMapRecordFullTopology(t *testing.T) {
	installed := time.Date(2019, 4, 2, 0, 0, 0, 0, time.UTC)

	record := resolutionRecord(
		assetNode("4:asset:1", map[string]any{
			"id":                "HPP-PUMP-221",
			"name":              "Hydraulic Power Pack Pump 221",
			"model_number":      "REX-A10VSO-140",
			"current_status":    "RUNNING",
			"installation_date": dbtype.Date(installed),
		}),
		[]any{
			assetNode("4:asset:2", map[string]any{"id": "MCC-L4-BUS-A", "current_status": "RUNNING"}),
			// Legacy camelCase loader output must still land in the radius.
			assetNode("4:asset:3", map[string]any{"assetId": "XFMR-L4-01", "modelNumber": "SGB-1000"}),
		},
		[]any{
			assetNode("4:asset:4", map[string]any{"id": "EXTRUDER-L4", "status": "DEGRADED"}),
		},
		operatorNode("4:op:9", map[string]any{
			"technician_id":       "TECH-8801",
			"name":                "J. de Vries",
			"certification_level": "L3-HYDRAULICS",
			"active_shift":        true,
		}),
	)

	resolved, err := mapRecord(record, discardLogger())
	if err != nil {
		t.Fatalf("mapRecord: %v", err)
	}

	if resolved.Target.AssetID != "HPP-PUMP-221" {
		t.Errorf("target asset id = %q, want %q", resolved.Target.AssetID, "HPP-PUMP-221")
	}
	if resolved.Target.ModelNumber != "REX-A10VSO-140" {
		t.Errorf("target model number = %q, want %q", resolved.Target.ModelNumber, "REX-A10VSO-140")
	}
	if !resolved.Target.InstallationDate.Equal(installed) {
		t.Errorf("installation date = %s, want %s", resolved.Target.InstallationDate, installed)
	}
	if got, want := len(resolved.Upstream), 2; got != want {
		t.Fatalf("upstream count = %d, want %d", got, want)
	}
	if resolved.Upstream[1].AssetID != "XFMR-L4-01" {
		t.Errorf("camelCase upstream id = %q, want %q", resolved.Upstream[1].AssetID, "XFMR-L4-01")
	}
	if !resolved.Upstream[0].InstallationDate.IsZero() {
		t.Error("missing installation_date should map to the zero time")
	}
	if got, want := resolved.Downstream[0].CurrentStatus, "DEGRADED"; got != want {
		t.Errorf("downstream status = %q, want %q", got, want)
	}
	if resolved.AssignedOperator == nil {
		t.Fatal("assigned operator is nil, want TECH-8801")
	}
	if !resolved.AssignedOperator.ActiveShift {
		t.Error("operator active shift = false, want true")
	}
	if got, want := resolved.BlastRadius(), 3; got != want {
		t.Errorf("blast radius = %d, want %d", got, want)
	}
}

// TestMapRecordHandlesEmptyOptionalMatches is the panic guard: an isolated
// asset returns empty collections and a nil technician.
func TestMapRecordHandlesEmptyOptionalMatches(t *testing.T) {
	for name, record := range map[string]*neo4j.Record{
		"empty lists": resolutionRecord(
			assetNode("4:asset:1", map[string]any{"id": "CNC-MILL-07"}),
			[]any{}, []any{}, nil,
		),
		"null lists": resolutionRecord(
			assetNode("4:asset:1", map[string]any{"id": "CNC-MILL-07"}),
			nil, nil, nil,
		),
		"nulls inside lists": resolutionRecord(
			assetNode("4:asset:1", map[string]any{"id": "CNC-MILL-07"}),
			[]any{nil}, []any{nil, nil}, nil,
		),
	} {
		t.Run(name, func(t *testing.T) {
			resolved, err := mapRecord(record, discardLogger())
			if err != nil {
				t.Fatalf("mapRecord: %v", err)
			}
			if resolved.Upstream == nil || resolved.Downstream == nil {
				t.Fatal("neighbour slices must be non-nil so callers can range unconditionally")
			}
			if resolved.BlastRadius() != 0 {
				t.Errorf("blast radius = %d, want 0", resolved.BlastRadius())
			}
			if resolved.AssignedOperator != nil {
				t.Errorf("assigned operator = %+v, want nil", resolved.AssignedOperator)
			}
			if resolved.Target.CurrentStatus != StatusUnknown {
				t.Errorf("absent status = %q, want %q", resolved.Target.CurrentStatus, StatusUnknown)
			}
		})
	}
}

func TestMapRecordDropsTargetFromItsOwnBlastRadius(t *testing.T) {
	target := assetNode("4:asset:1", map[string]any{"id": "PUMP-A"})

	// A recirculating loop (PUMP-A -> PUMP-B -> PUMP-A) puts the target back in
	// its own downstream collection, and an unidentifiable node rides along.
	record := resolutionRecord(
		target,
		[]any{},
		[]any{
			assetNode("4:asset:2", map[string]any{"id": "PUMP-B"}),
			target,
			assetNode("4:asset:2", map[string]any{"id": "PUMP-B"}),
			assetNode("4:asset:3", map[string]any{"name": "orphan with no identifier"}),
		},
		nil,
	)

	resolved, err := mapRecord(record, discardLogger())
	if err != nil {
		t.Fatalf("mapRecord: %v", err)
	}
	if got, want := len(resolved.Downstream), 1; got != want {
		t.Fatalf("downstream count = %d, want %d (%+v)", got, want, resolved.Downstream)
	}
	if resolved.Downstream[0].AssetID != "PUMP-B" {
		t.Errorf("downstream asset = %q, want %q", resolved.Downstream[0].AssetID, "PUMP-B")
	}
}

func TestMapRecordRejectsUnusableTarget(t *testing.T) {
	if _, err := mapRecord(resolutionRecord(nil, nil, nil, nil), discardLogger()); !errors.Is(err, ErrAssetNotFound) {
		t.Fatalf("nil target error = %v, want ErrAssetNotFound", err)
	}
	if _, err := mapRecord(resolutionRecord("not-a-node", nil, nil, nil), discardLogger()); err == nil {
		t.Fatal("non-node target mapped without an error")
	}
	unidentified := assetNode("4:asset:1", map[string]any{"name": "no id property"})
	if _, err := mapRecord(resolutionRecord(unidentified, nil, nil, nil), discardLogger()); err == nil {
		t.Fatal("target without an identifier mapped without an error")
	}
}

func TestPropertyCoercion(t *testing.T) {
	instant := time.Date(2021, 11, 5, 8, 30, 0, 0, time.UTC)

	for name, props := range map[string]map[string]any{
		"zoned datetime": {"installation_date": instant},
		"date":           {"installation_date": dbtype.Date(instant)},
		"localdatetime":  {"installation_date": dbtype.LocalDateTime(instant)},
		"rfc3339 string": {"installation_date": "2021-11-05T08:30:00Z"},
		"epoch millis":   {"installation_date": instant.UnixMilli()},
	} {
		t.Run("installation date/"+name, func(t *testing.T) {
			got, ok := timeProp(props, assetInstalledKeys...)
			if !ok {
				t.Fatalf("timeProp(%v) reported no value", props)
			}
			if !got.Equal(instant) {
				t.Errorf("timeProp = %s, want %s", got, instant)
			}
		})
	}

	for name, tc := range map[string]struct {
		props map[string]any
		want  bool
	}{
		"native bool":  {map[string]any{"active_shift": true}, true},
		"plc integer":  {map[string]any{"active_shift": int64(1)}, true},
		"csv word":     {map[string]any{"active_shift": "ON_SHIFT"}, true},
		"parsed false": {map[string]any{"active_shift": "false"}, false},
		"camelCase":    {map[string]any{"activeShift": "no"}, false},
	} {
		t.Run("active shift/"+name, func(t *testing.T) {
			got, ok := boolProp(tc.props, operatorShiftKeys...)
			if !ok {
				t.Fatalf("boolProp(%v) reported no value", tc.props)
			}
			if got != tc.want {
				t.Errorf("boolProp = %t, want %t", got, tc.want)
			}
		})
	}

	if _, ok := timeProp(map[string]any{"installation_date": "not a date"}, assetInstalledKeys...); ok {
		t.Error("timeProp accepted an unparseable string")
	}
	if _, ok := stringProp(map[string]any{"id": "  "}, assetIDKeys...); ok {
		t.Error("stringProp accepted a whitespace-only value")
	}
	if got, _ := stringProp(map[string]any{"id": int64(4711)}, assetIDKeys...); got != "4711" {
		t.Errorf("numeric identifier rendered as %q, want %q", got, "4711")
	}
}

// TestRootCausePeelsDriverWrappers locks down the reason timeouts are
// classified correctly: the driver's wrapper types carry their cause in a field
// and implement no Unwrap, so errors.Is cannot see through them unaided.
func TestRootCausePeelsDriverWrappers(t *testing.T) {
	wrapped := &neo4j.ConnectivityError{Inner: context.DeadlineExceeded}
	if errors.Is(wrapped, context.DeadlineExceeded) {
		t.Skip("the driver started implementing Unwrap; rootCause can be simplified")
	}

	for name, err := range map[string]error{
		"connectivity":         wrapped,
		"wrapped connectivity": fmt.Errorf("execute read: %w", wrapped),
		"retry limit": &neo4j.TransactionExecutionLimit{
			Cause:  "timeout",
			Errors: []error{errors.New("attempt 1"), wrapped},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if !errors.Is(rootCause(err), context.DeadlineExceeded) {
				t.Errorf("rootCause(%v) = %v, want context.DeadlineExceeded", err, rootCause(err))
			}
		})
	}

	plain := errors.New("nothing to peel")
	if rootCause(plain) != plain {
		t.Errorf("rootCause returned %v for an unwrapped error, want it unchanged", rootCause(plain))
	}
	if rootCause(nil) != nil {
		t.Error("rootCause(nil) should stay nil")
	}
}

// TestResolveAssetContextGuards covers the checks that run before the driver is
// touched, so they need no cluster.
func TestResolveAssetContextGuards(t *testing.T) {
	live := &Neo4jGraphResolver{log: discardLogger(), timeout: DefaultQueryTimeout, database: DefaultDatabase}
	if _, err := live.ResolveAssetContext(context.Background(), "   "); err == nil {
		t.Error("blank asset id resolved without an error")
	}

	closed := &Neo4jGraphResolver{log: discardLogger(), timeout: DefaultQueryTimeout, closed: true}
	_, err := closed.ResolveAssetContext(context.Background(), "HPP-PUMP-221")
	if !errors.Is(err, ErrResolverClosed) {
		t.Errorf("error after Close = %v, want ErrResolverClosed", err)
	}
}

// TestResolveAssetContextAgainstLiveGraph is the end-to-end path. It documents
// the production construction call and is skipped unless a cluster is pointed at
// it:
//
//	NEO4J_TEST_URI=bolt://localhost:7687 NEO4J_TEST_USER=neo4j \
//	NEO4J_TEST_PASSWORD=... NEO4J_TEST_ASSET=HPP-PUMP-221 go test ./internal/graph -run Live -v
func TestResolveAssetContextAgainstLiveGraph(t *testing.T) {
	uri, assetID := os.Getenv("NEO4J_TEST_URI"), os.Getenv("NEO4J_TEST_ASSET")
	if uri == "" || assetID == "" {
		t.Skip("set NEO4J_TEST_URI and NEO4J_TEST_ASSET (plus NEO4J_TEST_USER/PASSWORD) to run the live test")
	}

	resolver, err := NewNeo4jGraphResolver(
		uri,
		os.Getenv("NEO4J_TEST_USER"),
		os.Getenv("NEO4J_TEST_PASSWORD"),
		WithLogger(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))),
		WithDatabase(os.Getenv("NEO4J_TEST_DATABASE")),
		WithQueryTimeout(DefaultQueryTimeout),
		WithMaxConnectionPoolSize(16),
		WithMaxConnectionLifetime(55*time.Minute),
	)
	if err != nil {
		t.Fatalf("NewNeo4jGraphResolver: %v", err)
	}
	t.Cleanup(func() {
		if err := resolver.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
		// Close is idempotent, so shutdown paths may call it twice.
		if err := resolver.Close(); err != nil {
			t.Errorf("second Close: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resolved, err := resolver.ResolveAssetContext(ctx, assetID)
	switch {
	case errors.Is(err, ErrAssetNotFound):
		t.Fatalf("asset %q is not present in the test graph", assetID)
	case err != nil:
		t.Fatalf("ResolveAssetContext: %v", err)
	}

	if resolved.Target.AssetID != assetID {
		t.Errorf("target asset id = %q, want %q", resolved.Target.AssetID, assetID)
	}
	if resolved.ResolutionLatency > DefaultQueryTimeout {
		t.Errorf("resolution latency %s exceeded the %s budget", resolved.ResolutionLatency, DefaultQueryTimeout)
	}
	t.Logf("blast radius %d (upstream %d, downstream %d) in %s",
		resolved.BlastRadius(), len(resolved.Upstream), len(resolved.Downstream), resolved.ResolutionLatency)

	if stats := resolver.Stats(); stats.Resolutions != 1 || stats.Failures != 0 {
		t.Errorf("stats = %+v, want 1 resolution and 0 failures", stats)
	}
}
