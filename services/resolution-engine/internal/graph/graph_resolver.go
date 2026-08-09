// Package graph resolves an industrial asset's cyber-physical blast radius out
// of the OpenOntology Neo4j topology graph.
//
// One resolver is constructed at process start, shared by every ingestion
// worker and closed on shutdown. It is safe for concurrent use: the underlying
// Neo4j driver multiplexes a bounded connection pool, and each resolution runs
// in its own managed read transaction under a hard budget (3 seconds by
// default) so that a pathological traversal can never stall the high-velocity
// ingestion path.
package graph

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	neo4jcfg "github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	neo4jlog "github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
)

// ---------------------------------------------------------------------------
// Contract
// ---------------------------------------------------------------------------

// GraphResolver is the contract the rest of the application depends on. Both
// the live Neo4j implementation and any in-memory stand-in satisfy it, so the
// graph tier can be swapped without touching a caller.
type GraphResolver interface {
	ResolveAssetContext(ctx context.Context, assetID string) (*EnrichedBlastRadiusContext, error)
	Close() error
}

// Compile-time proof that the live resolver honours the contract.
var _ GraphResolver = (*Neo4jGraphResolver)(nil)

var (
	// ErrAssetNotFound reports that no (:Asset {id: ...}) node exists in the
	// topology. This is a data condition rather than a failure — callers are
	// expected to degrade to an unenriched payload rather than retry.
	ErrAssetNotFound = errors.New("asset not found in ontology graph")

	// ErrResolverClosed is returned when ResolveAssetContext is called after
	// Close has already released the driver.
	ErrResolverClosed = errors.New("graph resolver is closed")

	// ErrResolutionTimeout marks a traversal that outran its budget. It is
	// joined with context.DeadlineExceeded, so either sentinel matches.
	ErrResolutionTimeout = errors.New("graph resolution exceeded its budget")
)

// ---------------------------------------------------------------------------
// Domain model
// ---------------------------------------------------------------------------

// StatusUnknown is recorded when a node carries no status property, so
// downstream consumers never have to tell "" apart from "absent".
const StatusUnknown = "UNKNOWN"

// AssetNode is one piece of plant equipment as modelled in the topology graph.
type AssetNode struct {
	AssetID          string    `json:"asset_id"`
	Name             string    `json:"name,omitempty"`
	ModelNumber      string    `json:"model_number,omitempty"`
	CurrentStatus    string    `json:"current_status"`
	InstallationDate time.Time `json:"installation_date,omitempty"`

	// ElementID is Neo4j's own node identity. It is carried for provenance and
	// for de-duplicating nodes reached through more than one path.
	ElementID string `json:"element_id,omitempty"`
}

// OperatorNode is the human currently accountable for an asset.
type OperatorNode struct {
	TechnicianID       string `json:"technician_id"`
	Name               string `json:"name,omitempty"`
	CertificationLevel string `json:"certification_level,omitempty"`
	ActiveShift        bool   `json:"active_shift"`

	ElementID string `json:"element_id,omitempty"`
}

// EnrichedBlastRadiusContext is the structural subtree around a target asset:
// what feeds it, what it would take down, and who owns it right now.
//
// Upstream and Downstream are always non-nil, so consumers can range over them
// unconditionally and JSON encodes them as [] rather than null.
type EnrichedBlastRadiusContext struct {
	// Target is the asset the resolution was requested for.
	Target AssetNode `json:"target"`

	// Upstream holds assets that feed the target (incoming :FEEDS, 1..3 hops).
	// Losing one of these degrades the target.
	Upstream []AssetNode `json:"upstream_dependencies"`

	// Downstream holds assets the target feeds or controls (outgoing
	// :FEEDS|CONTROLS, 1..3 hops) — the physical blast radius of a fault.
	Downstream []AssetNode `json:"downstream_impacted"`

	// AssignedOperator is nil when no :ASSIGNED_TO edge exists.
	AssignedOperator *OperatorNode `json:"assigned_operator,omitempty"`

	ResolvedAt        time.Time     `json:"resolved_at"`
	ResolutionLatency time.Duration `json:"resolution_latency_ns"`
}

// BlastRadius is the number of distinct assets structurally implicated by a
// fault on the target, in either direction.
func (c *EnrichedBlastRadiusContext) BlastRadius() int {
	if c == nil {
		return 0
	}
	return len(c.Upstream) + len(c.Downstream)
}

// LogValue renders the context compactly for structured logging, so a resolved
// subtree never dumps every neighbour node into the log stream.
func (c *EnrichedBlastRadiusContext) LogValue() slog.Value {
	if c == nil {
		return slog.StringValue("<nil>")
	}
	operator := "unassigned"
	if c.AssignedOperator != nil {
		operator = c.AssignedOperator.TechnicianID
	}
	return slog.GroupValue(
		slog.String("asset_id", c.Target.AssetID),
		slog.String("status", c.Target.CurrentStatus),
		slog.Int("upstream", len(c.Upstream)),
		slog.Int("downstream", len(c.Downstream)),
		slog.String("operator", operator),
		slog.Duration("latency", c.ResolutionLatency),
	)
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

// CypherResolveAssetContext extracts the whole structural subtree in one round
// trip. The traversals are bounded at three hops: beyond that the "blast
// radius" stops being actionable and the cost of the expansion stops being
// predictable, which matters because this runs on the ingestion hot path.
const CypherResolveAssetContext = `
MATCH (a:Asset {id: $assetId})
OPTIONAL MATCH (p:Asset)-[:FEEDS*1..3]->(a)
OPTIONAL MATCH (a)-[:FEEDS|CONTROLS*1..3]->(d:Asset)
OPTIONAL MATCH (a)-[:ASSIGNED_TO]->(m:Operator)
RETURN a AS target, collect(distinct p) AS upstream, collect(distinct d) AS downstream, m AS technician
`

// Result columns, named once so the query and the mapper cannot drift apart.
const (
	columnTarget     = "target"
	columnUpstream   = "upstream"
	columnDownstream = "downstream"
	columnTechnician = "technician"
)

// Property keys, canonical form first. The ingest tier writes snake_case; a
// handful of legacy CSV loaders still emit camelCase, and rejecting those nodes
// outright would silently shrink a blast radius — the one failure mode this
// module exists to prevent.
var (
	assetIDKeys        = []string{"id", "asset_id", "assetId"}
	assetNameKeys      = []string{"name", "display_name"}
	assetModelKeys     = []string{"model_number", "modelNumber"}
	assetStatusKeys    = []string{"current_status", "status"}
	assetInstalledKeys = []string{"installation_date", "installationDate", "installed_at"}

	operatorIDKeys    = []string{"technician_id", "technicianId", "id"}
	operatorNameKeys  = []string{"name", "display_name"}
	operatorCertKeys  = []string{"certification_level", "certificationLevel"}
	operatorShiftKeys = []string{"active_shift", "activeShift", "on_shift"}

	acceptedTimeFormats = []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// Tuning defaults. These are deliberately conservative for a service that
// resolves graph context inline with telemetry ingestion.
const (
	// DefaultQueryTimeout is the hard ceiling on a single resolution. A slow
	// traversal is failed rather than allowed to back up the ingestion thread.
	DefaultQueryTimeout = 3 * time.Second

	// DefaultDatabase is Neo4j's default database name.
	DefaultDatabase = "neo4j"

	defaultMaxConnectionPoolSize = 64
	defaultMaxConnectionLifetime = 55 * time.Minute
	defaultAcquisitionTimeout    = 5 * time.Second
	defaultSocketConnectTimeout  = 5 * time.Second
	defaultConnectivityTimeout   = 5 * time.Second
	defaultUserAgent             = "openontology-resolution-engine/1.0 (neo4j-go-driver/v5)"

	// maxTxRetryRatio keeps the driver's own retry loop strictly inside the
	// query budget. Retrying past the deadline only burns pool capacity.
	maxTxRetryRatio = 2

	sessionCloseTimeout = 2 * time.Second
	driverCloseTimeout  = 5 * time.Second
)

// supportedSchemes are the URI schemes the Bolt driver understands. Validating
// up front turns a confusing driver-internal error into an actionable one.
var supportedSchemes = map[string]struct{}{
	"neo4j": {}, "neo4j+s": {}, "neo4j+ssc": {},
	"bolt": {}, "bolt+s": {}, "bolt+ssc": {},
}

type settings struct {
	logger               *slog.Logger
	database             string
	queryTimeout         time.Duration
	maxPoolSize          int
	maxConnLifetime      time.Duration
	acquisitionTimeout   time.Duration
	socketConnectTimeout time.Duration
	connectivityTimeout  time.Duration
	userAgent            string
}

func defaultSettings() settings {
	return settings{
		logger:               slog.Default(),
		database:             DefaultDatabase,
		queryTimeout:         DefaultQueryTimeout,
		maxPoolSize:          defaultMaxConnectionPoolSize,
		maxConnLifetime:      defaultMaxConnectionLifetime,
		acquisitionTimeout:   defaultAcquisitionTimeout,
		socketConnectTimeout: defaultSocketConnectTimeout,
		connectivityTimeout:  defaultConnectivityTimeout,
		userAgent:            defaultUserAgent,
	}
}

// Option customises a resolver at construction time. The zero-option form,
// NewNeo4jGraphResolver(uri, user, pass), yields the production defaults above.
type Option func(*settings)

// WithLogger installs the structured logger. Defaults to slog.Default().
func WithLogger(logger *slog.Logger) Option {
	return func(s *settings) {
		if logger != nil {
			s.logger = logger
		}
	}
}

// WithDatabase selects the Neo4j database to read from.
func WithDatabase(name string) Option {
	return func(s *settings) {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			s.database = trimmed
		}
	}
}

// WithQueryTimeout overrides the per-resolution budget.
func WithQueryTimeout(d time.Duration) Option {
	return func(s *settings) {
		if d > 0 {
			s.queryTimeout = d
		}
	}
}

// WithMaxConnectionPoolSize bounds concurrent Bolt connections. Size it to the
// engine's worker count plus headroom for the admin endpoints.
func WithMaxConnectionPoolSize(n int) Option {
	return func(s *settings) {
		if n > 0 {
			s.maxPoolSize = n
		}
	}
}

// WithMaxConnectionLifetime bounds how long a pooled connection is reused.
// Keep it under any load balancer or proxy idle timeout in front of the cluster.
func WithMaxConnectionLifetime(d time.Duration) Option {
	return func(s *settings) {
		if d > 0 {
			s.maxConnLifetime = d
		}
	}
}

// WithConnectivityTimeout bounds the startup handshake performed by
// NewNeo4jGraphResolver, so a wedged cluster fails the boot fast.
func WithConnectivityTimeout(d time.Duration) Option {
	return func(s *settings) {
		if d > 0 {
			s.connectivityTimeout = d
		}
	}
}

// WithUserAgent overrides the client identifier reported to the server.
func WithUserAgent(agent string) Option {
	return func(s *settings) {
		if trimmed := strings.TrimSpace(agent); trimmed != "" {
			s.userAgent = trimmed
		}
	}
}

// ---------------------------------------------------------------------------
// Resolver
// ---------------------------------------------------------------------------

// Stats are cumulative resolver counters for the metrics endpoint.
type Stats struct {
	Resolutions uint64 `json:"resolutions"`
	NotFound    uint64 `json:"not_found"`
	Failures    uint64 `json:"failures"`
	Timeouts    uint64 `json:"timeouts"`
}

// Neo4jGraphResolver is the live, pooled implementation of GraphResolver.
// A single value is safe to share across every ingestion worker.
type Neo4jGraphResolver struct {
	driver   neo4j.DriverWithContext
	database string
	timeout  time.Duration
	log      *slog.Logger

	// mu guards closed only. It is never held across a driver call, so Close
	// racing an in-flight resolution cannot deadlock.
	mu     sync.RWMutex
	closed bool

	resolutions atomic.Uint64
	notFound    atomic.Uint64
	failures    atomic.Uint64
	timeouts    atomic.Uint64
}

// NewNeo4jGraphResolver builds a pooled resolver and proves the cluster is
// reachable before returning. A failed handshake closes the driver rather than
// leaking its background goroutines.
//
// username may be empty for clusters running with authentication disabled, in
// which case the driver connects unauthenticated and the choice is logged.
func NewNeo4jGraphResolver(uri, username, password string, opts ...Option) (*Neo4jGraphResolver, error) {
	cfg := defaultSettings()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	uri = strings.TrimSpace(uri)
	if err := validateURI(uri); err != nil {
		return nil, err
	}

	username = strings.TrimSpace(username)
	logger := cfg.logger.With("component", "graph-resolver", "neo4j_uri", uri, "database", cfg.database)

	token := neo4j.BasicAuth(username, password, "")
	if username == "" {
		logger.Warn("neo4j username is empty; connecting without authentication")
		token = neo4j.NoAuth()
	}

	driver, err := neo4j.NewDriverWithContext(uri, token, func(c *neo4jcfg.Config) {
		c.MaxConnectionPoolSize = cfg.maxPoolSize
		c.MaxConnectionLifetime = cfg.maxConnLifetime
		c.ConnectionAcquisitionTimeout = cfg.acquisitionTimeout
		c.SocketConnectTimeout = cfg.socketConnectTimeout
		c.SocketKeepalive = true
		// Retries must finish inside the per-query budget; a retry that starts
		// after the deadline only holds a pooled connection hostage.
		c.MaxTransactionRetryTime = cfg.queryTimeout / maxTxRetryRatio
		// The resolution returns a single row, so streaming buys nothing and
		// fetching in one batch keeps the connection checked out for less time.
		c.FetchSize = neo4j.FetchAll
		c.UserAgent = cfg.userAgent
		c.Log = &driverLogBridge{log: logger.With("component", "neo4j-driver")}
	})
	if err != nil {
		return nil, fmt.Errorf("construct neo4j driver for %s: %w", uri, err)
	}

	verifyCtx, cancel := context.WithTimeout(context.Background(), cfg.connectivityTimeout)
	defer cancel()

	if err := driver.VerifyConnectivity(verifyCtx); err != nil {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), driverCloseTimeout)
		defer closeCancel()
		if closeErr := driver.Close(closeCtx); closeErr != nil {
			logger.Warn("closing neo4j driver after a failed handshake also failed", "error", closeErr)
		}
		return nil, fmt.Errorf("verify neo4j connectivity at %s within %s: %w", uri, cfg.connectivityTimeout, err)
	}

	logger.Info("neo4j graph resolver ready",
		"max_pool_size", cfg.maxPoolSize,
		"max_connection_lifetime", cfg.maxConnLifetime,
		"query_timeout", cfg.queryTimeout,
		"authenticated", username != "")

	return &Neo4jGraphResolver{
		driver:   driver,
		database: cfg.database,
		timeout:  cfg.queryTimeout,
		log:      logger,
	}, nil
}

// validateURI rejects a malformed or unsupported target before the driver is
// built, so operators get a message naming the schemes that actually work.
func validateURI(uri string) error {
	if uri == "" {
		return errors.New("neo4j uri must not be empty")
	}
	parsed, err := url.Parse(uri)
	if err != nil {
		return fmt.Errorf("parse neo4j uri %q: %w", uri, err)
	}
	scheme := strings.ToLower(parsed.Scheme)
	if _, ok := supportedSchemes[scheme]; !ok {
		return fmt.Errorf("neo4j uri %q uses unsupported scheme %q; expected one of neo4j, neo4j+s, neo4j+ssc, bolt, bolt+s, bolt+ssc", uri, parsed.Scheme)
	}
	if parsed.Host == "" {
		return fmt.Errorf("neo4j uri %q has no host", uri)
	}
	return nil
}

// Close releases the driver and its pool. It is idempotent and safe to call
// concurrently with in-flight resolutions, which will fail with a driver error
// rather than block shutdown.
func (r *Neo4jGraphResolver) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.mu.Unlock()

	stats := r.Stats()
	ctx, cancel := context.WithTimeout(context.Background(), driverCloseTimeout)
	defer cancel()

	if err := r.driver.Close(ctx); err != nil {
		r.log.Error("closing neo4j driver failed", "error", err)
		return fmt.Errorf("close neo4j driver: %w", err)
	}

	r.log.Info("neo4j graph resolver closed",
		"resolutions", stats.Resolutions,
		"not_found", stats.NotFound,
		"failures", stats.Failures,
		"timeouts", stats.Timeouts)
	return nil
}

// Stats returns a snapshot of the cumulative counters.
func (r *Neo4jGraphResolver) Stats() Stats {
	return Stats{
		Resolutions: r.resolutions.Load(),
		NotFound:    r.notFound.Load(),
		Failures:    r.failures.Load(),
		Timeouts:    r.timeouts.Load(),
	}
}

// ResolveAssetContext returns the blast-radius subtree around assetID.
//
// The incoming context is narrowed to the resolver's budget, so a caller that
// passes context.Background() still cannot pin an ingestion worker to a slow
// traversal. ErrAssetNotFound is returned — and counted separately from
// failures — when the asset is simply absent from the topology.
func (r *Neo4jGraphResolver) ResolveAssetContext(ctx context.Context, assetID string) (*EnrichedBlastRadiusContext, error) {
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
	log := r.log.With("asset_id", assetID)

	// Hard budget. Narrowing rather than replacing the caller's context keeps
	// an upstream cancellation (shutdown, consumer rebalance) effective.
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
		return readBlastRadius(queryCtx, tx, assetID, log)
	}, neo4j.WithTxTimeout(r.timeout))
	if err != nil {
		return nil, r.classify(ctx, queryCtx, err, assetID, started, log)
	}

	resolved, ok := result.(*EnrichedBlastRadiusContext)
	if !ok || resolved == nil {
		r.failures.Add(1)
		log.Error("read transaction returned an unexpected value", "type", fmt.Sprintf("%T", result))
		return nil, fmt.Errorf("resolve asset %q: read transaction returned %T, want *EnrichedBlastRadiusContext", assetID, result)
	}

	resolved.ResolvedAt = time.Now().UTC()
	resolved.ResolutionLatency = time.Since(started)

	log.Debug("resolved asset blast radius", "context", resolved)
	return resolved, nil
}

// classify turns a transaction failure into a typed, counted, logged error.
//
// callerCtx is the context handed in by the caller and queryCtx is the narrowed
// one this resolution ran under; comparing the two is what separates "the
// ingestion worker went away" from "our own budget fired", and it is authoritative
// where error inspection is not.
func (r *Neo4jGraphResolver) classify(
	callerCtx, queryCtx context.Context,
	err error,
	assetID string,
	started time.Time,
	log *slog.Logger,
) error {
	elapsed := time.Since(started)
	cause := rootCause(err)

	switch {
	case errors.Is(cause, ErrAssetNotFound):
		r.notFound.Add(1)
		log.Warn("asset absent from ontology graph", "elapsed", elapsed)
		return fmt.Errorf("resolve asset %q: %w", assetID, ErrAssetNotFound)

	case callerCtx.Err() != nil:
		// The caller gave up first — shutdown, consumer rebalance, or a deadline
		// tighter than ours. That is not a resolver fault, so it stays at warn.
		r.failures.Add(1)
		log.Warn("graph resolution abandoned by the caller",
			"elapsed", elapsed, "cause", callerCtx.Err())
		return fmt.Errorf("resolve asset %q: %w", assetID, callerCtx.Err())

	case errors.Is(queryCtx.Err(), context.DeadlineExceeded) || errors.Is(cause, context.DeadlineExceeded):
		r.timeouts.Add(1)
		r.failures.Add(1)
		log.Error("graph traversal exceeded its resolution budget",
			"budget", r.timeout, "elapsed", elapsed, "error", err)
		// Both sentinels are matchable by callers; the driver's own wording is
		// on the log line rather than in the returned message.
		return fmt.Errorf("resolve asset %q: %w of %s: %w",
			assetID, ErrResolutionTimeout, r.timeout, context.DeadlineExceeded)

	case errors.Is(cause, context.Canceled):
		r.failures.Add(1)
		log.Warn("graph resolution cancelled", "elapsed", elapsed)
		return fmt.Errorf("resolve asset %q: %w", assetID, context.Canceled)
	}

	r.failures.Add(1)

	// A server-side error carries a Neo4j status code; surfacing it makes the
	// difference between "bad Cypher" and "cluster unavailable" greppable.
	var dbErr *neo4j.Neo4jError
	if errors.As(err, &dbErr) || errors.As(cause, &dbErr) {
		log.Error("neo4j rejected the resolution query",
			"neo4j_code", dbErr.Code, "neo4j_message", dbErr.Msg, "elapsed", elapsed)
	} else {
		log.Error("graph resolution failed", "elapsed", elapsed, "error", err)
	}
	return fmt.Errorf("resolve asset %q: %w", assetID, err)
}

// maxCausePeelDepth bounds rootCause so a self-referential wrapper cannot spin.
const maxCausePeelDepth = 8

// rootCause peels the driver's wrapper types down to the underlying failure.
// Neither ConnectivityError nor TransactionExecutionLimit implements Unwrap, so
// without this a context deadline or a Neo4j status code is invisible to
// errors.Is and errors.As and every timeout would be misfiled as a generic
// failure.
func rootCause(err error) error {
	for i := 0; err != nil && i < maxCausePeelDepth; i++ {
		var connErr *neo4j.ConnectivityError
		var retryErr *neo4j.TransactionExecutionLimit

		switch {
		case errors.As(err, &connErr) && connErr.Inner != nil:
			err = connErr.Inner
		case errors.As(err, &retryErr) && len(retryErr.Errors) > 0:
			// The last attempt is the one that decided the outcome.
			err = retryErr.Errors[len(retryErr.Errors)-1]
		default:
			return err
		}
	}
	return err
}

// readBlastRadius runs the resolution query and materialises the result inside
// the managed transaction, so nothing streams back out of a closed scope.
func readBlastRadius(
	ctx context.Context,
	tx neo4j.ManagedTransaction,
	assetID string,
	log *slog.Logger,
) (*EnrichedBlastRadiusContext, error) {
	result, err := tx.Run(ctx, CypherResolveAssetContext, map[string]any{"assetId": assetID})
	if err != nil {
		return nil, fmt.Errorf("run resolution query: %w", err)
	}

	if !result.Next(ctx) {
		if err := result.Err(); err != nil {
			return nil, fmt.Errorf("read resolution record: %w", err)
		}
		// The leading MATCH is not optional, so zero rows means zero assets.
		return nil, ErrAssetNotFound
	}

	resolved, err := mapRecord(result.Record(), log)
	if err != nil {
		return nil, err
	}

	// The query groups on (a, m), so extra rows appear only when an asset
	// carries several :ASSIGNED_TO edges. Upstream and downstream are identical
	// across those rows; drain them so the connection returns to the pool
	// cleanly and account for what was discarded.
	extra := 0
	for result.Next(ctx) {
		extra++
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("drain resolution result: %w", err)
	}
	if extra > 0 {
		log.Warn("asset has multiple assigned operators; keeping the first",
			"additional_operators", extra)
	}

	return resolved, nil
}

// ---------------------------------------------------------------------------
// Record mapping
// ---------------------------------------------------------------------------

// mapRecord unwraps one driver record into the domain model. Every OPTIONAL
// MATCH column may legitimately arrive as nil or as an empty list, and a
// malformed neighbour must never take down an otherwise usable resolution.
func mapRecord(record *neo4j.Record, log *slog.Logger) (*EnrichedBlastRadiusContext, error) {
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

	resolved := &EnrichedBlastRadiusContext{
		Target: target,
		// A cyclic topology (a -FEEDS-> b -FEEDS-> a) can return the target
		// inside its own neighbour list; excluding it keeps the blast radius
		// honest.
		Upstream:   mapAssetNodes(record, columnUpstream, target.ElementID, log),
		Downstream: mapAssetNodes(record, columnDownstream, target.ElementID, log),
	}

	operatorRaw, ok := record.Get(columnTechnician)
	if !ok || operatorRaw == nil {
		// No :ASSIGNED_TO edge. Unowned equipment is normal, not an error.
		return resolved, nil
	}
	operatorNode, ok := asNode(operatorRaw)
	if !ok {
		log.Warn("technician column is not a graph node; leaving the asset unassigned",
			"column", columnTechnician, "type", fmt.Sprintf("%T", operatorRaw))
		return resolved, nil
	}
	operator, err := mapOperatorNode(operatorNode)
	if err != nil {
		log.Warn("assigned operator could not be mapped; leaving the asset unassigned",
			"element_id", operatorNode.ElementId, "error", err)
		return resolved, nil
	}
	resolved.AssignedOperator = &operator

	return resolved, nil
}

// mapAssetNodes unwraps a collect() column. It always returns a non-nil slice,
// skips anything unusable, and de-duplicates by element id because a node
// reachable at two different path lengths can survive `collect(distinct ...)`
// only once but can still repeat across the two directional columns.
func mapAssetNodes(record *neo4j.Record, column, excludeElementID string, log *slog.Logger) []AssetNode {
	raw, ok := record.Get(column)
	if !ok || raw == nil {
		return []AssetNode{}
	}

	list, ok := raw.([]any)
	if !ok {
		log.Warn("collected column is not a list; treating it as empty",
			"column", column, "type", fmt.Sprintf("%T", raw))
		return []AssetNode{}
	}

	assets := make([]AssetNode, 0, len(list))
	seen := make(map[string]struct{}, len(list))
	skipped := 0

	for _, item := range list {
		if item == nil {
			// OPTIONAL MATCH misses collapse to nulls inside the list.
			continue
		}
		node, ok := asNode(item)
		if !ok {
			skipped++
			continue
		}
		if node.ElementId != "" {
			if node.ElementId == excludeElementID {
				continue
			}
			if _, duplicate := seen[node.ElementId]; duplicate {
				continue
			}
		}
		asset, err := mapAssetNode(node)
		if err != nil {
			skipped++
			log.Warn("skipping unmappable neighbour node",
				"column", column, "element_id", node.ElementId, "error", err)
			continue
		}
		if node.ElementId != "" {
			seen[node.ElementId] = struct{}{}
		}
		assets = append(assets, asset)
	}

	if skipped > 0 {
		log.Warn("neighbour nodes dropped during mapping", "column", column, "skipped", skipped)
	}
	return assets
}

// mapAssetNode projects a graph node onto AssetNode. A node without an
// identifier cannot be correlated with telemetry, so it is an error rather than
// a partially-populated struct.
func mapAssetNode(node neo4j.Node) (AssetNode, error) {
	id, ok := stringProp(node.Props, assetIDKeys...)
	if !ok {
		return AssetNode{}, fmt.Errorf("node %s with labels %v has none of the identifier properties %v",
			node.ElementId, node.Labels, assetIDKeys)
	}

	asset := AssetNode{
		AssetID:       id,
		ElementID:     node.ElementId,
		CurrentStatus: StatusUnknown,
	}
	asset.Name, _ = stringProp(node.Props, assetNameKeys...)
	asset.ModelNumber, _ = stringProp(node.Props, assetModelKeys...)
	if status, ok := stringProp(node.Props, assetStatusKeys...); ok {
		asset.CurrentStatus = status
	}
	asset.InstallationDate, _ = timeProp(node.Props, assetInstalledKeys...)

	return asset, nil
}

// mapOperatorNode projects a graph node onto OperatorNode.
func mapOperatorNode(node neo4j.Node) (OperatorNode, error) {
	id, ok := stringProp(node.Props, operatorIDKeys...)
	if !ok {
		return OperatorNode{}, fmt.Errorf("operator node %s with labels %v has none of the identifier properties %v",
			node.ElementId, node.Labels, operatorIDKeys)
	}

	operator := OperatorNode{
		TechnicianID: id,
		ElementID:    node.ElementId,
	}
	operator.Name, _ = stringProp(node.Props, operatorNameKeys...)
	operator.CertificationLevel, _ = stringProp(node.Props, operatorCertKeys...)
	operator.ActiveShift, _ = boolProp(node.Props, operatorShiftKeys...)

	return operator, nil
}

// asNode narrows a driver value to a node. The driver hands back dbtype.Node by
// value; the pointer case is accepted so hand-built fixtures also work.
func asNode(raw any) (neo4j.Node, bool) {
	switch v := raw.(type) {
	case neo4j.Node:
		return v, true
	case *neo4j.Node:
		if v == nil {
			return neo4j.Node{}, false
		}
		return *v, true
	default:
		return neo4j.Node{}, false
	}
}

// stringProp returns the first non-empty value among keys, coercing the scalar
// types Bolt can deliver. Numeric identifiers are common in equipment
// registries exported from historians, so they are rendered rather than
// rejected.
func stringProp(props map[string]any, keys ...string) (string, bool) {
	for _, key := range keys {
		raw, ok := props[key]
		if !ok || raw == nil {
			continue
		}
		switch v := raw.(type) {
		case string:
			if trimmed := strings.TrimSpace(v); trimmed != "" {
				return trimmed, true
			}
		case int64:
			return strconv.FormatInt(v, 10), true
		case float64:
			return strconv.FormatFloat(v, 'f', -1, 64), true
		case bool:
			return strconv.FormatBool(v), true
		}
	}
	return "", false
}

// boolProp resolves a flag that field loaders spell inconsistently: a real
// boolean from the ingest tier, 0/1 from PLC exports, and words from CSV.
func boolProp(props map[string]any, keys ...string) (bool, bool) {
	for _, key := range keys {
		raw, ok := props[key]
		if !ok || raw == nil {
			continue
		}
		switch v := raw.(type) {
		case bool:
			return v, true
		case int64:
			return v != 0, true
		case float64:
			return v != 0, true
		case string:
			trimmed := strings.TrimSpace(v)
			if parsed, err := strconv.ParseBool(trimmed); err == nil {
				return parsed, true
			}
			switch strings.ToUpper(trimmed) {
			case "Y", "YES", "ACTIVE", "ON_SHIFT", "ON-SHIFT":
				return true, true
			case "N", "NO", "INACTIVE", "OFF_SHIFT", "OFF-SHIFT":
				return false, true
			}
		}
	}
	return false, false
}

// timeProp resolves an instant from any of the temporal shapes Bolt returns —
// a zoned datetime arrives as time.Time, date and localdatetime as their
// dbtype wrappers — plus the ISO strings and epoch milliseconds that
// string-typed legacy properties still carry. The result is always UTC.
func timeProp(props map[string]any, keys ...string) (time.Time, bool) {
	for _, key := range keys {
		raw, ok := props[key]
		if !ok || raw == nil {
			continue
		}
		switch v := raw.(type) {
		case time.Time:
			return v.UTC(), true
		case dbtype.Date:
			return v.Time().UTC(), true
		case dbtype.LocalDateTime:
			return v.Time().UTC(), true
		case int64:
			return time.UnixMilli(v).UTC(), true
		case string:
			trimmed := strings.TrimSpace(v)
			if trimmed == "" {
				continue
			}
			for _, layout := range acceptedTimeFormats {
				if parsed, err := time.Parse(layout, trimmed); err == nil {
					return parsed.UTC(), true
				}
			}
		}
	}
	return time.Time{}, false
}

// ---------------------------------------------------------------------------
// Driver logging
// ---------------------------------------------------------------------------

// driverLogBridge forwards the Bolt driver's internal logging into slog, so the
// connection pool and retry machinery appear in the same structured stream as
// everything else the service emits.
type driverLogBridge struct {
	log *slog.Logger
}

var _ neo4jlog.Logger = (*driverLogBridge)(nil)

func (b *driverLogBridge) Error(name, id string, err error) {
	b.log.Error("neo4j driver error", "driver_component", name, "driver_id", id, "error", err)
}

func (b *driverLogBridge) Warnf(name, id, msg string, args ...any) {
	b.log.Warn(fmt.Sprintf(msg, args...), "driver_component", name, "driver_id", id)
}

func (b *driverLogBridge) Infof(name, id, msg string, args ...any) {
	if !b.log.Enabled(context.Background(), slog.LevelInfo) {
		return
	}
	b.log.Info(fmt.Sprintf(msg, args...), "driver_component", name, "driver_id", id)
}

func (b *driverLogBridge) Debugf(name, id, msg string, args ...any) {
	// The driver logs per-message at debug level; skipping the format call
	// keeps that free when debug logging is off, which is the hot-path case.
	if !b.log.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	b.log.Debug(fmt.Sprintf(msg, args...), "driver_component", name, "driver_id", id)
}
