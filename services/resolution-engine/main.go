// Command resolver is the OpenOntology Ontology Resolution Engine.
//
// It consumes multi-variable telemetry from Kafka, maintains the live digital
// twin state in Redis, evaluates the anomaly rules, resolves the graph context
// of any asset that breaches one, and publishes an Enriched Context Payload to
// the ontology.mutations topic for downstream consumers — including the
// commercial AI-agent interceptor.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// buildVersion is injected at build time via -ldflags "-X main.buildVersion=...".
var buildVersion = "dev"

func main() {
	if err := run(); err != nil {
		slog.Error("resolution engine terminated", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := LoadConfig()
	if err != nil {
		// The logger is not configured yet, so write plainly to stderr.
		fmt.Fprintf(os.Stderr, "invalid configuration:\n%v\n", err)
		return err
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	logger = logger.With("service", Producer, "version", buildVersion)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("starting ontology resolution engine",
		"brokers", cfg.KafkaBrokers,
		"source_topic", cfg.SourceTopic,
		"mutation_topic", cfg.MutationTopic,
		"dlq_topic", cfg.DLQTopic,
		"redis", cfg.RedisAddr,
		"workers", cfg.Workers,
		"vibration_limit", cfg.Thresholds[SensorVibrationIndex].Limit,
		"temperature_limit", cfg.Thresholds[SensorTemperatureCelsius].Limit)

	cache, err := NewStateCache(ctx, cfg, logger)
	if err != nil {
		return fmt.Errorf("state cache: %w", err)
	}
	defer func() {
		if err := cache.Close(); err != nil {
			logger.Warn("closing redis client failed", "error", err)
		}
	}()

	idempotencyCfg, err := LoadIdempotencyConfig(cfg.OpTimeout)
	if err != nil {
		return fmt.Errorf("idempotency configuration: %w", err)
	}
	dedupe, err := NewIdempotencyEngine(idempotencyCfg, cache.Redis(), logger)
	if err != nil {
		return fmt.Errorf("idempotency filter: %w", err)
	}
	defer func() {
		if err := dedupe.Close(); err != nil {
			logger.Warn("closing idempotency filter failed", "error", err)
		}
	}()
	logger.Info("distributed idempotency filter configured",
		"enabled", idempotencyCfg.Enabled,
		"scope", string(idempotencyCfg.Scope),
		"window", idempotencyCfg.TTL.String(),
		"key_prefix", idempotencyCfg.KeyPrefix,
		"fail_open", idempotencyCfg.FailOpen)

	metrics := NewMetrics()

	graph, err := newGraphResolver(cfg, logger)
	if err != nil {
		return fmt.Errorf("graph resolver: %w", err)
	}
	defer func() {
		if err := graph.Close(); err != nil {
			logger.Warn("closing graph resolver failed", "error", err)
		}
	}()
	logger.Info("graph tier configured",
		"provider", graph.Provider(),
		"cache_ttl", cfg.GraphCacheTTL.String(),
		"query_budget", cfg.GraphQueryBudget.String())

	tracker := NewStateTracker(cfg.ReAlertInterval, cfg.HysteresisRatio)

	replicaCfg, err := LoadReplicaConfig()
	if err != nil {
		logger.Error("invalid replication configuration", "error", err)
		return err
	}
	replica := NewTopologyReplica(replicaCfg, logger)
	if replicaCfg.Enabled {
		logger.Info("topology replication enabled",
			"replica_id", replicaCfg.ID,
			"peers", replicaCfg.Peers,
			"sync_interval", replicaCfg.SyncInterval.String(),
			"reconcile_budget", replicaCfg.ReconcileBudget.String())
	}

	engine := NewEngine(cfg, logger, cache, graph, tracker, dedupe, metrics, replica)

	server := newAdminServer(cfg, cache, tracker, graph, dedupe, metrics, replica, logger)
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("admin endpoints listening", "addr", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	var wg sync.WaitGroup
	workerErr := make(chan error, cfg.Workers)

	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := engine.RunWorker(ctx, id); err != nil {
				workerErr <- fmt.Errorf("worker %d: %w", id, err)
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		engine.RunStateJanitor(ctx, 10*time.Minute)
	}()

	// Anti-entropy. Runs in the same process as ingestion deliberately: the
	// replica exists to describe what *this* engine has resolved, so a separate
	// sync sidecar would be replicating a graph it never observed.
	wg.Add(1)
	go func() {
		defer wg.Done()
		replica.RunSync(ctx, NewHTTPPeerClient())
	}()

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case runErr = <-serverErr:
		logger.Error("admin server failed", "error", runErr)
		stop()
	case runErr = <-workerErr:
		logger.Error("consumer worker failed", "error", runErr)
		stop()
	}

	// Graceful drain: workers exit as soon as their fetch returns, then the
	// producers are flushed so no enriched payload is lost on the way out.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Warn("admin server shutdown returned an error", "error", err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("all workers stopped")
	case <-shutdownCtx.Done():
		logger.Warn("shutdown deadline exceeded before workers stopped")
	}

	if err := engine.Close(); err != nil {
		logger.Warn("closing kafka producers failed", "error", err)
	}

	logger.Info("resolution engine stopped",
		"consumed", metrics.EventsConsumed.Load(),
		"mutations", metrics.MutationsEmitted.Load(),
		"rejected", metrics.EventsRejected.Load(),
		"duplicates", metrics.EventsDuplicate.Load())
	return runErr
}

// newGraphResolver selects the graph tier named by GRAPH_PROVIDER.
//
// Neither provider can fail the boot. An unreachable Neo4j is a degraded state,
// not a fatal one: an engine that refuses to start because the topology store
// is down drops every alarm, which is the exact failure the degradation path
// exists to prevent. The condition is loud in the log and visible on /stats.
func newGraphResolver(cfg Config, log *slog.Logger) (GraphResolver, error) {
	switch cfg.GraphProvider {
	case GraphProviderNeo4j:
		return NewNeo4jGraphAdapter(cfg, log), nil
	case GraphProviderMock:
		return NewMockNeo4jResolver(cfg, log), nil
	default:
		// Validate already rejected anything else; this keeps the switch total.
		return nil, fmt.Errorf("unknown graph provider %q", cfg.GraphProvider)
	}
}

// graphDetailer is implemented by providers with counters worth reporting
// beyond the three the metrics endpoint already renders.
type graphDetailer interface {
	Detail() map[string]any
}

// newAdminServer exposes liveness, readiness and metrics. It is deliberately
// separate from the data path so a stalled consumer is still observable.
func newAdminServer(
	cfg Config,
	cache *StateCache,
	tracker *StateTracker,
	graph GraphResolver,
	dedupe *IdempotencyEngine,
	metrics *Metrics,
	replica *TopologyReplica,
	logger *slog.Logger,
) *http.Server {
	mux := http.NewServeMux()

	registerReplicaRoutes(mux, replica)

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "ok",
			"service": Producer,
			"version": buildVersion,
			"uptime":  time.Since(metrics.StartedAt).String(),
		})
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		pingCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if err := cache.Ping(pingCtx); err != nil {
			logger.Warn("readiness probe failed", "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"status": "degraded",
				"redis":  err.Error(),
			})
			return
		}
		if err := dedupe.Ping(pingCtx); err != nil {
			logger.Warn("readiness probe failed", "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"status":      "degraded",
				"redis":       "ok",
				"idempotency": err.Error(),
			})
			return
		}
		// The graph tier is reported but never gates readiness. It is a
		// degradable dependency: with it down the engine still consumes, still
		// evaluates rules and still emits mutations, just with degraded=true.
		writeJSON(w, http.StatusOK, map[string]any{
			"status":      "ready",
			"redis":       "ok",
			"idempotency": "ok",
			"graph":       graphReadiness(graph),
		})
	})

	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		tracked, anomalous := tracker.Tracked()
		lookups, hits, errs := graph.Stats()
		writeJSON(w, http.StatusOK, map[string]any{
			"service": Producer,
			"version": buildVersion,
			"config": map[string]any{
				"source_topic":   cfg.SourceTopic,
				"mutation_topic": cfg.MutationTopic,
				"workers":        cfg.Workers,
				"thresholds":     cfg.Thresholds,
				"graph_provider": graph.Provider(),
			},
			"graph":       graphStats(cfg, graph, lookups, hits, errs),
			"metrics":     metrics.JSON(tracked, anomalous, lookups, hits, errs),
			"transitions": metrics.TransitionCounts(),
			"idempotency": dedupe.Stats(),
			"replica":     replica.Stats(),
		})
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		tracked, anomalous := tracker.Tracked()
		lookups, hits, errs := graph.Stats()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(metrics.Prometheus(tracked, anomalous, lookups, hits, errs)))
		_, _ = w.Write([]byte(graphProviderMetric(graph.Provider())))
		_, _ = w.Write([]byte(dedupe.Prometheus()))
		_, _ = w.Write([]byte(replica.Prometheus()))
	})

	return &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// graphReadiness summarises the graph tier for /readyz in one string, so an
// operator can see "neo4j:disconnected" without reading /stats.
func graphReadiness(graph GraphResolver) string {
	connected, ok := graph.(interface{ Connected() bool })
	if !ok {
		return graph.Provider()
	}
	if connected.Connected() {
		return graph.Provider() + ":connected"
	}
	return graph.Provider() + ":disconnected"
}

// graphStats renders the graph block of /stats: which tier is live, how it is
// tuned, and how it has been behaving. Provider-specific counters are merged in
// when the provider has any.
func graphStats(cfg Config, graph GraphResolver, lookups, hits, errs uint64) map[string]any {
	stats := map[string]any{
		"provider":   graph.Provider(),
		"cache_ttl":  cfg.GraphCacheTTL.String(),
		"lookups":    lookups,
		"cache_hits": hits,
		"errors":     errs,
	}
	if detailer, ok := graph.(graphDetailer); ok {
		for key, value := range detailer.Detail() {
			stats[key] = value
		}
	}
	if graph.Provider() == GraphProviderNeo4j {
		stats["uri"] = cfg.Neo4jURI
		stats["database"] = cfg.Neo4jDatabase
	}
	return stats
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Warn("failed to write JSON response", "error", err)
	}
}
