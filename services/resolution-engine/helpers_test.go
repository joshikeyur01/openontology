package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/segmentio/kafka-go"
)

// Shared fixtures for the main package's tests. Everything here is deliberately
// deterministic: no wall-clock reads leak into an assertion, and the only
// external dependency is an in-process miniredis.

const (
	testVibrationLimit  = 8.5
	testCriticalRatio   = 0.15
	testHysteresisRatio = 0.05
	testReAlertInterval = 5 * time.Minute
)

// testClearAt is the hysteresis release point, computed with the exact same
// expression state.go uses so the boundary cases compare identical float64s.
var testClearAt = testVibrationLimit * (1 - testHysteresisRatio)

// baseTime is the instant every time-sensitive test measures offsets from.
var baseTime = time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// testConfig mirrors the production defaults but points Kafka at a reserved
// port, so any publish fails immediately instead of hanging a test.
func testConfig(redisAddr string) Config {
	return Config{
		KafkaBrokers:   []string{"127.0.0.1:1"},
		ConsumerGroup:  "resolution-engine-test",
		SourceTopic:    "telemetry.raw",
		MutationTopic:  "ontology.mutations",
		DLQTopic:       "telemetry.dlq",
		MinFetchBytes:  1,
		MaxFetchBytes:  1 << 20,
		MaxWait:        50 * time.Millisecond,
		RequiredAcks:   1,
		Workers:        1,
		MaxAttempts:    1,
		SessionTimeout: 10 * time.Second,

		RedisAddr:     redisAddr,
		RedisPoolSize: 8,
		StateTTL:      time.Hour,

		Thresholds: map[string]Threshold{
			SensorVibrationIndex: {
				RuleID:      "rule.vibration_index.max",
				SensorID:    SensorVibrationIndex,
				Limit:       testVibrationLimit,
				Unit:        "mm/s",
				Description: "ISO 10816 broadband vibration index ceiling",
			},
			SensorTemperatureCelsius: {
				RuleID:      "rule.temperature_celsius.max",
				SensorID:    SensorTemperatureCelsius,
				Limit:       110,
				Unit:        "degC",
				Description: "Bearing/EGT thermal ceiling",
			},
		},
		CriticalRatio:   testCriticalRatio,
		HysteresisRatio: testHysteresisRatio,
		ReAlertInterval: testReAlertInterval,

		GraphProvider:    GraphProviderMock,
		GraphCacheTTL:    time.Minute,
		GraphQueryBudget: 500 * time.Millisecond,
		GraphLatency:     0,

		OpTimeout:       time.Second,
		ShutdownTimeout: time.Second,
		StateIdleTTL:    time.Hour,
		HTTPAddr:        "127.0.0.1:0",
		LogLevel:        slog.LevelDebug,
	}
}

func testIdempotencyConfig() IdempotencyConfig {
	return IdempotencyConfig{
		Enabled:   true,
		KeyPrefix: DefaultIdempotencyKeyPrefix,
		TTL:       DefaultIdempotencyTTL,
		Scope:     ScopeAssetSensor,
		FailOpen:  true,
		OpTimeout: time.Second,
	}
}

func newTestCache(t *testing.T, mr *miniredis.Miniredis) *StateCache {
	t.Helper()
	cache, err := NewStateCache(context.Background(), testConfig(mr.Addr()), discardLogger())
	if err != nil {
		t.Fatalf("NewStateCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return cache
}

func newTestDedupe(t *testing.T, cache *StateCache, cfg IdempotencyConfig) *IdempotencyEngine {
	t.Helper()
	dedupe, err := NewIdempotencyEngine(cfg, cache.Redis(), discardLogger())
	if err != nil {
		t.Fatalf("NewIdempotencyEngine: %v", err)
	}
	return dedupe
}

// reading builds a valid, normalized telemetry event.
func reading(assetID, sensorID string, value float64, at time.Time) TelemetryEvent {
	ev := TelemetryEvent{
		AssetID:   assetID,
		SensorID:  sensorID,
		Value:     value,
		Unit:      "mm/s",
		Timestamp: EventTime{Time: at.UTC()},
	}
	ev.Normalize()
	return ev
}

func mustMessage(t *testing.T, ev TelemetryEvent) kafka.Message {
	t.Helper()
	body, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal telemetry event: %v", err)
	}
	return kafka.Message{Topic: "telemetry.raw", Partition: 3, Offset: 4242, Key: []byte(ev.AssetID), Value: body}
}

// stubGraph is a GraphResolver that returns a fixed context or a fixed error.
type stubGraph struct {
	context OntologyContext
	err     error

	lookups atomic.Uint64
	hits    atomic.Uint64
	errs    atomic.Uint64
}

func (s *stubGraph) ResolveAsset(_ context.Context, assetID string) (OntologyContext, error) {
	s.lookups.Add(1)
	if s.err != nil {
		s.errs.Add(1)
		return OntologyContext{}, s.err
	}
	out := s.context.Clone()
	out.AssetID = assetID
	return out, nil
}

func (s *stubGraph) Stats() (uint64, uint64, uint64) {
	return s.lookups.Load(), s.hits.Load(), s.errs.Load()
}

func (s *stubGraph) Provider() string { return "stub" }

func (s *stubGraph) Close() error { return nil }

func richContext() OntologyContext {
	return OntologyContext{
		AssetName:   "Hydraulic Power Pack Pump 221",
		AssetClass:  "industrial.hydraulics.pump",
		Site:        "PLANT-ROTTERDAM-L4",
		Criticality: "HIGH",
		ParentSystems: []SystemNode{
			{NodeID: "SYS-HYD-L4", Name: "Line 4 Hydraulic Loop", Type: "Subsystem", Depth: 1},
		},
		Components: []string{"drive_coupling", "seal_pack"},
		AssignedOperators: []Operator{
			{OperatorID: "OP-8815", Name: "M. Okafor", Role: "Line Supervisor", EscalationOrder: 2},
			{OperatorID: "OP-8801", Name: "J. de Vries", Role: "Maintenance Technician", EscalationOrder: 1},
		},
		Source: "stub",
	}
}

// testEngine wires an Engine over miniredis with stubbed graph resolution.
type testEngine struct {
	engine  *Engine
	cache   *StateCache
	tracker *StateTracker
	dedupe  *IdempotencyEngine
	graph   *stubGraph
	metrics *Metrics
	mr      *miniredis.Miniredis
}

func newTestEngine(t *testing.T, graph *stubGraph) *testEngine {
	t.Helper()

	mr := miniredis.RunT(t)
	cfg := testConfig(mr.Addr())
	cache := newTestCache(t, mr)
	tracker := NewStateTracker(cfg.ReAlertInterval, cfg.HysteresisRatio)
	dedupe := newTestDedupe(t, cache, testIdempotencyConfig())
	metrics := NewMetrics()

	// Replication disabled: these tests are about the ingestion path, and a
	// disabled replica exercises the same nil-safe code the single-site
	// deployment runs. testReplica() covers the enabled case.
	engine := NewEngine(cfg, discardLogger(), cache, graph, tracker, dedupe, metrics,
		NewTopologyReplica(ReplicaConfig{Enabled: false}, discardLogger()))
	t.Cleanup(func() { _ = engine.Close() })

	return &testEngine{
		engine:  engine,
		cache:   cache,
		tracker: tracker,
		dedupe:  dedupe,
		graph:   graph,
		metrics: metrics,
		mr:      mr,
	}
}
