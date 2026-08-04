package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Graph providers selectable through GRAPH_PROVIDER.
const (
	// GraphProviderMock is the fixture-backed stand-in in graph.go. It needs no
	// server, which is what keeps the offline path working.
	GraphProviderMock = "mock"

	// GraphProviderNeo4j is the driver-backed resolver in internal/graph,
	// reached through Neo4jGraphAdapter.
	GraphProviderNeo4j = "neo4j"
)

// Config is the complete runtime configuration of the resolution engine.
// Everything is sourced from the environment so the binary behaves identically
// on a laptop, in docker-compose and on a plant-floor Kubernetes node.
type Config struct {
	// Kafka
	KafkaBrokers   []string
	ConsumerGroup  string
	SourceTopic    string
	MutationTopic  string
	DLQTopic       string
	MinFetchBytes  int
	MaxFetchBytes  int
	MaxWait        time.Duration
	RequiredAcks   int
	Workers        int
	MaxAttempts    int
	SessionTimeout time.Duration

	// Redis
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	RedisPoolSize int
	StateTTL      time.Duration

	// Anomaly rules
	Thresholds      map[string]Threshold
	CriticalRatio   float64
	HysteresisRatio float64
	ReAlertInterval time.Duration

	// Graph tier. GraphProvider selects which implementation of GraphResolver
	// goes live; everything below GraphQueryBudget applies only to "neo4j".
	GraphProvider    string
	GraphCacheTTL    time.Duration
	GraphQueryBudget time.Duration
	GraphLatency     time.Duration

	Neo4jURI            string
	Neo4jUsername       string
	Neo4jPassword       string
	Neo4jDatabase       string
	Neo4jMaxPoolSize    int
	Neo4jConnectTimeout time.Duration

	// Runtime
	OpTimeout       time.Duration
	ShutdownTimeout time.Duration
	StateIdleTTL    time.Duration
	HTTPAddr        string
	LogLevel        slog.Level
}

// LoadConfig reads every tunable from the environment, applying production
// sane defaults. All parse failures are collected so an operator sees every
// broken variable at once instead of fixing them one restart at a time.
func LoadConfig() (Config, error) {
	env := &envReader{}

	cfg := Config{
		KafkaBrokers:   env.csv("KAFKA_BROKERS", []string{"localhost:9092"}),
		ConsumerGroup:  env.str("KAFKA_CONSUMER_GROUP", "ontology-resolution-engine"),
		SourceTopic:    env.str("KAFKA_SOURCE_TOPIC", "telemetry.raw"),
		MutationTopic:  env.str("KAFKA_MUTATION_TOPIC", "ontology.mutations"),
		DLQTopic:       env.str("KAFKA_DLQ_TOPIC", "telemetry.dlq"),
		MinFetchBytes:  env.int("KAFKA_MIN_BYTES", 1),
		MaxFetchBytes:  env.int("KAFKA_MAX_BYTES", 10<<20),
		MaxWait:        env.duration("KAFKA_MAX_WAIT", 500*time.Millisecond),
		RequiredAcks:   env.int("KAFKA_REQUIRED_ACKS", -1), // -1 == all in-sync replicas
		Workers:        env.int("ENGINE_WORKERS", 4),
		MaxAttempts:    env.int("ENGINE_MAX_ATTEMPTS", 4),
		SessionTimeout: env.duration("KAFKA_SESSION_TIMEOUT", 30*time.Second),

		RedisAddr:     env.str("REDIS_ADDR", "localhost:6379"),
		RedisPassword: env.str("REDIS_PASSWORD", ""),
		RedisDB:       env.int("REDIS_DB", 0),
		RedisPoolSize: env.int("REDIS_POOL_SIZE", 32),
		StateTTL:      env.duration("TWIN_STATE_TTL", 24*time.Hour),

		CriticalRatio:   env.float("RULE_CRITICAL_RATIO", 0.15),
		HysteresisRatio: env.float("RULE_HYSTERESIS_RATIO", 0.05),
		ReAlertInterval: env.duration("RULE_REALERT_INTERVAL", 5*time.Minute),

		// The default is deliberately the stand-in: a checkout with nothing but
		// Kafka and Redis running still resolves context and still emits
		// enriched mutations.
		GraphProvider:    strings.ToLower(env.str("GRAPH_PROVIDER", GraphProviderMock)),
		GraphCacheTTL:    env.duration("GRAPH_CACHE_TTL", 5*time.Minute),
		GraphQueryBudget: env.duration("GRAPH_QUERY_BUDGET", 3*time.Second),
		GraphLatency:     env.duration("GRAPH_SIMULATED_LATENCY", 12*time.Millisecond),

		Neo4jURI:      env.str("NEO4J_URI", "bolt://neo4j:7687"),
		Neo4jUsername: env.str("NEO4J_USERNAME", "neo4j"),
		Neo4jPassword: env.str("NEO4J_PASSWORD", ""),
		Neo4jDatabase: env.str("NEO4J_DATABASE", "neo4j"),
		// Sized to the worker count plus headroom for the admin endpoints.
		Neo4jMaxPoolSize:    env.int("NEO4J_MAX_POOL_SIZE", 32),
		Neo4jConnectTimeout: env.duration("NEO4J_CONNECT_TIMEOUT", 10*time.Second),

		OpTimeout:       env.duration("ENGINE_OP_TIMEOUT", 5*time.Second),
		ShutdownTimeout: env.duration("ENGINE_SHUTDOWN_TIMEOUT", 20*time.Second),
		StateIdleTTL:    env.duration("ENGINE_STATE_IDLE_TTL", 6*time.Hour),
		HTTPAddr:        env.str("HTTP_ADDR", ":8081"),
		LogLevel:        env.level("LOG_LEVEL", slog.LevelInfo),
	}

	// The two rules mandated by the ontology specification. Both limits are
	// overridable so a site can tighten them without a rebuild.
	cfg.Thresholds = map[string]Threshold{
		SensorVibrationIndex: {
			RuleID:      "rule.vibration_index.max",
			SensorID:    SensorVibrationIndex,
			Limit:       env.float("RULE_VIBRATION_INDEX_MAX", 8.5),
			Unit:        "mm/s",
			Description: "ISO 10816 broadband vibration index ceiling",
		},
		SensorTemperatureCelsius: {
			RuleID:      "rule.temperature_celsius.max",
			SensorID:    SensorTemperatureCelsius,
			Limit:       env.float("RULE_TEMPERATURE_CELSIUS_MAX", 110.0),
			Unit:        "degC",
			Description: "Bearing/EGT thermal ceiling",
		},
	}

	if err := env.err(); err != nil {
		return Config{}, err
	}
	return cfg, cfg.Validate()
}

// Validate rejects configurations that would fail later in a confusing way.
func (c Config) Validate() error {
	var errs []error

	if len(c.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS must list at least one broker"))
	}
	for _, topic := range []struct{ name, value string }{
		{"KAFKA_SOURCE_TOPIC", c.SourceTopic},
		{"KAFKA_MUTATION_TOPIC", c.MutationTopic},
		{"KAFKA_DLQ_TOPIC", c.DLQTopic},
	} {
		if strings.TrimSpace(topic.value) == "" {
			errs = append(errs, fmt.Errorf("%s must not be empty", topic.name))
		}
	}
	if c.SourceTopic == c.MutationTopic {
		errs = append(errs, errors.New("source and mutation topics must differ or the engine will consume its own output"))
	}
	if c.Workers < 1 {
		errs = append(errs, errors.New("ENGINE_WORKERS must be >= 1"))
	}
	if c.MaxAttempts < 1 {
		errs = append(errs, errors.New("ENGINE_MAX_ATTEMPTS must be >= 1"))
	}
	if c.RedisAddr == "" {
		errs = append(errs, errors.New("REDIS_ADDR must not be empty"))
	}
	if c.StateTTL <= 0 {
		errs = append(errs, errors.New("TWIN_STATE_TTL must be > 0"))
	}
	if c.OpTimeout <= 0 {
		errs = append(errs, errors.New("ENGINE_OP_TIMEOUT must be > 0"))
	}
	if c.CriticalRatio < 0 {
		errs = append(errs, errors.New("RULE_CRITICAL_RATIO must be >= 0"))
	}
	if c.HysteresisRatio < 0 || c.HysteresisRatio >= 1 {
		errs = append(errs, errors.New("RULE_HYSTERESIS_RATIO must be in [0, 1)"))
	}
	for _, t := range c.Thresholds {
		if t.Limit <= 0 {
			errs = append(errs, fmt.Errorf("threshold %s must be > 0", t.RuleID))
		}
	}

	switch c.GraphProvider {
	case GraphProviderMock:
	case GraphProviderNeo4j:
		if strings.TrimSpace(c.Neo4jURI) == "" {
			errs = append(errs, errors.New("NEO4J_URI must be set when GRAPH_PROVIDER=neo4j"))
		}
		if strings.TrimSpace(c.Neo4jPassword) == "" {
			// Neo4j refuses BasicAuth with an empty password, and connecting
			// unauthenticated against a server that wants credentials fails at
			// the handshake with a much less obvious message.
			errs = append(errs, errors.New("NEO4J_PASSWORD must be set when GRAPH_PROVIDER=neo4j"))
		}
		if c.Neo4jMaxPoolSize < 1 {
			errs = append(errs, errors.New("NEO4J_MAX_POOL_SIZE must be >= 1"))
		}
	default:
		errs = append(errs, fmt.Errorf("GRAPH_PROVIDER %q is not one of %s|%s",
			c.GraphProvider, GraphProviderMock, GraphProviderNeo4j))
	}
	if c.GraphQueryBudget <= 0 {
		errs = append(errs, errors.New("GRAPH_QUERY_BUDGET must be > 0"))
	}
	if c.GraphQueryBudget > c.OpTimeout {
		// The graph read runs inside the per-message operation deadline. A
		// budget larger than that can never fire, which silently removes the
		// bound the ingestion path relies on.
		errs = append(errs, fmt.Errorf(
			"GRAPH_QUERY_BUDGET (%s) must not exceed ENGINE_OP_TIMEOUT (%s)", c.GraphQueryBudget, c.OpTimeout))
	}

	return errors.Join(errs...)
}

// envReader parses environment variables while accumulating every failure.
type envReader struct {
	errs []error
}

func (e *envReader) err() error { return errors.Join(e.errs...) }

func (e *envReader) raw(key string) (string, bool) {
	v, ok := os.LookupEnv(key)
	v = strings.TrimSpace(v)
	if !ok || v == "" {
		return "", false
	}
	return v, true
}

func (e *envReader) str(key, def string) string {
	if v, ok := e.raw(key); ok {
		return v
	}
	return def
}

func (e *envReader) int(key string, def int) int {
	v, ok := e.raw(key)
	if !ok {
		return def
	}
	parsed, err := strconv.Atoi(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %q is not an integer", key, v))
		return def
	}
	return parsed
}

func (e *envReader) float(key string, def float64) float64 {
	v, ok := e.raw(key)
	if !ok {
		return def
	}
	parsed, err := strconv.ParseFloat(v, 64)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %q is not a float", key, v))
		return def
	}
	return parsed
}

func (e *envReader) duration(key string, def time.Duration) time.Duration {
	v, ok := e.raw(key)
	if !ok {
		return def
	}
	parsed, err := time.ParseDuration(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %q is not a duration (e.g. 250ms, 5m)", key, v))
		return def
	}
	return parsed
}

func (e *envReader) csv(key string, def []string) []string {
	v, ok := e.raw(key)
	if !ok {
		return def
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	if len(out) == 0 {
		e.errs = append(e.errs, fmt.Errorf("%s: %q contained no usable values", key, v))
		return def
	}
	return out
}

func (e *envReader) level(key string, def slog.Level) slog.Level {
	v, ok := e.raw(key)
	if !ok {
		return def
	}
	switch strings.ToLower(v) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		e.errs = append(e.errs, fmt.Errorf("%s: %q is not one of debug|info|warn|error", key, v))
		return def
	}
}
