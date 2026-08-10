package main

// Module 2, Part A — the distributed idempotency filter.
//
// Kafka guarantees at-least-once delivery, and the plant-floor gateways feeding
// telemetry.raw retry aggressively whenever an ack is slow. Both facts mean the
// same physical sample reaches this engine more than once on a regular basis.
// Without a filter, one vibration spike replayed three times produces three
// cache writes, three rule evaluations and — worst of all — three Enriched
// Context Payloads, each of which costs a downstream LLM inference and can
// materialise as three actuation commands against a live asset.
//
// The filter is a structural step at the head of the ingestion loop: it claims
// a short-lived key in Redis for the (asset, timestamp) coordinate of every
// event. The claim is taken with SETNX, which is atomic across every engine
// replica in the cluster, so the deduplication window is genuinely distributed
// rather than per-process. A 5 second TTL is deliberately narrow: it is far
// wider than any realistic redelivery gap and far narrower than the interval at
// which an asset legitimately re-reports the same coordinate, so the filter
// suppresses retries without ever swallowing real telemetry.

import (
	"context"
	"crypto/md5" //nolint:gosec // MD5 is used as a fast content fingerprint, never as a security primitive.
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// DefaultIdempotencyTTL is the deduplication window mandated by the
	// ingestion contract. Every claim expires exactly this long after it is
	// taken, which bounds the filter's Redis footprint to one key per distinct
	// event coordinate observed in the last five seconds.
	DefaultIdempotencyTTL = 5 * time.Second

	// DefaultIdempotencyKeyPrefix namespaces claim keys away from twin:,
	// twinindex: and twinalarm:, so a claim can never shadow live twin state.
	DefaultIdempotencyKeyPrefix = "dedupe:"

	// idempotencyClaimValue is the generic sentinel stored under a claim. The
	// value carries no information — presence of the key is the entire signal —
	// so a single byte keeps the working set small.
	idempotencyClaimValue = "1"
)

// Sentinel errors returned by this file. They are wrapped, never replaced, so
// callers can branch with errors.Is while still logging the underlying cause.
var (
	// ErrIdempotencyClientMissing is returned when the engine is constructed
	// without a Redis client to claim keys against.
	ErrIdempotencyClientMissing = errors.New("idempotency: a redis client is required")

	// ErrIdempotencyInvalidInput is returned when a fingerprint is requested
	// for a coordinate that cannot identify an event.
	ErrIdempotencyInvalidInput = errors.New("idempotency: asset_id must be non-empty and timestamp must be a positive epoch value")

	// ErrIdempotencyInvalidConfig is returned by IdempotencyConfig.Validate.
	ErrIdempotencyInvalidConfig = errors.New("idempotency: invalid configuration")
)

// IdempotencyScope selects which fields of a telemetry event participate in the
// deduplication fingerprint.
type IdempotencyScope string

const (
	// ScopeAsset fingerprints (asset_id, timestamp) exactly as specified by the
	// ingestion contract. Use it when a gateway emits one multi-variable frame
	// per asset per timestamp.
	ScopeAsset IdempotencyScope = "asset"

	// ScopeAssetSensor fingerprints (asset_id, sensor_id, timestamp).
	//
	// This engine's wire format is one reading per message, so a single asset
	// legitimately publishes vibration_index and temperature_celsius bearing an
	// identical timestamp. Under ScopeAsset the second of those two readings is
	// indistinguishable from a redelivery of the first and is discarded. This
	// scope is therefore the safe default for the current telemetry.raw schema;
	// switch to ScopeAsset only when the producer batches every sensor of an
	// asset into one message.
	ScopeAssetSensor IdempotencyScope = "asset_sensor"
)

// Valid reports whether the scope is one the filter understands.
func (s IdempotencyScope) Valid() bool {
	switch s {
	case ScopeAsset, ScopeAssetSensor:
		return true
	default:
		return false
	}
}

// IdempotencyConfig is the complete runtime configuration of the filter.
type IdempotencyConfig struct {
	// Enabled turns the whole filter off. A disabled engine admits everything
	// and performs no Redis round trips, which makes it safe to leave the call
	// site wired in permanently.
	Enabled bool

	// KeyPrefix namespaces claim keys.
	KeyPrefix string

	// TTL is the deduplication window.
	TTL time.Duration

	// Scope selects the fingerprint composition.
	Scope IdempotencyScope

	// FailOpen decides what happens when Redis itself is unreachable. Open
	// (the default) admits the event and accepts the risk of a duplicate;
	// closed rejects it, which surfaces as a retry and then a dead letter.
	// Dropping real telemetry is a worse failure than processing a duplicate —
	// the downstream state cache is last-writer-wins on a monotonic timestamp
	// and the alarm state machine suppresses repeated transitions — so open is
	// the correct posture for an availability-first ingestion path.
	FailOpen bool

	// OpTimeout bounds a single Redis round trip.
	OpTimeout time.Duration
}

// LoadIdempotencyConfig reads the filter's tunables from the environment. It
// mirrors LoadConfig: every parse failure is accumulated so an operator sees
// all broken variables at once.
func LoadIdempotencyConfig(opTimeout time.Duration) (IdempotencyConfig, error) {
	env := &envReader{}

	cfg := IdempotencyConfig{
		Enabled:   env.boolean("IDEMPOTENCY_ENABLED", true),
		KeyPrefix: env.str("IDEMPOTENCY_KEY_PREFIX", DefaultIdempotencyKeyPrefix),
		TTL:       env.duration("IDEMPOTENCY_TTL", DefaultIdempotencyTTL),
		Scope:     IdempotencyScope(strings.ToLower(env.str("IDEMPOTENCY_SCOPE", string(ScopeAssetSensor)))),
		FailOpen:  env.boolean("IDEMPOTENCY_FAIL_OPEN", true),
		OpTimeout: env.duration("IDEMPOTENCY_OP_TIMEOUT", opTimeout),
	}

	if err := env.err(); err != nil {
		return IdempotencyConfig{}, fmt.Errorf("%w: %w", ErrIdempotencyInvalidConfig, err)
	}
	if err := cfg.Validate(); err != nil {
		return IdempotencyConfig{}, err
	}
	return cfg, nil
}

// Validate rejects configurations that would misbehave at runtime.
func (c IdempotencyConfig) Validate() error {
	var errs []error

	if strings.TrimSpace(c.KeyPrefix) == "" {
		errs = append(errs, errors.New("IDEMPOTENCY_KEY_PREFIX must not be empty"))
	}
	if c.TTL <= 0 {
		errs = append(errs, errors.New("IDEMPOTENCY_TTL must be > 0"))
	}
	if c.TTL > time.Hour {
		errs = append(errs, errors.New("IDEMPOTENCY_TTL must be <= 1h; a wide window suppresses legitimate re-reports"))
	}
	if !c.Scope.Valid() {
		errs = append(errs, fmt.Errorf("IDEMPOTENCY_SCOPE %q must be one of asset|asset_sensor", c.Scope))
	}
	if c.OpTimeout <= 0 {
		errs = append(errs, errors.New("IDEMPOTENCY_OP_TIMEOUT must be > 0"))
	}

	if joined := errors.Join(errs...); joined != nil {
		return fmt.Errorf("%w: %w", ErrIdempotencyInvalidConfig, joined)
	}
	return nil
}

// boolean parses a boolean environment variable, accumulating parse failures on
// the shared envReader. It lives here rather than in config.go so the filter is
// self-contained.
func (e *envReader) boolean(key string, def bool) bool {
	v, ok := e.raw(key)
	if !ok {
		return def
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %q is not a boolean (true|false|1|0)", key, v))
		return def
	}
	return parsed
}

// ---------------------------------------------------------------------------
// Fingerprinting
// ---------------------------------------------------------------------------

// IdempotencyFingerprint computes the deterministic MD5 fingerprint of an
// (asset_id, timestamp) coordinate.
//
// The components are joined with a byte that identifier validation forbids
// inside an asset id, so ("A|B", 1) and ("A", ...) can never collide by string
// concatenation. MD5 is chosen for speed: this runs on every message in the hot
// path, the digest is a lookup key rather than a security token, and a 128-bit
// space makes accidental collisions within a five second window vanishingly
// unlikely.
func IdempotencyFingerprint(assetID string, timestamp int64) (string, error) {
	return idempotencyFingerprint(assetID, "", timestamp)
}

// IdempotencyFingerprintWith adds a discriminator — in practice the sensor id —
// to the fingerprint, so two sensors of the same asset sharing a timestamp do
// not alias onto one claim.
func IdempotencyFingerprintWith(assetID, discriminator string, timestamp int64) (string, error) {
	return idempotencyFingerprint(assetID, discriminator, timestamp)
}

func idempotencyFingerprint(assetID, discriminator string, timestamp int64) (string, error) {
	asset := strings.TrimSpace(assetID)
	if asset == "" || timestamp <= 0 {
		return "", fmt.Errorf("%w (asset_id=%q timestamp=%d)", ErrIdempotencyInvalidInput, assetID, timestamp)
	}

	var b strings.Builder
	b.Grow(len(asset) + len(discriminator) + 24)
	b.WriteString(asset)
	b.WriteByte('|')
	if discriminator != "" {
		b.WriteString(strings.TrimSpace(discriminator))
		b.WriteByte('|')
	}
	b.WriteString(strconv.FormatInt(timestamp, 10))

	//nolint:gosec // Content fingerprint, not a message-authentication code.
	sum := md5.Sum([]byte(b.String()))
	return hex.EncodeToString(sum[:]), nil
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

// IdempotencyEngine is the distributed deduplication filter.
//
// It holds no mutable state beyond lock-free counters, so a single value is
// shared by every consumer worker goroutine. All coordination happens inside
// Redis, which is what makes the filter correct across replicas as well as
// across goroutines.
type IdempotencyEngine struct {
	client     *redis.Client
	cfg        IdempotencyConfig
	log        *slog.Logger
	ownsClient bool

	checked    atomic.Uint64
	admitted   atomic.Uint64
	duplicates atomic.Uint64
	released   atomic.Uint64
	failedOpen atomic.Uint64
	errs       atomic.Uint64
}

// Redis exposes the state cache's connection pool so the filter can share it.
// One pool serving both the twin state and the deduplication claims keeps the
// connection count predictable and guarantees both see the same Redis instance.
func (c *StateCache) Redis() *redis.Client { return c.client }

// NewIdempotencyEngine builds a filter on top of an existing Redis client. The
// caller retains ownership of the client; Close is a no-op in this mode.
func NewIdempotencyEngine(cfg IdempotencyConfig, client *redis.Client, log *slog.Logger) (*IdempotencyEngine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if client == nil {
		return nil, ErrIdempotencyClientMissing
	}
	return &IdempotencyEngine{
		client: client,
		cfg:    cfg,
		log:    log.With("component", "idempotency-filter"),
	}, nil
}

// NewDedicatedIdempotencyEngine dials its own Redis connection pool and verifies
// connectivity before returning. Use it when the deduplication keyspace must be
// isolated from live twin state — for example when the filter points at a
// separate, memory-only Redis with no persistence configured.
func NewDedicatedIdempotencyEngine(
	ctx context.Context,
	base Config,
	cfg IdempotencyConfig,
	log *slog.Logger,
) (*IdempotencyEngine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	client := redis.NewClient(&redis.Options{
		Addr:            base.RedisAddr,
		Password:        base.RedisPassword,
		DB:              base.RedisDB,
		PoolSize:        base.RedisPoolSize,
		MinIdleConns:    base.RedisPoolSize / 4,
		ReadTimeout:     cfg.OpTimeout,
		WriteTimeout:    cfg.OpTimeout,
		ConnMaxIdleTime: 5 * time.Minute,
	})

	pingCtx, cancel := context.WithTimeout(ctx, cfg.OpTimeout)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		if closeErr := client.Close(); closeErr != nil {
			return nil, fmt.Errorf("idempotency: redis ping %s: %w (and closing the client failed: %w)", base.RedisAddr, err, closeErr)
		}
		return nil, fmt.Errorf("idempotency: redis ping %s: %w", base.RedisAddr, err)
	}

	engine, err := NewIdempotencyEngine(cfg, client, log)
	if err != nil {
		if closeErr := client.Close(); closeErr != nil {
			return nil, errors.Join(err, fmt.Errorf("closing redis client: %w", closeErr))
		}
		return nil, err
	}
	engine.ownsClient = true
	return engine, nil
}

// Config returns the filter's effective configuration.
func (e *IdempotencyEngine) Config() IdempotencyConfig {
	if e == nil {
		return IdempotencyConfig{}
	}
	return e.cfg
}

// Enabled reports whether the filter performs any work.
func (e *IdempotencyEngine) Enabled() bool { return e != nil && e.cfg.Enabled }

// Ping verifies the filter can reach Redis. It is wired into the readiness
// probe alongside the state cache.
func (e *IdempotencyEngine) Ping(ctx context.Context) error {
	if !e.Enabled() {
		return nil
	}
	pingCtx, cancel := context.WithTimeout(ctx, e.cfg.OpTimeout)
	defer cancel()
	if err := e.client.Ping(pingCtx).Err(); err != nil {
		return fmt.Errorf("idempotency: redis ping: %w", err)
	}
	return nil
}

// Close releases the connection pool when this engine dialled it itself.
func (e *IdempotencyEngine) Close() error {
	if e == nil || !e.ownsClient {
		return nil
	}
	if err := e.client.Close(); err != nil {
		return fmt.Errorf("idempotency: closing redis client: %w", err)
	}
	return nil
}

// Key renders the Redis key for a fingerprint.
func (e *IdempotencyEngine) Key(fingerprint string) string {
	if e == nil {
		return DefaultIdempotencyKeyPrefix + fingerprint
	}
	return e.cfg.KeyPrefix + fingerprint
}

// IsDuplicate reports whether the (assetID, timestamp) coordinate has already
// been claimed inside the deduplication window.
//
// The check and the claim are one atomic Redis SETNX: the key is set to the
// generic sentinel with the configured TTL only if it does not already exist.
// SETNX returning false means another delivery — on this worker, another
// goroutine or another replica — already owns the coordinate, so this delivery
// is a duplicate and the ingestion worker must discard the message before it
// reaches the state cache, the rule engine or the mutation topic.
//
// A returned error means the verdict is unknown, never that the event is
// unique; callers must apply an explicit fail-open or fail-closed policy.
// Admit does exactly that and is the preferred entry point for the ingestion
// loop.
func (e *IdempotencyEngine) IsDuplicate(ctx context.Context, assetID string, timestamp int64) (bool, error) {
	if !e.Enabled() {
		return false, nil
	}
	fingerprint, err := IdempotencyFingerprint(assetID, timestamp)
	if err != nil {
		e.errs.Add(1)
		return false, err
	}
	return e.claim(ctx, fingerprint, assetID)
}

// IsDuplicateEvent applies the configured scope to a decoded telemetry event.
func (e *IdempotencyEngine) IsDuplicateEvent(ctx context.Context, ev TelemetryEvent) (bool, error) {
	if !e.Enabled() {
		return false, nil
	}
	fingerprint, err := e.fingerprintFor(ev)
	if err != nil {
		e.errs.Add(1)
		return false, err
	}
	return e.claim(ctx, fingerprint, ev.AssetID)
}

// claim executes the atomic SETNX that is the whole filter.
func (e *IdempotencyEngine) claim(ctx context.Context, fingerprint, assetID string) (bool, error) {
	e.checked.Add(1)
	key := e.Key(fingerprint)

	opCtx, cancel := context.WithTimeout(ctx, e.cfg.OpTimeout)
	defer cancel()

	acquired, err := e.client.SetNX(opCtx, key, idempotencyClaimValue, e.cfg.TTL).Result()
	if err != nil {
		e.errs.Add(1)
		// A cancelled parent context is a shutdown, not an infrastructure
		// fault; keep it distinguishable so the caller does not dead-letter a
		// message that was never actually rejected.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, fmt.Errorf("idempotency: claim %s aborted: %w", key, ctxErr)
		}
		return false, fmt.Errorf("idempotency: SETNX %s (asset %s): %w", key, assetID, err)
	}

	if acquired {
		return false, nil
	}

	e.duplicates.Add(1)
	return true, nil
}

// Admit is the structural step the ingestion loop calls. It returns true when
// the event must be processed and false when it must be discarded, having
// already applied the configured failure policy and recorded the outcome.
//
// The claim it takes is provisional: if the caller subsequently fails to
// process the event it must call ReleaseEvent so the redelivery Kafka is about
// to perform is not silently swallowed by this filter.
func (e *IdempotencyEngine) Admit(ctx context.Context, ev TelemetryEvent) (bool, error) {
	if !e.Enabled() {
		return true, nil
	}

	duplicate, err := e.IsDuplicateEvent(ctx, ev)
	switch {
	case err == nil:
		// Fall through to the verdict below.
	case errors.Is(err, ErrIdempotencyInvalidInput):
		// A malformed coordinate is a payload defect, not an outage. Report it
		// so the caller dead-letters the message rather than looping on it.
		return false, err
	case ctx.Err() != nil:
		// Shutting down: surface the cancellation untouched.
		return false, err
	case e.cfg.FailOpen:
		e.failedOpen.Add(1)
		e.log.Warn("deduplication unavailable; admitting event without a claim",
			"asset_id", ev.AssetID,
			"sensor_id", ev.SensorID,
			"observed_at", ev.Timestamp.Time,
			"error", err)
		e.admitted.Add(1)
		return true, nil
	default:
		return false, err
	}

	if duplicate {
		e.log.Debug("discarding duplicate telemetry event",
			"asset_id", ev.AssetID,
			"sensor_id", ev.SensorID,
			"observed_at", ev.Timestamp.Time,
			"scope", string(e.cfg.Scope),
			"window", e.cfg.TTL.String())
		return false, nil
	}

	e.admitted.Add(1)
	return true, nil
}

// Release drops the claim on an (assetID, timestamp) coordinate.
func (e *IdempotencyEngine) Release(ctx context.Context, assetID string, timestamp int64) error {
	if !e.Enabled() {
		return nil
	}
	fingerprint, err := IdempotencyFingerprint(assetID, timestamp)
	if err != nil {
		return err
	}
	return e.release(ctx, fingerprint)
}

// ReleaseEvent drops the claim taken by Admit for this event.
//
// It is called when processing failed after the claim was granted. Without it,
// a message that fails on its first attempt would be classified as a duplicate
// on redelivery and dropped, converting a transient downstream error into
// permanent data loss.
func (e *IdempotencyEngine) ReleaseEvent(ctx context.Context, ev TelemetryEvent) error {
	if !e.Enabled() {
		return nil
	}
	fingerprint, err := e.fingerprintFor(ev)
	if err != nil {
		return err
	}
	return e.release(ctx, fingerprint)
}

func (e *IdempotencyEngine) release(ctx context.Context, fingerprint string) error {
	key := e.Key(fingerprint)

	// The rollback must survive the cancellation of the context that failed;
	// otherwise a shutdown mid-flight leaves a stale claim blocking redelivery
	// for the remainder of the window.
	opCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.cfg.OpTimeout)
	defer cancel()

	removed, err := e.client.Del(opCtx, key).Result()
	if err != nil {
		e.errs.Add(1)
		return fmt.Errorf("idempotency: releasing claim %s: %w", key, err)
	}
	if removed > 0 {
		e.released.Add(1)
	}
	return nil
}

func (e *IdempotencyEngine) fingerprintFor(ev TelemetryEvent) (string, error) {
	timestamp := ev.Timestamp.UnixMilli()
	if e.cfg.Scope == ScopeAssetSensor {
		return IdempotencyFingerprintWith(ev.AssetID, ev.SensorID, timestamp)
	}
	return IdempotencyFingerprint(ev.AssetID, timestamp)
}

// ---------------------------------------------------------------------------
// Observability
// ---------------------------------------------------------------------------

// IdempotencyStats is the filter's contribution to /stats.
type IdempotencyStats struct {
	Enabled        bool    `json:"enabled"`
	Scope          string  `json:"scope"`
	TTLSeconds     float64 `json:"ttl_seconds"`
	KeyPrefix      string  `json:"key_prefix"`
	FailOpen       bool    `json:"fail_open"`
	Checked        uint64  `json:"checked"`
	Admitted       uint64  `json:"admitted"`
	Duplicates     uint64  `json:"duplicates_discarded"`
	Released       uint64  `json:"claims_released"`
	FailedOpen     uint64  `json:"failed_open"`
	Errors         uint64  `json:"errors"`
	DuplicateRatio float64 `json:"duplicate_ratio"`
}

// Stats snapshots the counters. The reads are individually atomic but not
// mutually consistent, which is the accepted trade-off for a lock-free hot path.
func (e *IdempotencyEngine) Stats() IdempotencyStats {
	if e == nil {
		return IdempotencyStats{Enabled: false, Scope: string(ScopeAssetSensor)}
	}

	checked := e.checked.Load()
	duplicates := e.duplicates.Load()

	ratio := 0.0
	if checked > 0 {
		ratio = float64(duplicates) / float64(checked)
	}

	return IdempotencyStats{
		Enabled:        e.cfg.Enabled,
		Scope:          string(e.cfg.Scope),
		TTLSeconds:     e.cfg.TTL.Seconds(),
		KeyPrefix:      e.cfg.KeyPrefix,
		FailOpen:       e.cfg.FailOpen,
		Checked:        checked,
		Admitted:       e.admitted.Load(),
		Duplicates:     duplicates,
		Released:       e.released.Load(),
		FailedOpen:     e.failedOpen.Load(),
		Errors:         e.errs.Load(),
		DuplicateRatio: round(ratio, 6),
	}
}

// Prometheus renders the filter's counters in the text exposition format, to be
// appended to the engine's own /metrics output.
func (e *IdempotencyEngine) Prometheus() string {
	stats := e.Stats()

	enabled := 0.0
	if stats.Enabled {
		enabled = 1.0
	}

	return renderExposition([]metricSample{
		{"openontology_idempotency_enabled", "1 when the distributed idempotency filter is active.", "gauge", enabled, nil},
		{"openontology_idempotency_window_seconds", "Deduplication window applied to every claim.", "gauge", stats.TTLSeconds, nil},
		{"openontology_idempotency_checked_total", "Events submitted to the idempotency filter.", "counter", float64(stats.Checked), nil},
		{"openontology_idempotency_admitted_total", "Events admitted for processing by the filter.", "counter", float64(stats.Admitted), nil},
		{"openontology_idempotency_duplicates_total", "Events discarded as duplicates within the window.", "counter", float64(stats.Duplicates), nil},
		{"openontology_idempotency_released_total", "Provisional claims rolled back after a processing failure.", "counter", float64(stats.Released), nil},
		{"openontology_idempotency_failed_open_total", "Events admitted because the filter could not reach Redis.", "counter", float64(stats.FailedOpen), nil},
		{"openontology_idempotency_errors_total", "Idempotency filter errors.", "counter", float64(stats.Errors), nil},
		{"openontology_idempotency_duplicate_ratio", "Duplicates as a fraction of events checked.", "gauge", stats.DuplicateRatio, nil},
	})
}
