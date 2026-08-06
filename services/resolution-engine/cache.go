package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// applyReadingScript performs the live-state write atomically and rejects
// out-of-order samples. Kafka only guarantees ordering per partition, so a
// re-keyed topic or a replaying gateway can deliver an older reading after a
// newer one; comparing observed_at_ms server-side is the only way to make the
// twin monotonic without a read-modify-write race.
//
//	KEYS[1] twin:<asset>:<sensor>
//	KEYS[2] twinindex:<asset>
//	ARGV    observed_at_ms, asset_id, sensor_id, value, unit, ingested_at_ms, ttl_ms
//
// Returns 1 when written, 0 when the sample was stale, 2 when the twin already
// held this exact coordinate. See ApplyOutcome for why those last two are not
// the same answer.
var applyReadingScript = redis.NewScript(`
local existing = redis.call('HGET', KEYS[1], 'observed_at_ms')
if existing then
  local held = tonumber(existing)
  local incoming = tonumber(ARGV[1])
  if held > incoming then
    return 0
  end
  if held == incoming then
    return 2
  end
end
redis.call('HSET', KEYS[1],
  'asset_id', ARGV[2],
  'sensor_id', ARGV[3],
  'value', ARGV[4],
  'unit', ARGV[5],
  'observed_at_ms', ARGV[1],
  'ingested_at_ms', ARGV[6])
redis.call('PEXPIRE', KEYS[1], ARGV[7])
redis.call('SADD', KEYS[2], ARGV[3])
redis.call('PEXPIRE', KEYS[2], ARGV[7])
return 1
`)

// StateCache is the live twin state in Redis.
type StateCache struct {
	client  *redis.Client
	ttl     time.Duration
	timeout time.Duration
	log     *slog.Logger
}

// NewStateCache dials Redis and verifies connectivity before the engine starts
// consuming, so a misconfigured deployment fails fast instead of dead-lettering
// its first thousand messages.
func NewStateCache(ctx context.Context, cfg Config, log *slog.Logger) (*StateCache, error) {
	client := redis.NewClient(&redis.Options{
		Addr:            cfg.RedisAddr,
		Password:        cfg.RedisPassword,
		DB:              cfg.RedisDB,
		PoolSize:        cfg.RedisPoolSize,
		MinIdleConns:    cfg.RedisPoolSize / 4,
		ReadTimeout:     cfg.OpTimeout,
		WriteTimeout:    cfg.OpTimeout,
		ConnMaxIdleTime: 5 * time.Minute,
	})

	pingCtx, cancel := context.WithTimeout(ctx, cfg.OpTimeout)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis ping %s: %w", cfg.RedisAddr, err)
	}

	return &StateCache{
		client:  client,
		ttl:     cfg.StateTTL,
		timeout: cfg.OpTimeout,
		log:     log.With("component", "state-cache"),
	}, nil
}

// Close releases the connection pool.
func (c *StateCache) Close() error { return c.client.Close() }

// Ping is used by the health endpoint.
func (c *StateCache) Ping(ctx context.Context) error { return c.client.Ping(ctx).Err() }

// ApplyOutcome reports what the staleness guard did with a sample.
type ApplyOutcome int

const (
	// ApplyStale means a strictly newer sample already owns the channel. The
	// reading arrived out of order and must be discarded.
	ApplyStale ApplyOutcome = iota

	// ApplyWritten means the twin was advanced to this reading.
	ApplyWritten

	// ApplyUnchanged means the twin already held this exact coordinate. The
	// write is refused — a replay must not rewrite the live value or make stale
	// state look freshly ingested — but the reading is current, not superseded.
	ApplyUnchanged
)

// Fresh reports whether the twin now reflects this reading, which is the
// question the ingestion loop actually needs answered.
//
// A refused write is not the same thing as a superseded reading. An attempt
// that writes the cache and then fails to publish its mutation is retried from
// the top, and its second cache write is refused as an exact replay of its
// first; treating that as staleness would drop the retry on the floor and lose
// the alarm it was carrying.
func (o ApplyOutcome) Fresh() bool { return o != ApplyStale }

// String names the outcome for logs and test failures.
func (o ApplyOutcome) String() string {
	switch o {
	case ApplyWritten:
		return "written"
	case ApplyUnchanged:
		return "unchanged"
	default:
		return "stale"
	}
}

// Apply writes one reading to twin:<asset>:<sensor> and registers the sensor in
// the asset's index. The write is refused unless the sample is strictly newer
// than the value already held, which is what keeps the twin monotonic under
// Kafka's per-partition-only ordering.
func (c *StateCache) Apply(ctx context.Context, ev TelemetryEvent) (ApplyOutcome, error) {
	keys := []string{ev.CacheKey(), assetIndexKey(ev.AssetID)}
	args := []interface{}{
		ev.Timestamp.UnixMilli(),
		ev.AssetID,
		ev.SensorID,
		strconv.FormatFloat(ev.Value, 'f', -1, 64),
		ev.Unit,
		time.Now().UTC().UnixMilli(),
		c.ttl.Milliseconds(),
	}

	code, err := applyReadingScript.Run(ctx, c.client, keys, args...).Int64()
	if err != nil {
		return ApplyStale, fmt.Errorf("apply reading %s: %w", ev.CacheKey(), err)
	}
	switch code {
	case 1:
		return ApplyWritten, nil
	case 2:
		return ApplyUnchanged, nil
	default:
		return ApplyStale, nil
	}
}

// MarkAlarm records the mutated alarm state for a twin channel. This is the
// state mutation an operator sees when inspecting the live twin, independent
// of the mutation event published to Kafka.
func (c *StateCache) MarkAlarm(ctx context.Context, ev TelemetryEvent, t Transition, eventID string) error {
	key := alarmKey(ev.AssetID, ev.SensorID)

	if t.Kind == TransitionCleared {
		if err := c.client.Del(ctx, key).Err(); err != nil {
			return fmt.Errorf("clear alarm %s: %w", key, err)
		}
		return nil
	}

	fields := map[string]interface{}{
		"asset_id":     ev.AssetID,
		"sensor_id":    ev.SensorID,
		"severity":     string(t.Severity),
		"transition":   string(t.Kind),
		"event_id":     eventID,
		"active_since": t.ActiveSince.UTC().Format(time.RFC3339Nano),
		"breach_count": strconv.FormatUint(t.BreachCount, 10),
		"value":        strconv.FormatFloat(ev.Value, 'f', -1, 64),
		"updated_at":   time.Now().UTC().Format(time.RFC3339Nano),
	}

	pipe := c.client.TxPipeline()
	pipe.HSet(ctx, key, fields)
	pipe.Expire(ctx, key, c.ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("mark alarm %s: %w", key, err)
	}
	return nil
}

// Snapshot reads every live sensor value for an asset via the sensor index,
// never SCAN or KEYS. Index entries whose hash has expired are pruned on read,
// which keeps the index self-healing.
func (c *StateCache) Snapshot(ctx context.Context, assetID string, now time.Time) ([]SensorReading, error) {
	sensors, err := c.client.SMembers(ctx, assetIndexKey(assetID)).Result()
	if err != nil {
		return nil, fmt.Errorf("read sensor index for %s: %w", assetID, err)
	}
	if len(sensors) == 0 {
		return nil, nil
	}

	pipe := c.client.Pipeline()
	cmds := make(map[string]*redis.MapStringStringCmd, len(sensors))
	for _, sensorID := range sensors {
		cmds[sensorID] = pipe.HGetAll(ctx, twinKey(assetID, sensorID))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("read twin hashes for %s: %w", assetID, err)
	}

	readings := make([]SensorReading, 0, len(sensors))
	var expired []interface{}

	for sensorID, cmd := range cmds {
		fields, err := cmd.Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("read twin %s: %w", twinKey(assetID, sensorID), err)
		}
		if len(fields) == 0 {
			expired = append(expired, sensorID)
			continue
		}

		value, err := strconv.ParseFloat(fields["value"], 64)
		if err != nil {
			c.log.Warn("discarding twin field with unparseable value",
				"asset_id", assetID, "sensor_id", sensorID, "raw", fields["value"])
			continue
		}
		observedMillis, _ := strconv.ParseInt(fields["observed_at_ms"], 10, 64)
		observedAt := time.UnixMilli(observedMillis).UTC()

		readings = append(readings, SensorReading{
			SensorID:   sensorID,
			Value:      value,
			Unit:       fields["unit"],
			ObservedAt: observedAt,
			AgeSeconds: round(now.Sub(observedAt).Seconds(), 3),
		})
	}

	if len(expired) > 0 {
		if err := c.client.SRem(ctx, assetIndexKey(assetID), expired...).Err(); err != nil {
			c.log.Warn("failed to prune expired sensors from index", "asset_id", assetID, "error", err)
		}
	}

	sort.Slice(readings, func(i, j int) bool { return readings[i].SensorID < readings[j].SensorID })
	return readings, nil
}

// withRetry runs op with exponential backoff and full jitter. It aborts
// immediately if the context is done so shutdown is never delayed by retries.
func withRetry(ctx context.Context, attempts int, base time.Duration, op func(context.Context) error) error {
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return lastErr
			}
			return err
		}

		lastErr = op(ctx)
		if lastErr == nil {
			return nil
		}
		if errors.Is(lastErr, context.Canceled) || errors.Is(lastErr, context.DeadlineExceeded) {
			return lastErr
		}
		if attempt == attempts-1 {
			break
		}

		backoff := base * time.Duration(1<<attempt)
		if backoff > 5*time.Second {
			backoff = 5 * time.Second
		}
		//nolint:gosec // jitter does not need a cryptographic source
		sleep := time.Duration(rand.Int63n(int64(backoff)) + int64(backoff)/2)

		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return lastErr
		case <-timer.C:
		}
	}
	return lastErr
}
