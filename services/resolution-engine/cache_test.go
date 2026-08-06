package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

type cacheFixture struct {
	cache  *StateCache
	client *redis.Client
	mr     *miniredis.Miniredis
}

func newCacheFixture(t *testing.T) *cacheFixture {
	t.Helper()
	mr := miniredis.RunT(t)
	cache := newTestCache(t, mr)
	return &cacheFixture{cache: cache, client: cache.Redis(), mr: mr}
}

// applyReading is the single call site for StateCache.Apply in these tests.
func applyReading(t *testing.T, cache *StateCache, ev TelemetryEvent) ApplyOutcome {
	t.Helper()
	outcome, err := cache.Apply(context.Background(), ev)
	if err != nil {
		t.Fatalf("Apply(%s @ %s): %v", ev.CacheKey(), ev.Timestamp.Time, err)
	}
	return outcome
}

func (f *cacheFixture) hash(t *testing.T, key string) map[string]string {
	t.Helper()
	fields, err := f.client.HGetAll(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("HGETALL %s: %v", key, err)
	}
	return fields
}

func (f *cacheFixture) field(t *testing.T, key, field string) string {
	t.Helper()
	return f.hash(t, key)[field]
}

// ---------------------------------------------------------------------------
// The Lua staleness guard
// ---------------------------------------------------------------------------

// TestApplyStalenessGuard is the core monotonicity contract. Kafka only orders
// within a partition, so a replaying gateway can hand this engine an older — or
// an identical — sample after a newer one, and the twin must never move
// backwards or be rewritten in place.
func TestApplyStalenessGuard(t *testing.T) {
	const asset, sensor = "PUMP-221", SensorVibrationIndex
	key := twinKey(asset, sensor)

	seed := baseTime
	cases := []struct {
		name string
		// offset from the seeded reading's timestamp
		offset       time.Duration
		value        float64
		wantOutcome  ApplyOutcome
		wantStoredAt time.Time
		wantStored   float64
	}{
		{
			name:         "a strictly newer sample advances the twin",
			offset:       time.Second,
			value:        7.7,
			wantOutcome:  ApplyWritten,
			wantStoredAt: seed.Add(time.Second),
			wantStored:   7.7,
		},
		{
			name:         "a strictly older sample is rejected",
			offset:       -time.Second,
			value:        99.9,
			wantOutcome:  ApplyStale,
			wantStoredAt: seed,
			wantStored:   4.2,
		},
		{
			name:         "a sample one millisecond older is rejected",
			offset:       -time.Millisecond,
			value:        99.9,
			wantOutcome:  ApplyStale,
			wantStoredAt: seed,
			wantStored:   4.2,
		},
		{
			name:         "an identical coordinate is rejected",
			offset:       0,
			value:        99.9,
			wantOutcome:  ApplyUnchanged,
			wantStoredAt: seed,
			wantStored:   4.2,
		},
		{
			name:         "a sample one millisecond newer applies",
			offset:       time.Millisecond,
			value:        7.7,
			wantOutcome:  ApplyWritten,
			wantStoredAt: seed.Add(time.Millisecond),
			wantStored:   7.7,
		},
		{
			name: "sub-millisecond jitter within the same millisecond is rejected",
			// Kafka can redeliver a sample whose RFC3339Nano timestamp differs
			// below the millisecond the guard compares on.
			offset:       500 * time.Microsecond,
			value:        99.9,
			wantOutcome:  ApplyUnchanged,
			wantStoredAt: seed,
			wantStored:   4.2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newCacheFixture(t)

			if got := applyReading(t, f.cache, reading(asset, sensor, 4.2, seed)); got != ApplyWritten {
				t.Fatalf("the seeding write returned %s, want written", got)
			}
			ingestedAtSeed := f.field(t, key, "ingested_at_ms")

			outcome := applyReading(t, f.cache, reading(asset, sensor, tc.value, seed.Add(tc.offset)))
			if outcome != tc.wantOutcome {
				t.Errorf("Apply = %s, want %s", outcome, tc.wantOutcome)
			}

			fields := f.hash(t, key)
			gotAt, err := strconv.ParseInt(fields["observed_at_ms"], 10, 64)
			if err != nil {
				t.Fatalf("observed_at_ms %q is not an integer: %v", fields["observed_at_ms"], err)
			}
			if got := time.UnixMilli(gotAt).UTC(); !got.Equal(tc.wantStoredAt) {
				t.Errorf("stored observed_at = %s, want %s", got, tc.wantStoredAt)
			}
			if got, _ := strconv.ParseFloat(fields["value"], 64); got != tc.wantStored {
				t.Errorf("stored value = %g, want %g", got, tc.wantStored)
			}

			// A refused write must leave the record completely untouched — the
			// ingest stamp included, otherwise a replay silently makes stale
			// state look freshly observed.
			if tc.wantOutcome != ApplyWritten && fields["ingested_at_ms"] != ingestedAtSeed {
				t.Errorf("a refused write rewrote ingested_at_ms (%s -> %s)",
					ingestedAtSeed, fields["ingested_at_ms"])
			}
		})
	}
}

// TestApplyDistinguishesStaleFromReplayed pins the distinction the ingestion
// loop depends on. Both outcomes refuse the write, but only one of them means
// "stop processing this message".
//
// An attempt that writes the cache and then fails downstream is retried from
// the top of process(); its second cache write is an exact replay of its first.
// If that were reported as staleness the retry would return early and the alarm
// it was carrying would never be published.
func TestApplyDistinguishesStaleFromReplayed(t *testing.T) {
	f := newCacheFixture(t)
	const asset, sensor = "PUMP-221", SensorVibrationIndex

	ev := reading(asset, sensor, 9.0, baseTime)
	if got := applyReading(t, f.cache, ev); got != ApplyWritten {
		t.Fatalf("first write = %s, want written", got)
	}

	replay := applyReading(t, f.cache, ev)
	if replay != ApplyUnchanged {
		t.Fatalf("replaying the same sample = %s, want unchanged", replay)
	}
	if !replay.Fresh() {
		t.Fatal("an exact replay was reported as stale; a retry after a failed publish would be dropped")
	}

	stale := applyReading(t, f.cache, reading(asset, sensor, 9.0, baseTime.Add(-time.Millisecond)))
	if stale != ApplyStale {
		t.Fatalf("an older sample = %s, want stale", stale)
	}
	if stale.Fresh() {
		t.Fatal("an out-of-order sample was reported as fresh")
	}

	for outcome, want := range map[ApplyOutcome]string{
		ApplyWritten:   "written",
		ApplyUnchanged: "unchanged",
		ApplyStale:     "stale",
	} {
		if got := outcome.String(); got != want {
			t.Errorf("ApplyOutcome(%d).String() = %q, want %q", outcome, got, want)
		}
	}
}

// TestApplyIsMonotonicUnderShuffledDelivery replays a burst out of order and
// asserts the twin ends up holding the newest sample, whatever order it arrived.
func TestApplyIsMonotonicUnderShuffledDelivery(t *testing.T) {
	f := newCacheFixture(t)
	const asset, sensor = "PUMP-221", SensorVibrationIndex

	// Deliberately shuffled offsets, newest (9) delivered in the middle.
	offsets := []int{3, 7, 1, 9, 0, 8, 2, 9, 4}
	for _, offset := range offsets {
		applyReading(t, f.cache, reading(asset, sensor, float64(offset), baseTime.Add(time.Duration(offset)*time.Second)))
	}

	fields := f.hash(t, twinKey(asset, sensor))
	gotAt, _ := strconv.ParseInt(fields["observed_at_ms"], 10, 64)
	if want := baseTime.Add(9 * time.Second).UnixMilli(); gotAt != want {
		t.Fatalf("observed_at_ms = %d, want %d (the newest sample)", gotAt, want)
	}
	if got, _ := strconv.ParseFloat(fields["value"], 64); got != 9 {
		t.Fatalf("stored value = %g, want 9 — the twin holds a value from a different sample", got)
	}
}

// TestApplyConcurrentWritersCannotLoseAnUpdate hammers one twin channel from
// many goroutines. The guard is a compare-and-set inside a Lua script, so no
// interleaving may leave the hash holding a timestamp from one sample and a
// value from another, and the newest sample must always win.
func TestApplyConcurrentWritersCannotLoseAnUpdate(t *testing.T) {
	const (
		asset      = "PUMP-221"
		sensor     = SensorVibrationIndex
		goroutines = 32
		perWriter  = 40
	)

	f := newCacheFixture(t)
	key := twinKey(asset, sensor)

	// valueFor pairs a value with its timestamp so a torn write is detectable:
	// any hash holding value != valueFor(observed_at_ms) is a lost update.
	valueFor := func(offset int) float64 { return float64(offset) * 1.5 }

	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			<-start
			for i := 0; i < perWriter; i++ {
				// Writers interleave over the same timestamp space, so every
				// goroutine races every other on the same coordinates.
				offset := (g*7 + i*13) % (goroutines * perWriter)
				ev := reading(asset, sensor, valueFor(offset), baseTime.Add(time.Duration(offset)*time.Millisecond))
				if _, err := f.cache.Apply(context.Background(), ev); err != nil {
					t.Errorf("Apply: %v", err)
					return
				}
			}
		}(g)
	}
	close(start)
	wg.Wait()

	fields := f.hash(t, key)
	gotAt, err := strconv.ParseInt(fields["observed_at_ms"], 10, 64)
	if err != nil {
		t.Fatalf("observed_at_ms %q is not an integer: %v", fields["observed_at_ms"], err)
	}
	gotValue, err := strconv.ParseFloat(fields["value"], 64)
	if err != nil {
		t.Fatalf("value %q is not a float: %v", fields["value"], err)
	}

	offset := int(gotAt - baseTime.UnixMilli())
	if want := valueFor(offset); gotValue != want {
		t.Fatalf("lost update: hash holds observed_at_ms=%d (offset %d) with value %g, want %g",
			gotAt, offset, gotValue, want)
	}

	// Every writer covered offsets below goroutines*perWriter; the highest one
	// any of them produced must be the one left standing.
	var highest int
	for g := 0; g < goroutines; g++ {
		for i := 0; i < perWriter; i++ {
			if o := (g*7 + i*13) % (goroutines * perWriter); o > highest {
				highest = o
			}
		}
	}
	if offset != highest {
		t.Fatalf("twin settled on offset %d, want the newest offset %d", offset, highest)
	}
}

// TestApplyMaintainsSensorIndexAndTTL covers the half of the script that makes
// Snapshot possible without SCAN or KEYS.
func TestApplyMaintainsSensorIndexAndTTL(t *testing.T) {
	f := newCacheFixture(t)
	ctx := context.Background()
	const asset = "PUMP-221"

	applyReading(t, f.cache, reading(asset, SensorVibrationIndex, 9.0, baseTime))
	applyReading(t, f.cache, reading(asset, SensorTemperatureCelsius, 88.0, baseTime))
	applyReading(t, f.cache, reading("OTHER-ASSET", SensorVibrationIndex, 1.0, baseTime))

	members, err := f.client.SMembers(ctx, assetIndexKey(asset)).Result()
	if err != nil {
		t.Fatalf("SMEMBERS: %v", err)
	}
	if len(members) != 2 {
		t.Fatalf("sensor index holds %v, want both sensors of %s only", members, asset)
	}

	// Re-applying an existing sensor must not duplicate the index entry.
	applyReading(t, f.cache, reading(asset, SensorVibrationIndex, 9.5, baseTime.Add(time.Second)))
	if n, _ := f.client.SCard(ctx, assetIndexKey(asset)).Result(); n != 2 {
		t.Fatalf("sensor index grew to %d entries on re-apply, want 2", n)
	}

	for _, key := range []string{twinKey(asset, SensorVibrationIndex), assetIndexKey(asset)} {
		ttl, err := f.client.PTTL(ctx, key).Result()
		if err != nil {
			t.Fatalf("PTTL %s: %v", key, err)
		}
		if ttl <= 0 || ttl > time.Hour {
			t.Fatalf("PTTL %s = %s, want a positive TTL bounded by the configured hour", key, ttl)
		}
	}
}

// TestApplyStoresFullPrecisionValues guards the float formatting: the twin is
// what an operator reads, so a value must survive the round trip intact.
func TestApplyStoresFullPrecisionValues(t *testing.T) {
	f := newCacheFixture(t)

	for i, value := range []float64{0, -273.15, 8.5, 1e-9, 1234567.891234567, 1e21} {
		ev := reading("PUMP-221", SensorVibrationIndex, value, baseTime.Add(time.Duration(i)*time.Second))
		applyReading(t, f.cache, ev)

		raw := f.field(t, ev.CacheKey(), "value")
		got, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			t.Fatalf("stored %q for %g, which does not parse: %v", raw, value, err)
		}
		if got != value {
			t.Fatalf("round trip of %g produced %g (stored as %q)", value, got, raw)
		}
	}
}

func TestApplyReportsRedisFailure(t *testing.T) {
	f := newCacheFixture(t)
	f.mr.Close()

	if _, err := f.cache.Apply(context.Background(), reading("PUMP-221", SensorVibrationIndex, 9.0, baseTime)); err == nil {
		t.Fatal("Apply succeeded against a closed Redis, want an error so the caller retries")
	}
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

func TestSnapshot(t *testing.T) {
	ctx := context.Background()
	const asset = "PUMP-221"
	now := baseTime.Add(10 * time.Second)

	t.Run("returns every live sensor, sorted, with ages", func(t *testing.T) {
		f := newCacheFixture(t)
		applyReading(t, f.cache, reading(asset, SensorVibrationIndex, 9.0, baseTime))
		applyReading(t, f.cache, reading(asset, SensorTemperatureCelsius, 88.0, baseTime.Add(4*time.Second)))

		readings, err := f.cache.Snapshot(ctx, asset, now)
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		if len(readings) != 2 {
			t.Fatalf("Snapshot returned %d readings, want 2", len(readings))
		}
		if readings[0].SensorID != SensorTemperatureCelsius || readings[1].SensorID != SensorVibrationIndex {
			t.Fatalf("Snapshot is not sorted by sensor id: %+v", readings)
		}
		if readings[1].Value != 9.0 || readings[1].AgeSeconds != 10 {
			t.Fatalf("vibration reading = %+v, want value 9 aged 10s", readings[1])
		}
		if readings[0].AgeSeconds != 6 {
			t.Fatalf("temperature age = %g, want 6", readings[0].AgeSeconds)
		}
		if readings[1].Unit != "mm/s" {
			t.Fatalf("unit = %q, want mm/s", readings[1].Unit)
		}
	})

	t.Run("no index yields no readings and no error", func(t *testing.T) {
		f := newCacheFixture(t)
		readings, err := f.cache.Snapshot(ctx, "NEVER-SEEN", now)
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		if len(readings) != 0 {
			t.Fatalf("Snapshot returned %+v, want nothing", readings)
		}
	})

	t.Run("prunes index entries whose twin hash has expired", func(t *testing.T) {
		f := newCacheFixture(t)
		applyReading(t, f.cache, reading(asset, SensorVibrationIndex, 9.0, baseTime))
		applyReading(t, f.cache, reading(asset, SensorTemperatureCelsius, 88.0, baseTime))

		// Evict one twin hash while leaving the index entry behind.
		if err := f.client.Del(ctx, twinKey(asset, SensorTemperatureCelsius)).Err(); err != nil {
			t.Fatalf("DEL: %v", err)
		}

		readings, err := f.cache.Snapshot(ctx, asset, now)
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		if len(readings) != 1 || readings[0].SensorID != SensorVibrationIndex {
			t.Fatalf("Snapshot returned %+v, want only the live sensor", readings)
		}

		members, _ := f.client.SMembers(ctx, assetIndexKey(asset)).Result()
		if len(members) != 1 || members[0] != SensorVibrationIndex {
			t.Fatalf("index holds %v after the read, want the dead sensor pruned", members)
		}
	})

	t.Run("skips a twin whose value is unparseable", func(t *testing.T) {
		f := newCacheFixture(t)
		applyReading(t, f.cache, reading(asset, SensorVibrationIndex, 9.0, baseTime))
		applyReading(t, f.cache, reading(asset, SensorTemperatureCelsius, 88.0, baseTime))

		if err := f.client.HSet(ctx, twinKey(asset, SensorTemperatureCelsius), "value", "n/a").Err(); err != nil {
			t.Fatalf("HSET: %v", err)
		}

		readings, err := f.cache.Snapshot(ctx, asset, now)
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		if len(readings) != 1 || readings[0].SensorID != SensorVibrationIndex {
			t.Fatalf("Snapshot returned %+v, want the corrupt reading discarded and the rest kept", readings)
		}
	})

	t.Run("reports a Redis failure so the payload can degrade", func(t *testing.T) {
		f := newCacheFixture(t)
		applyReading(t, f.cache, reading(asset, SensorVibrationIndex, 9.0, baseTime))
		f.mr.Close()

		if _, err := f.cache.Snapshot(ctx, asset, now); err == nil {
			t.Fatal("Snapshot succeeded against a closed Redis, want an error")
		}
	})
}

// ---------------------------------------------------------------------------
// MarkAlarm
// ---------------------------------------------------------------------------

func TestMarkAlarm(t *testing.T) {
	ctx := context.Background()
	ev := reading("PUMP-221", SensorVibrationIndex, 9.4, baseTime)
	key := alarmKey(ev.AssetID, ev.SensorID)

	t.Run("records the alarm and gives it a TTL", func(t *testing.T) {
		f := newCacheFixture(t)
		transition := Transition{
			Kind:        TransitionRaised,
			Severity:    SeverityHigh,
			ActiveSince: baseTime,
			BreachCount: 3,
		}
		if err := f.cache.MarkAlarm(ctx, ev, transition, "evt_test"); err != nil {
			t.Fatalf("MarkAlarm: %v", err)
		}

		fields := f.hash(t, key)
		want := map[string]string{
			"asset_id":     "PUMP-221",
			"sensor_id":    SensorVibrationIndex,
			"severity":     "HIGH",
			"transition":   "RAISED",
			"event_id":     "evt_test",
			"active_since": baseTime.Format(time.RFC3339Nano),
			"breach_count": "3",
			"value":        "9.4",
		}
		for field, wantValue := range want {
			if fields[field] != wantValue {
				t.Errorf("%s = %q, want %q", field, fields[field], wantValue)
			}
		}
		if fields["updated_at"] == "" {
			t.Error("updated_at was not written")
		}
		if ttl, _ := f.client.PTTL(ctx, key).Result(); ttl <= 0 {
			t.Errorf("PTTL = %s, want a positive TTL", ttl)
		}
	})

	t.Run("a CLEARED transition deletes the record", func(t *testing.T) {
		f := newCacheFixture(t)
		if err := f.cache.MarkAlarm(ctx, ev, Transition{Kind: TransitionRaised, Severity: SeverityHigh}, "evt_1"); err != nil {
			t.Fatalf("MarkAlarm: %v", err)
		}
		if err := f.cache.MarkAlarm(ctx, ev, Transition{Kind: TransitionCleared, Severity: SeverityInfo}, "evt_2"); err != nil {
			t.Fatalf("MarkAlarm(CLEARED): %v", err)
		}
		if n, _ := f.client.Exists(ctx, key).Result(); n != 0 {
			t.Fatal("the alarm record survived a CLEARED transition")
		}
	})

	t.Run("clearing an alarm that was never raised is not an error", func(t *testing.T) {
		f := newCacheFixture(t)
		if err := f.cache.MarkAlarm(ctx, ev, Transition{Kind: TransitionCleared}, "evt_1"); err != nil {
			t.Fatalf("MarkAlarm(CLEARED) on a missing key: %v", err)
		}
	})

	t.Run("reports a Redis failure", func(t *testing.T) {
		f := newCacheFixture(t)
		f.mr.Close()
		if err := f.cache.MarkAlarm(ctx, ev, Transition{Kind: TransitionRaised}, "evt_1"); err == nil {
			t.Fatal("MarkAlarm succeeded against a closed Redis")
		}
	})
}

func TestPingReportsConnectivity(t *testing.T) {
	f := newCacheFixture(t)
	if err := f.cache.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	f.mr.Close()
	if err := f.cache.Ping(context.Background()); err == nil {
		t.Fatal("Ping succeeded against a closed Redis, want the readiness probe to fail")
	}
}

func TestNewStateCacheFailsFastWhenRedisIsUnreachable(t *testing.T) {
	cfg := testConfig("127.0.0.1:1")
	cfg.OpTimeout = 500 * time.Millisecond

	cache, err := NewStateCache(context.Background(), cfg, discardLogger())
	if err == nil {
		_ = cache.Close()
		t.Fatal("NewStateCache succeeded against a dead endpoint, want a startup failure")
	}
	if cache != nil {
		t.Fatal("NewStateCache returned a cache alongside an error")
	}
}

// ---------------------------------------------------------------------------
// withRetry
// ---------------------------------------------------------------------------

func TestWithRetry(t *testing.T) {
	errBoom := errors.New("boom")

	t.Run("returns on the first success", func(t *testing.T) {
		calls := 0
		err := withRetry(context.Background(), 4, time.Millisecond, func(context.Context) error {
			calls++
			return nil
		})
		if err != nil || calls != 1 {
			t.Fatalf("err = %v after %d calls, want nil after 1", err, calls)
		}
	})

	t.Run("retries up to the attempt budget then returns the last error", func(t *testing.T) {
		calls := 0
		err := withRetry(context.Background(), 4, time.Millisecond, func(context.Context) error {
			calls++
			return fmt.Errorf("attempt %d: %w", calls, errBoom)
		})
		if calls != 4 {
			t.Fatalf("op ran %d times, want 4", calls)
		}
		if !errors.Is(err, errBoom) {
			t.Fatalf("err = %v, want it to wrap the last failure", err)
		}
	})

	t.Run("succeeds on a later attempt", func(t *testing.T) {
		calls := 0
		err := withRetry(context.Background(), 4, time.Millisecond, func(context.Context) error {
			calls++
			if calls < 3 {
				return errBoom
			}
			return nil
		})
		if err != nil || calls != 3 {
			t.Fatalf("err = %v after %d calls, want nil after 3", err, calls)
		}
	})

	t.Run("does not retry a context error", func(t *testing.T) {
		calls := 0
		err := withRetry(context.Background(), 4, time.Millisecond, func(context.Context) error {
			calls++
			return context.DeadlineExceeded
		})
		if calls != 1 {
			t.Fatalf("op ran %d times, want 1 — a context error must not be retried", calls)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want context.DeadlineExceeded", err)
		}
	})

	t.Run("a cancelled context aborts before the first attempt", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		calls := 0
		err := withRetry(ctx, 4, time.Millisecond, func(context.Context) error {
			calls++
			return nil
		})
		if calls != 0 {
			t.Fatalf("op ran %d times against a cancelled context, want 0", calls)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	})

	t.Run("a cancellation mid-flight surfaces the operation's error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		err := withRetry(ctx, 8, 50*time.Millisecond, func(context.Context) error {
			calls++
			cancel()
			return errBoom
		})
		if !errors.Is(err, errBoom) {
			t.Fatalf("err = %v, want the operation's own failure rather than the cancellation", err)
		}
		if calls != 1 {
			t.Fatalf("op ran %d times, want 1", calls)
		}
	})
}
