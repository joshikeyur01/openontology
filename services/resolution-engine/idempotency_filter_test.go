package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

type dedupeFixture struct {
	engine *IdempotencyEngine
	cache  *StateCache
	mr     *miniredis.Miniredis
}

func newDedupeFixture(t *testing.T, mutate func(*IdempotencyConfig)) *dedupeFixture {
	t.Helper()
	cfg := testIdempotencyConfig()
	if mutate != nil {
		mutate(&cfg)
	}
	mr := miniredis.RunT(t)
	cache := newTestCache(t, mr)
	return &dedupeFixture{engine: newTestDedupe(t, cache, cfg), cache: cache, mr: mr}
}

// claimKey renders the Redis key the filter uses for an event under the
// fixture's configured scope.
func (f *dedupeFixture) claimKey(t *testing.T, ev TelemetryEvent) string {
	t.Helper()
	fingerprint, err := f.engine.fingerprintFor(ev)
	if err != nil {
		t.Fatalf("fingerprintFor(%+v): %v", ev, err)
	}
	return f.engine.Key(fingerprint)
}

func (f *dedupeFixture) claimExists(t *testing.T, ev TelemetryEvent) bool {
	t.Helper()
	n, err := f.cache.Redis().Exists(context.Background(), f.claimKey(t, ev)).Result()
	if err != nil {
		t.Fatalf("EXISTS: %v", err)
	}
	return n == 1
}

func mustAdmit(t *testing.T, e *IdempotencyEngine, ev TelemetryEvent) bool {
	t.Helper()
	admitted, err := e.Admit(context.Background(), ev)
	if err != nil {
		t.Fatalf("Admit(%s/%s): %v", ev.AssetID, ev.SensorID, err)
	}
	return admitted
}

// ---------------------------------------------------------------------------
// Claim and release
// ---------------------------------------------------------------------------

// TestAdmitClaimsOnceAndSuppressesRedeliveries is the filter's whole purpose:
// one physical sample delivered N times costs one downstream inference.
func TestAdmitClaimsOnceAndSuppressesRedeliveries(t *testing.T) {
	f := newDedupeFixture(t, nil)
	ev := reading("PUMP-221", SensorVibrationIndex, 9.0, baseTime)

	if !mustAdmit(t, f.engine, ev) {
		t.Fatal("the first delivery was not admitted")
	}
	if !f.claimExists(t, ev) {
		t.Fatalf("no claim was taken at %s", f.claimKey(t, ev))
	}
	for i := 0; i < 5; i++ {
		if mustAdmit(t, f.engine, ev) {
			t.Fatalf("redelivery %d was admitted, want it discarded as a duplicate", i+1)
		}
	}

	stats := f.engine.Stats()
	if stats.Checked != 6 || stats.Admitted != 1 || stats.Duplicates != 5 {
		t.Fatalf("Stats() = checked %d / admitted %d / duplicates %d, want 6 / 1 / 5",
			stats.Checked, stats.Admitted, stats.Duplicates)
	}
	if stats.DuplicateRatio != round(5.0/6.0, 6) {
		t.Fatalf("DuplicateRatio = %g, want %g", stats.DuplicateRatio, round(5.0/6.0, 6))
	}
}

// TestDistinctCoordinatesAreIndependent guards against a fingerprint that is
// too coarse: two different samples must never share a claim.
func TestDistinctCoordinatesAreIndependent(t *testing.T) {
	f := newDedupeFixture(t, nil)
	base := reading("PUMP-221", SensorVibrationIndex, 9.0, baseTime)

	variants := map[string]TelemetryEvent{
		"different asset":     reading("PUMP-222", SensorVibrationIndex, 9.0, baseTime),
		"different sensor":    reading("PUMP-221", SensorTemperatureCelsius, 9.0, baseTime),
		"different timestamp": reading("PUMP-221", SensorVibrationIndex, 9.0, baseTime.Add(time.Millisecond)),
	}

	if !mustAdmit(t, f.engine, base) {
		t.Fatal("the base event was not admitted")
	}
	for name, ev := range variants {
		if !mustAdmit(t, f.engine, ev) {
			t.Errorf("%s was suppressed as a duplicate of the base event", name)
		}
	}

	// A different *value* at the same coordinate is still the same coordinate:
	// the fingerprint deliberately excludes the reading itself, so a gateway
	// resending a corrected value does not slip past the filter.
	corrected := reading("PUMP-221", SensorVibrationIndex, 12.0, baseTime)
	if mustAdmit(t, f.engine, corrected) {
		t.Error("a re-send at the same coordinate with a different value was admitted")
	}
}

// TestScopeAssetAliasesSensorsSharingATimestamp pins the trade-off documented
// on ScopeAsset: with one reading per message, ScopeAsset discards the second
// sensor of an asset. That is why ScopeAssetSensor is the default.
func TestScopeAssetAliasesSensorsSharingATimestamp(t *testing.T) {
	vibration := reading("PUMP-221", SensorVibrationIndex, 9.0, baseTime)
	temperature := reading("PUMP-221", SensorTemperatureCelsius, 120.0, baseTime)

	t.Run("asset scope aliases them", func(t *testing.T) {
		f := newDedupeFixture(t, func(c *IdempotencyConfig) { c.Scope = ScopeAsset })
		if !mustAdmit(t, f.engine, vibration) {
			t.Fatal("the first sensor was not admitted")
		}
		if mustAdmit(t, f.engine, temperature) {
			t.Fatal("test premise broken: ScopeAsset admitted a second sensor at the same timestamp")
		}
	})

	t.Run("asset_sensor scope keeps them apart", func(t *testing.T) {
		f := newDedupeFixture(t, func(c *IdempotencyConfig) { c.Scope = ScopeAssetSensor })
		if !mustAdmit(t, f.engine, vibration) || !mustAdmit(t, f.engine, temperature) {
			t.Fatal("ScopeAssetSensor suppressed a legitimate second sensor")
		}
	})
}

// TestReleaseEventRestoresRedeliverability is the rollback contract: a claim
// that outlives a failed attempt turns a transient error into data loss.
func TestReleaseEventRestoresRedeliverability(t *testing.T) {
	f := newDedupeFixture(t, nil)
	ctx := context.Background()
	ev := reading("PUMP-221", SensorVibrationIndex, 9.0, baseTime)

	if !mustAdmit(t, f.engine, ev) {
		t.Fatal("the first delivery was not admitted")
	}
	if mustAdmit(t, f.engine, ev) {
		t.Fatal("the redelivery was admitted before the claim was released")
	}

	if err := f.engine.ReleaseEvent(ctx, ev); err != nil {
		t.Fatalf("ReleaseEvent: %v", err)
	}
	if f.claimExists(t, ev) {
		t.Fatal("the claim key survived ReleaseEvent")
	}
	if !mustAdmit(t, f.engine, ev) {
		t.Fatal("the redelivery was still suppressed after the claim was released")
	}
	if got := f.engine.Stats().Released; got != 1 {
		t.Fatalf("Stats().Released = %d, want 1", got)
	}
}

// TestReleaseSurvivesACancelledContext: the rollback runs on the way out of a
// failed attempt, often while the process is already shutting down.
func TestReleaseSurvivesACancelledContext(t *testing.T) {
	f := newDedupeFixture(t, nil)
	ev := reading("PUMP-221", SensorVibrationIndex, 9.0, baseTime)

	if !mustAdmit(t, f.engine, ev) {
		t.Fatal("the first delivery was not admitted")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.engine.ReleaseEvent(ctx, ev); err != nil {
		t.Fatalf("ReleaseEvent with a cancelled context: %v", err)
	}
	if f.claimExists(t, ev) {
		t.Fatal("a cancelled context left the claim in place, blocking redelivery for the rest of the window")
	}
}

func TestReleaseOfAnUnclaimedCoordinateIsNotAnError(t *testing.T) {
	f := newDedupeFixture(t, nil)
	if err := f.engine.ReleaseEvent(context.Background(), reading("PUMP-221", SensorVibrationIndex, 9.0, baseTime)); err != nil {
		t.Fatalf("ReleaseEvent on an unclaimed coordinate: %v", err)
	}
	if got := f.engine.Stats().Released; got != 0 {
		t.Fatalf("Stats().Released = %d, want 0 — nothing was actually released", got)
	}
}

// TestCoordinateAPIClaimsAndReleases covers the (asset, timestamp) entry points
// used by callers that hold a coordinate rather than a decoded event. They
// always fingerprint on the asset alone, whatever scope is configured.
func TestCoordinateAPIClaimsAndReleases(t *testing.T) {
	f := newDedupeFixture(t, nil)
	ctx := context.Background()
	const asset = "PUMP-221"
	const timestamp int64 = 1755079800000

	duplicate, err := f.engine.IsDuplicate(ctx, asset, timestamp)
	if err != nil {
		t.Fatalf("IsDuplicate: %v", err)
	}
	if duplicate {
		t.Fatal("the first claim reported a duplicate")
	}

	if duplicate, err = f.engine.IsDuplicate(ctx, asset, timestamp); err != nil || !duplicate {
		t.Fatalf("IsDuplicate = %t (err %v), want true", duplicate, err)
	}

	if err := f.engine.Release(ctx, asset, timestamp); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if duplicate, err = f.engine.IsDuplicate(ctx, asset, timestamp); err != nil || duplicate {
		t.Fatalf("IsDuplicate = %t (err %v) after Release, want false", duplicate, err)
	}

	if _, err := f.engine.IsDuplicate(ctx, "", timestamp); !errors.Is(err, ErrIdempotencyInvalidInput) {
		t.Fatalf("IsDuplicate on an empty asset = %v, want ErrIdempotencyInvalidInput", err)
	}
	if err := f.engine.Release(ctx, asset, 0); !errors.Is(err, ErrIdempotencyInvalidInput) {
		t.Fatalf("Release on a zero timestamp = %v, want ErrIdempotencyInvalidInput", err)
	}
}

func TestNewDedicatedIdempotencyEngine(t *testing.T) {
	t.Run("dials its own pool and owns it", func(t *testing.T) {
		mr := miniredis.RunT(t)
		base := testConfig(mr.Addr())

		engine, err := NewDedicatedIdempotencyEngine(context.Background(), base, testIdempotencyConfig(), discardLogger())
		if err != nil {
			t.Fatalf("NewDedicatedIdempotencyEngine: %v", err)
		}
		if err := engine.Ping(context.Background()); err != nil {
			t.Fatalf("Ping: %v", err)
		}
		if err := engine.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := engine.Ping(context.Background()); err == nil {
			t.Fatal("the pool survived Close, so the engine does not own it")
		}
	})

	t.Run("fails fast when Redis is unreachable", func(t *testing.T) {
		base := testConfig("127.0.0.1:1")
		cfg := testIdempotencyConfig()
		cfg.OpTimeout = 500 * time.Millisecond

		engine, err := NewDedicatedIdempotencyEngine(context.Background(), base, cfg, discardLogger())
		if err == nil {
			_ = engine.Close()
			t.Fatal("NewDedicatedIdempotencyEngine succeeded against a dead endpoint")
		}
		if engine != nil {
			t.Fatal("an engine was returned alongside the error")
		}
	})

	t.Run("rejects an invalid configuration before dialling", func(t *testing.T) {
		cfg := testIdempotencyConfig()
		cfg.Scope = "nonsense"
		if _, err := NewDedicatedIdempotencyEngine(context.Background(), testConfig("127.0.0.1:1"), cfg, discardLogger()); !errors.Is(err, ErrIdempotencyInvalidConfig) {
			t.Fatalf("err = %v, want ErrIdempotencyInvalidConfig", err)
		}
	})
}

// ---------------------------------------------------------------------------
// TTL
// ---------------------------------------------------------------------------

// TestClaimExpiresAfterTTL: the window must be narrow enough that an asset
// legitimately re-reporting the same coordinate later is not swallowed.
func TestClaimExpiresAfterTTL(t *testing.T) {
	f := newDedupeFixture(t, func(c *IdempotencyConfig) { c.TTL = 5 * time.Second })
	ev := reading("PUMP-221", SensorVibrationIndex, 9.0, baseTime)

	if !mustAdmit(t, f.engine, ev) {
		t.Fatal("the first delivery was not admitted")
	}

	ttl, err := f.cache.Redis().PTTL(context.Background(), f.claimKey(t, ev)).Result()
	if err != nil {
		t.Fatalf("PTTL: %v", err)
	}
	if ttl <= 0 || ttl > 5*time.Second {
		t.Fatalf("claim TTL = %s, want (0, 5s]", ttl)
	}

	f.mr.FastForward(4 * time.Second)
	if mustAdmit(t, f.engine, ev) {
		t.Fatal("a redelivery inside the window was admitted")
	}

	f.mr.FastForward(2 * time.Second)
	if !mustAdmit(t, f.engine, ev) {
		t.Fatal("the coordinate was still suppressed after the window elapsed")
	}
	if !f.claimExists(t, ev) {
		t.Fatal("re-admission did not take a fresh claim")
	}
}

// ---------------------------------------------------------------------------
// Failure policy
// ---------------------------------------------------------------------------

// TestAdmitAppliesTheFailurePolicy covers what happens when Redis itself is
// gone. Fail-open accepts a possible duplicate; fail-closed refuses the event
// and lets the caller retry and eventually dead-letter it.
func TestAdmitAppliesTheFailurePolicy(t *testing.T) {
	ev := reading("PUMP-221", SensorVibrationIndex, 9.0, baseTime)

	t.Run("fail open admits and records the outage", func(t *testing.T) {
		f := newDedupeFixture(t, func(c *IdempotencyConfig) { c.FailOpen = true })
		f.mr.Close()

		admitted, err := f.engine.Admit(context.Background(), ev)
		if err != nil {
			t.Fatalf("Admit returned %v, want a fail-open admission", err)
		}
		if !admitted {
			t.Fatal("Admit discarded an event because Redis was unreachable; dropping telemetry is worse than a duplicate")
		}

		stats := f.engine.Stats()
		if stats.FailedOpen != 1 {
			t.Errorf("Stats().FailedOpen = %d, want 1", stats.FailedOpen)
		}
		if stats.Admitted != 1 {
			t.Errorf("Stats().Admitted = %d, want 1", stats.Admitted)
		}
		if stats.Errors == 0 {
			t.Error("Stats().Errors = 0, want the outage counted")
		}
	})

	t.Run("fail closed rejects and surfaces the error", func(t *testing.T) {
		f := newDedupeFixture(t, func(c *IdempotencyConfig) { c.FailOpen = false })
		f.mr.Close()

		admitted, err := f.engine.Admit(context.Background(), ev)
		if err == nil {
			t.Fatal("Admit succeeded against a closed Redis with FailOpen=false")
		}
		if admitted {
			t.Fatal("Admit admitted the event despite failing closed")
		}
		if errors.Is(err, ErrIdempotencyInvalidInput) {
			t.Fatal("an outage was reported as invalid input; the caller would dead-letter a good message")
		}
		if got := f.engine.Stats().FailedOpen; got != 0 {
			t.Errorf("Stats().FailedOpen = %d, want 0", got)
		}
	})

	t.Run("a malformed coordinate is reported as invalid input even when failing open", func(t *testing.T) {
		f := newDedupeFixture(t, func(c *IdempotencyConfig) { c.FailOpen = true })

		// A timestamp of exactly the epoch cannot identify an event.
		ev := reading("PUMP-221", SensorVibrationIndex, 9.0, time.UnixMilli(0))
		admitted, err := f.engine.Admit(context.Background(), ev)
		if !errors.Is(err, ErrIdempotencyInvalidInput) {
			t.Fatalf("Admit error = %v, want ErrIdempotencyInvalidInput so the caller dead-letters it", err)
		}
		if admitted {
			t.Fatal("a malformed coordinate was admitted")
		}
	})

	t.Run("a cancelled context is not treated as an outage", func(t *testing.T) {
		f := newDedupeFixture(t, func(c *IdempotencyConfig) { c.FailOpen = true })
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		admitted, err := f.engine.Admit(ctx, ev)
		if err == nil {
			t.Fatal("Admit succeeded on a cancelled context")
		}
		if admitted {
			t.Fatal("Admit failed open on a shutdown; a cancellation must surface untouched")
		}
		if got := f.engine.Stats().FailedOpen; got != 0 {
			t.Fatalf("Stats().FailedOpen = %d, want 0 — a shutdown is not an infrastructure fault", got)
		}
	})
}

// TestDisabledFilterAdmitsEverythingWithoutTouchingRedis proves the call site
// can be left wired in permanently.
func TestDisabledFilterAdmitsEverythingWithoutTouchingRedis(t *testing.T) {
	f := newDedupeFixture(t, func(c *IdempotencyConfig) { c.Enabled = false })
	f.mr.Close()

	ev := reading("PUMP-221", SensorVibrationIndex, 9.0, baseTime)
	for i := 0; i < 3; i++ {
		if !mustAdmit(t, f.engine, ev) {
			t.Fatalf("delivery %d was discarded by a disabled filter", i+1)
		}
	}
	if err := f.engine.ReleaseEvent(context.Background(), ev); err != nil {
		t.Fatalf("ReleaseEvent on a disabled filter: %v", err)
	}
	if err := f.engine.Ping(context.Background()); err != nil {
		t.Fatalf("Ping on a disabled filter: %v", err)
	}
	if got := f.engine.Stats().Checked; got != 0 {
		t.Fatalf("Stats().Checked = %d, want 0 — a disabled filter performs no round trips", got)
	}
}

// ---------------------------------------------------------------------------
// Fingerprinting
// ---------------------------------------------------------------------------

func TestIdempotencyFingerprint(t *testing.T) {
	t.Run("is deterministic and 128 bits wide", func(t *testing.T) {
		first, err := IdempotencyFingerprint("PUMP-221", 1755079800000)
		if err != nil {
			t.Fatalf("IdempotencyFingerprint: %v", err)
		}
		second, _ := IdempotencyFingerprint("PUMP-221", 1755079800000)
		if first != second {
			t.Fatalf("fingerprint is not deterministic: %s then %s", first, second)
		}
		if len(first) != 32 {
			t.Fatalf("fingerprint %q is %d chars, want 32 hex chars", first, len(first))
		}
	})

	t.Run("distinguishes every component", func(t *testing.T) {
		seen := make(map[string]string)
		record := func(label, fingerprint string) {
			if prior, clash := seen[fingerprint]; clash {
				t.Fatalf("%s and %s collide on %s", prior, label, fingerprint)
			}
			seen[fingerprint] = label
		}

		for _, tc := range []struct {
			label, asset, discriminator string
			timestamp                   int64
		}{
			{"asset only", "PUMP-221", "", 1},
			{"asset only, later", "PUMP-221", "", 2},
			{"other asset", "PUMP-222", "", 1},
			{"with sensor", "PUMP-221", "vibration_index", 1},
			{"with other sensor", "PUMP-221", "temperature_celsius", 1},
			// The separator byte is forbidden inside an identifier, so string
			// concatenation cannot make these two collide.
			{"prefix-shifted asset", "PUMP-2", "21", 1},
		} {
			fingerprint, err := IdempotencyFingerprintWith(tc.asset, tc.discriminator, tc.timestamp)
			if err != nil {
				t.Fatalf("%s: %v", tc.label, err)
			}
			record(tc.label, fingerprint)
		}
	})

	t.Run("trims surrounding whitespace", func(t *testing.T) {
		trimmed, _ := IdempotencyFingerprint("PUMP-221", 1)
		padded, err := IdempotencyFingerprint("  PUMP-221  ", 1)
		if err != nil {
			t.Fatalf("IdempotencyFingerprint: %v", err)
		}
		if trimmed != padded {
			t.Fatal("a padded asset id produced a different fingerprint")
		}
	})

	t.Run("rejects coordinates that cannot identify an event", func(t *testing.T) {
		cases := []struct {
			name      string
			asset     string
			timestamp int64
		}{
			{"empty asset", "", 1},
			{"whitespace asset", "   ", 1},
			{"zero timestamp", "PUMP-221", 0},
			{"negative timestamp", "PUMP-221", -1},
			{"both", "", 0},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				fingerprint, err := IdempotencyFingerprint(tc.asset, tc.timestamp)
				if !errors.Is(err, ErrIdempotencyInvalidInput) {
					t.Fatalf("err = %v, want ErrIdempotencyInvalidInput", err)
				}
				if fingerprint != "" {
					t.Fatalf("a rejected coordinate still produced %q", fingerprint)
				}
			})
		}
	})
}

func TestKeyIsNamespaced(t *testing.T) {
	f := newDedupeFixture(t, func(c *IdempotencyConfig) { c.KeyPrefix = "dedupe:" })
	key := f.engine.Key("deadbeef")

	if key != "dedupe:deadbeef" {
		t.Fatalf("Key() = %q, want dedupe:deadbeef", key)
	}
	for _, reserved := range []string{"twin:", "twinindex:", "twinalarm:"} {
		if strings.HasPrefix(key, reserved) {
			t.Fatalf("claim key %q lands in the reserved %s namespace", key, reserved)
		}
	}

	var nilEngine *IdempotencyEngine
	if got := nilEngine.Key("deadbeef"); got != DefaultIdempotencyKeyPrefix+"deadbeef" {
		t.Fatalf("nil engine Key() = %q, want the default prefix", got)
	}
}

// ---------------------------------------------------------------------------
// Construction and configuration
// ---------------------------------------------------------------------------

func TestNewIdempotencyEngineValidatesItsInputs(t *testing.T) {
	mr := miniredis.RunT(t)
	cache := newTestCache(t, mr)

	t.Run("a nil client is rejected", func(t *testing.T) {
		engine, err := NewIdempotencyEngine(testIdempotencyConfig(), nil, discardLogger())
		if !errors.Is(err, ErrIdempotencyClientMissing) {
			t.Fatalf("err = %v, want ErrIdempotencyClientMissing", err)
		}
		if engine != nil {
			t.Fatal("an engine was returned alongside the error")
		}
	})

	t.Run("an invalid configuration is rejected", func(t *testing.T) {
		cfg := testIdempotencyConfig()
		cfg.TTL = 0
		if _, err := NewIdempotencyEngine(cfg, cache.Redis(), discardLogger()); !errors.Is(err, ErrIdempotencyInvalidConfig) {
			t.Fatalf("err = %v, want ErrIdempotencyInvalidConfig", err)
		}
	})

	t.Run("Close is a no-op for a borrowed client", func(t *testing.T) {
		engine := newTestDedupe(t, cache, testIdempotencyConfig())
		if err := engine.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		// The borrowed pool must still work.
		if err := cache.Ping(context.Background()); err != nil {
			t.Fatalf("closing the filter closed the shared pool: %v", err)
		}
	})
}

func TestIdempotencyConfigValidate(t *testing.T) {
	cases := map[string]struct {
		mutate  func(*IdempotencyConfig)
		wantErr string
	}{
		"valid":                  {mutate: func(*IdempotencyConfig) {}},
		"empty key prefix":       {mutate: func(c *IdempotencyConfig) { c.KeyPrefix = "" }, wantErr: "KEY_PREFIX"},
		"blank key prefix":       {mutate: func(c *IdempotencyConfig) { c.KeyPrefix = "   " }, wantErr: "KEY_PREFIX"},
		"zero ttl":               {mutate: func(c *IdempotencyConfig) { c.TTL = 0 }, wantErr: "TTL must be > 0"},
		"negative ttl":           {mutate: func(c *IdempotencyConfig) { c.TTL = -time.Second }, wantErr: "TTL must be > 0"},
		"ttl over an hour":       {mutate: func(c *IdempotencyConfig) { c.TTL = time.Hour + time.Second }, wantErr: "<= 1h"},
		"ttl of exactly an hour": {mutate: func(c *IdempotencyConfig) { c.TTL = time.Hour }},
		"unknown scope":          {mutate: func(c *IdempotencyConfig) { c.Scope = "asset_sensor_value" }, wantErr: "SCOPE"},
		"empty scope":            {mutate: func(c *IdempotencyConfig) { c.Scope = "" }, wantErr: "SCOPE"},
		"zero op timeout":        {mutate: func(c *IdempotencyConfig) { c.OpTimeout = 0 }, wantErr: "OP_TIMEOUT"},
		"disabled but invalid is still invalid": {
			mutate:  func(c *IdempotencyConfig) { c.Enabled = false; c.TTL = 0 },
			wantErr: "TTL must be > 0",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := testIdempotencyConfig()
			tc.mutate(&cfg)

			err := cfg.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want an error mentioning %q", tc.wantErr)
			}
			if !errors.Is(err, ErrIdempotencyInvalidConfig) {
				t.Fatalf("Validate() = %v, want it to wrap ErrIdempotencyInvalidConfig", err)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Validate() = %v, want it to mention %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidateReportsEveryBrokenSettingAtOnce(t *testing.T) {
	cfg := IdempotencyConfig{KeyPrefix: "", TTL: 0, Scope: "nonsense", OpTimeout: 0}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() accepted a wholly invalid configuration")
	}
	for _, want := range []string{"KEY_PREFIX", "TTL", "SCOPE", "OP_TIMEOUT"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestLoadIdempotencyConfig(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		cfg, err := LoadIdempotencyConfig(4 * time.Second)
		if err != nil {
			t.Fatalf("LoadIdempotencyConfig: %v", err)
		}
		want := IdempotencyConfig{
			Enabled:   true,
			KeyPrefix: DefaultIdempotencyKeyPrefix,
			TTL:       DefaultIdempotencyTTL,
			Scope:     ScopeAssetSensor,
			FailOpen:  true,
			OpTimeout: 4 * time.Second,
		}
		if cfg != want {
			t.Fatalf("LoadIdempotencyConfig() = %+v, want %+v", cfg, want)
		}
	})

	t.Run("environment overrides", func(t *testing.T) {
		t.Setenv("IDEMPOTENCY_ENABLED", "false")
		t.Setenv("IDEMPOTENCY_KEY_PREFIX", "dd:")
		t.Setenv("IDEMPOTENCY_TTL", "250ms")
		t.Setenv("IDEMPOTENCY_SCOPE", "ASSET")
		t.Setenv("IDEMPOTENCY_FAIL_OPEN", "0")
		t.Setenv("IDEMPOTENCY_OP_TIMEOUT", "1s")

		cfg, err := LoadIdempotencyConfig(4 * time.Second)
		if err != nil {
			t.Fatalf("LoadIdempotencyConfig: %v", err)
		}
		want := IdempotencyConfig{
			Enabled:   false,
			KeyPrefix: "dd:",
			TTL:       250 * time.Millisecond,
			Scope:     ScopeAsset,
			FailOpen:  false,
			OpTimeout: time.Second,
		}
		if cfg != want {
			t.Fatalf("LoadIdempotencyConfig() = %+v, want %+v", cfg, want)
		}
	})

	t.Run("reports every broken variable at once", func(t *testing.T) {
		t.Setenv("IDEMPOTENCY_ENABLED", "yes-please")
		t.Setenv("IDEMPOTENCY_TTL", "forever")

		_, err := LoadIdempotencyConfig(4 * time.Second)
		if err == nil {
			t.Fatal("LoadIdempotencyConfig accepted unparseable variables")
		}
		if !errors.Is(err, ErrIdempotencyInvalidConfig) {
			t.Fatalf("err = %v, want it to wrap ErrIdempotencyInvalidConfig", err)
		}
		for _, want := range []string{"IDEMPOTENCY_ENABLED", "IDEMPOTENCY_TTL"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not mention %s", err, want)
			}
		}
	})

	t.Run("a valid parse that fails validation is rejected", func(t *testing.T) {
		t.Setenv("IDEMPOTENCY_TTL", "2h")
		if _, err := LoadIdempotencyConfig(4 * time.Second); err == nil {
			t.Fatal("a 2h deduplication window was accepted")
		}
	})
}

func TestScopeValid(t *testing.T) {
	for _, scope := range []IdempotencyScope{ScopeAsset, ScopeAssetSensor} {
		if !scope.Valid() {
			t.Errorf("%q.Valid() = false", scope)
		}
	}
	for _, scope := range []IdempotencyScope{"", "ASSET", "asset_sensor_value", "value"} {
		if scope.Valid() {
			t.Errorf("%q.Valid() = true", scope)
		}
	}
}

// ---------------------------------------------------------------------------
// Observability
// ---------------------------------------------------------------------------

func TestStatsAndPrometheus(t *testing.T) {
	t.Run("a nil engine reports a disabled filter", func(t *testing.T) {
		var nilEngine *IdempotencyEngine
		stats := nilEngine.Stats()
		if stats.Enabled {
			t.Error("a nil engine reported itself enabled")
		}
		if stats.Scope != string(ScopeAssetSensor) {
			t.Errorf("Scope = %q, want the default scope", stats.Scope)
		}
		if !nilEngine.Enabled() {
			// Enabled() must be false, exercised here so a nil receiver cannot panic.
			_ = nilEngine.Config()
		}
		if body := nilEngine.Prometheus(); !strings.Contains(body, "openontology_idempotency_enabled 0") {
			t.Errorf("Prometheus() for a nil engine = %q", body)
		}
	})

	t.Run("exposition format", func(t *testing.T) {
		f := newDedupeFixture(t, nil)
		ev := reading("PUMP-221", SensorVibrationIndex, 9.0, baseTime)
		mustAdmit(t, f.engine, ev)
		mustAdmit(t, f.engine, ev)

		body := f.engine.Prometheus()
		for _, want := range []string{
			"openontology_idempotency_enabled 1",
			"openontology_idempotency_checked_total 2",
			"openontology_idempotency_admitted_total 1",
			"openontology_idempotency_duplicates_total 1",
			"openontology_idempotency_window_seconds 5",
			"# TYPE openontology_idempotency_checked_total counter",
			"# HELP openontology_idempotency_enabled",
		} {
			if !strings.Contains(body, want) {
				t.Errorf("Prometheus() is missing %q\n%s", want, body)
			}
		}
	})

	t.Run("Config round trips", func(t *testing.T) {
		f := newDedupeFixture(t, nil)
		if got := f.engine.Config(); got != testIdempotencyConfig() {
			t.Fatalf("Config() = %+v", got)
		}
	})
}

func TestPingReportsFilterConnectivity(t *testing.T) {
	f := newDedupeFixture(t, nil)
	if err := f.engine.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	f.mr.Close()
	if err := f.engine.Ping(context.Background()); err == nil {
		t.Fatal("Ping succeeded against a closed Redis")
	}
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

// TestAdmitIsAtomicUnderConcurrency is the property that makes the filter
// distributed rather than per-process: however many workers race on one
// coordinate, SETNX grants the claim exactly once. Run with -race.
func TestAdmitIsAtomicUnderConcurrency(t *testing.T) {
	const (
		goroutines  = 48
		coordinates = 16
	)

	f := newDedupeFixture(t, nil)

	var admitted [coordinates]atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for c := 0; c < coordinates; c++ {
				ev := reading("PUMP-221", SensorVibrationIndex, 9.0, baseTime.Add(time.Duration(c)*time.Millisecond))
				ok, err := f.engine.Admit(context.Background(), ev)
				if err != nil {
					t.Errorf("Admit: %v", err)
					return
				}
				if ok {
					admitted[c].Add(1)
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	for c := 0; c < coordinates; c++ {
		if got := admitted[c].Load(); got != 1 {
			t.Errorf("coordinate %d was admitted %d times, want exactly 1", c, got)
		}
	}

	stats := f.engine.Stats()
	if want := uint64(goroutines * coordinates); stats.Checked != want {
		t.Errorf("Stats().Checked = %d, want %d", stats.Checked, want)
	}
	if stats.Admitted != coordinates {
		t.Errorf("Stats().Admitted = %d, want %d", stats.Admitted, coordinates)
	}
	if want := uint64(goroutines*coordinates - coordinates); stats.Duplicates != want {
		t.Errorf("Stats().Duplicates = %d, want %d", stats.Duplicates, want)
	}
}

// TestAdmitAndReleaseRaceOnOneCoordinate overlaps the claim and the rollback
// paths so -race can observe them together.
func TestAdmitAndReleaseRaceOnOneCoordinate(t *testing.T) {
	f := newDedupeFixture(t, nil)
	ev := reading("PUMP-221", SensorVibrationIndex, 9.0, baseTime)

	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if g%2 == 0 {
					if _, err := f.engine.Admit(context.Background(), ev); err != nil {
						t.Errorf("Admit: %v", err)
						return
					}
					continue
				}
				if err := f.engine.ReleaseEvent(context.Background(), ev); err != nil {
					t.Errorf("ReleaseEvent: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if stats := f.engine.Stats(); stats.Admitted+stats.Duplicates != stats.Checked {
		t.Fatalf("counters do not reconcile: admitted %d + duplicates %d != checked %d",
			stats.Admitted, stats.Duplicates, stats.Checked)
	}
}
