package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// Engine wires the consume -> cache -> evaluate -> enrich -> emit loop.
// One Engine is shared by all workers; every field it touches is either
// immutable or internally synchronised.
type Engine struct {
	cfg     Config
	log     *slog.Logger
	cache   *StateCache
	graph   GraphResolver
	rules   RuleEngine
	state   *StateTracker
	dedupe  *IdempotencyEngine
	writer  *kafka.Writer
	dlq     *kafka.Writer
	metrics *Metrics
	replica *TopologyReplica
}

// NewEngine constructs the engine and its Kafka producers.
func NewEngine(
	cfg Config,
	log *slog.Logger,
	cache *StateCache,
	graph GraphResolver,
	state *StateTracker,
	dedupe *IdempotencyEngine,
	metrics *Metrics,
	replica *TopologyReplica,
) *Engine {
	return &Engine{
		cfg:     cfg,
		log:     log.With("component", "engine"),
		cache:   cache,
		graph:   graph,
		rules:   NewRuleEngine(cfg),
		state:   state,
		dedupe:  dedupe,
		writer:  newWriter(cfg, cfg.MutationTopic, log),
		dlq:     newWriter(cfg, cfg.DLQTopic, log),
		metrics: metrics,
		replica: replica,
	}
}

func newWriter(cfg Config, topic string, log *slog.Logger) *kafka.Writer {
	return &kafka.Writer{
		Addr:  kafka.TCP(cfg.KafkaBrokers...),
		Topic: topic,
		// Hash on the message key so every mutation for an asset lands on the
		// same partition and downstream consumers observe them in order.
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequiredAcks(cfg.RequiredAcks),
		Compression:  kafka.Snappy,
		Async:        false,
		BatchTimeout: 20 * time.Millisecond,
		MaxAttempts:  cfg.MaxAttempts,
		WriteTimeout: cfg.OpTimeout,
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			log.Error(fmt.Sprintf(msg, args...), "component", "kafka-writer", "topic", topic)
		}),
	}
}

// Close flushes and shuts down both producers.
func (e *Engine) Close() error {
	return errors.Join(e.writer.Close(), e.dlq.Close())
}

// RunWorker owns one consumer-group member. Running several of these in the
// same group lets Kafka assign partitions across them; each worker commits
// only after a message is fully processed, giving at-least-once delivery.
func (e *Engine) RunWorker(ctx context.Context, id int) error {
	log := e.log.With("worker", id)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        e.cfg.KafkaBrokers,
		GroupID:        e.cfg.ConsumerGroup,
		Topic:          e.cfg.SourceTopic,
		MinBytes:       e.cfg.MinFetchBytes,
		MaxBytes:       e.cfg.MaxFetchBytes,
		MaxWait:        e.cfg.MaxWait,
		SessionTimeout: e.cfg.SessionTimeout,
		// CommitInterval 0 means commits are synchronous and explicit.
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			log.Error(fmt.Sprintf(msg, args...), "component", "kafka-reader")
		}),
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Warn("reader close returned an error", "error", err)
		}
	}()

	log.Info("consumer worker started", "topic", e.cfg.SourceTopic, "group", e.cfg.ConsumerGroup)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) {
				log.Info("consumer worker stopping")
				return nil
			}
			log.Error("fetch failed, backing off", "error", err)
			if !sleepCtx(ctx, time.Second) {
				return nil
			}
			continue
		}

		e.processWithRetry(ctx, msg, log)

		if err := reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// A failed commit means the message will be redelivered. Every
			// downstream write is idempotent (Redis is last-writer-wins on a
			// monotonic timestamp, and the state machine suppresses duplicate
			// transitions), so redelivery is safe.
			log.Error("commit failed; message will be redelivered",
				"error", err, "partition", msg.Partition, "offset", msg.Offset)
		}
	}
}

// processWithRetry retries transient infrastructure failures, then dead-letters
// so a single unhealthy dependency cannot wedge a partition indefinitely.
func (e *Engine) processWithRetry(ctx context.Context, msg kafka.Message, log *slog.Logger) {
	err := withRetry(ctx, e.cfg.MaxAttempts, 200*time.Millisecond, func(attemptCtx context.Context) error {
		return e.process(attemptCtx, msg)
	})
	if err == nil {
		return
	}
	if ctx.Err() != nil {
		return
	}

	e.metrics.ProcessErrors.Add(1)
	log.Error("processing failed after retries; dead-lettering",
		"error", err, "partition", msg.Partition, "offset", msg.Offset)

	if dlqErr := e.deadLetter(ctx, msg, "processing_failed", err); dlqErr != nil {
		log.Error("dead-letter publish failed", "error", dlqErr)
	}
}

// process handles exactly one telemetry event end to end.
func (e *Engine) process(ctx context.Context, msg kafka.Message) (retErr error) {
	e.metrics.EventsConsumed.Add(1)

	opCtx, cancel := context.WithTimeout(ctx, e.cfg.OpTimeout)
	defer cancel()

	var ev TelemetryEvent
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		return e.reject(opCtx, msg, "decode_failed", err)
	}
	ev.Normalize()
	if err := ev.Validate(); err != nil {
		return e.reject(opCtx, msg, "validation_failed", err)
	}

	// 0. Distributed idempotency filter. Kafka is at-least-once and field
	// gateways retry on slow acks, so the same sample arrives more than once as
	// a matter of course; claiming the event's coordinate in Redis stops the
	// redelivery before it reaches the cache, the rules or the mutation topic.
	admitted, err := e.dedupe.Admit(opCtx, ev)
	if err != nil {
		if errors.Is(err, ErrIdempotencyInvalidInput) {
			return e.reject(opCtx, msg, "idempotency_input_invalid", err)
		}
		return fmt.Errorf("idempotency filter: %w", err)
	}
	if !admitted {
		e.metrics.EventsDuplicate.Add(1)
		return nil
	}

	// The claim is provisional until this message is fully processed. Rolling
	// it back on failure keeps the retry that Kafka is about to perform from
	// being discarded as a duplicate of the attempt that just failed.
	defer func() {
		if retErr == nil {
			return
		}
		if releaseErr := e.dedupe.ReleaseEvent(ctx, ev); releaseErr != nil {
			e.log.Warn("failed to roll back idempotency claim; redelivery may be suppressed",
				"asset_id", ev.AssetID, "sensor_id", ev.SensorID, "error", releaseErr)
		}
	}()

	// 1. Live state cache: twin:<AssetID>:<SensorID>.
	outcome, err := e.cache.Apply(opCtx, ev)
	if err != nil {
		return fmt.Errorf("cache apply: %w", err)
	}
	if !outcome.Fresh() {
		e.metrics.EventsStale.Add(1)
		e.log.Debug("dropped out-of-order reading",
			"asset_id", ev.AssetID, "sensor_id", ev.SensorID, "observed_at", ev.Timestamp.Time)
		return nil
	}
	if outcome == ApplyWritten {
		e.metrics.CacheWrites.Add(1)
	}

	// 2. Anomaly rules. Sensors without a rule are cached but never alarmed.
	eval, governed := e.rules.Evaluate(ev)
	if !governed {
		return nil
	}
	e.metrics.RulesEvaluated.Add(1)
	if eval.Breached {
		e.metrics.Anomalies.Add(1)
	}

	// 3. State machine. Wall-clock time drives the alarm timers so a gateway
	// with a skewed clock cannot suppress or spam re-alerts; the event's own
	// timestamp is preserved in the payload.
	now := time.Now().UTC()
	transition, emit, undo := e.state.Evaluate(ev.CacheKey(), eval, now)
	if !emit {
		return nil
	}

	// The transition is provisional in exactly the way the idempotency claim
	// above is. If the mutation never reaches ontology.mutations, the channel
	// must go back to how it was, or the redelivery Kafka is about to perform
	// finds the channel already ANOMALOUS and absorbs the RAISED as a duplicate
	// transition — losing the alarm without even a dead letter to show for it.
	defer func() {
		if retErr != nil {
			e.state.Rollback(undo)
		}
	}()

	// 4. Enrich with graph context and the multi-variable snapshot.
	payload, err := e.buildPayload(opCtx, ev, eval, transition, msg, now)
	if err != nil {
		return err
	}

	// 5. Mutate the live twin's alarm state, then publish.
	if err := e.cache.MarkAlarm(opCtx, ev, transition, payload.EventID); err != nil {
		return fmt.Errorf("mark alarm: %w", err)
	}
	if err := e.emit(opCtx, payload); err != nil {
		e.metrics.MutationsFailed.Add(1)
		return fmt.Errorf("emit mutation: %w", err)
	}

	e.metrics.MutationsEmitted.Add(1)
	e.metrics.RecordTransition(payload.Transition, payload.Severity)
	e.log.Info("ontology mutation emitted",
		"event_id", payload.EventID,
		"asset_id", payload.AssetID,
		"sensor_id", payload.Rule.SensorID,
		"transition", payload.Transition,
		"severity", payload.Severity,
		"observed_value", payload.Rule.ObservedValue,
		"threshold", payload.Rule.Threshold,
		"degraded", payload.Degraded)
	return nil
}

// buildPayload assembles the Enriched Context Payload. Graph and snapshot
// failures degrade the payload rather than suppressing the alarm: an operator
// would rather receive a thin CRITICAL notification than none at all.
func (e *Engine) buildPayload(
	ctx context.Context,
	ev TelemetryEvent,
	eval RuleEvaluation,
	transition Transition,
	msg kafka.Message,
	now time.Time,
) (EnrichedContextPayload, error) {
	trigger := SensorReading{
		SensorID:   ev.SensorID,
		Value:      ev.Value,
		Unit:       firstNonEmpty(ev.Unit, eval.Threshold.Unit),
		ObservedAt: ev.Timestamp.UTC(),
		AgeSeconds: round(now.Sub(ev.Timestamp.Time).Seconds(), 3),
	}

	payload := EnrichedContextPayload{
		EventID:            newEventID(),
		SchemaVersion:      SchemaVersion,
		Producer:           Producer,
		EmittedAt:          now,
		AssetID:            ev.AssetID,
		Transition:         transition.Kind,
		Severity:           transition.Severity,
		AnomalyActiveSince: transition.ActiveSince.UTC(),
		BreachCount:        transition.BreachCount,
		Rule:               eval.Trigger(),
		TelemetrySnapshot: TelemetrySnapshot{
			Trigger:    trigger,
			Readings:   []SensorReading{trigger},
			CapturedAt: now,
			Complete:   false,
		},
		SourcePartition: msg.Partition,
		SourceOffset:    msg.Offset,
	}

	if readings, err := e.cache.Snapshot(ctx, ev.AssetID, now); err != nil {
		payload.Degraded = true
		payload.DegradedReason = "snapshot_unavailable: " + err.Error()
		e.log.Warn("telemetry snapshot unavailable", "asset_id", ev.AssetID, "error", err)
	} else if len(readings) > 0 {
		payload.TelemetrySnapshot.Readings = readings
		payload.TelemetrySnapshot.Complete = true
	}

	graphCtx, err := e.graph.ResolveAsset(ctx, ev.AssetID)
	if err != nil {
		e.metrics.GraphDegraded.Add(1)
		payload.Degraded = true
		payload.DegradedReason = appendReason(payload.DegradedReason, "graph_unavailable: "+err.Error())
		payload.OntologyContext = OntologyContext{
			AssetID:    ev.AssetID,
			Source:     "unavailable",
			ResolvedAt: now,
		}
		e.log.Warn("graph context unavailable; emitting degraded mutation",
			"asset_id", ev.AssetID, "error", err)
		payload.OntologyContext.normaliseCollections()
		e.stampReplica(&payload)
		return payload, nil
	}
	payload.OntologyContext = graphCtx
	payload.OntologyContext.normaliseCollections()

	// Fold the resolved topology into the local replica before stamping, so the
	// graph_revision on this mutation includes what this mutation just learned.
	e.replica.Observe(payload.OntologyContext)
	e.stampReplica(&payload)
	return payload, nil
}

// stampReplica attaches replication provenance to a mutation.
//
// A no-op when replication is disabled: the fields are omitempty, so a
// single-site deployment emits exactly the payload it did before.
func (e *Engine) stampReplica(payload *EnrichedContextPayload) {
	if e.replica == nil || !e.replica.cfg.Enabled {
		return
	}
	payload.OriginReplica = e.replica.ID()
	payload.LamportClock = e.replica.Clock()
	payload.GraphRevision = e.replica.Digest()
	payload.OntologyContext.ReplicaObservations = e.replica.Observations(payload.AssetID)
}

// emit publishes the Enriched Context Payload to ontology.mutations.
func (e *Engine) emit(ctx context.Context, payload EnrichedContextPayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload %s: %w", payload.EventID, err)
	}

	return e.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(payload.AssetID),
		Value: body,
		Time:  payload.EmittedAt,
		Headers: []kafka.Header{
			{Key: "schema_version", Value: []byte(payload.SchemaVersion)},
			{Key: "event_id", Value: []byte(payload.EventID)},
			{Key: "asset_id", Value: []byte(payload.AssetID)},
			{Key: "severity", Value: []byte(payload.Severity)},
			{Key: "transition", Value: []byte(payload.Transition)},
			{Key: "producer", Value: []byte(payload.Producer)},
		},
	})
}

// reject dead-letters a message that can never succeed and returns nil so the
// offset is committed. A poison pill must not block the partition.
func (e *Engine) reject(ctx context.Context, msg kafka.Message, reason string, cause error) error {
	e.metrics.EventsRejected.Add(1)
	e.log.Warn("rejecting malformed telemetry",
		"reason", reason, "error", cause, "partition", msg.Partition, "offset", msg.Offset)

	if err := e.deadLetter(ctx, msg, reason, cause); err != nil {
		e.log.Error("dead-letter publish failed", "error", err, "reason", reason)
	}
	return nil
}

func (e *Engine) deadLetter(ctx context.Context, msg kafka.Message, reason string, cause error) error {
	e.metrics.DLQMessages.Add(1)

	headers := append([]kafka.Header(nil), msg.Headers...)
	headers = append(headers,
		kafka.Header{Key: "dlq_reason", Value: []byte(reason)},
		kafka.Header{Key: "dlq_error", Value: []byte(truncate(cause.Error(), 900))},
		kafka.Header{Key: "dlq_source_topic", Value: []byte(msg.Topic)},
		kafka.Header{Key: "dlq_source_partition", Value: []byte(strconv.Itoa(msg.Partition))},
		kafka.Header{Key: "dlq_source_offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
		kafka.Header{Key: "dlq_at", Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
	)

	// Publishing to the DLQ must not inherit an already-expired deadline.
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.cfg.OpTimeout)
	defer cancel()

	return e.dlq.WriteMessages(writeCtx, kafka.Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	})
}

// RunStateJanitor periodically evicts idle twin channels from the in-memory
// state tracker.
func (e *Engine) RunStateJanitor(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			removed := e.state.Prune(time.Now().UTC(), e.cfg.StateIdleTTL)
			if removed > 0 {
				e.metrics.StatePruned.Add(uint64(removed))
				e.log.Debug("pruned idle twin channels", "count", removed)
			}
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func appendReason(existing, addition string) string {
	if existing == "" {
		return addition
	}
	return existing + "; " + addition
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
