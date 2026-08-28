package main

import (
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// Metrics holds lock-free counters shared by every worker goroutine.
type Metrics struct {
	StartedAt time.Time

	EventsConsumed   atomic.Uint64
	EventsRejected   atomic.Uint64
	EventsDuplicate  atomic.Uint64
	EventsStale      atomic.Uint64
	CacheWrites      atomic.Uint64
	RulesEvaluated   atomic.Uint64
	Anomalies        atomic.Uint64
	MutationsEmitted atomic.Uint64
	MutationsFailed  atomic.Uint64
	DLQMessages      atomic.Uint64
	GraphDegraded    atomic.Uint64
	ProcessErrors    atomic.Uint64
	StatePruned      atomic.Uint64

	// transitions counts emitted mutations split by (transition, severity).
	//
	// The aggregate MutationsEmitted above answers "how much is happening";
	// this answers "what kind", which is the question an operator actually
	// asks. A rising RAISED count is a fleet developing faults, a rising
	// SUSTAINED count is a fleet nobody is fixing, and a CLEARED count that
	// never catches up to RAISED is the one that matters at 3am.
	//
	// The key space is fixed and small — four transitions by two severities —
	// so the map is fully populated at construction and never written to
	// again. That keeps it lock-free like the rest of this struct: only the
	// atomic values inside it are ever mutated.
	transitions map[TransitionKind]map[Severity]*atomic.Uint64
}

// transitionKinds and severityKinds enumerate the label space. They are
// declared here rather than derived so that a new transition or severity shows
// up as a compile-time decision to also export it, instead of a series that
// silently never appears on /metrics.
var (
	transitionKinds = []TransitionKind{
		TransitionRaised,
		TransitionEscalated,
		TransitionSustained,
		TransitionCleared,
	}
	// SeverityInfo belongs here even though no *breach* carries it: a CLEARED
	// transition reports INFO, because the channel has recovered and there is
	// no severity left to report. Omitting it meant every CLEARED was dropped
	// by the unknown-pair branch in RecordTransition — silently, since that
	// branch exists precisely to avoid writing to the map from a worker
	// goroutine. The dashboard showed alarms being raised and never resolving.
	severityKinds = []Severity{SeverityInfo, SeverityHigh, SeverityCritical}
)

// NewMetrics starts the uptime clock.
func NewMetrics() *Metrics {
	m := &Metrics{
		StartedAt:   time.Now().UTC(),
		transitions: make(map[TransitionKind]map[Severity]*atomic.Uint64, len(transitionKinds)),
	}
	// Pre-populating every combination means a series exists at zero before it
	// first fires. A counter that springs into existence on first use makes
	// rate() over the preceding window undefined, so a dashboard shows a gap
	// exactly when the first interesting thing happened.
	for _, kind := range transitionKinds {
		bySeverity := make(map[Severity]*atomic.Uint64, len(severityKinds))
		for _, severity := range severityKinds {
			bySeverity[severity] = new(atomic.Uint64)
		}
		m.transitions[kind] = bySeverity
	}
	return m
}

// RecordTransition counts one emitted mutation against its labels.
//
// An unknown pair is dropped rather than added, because the alternative is
// writing to the map from a worker goroutine — the thing that would make this
// struct need a mutex. transitionKinds above is the contract; a transition
// missing from it is a bug in this file, not in the caller.
func (m *Metrics) RecordTransition(kind TransitionKind, severity Severity) {
	bySeverity, ok := m.transitions[kind]
	if !ok {
		return
	}
	if counter, ok := bySeverity[severity]; ok {
		counter.Add(1)
	}
}

// TransitionCounts renders the labelled series as nested maps, for /stats.
func (m *Metrics) TransitionCounts() map[string]map[string]uint64 {
	out := make(map[string]map[string]uint64, len(m.transitions))
	for kind, bySeverity := range m.transitions {
		row := make(map[string]uint64, len(bySeverity))
		for severity, counter := range bySeverity {
			row[string(severity)] = counter.Load()
		}
		out[string(kind)] = row
	}
	return out
}

type metricSample struct {
	Name   string
	Help   string
	Type   string
	Value  float64
	Labels map[string]string
}

// labelPairs renders a sample's labels in the text exposition format, with keys
// sorted so repeated scrapes produce byte-identical output.
func (s metricSample) labelPairs() string {
	if len(s.Labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(s.Labels))
	for key := range s.Labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteByte('{')
	for i, key := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		// %s with explicit quotes, not %q: %q would apply Go's own escaping on
		// top of escapeLabelValue's, and would additionally render non-ASCII
		// as \u sequences, which the text format does not ask for.
		fmt.Fprintf(&b, `%s="%s"`, key, escapeLabelValue(s.Labels[key]))
	}
	b.WriteByte('}')
	return b.String()
}

// escapeLabelValue applies the three escapes the text format requires. Label
// values here are drawn from closed enums, so this never fires in practice —
// it is here so that stops being load-bearing if a label ever carries an
// asset id or an error string.
func escapeLabelValue(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return replacer.Replace(value)
}

func (m *Metrics) samples(trackedChannels, anomalousChannels int, graphLookups, graphHits, graphErrs uint64) []metricSample {
	samples := []metricSample{
		{"openontology_uptime_seconds", "Process uptime in seconds.", "gauge", time.Since(m.StartedAt).Seconds(), nil},
		{"openontology_events_consumed_total", "Telemetry events read from the source topic.", "counter", float64(m.EventsConsumed.Load()), nil},
		{"openontology_events_rejected_total", "Telemetry events rejected as malformed or invalid.", "counter", float64(m.EventsRejected.Load()), nil},
		{"openontology_events_duplicate_total", "Telemetry events discarded by the distributed idempotency filter.", "counter", float64(m.EventsDuplicate.Load()), nil},
		{"openontology_events_stale_total", "Telemetry events discarded as out-of-order.", "counter", float64(m.EventsStale.Load()), nil},
		{"openontology_cache_writes_total", "Live-state writes applied to Redis.", "counter", float64(m.CacheWrites.Load()), nil},
		{"openontology_rules_evaluated_total", "Readings evaluated against an anomaly rule.", "counter", float64(m.RulesEvaluated.Load()), nil},
		{"openontology_anomalies_total", "Readings that breached a threshold.", "counter", float64(m.Anomalies.Load()), nil},
		{"openontology_mutations_emitted_total", "Enriched context payloads published.", "counter", float64(m.MutationsEmitted.Load()), nil},
		{"openontology_mutations_failed_total", "Enriched context payloads that failed to publish.", "counter", float64(m.MutationsFailed.Load()), nil},
		{"openontology_dlq_messages_total", "Messages routed to the dead-letter topic.", "counter", float64(m.DLQMessages.Load()), nil},
		{"openontology_graph_degraded_total", "Mutations emitted without full graph context.", "counter", float64(m.GraphDegraded.Load()), nil},
		{"openontology_process_errors_total", "Processing errors encountered by workers.", "counter", float64(m.ProcessErrors.Load()), nil},
		{"openontology_state_pruned_total", "Idle twin channels evicted from the state tracker.", "counter", float64(m.StatePruned.Load()), nil},
		{"openontology_graph_lookups_total", "Graph context resolutions attempted.", "counter", float64(graphLookups), nil},
		{"openontology_graph_cache_hits_total", "Graph context resolutions served from cache.", "counter", float64(graphHits), nil},
		{"openontology_graph_errors_total", "Graph context resolutions that failed.", "counter", float64(graphErrs), nil},
		{"openontology_tracked_channels", "Twin channels currently held in the state tracker.", "gauge", float64(trackedChannels), nil},
		{"openontology_anomalous_channels", "Twin channels currently in the ANOMALOUS state.", "gauge", float64(anomalousChannels), nil},
	}

	// The labelled series are appended rather than declared inline so the
	// aggregate set above stays readable as a flat table.
	for _, kind := range transitionKinds {
		for _, severity := range severityKinds {
			samples = append(samples, metricSample{
				Name:  "openontology_mutation_transitions_total",
				Help:  "Enriched context payloads published, by state-machine transition and severity.",
				Type:  "counter",
				Value: float64(m.transitions[kind][severity].Load()),
				Labels: map[string]string{
					"transition": string(kind),
					"severity":   string(severity),
				},
			})
		}
	}

	return samples
}

// JSON renders a flat map suitable for the /stats endpoint.
//
// Labelled series are skipped: the map is keyed on metric name, and eight
// (transition, severity) samples share one name, so including them would have
// each overwrite the last and leave one arbitrary value behind. /stats exposes
// them properly through TransitionCounts instead.
func (m *Metrics) JSON(trackedChannels, anomalousChannels int, graphLookups, graphHits, graphErrs uint64) map[string]float64 {
	out := make(map[string]float64)
	for _, s := range m.samples(trackedChannels, anomalousChannels, graphLookups, graphHits, graphErrs) {
		if len(s.Labels) > 0 {
			continue
		}
		out[strings.TrimPrefix(s.Name, "openontology_")] = round(s.Value, 4)
	}
	return out
}

// renderExposition writes samples in the Prometheus text format.
//
// Both this file and idempotency_filter.go produce a fragment of the same
// /metrics document, and they used to carry a copy of this loop each. Once one
// of them grew labelled series the copies stopped being equivalent, so the
// rendering rules — sorting, HELP/TYPE per family, label escaping — live here
// once and both callers get them.
func renderExposition(samples []metricSample) string {
	// Sort by name, then by rendered labels, so a metric family's series are
	// contiguous and the whole document is stable across scrapes. Prometheus
	// tolerates unsorted input, but a diffable /metrics is worth the sort when
	// debugging by eye.
	sort.Slice(samples, func(i, j int) bool {
		if samples[i].Name != samples[j].Name {
			return samples[i].Name < samples[j].Name
		}
		return samples[i].labelPairs() < samples[j].labelPairs()
	})

	var b strings.Builder
	// HELP and TYPE are per metric family, not per series. Emitting them again
	// for each of the eight labelled transition series would be a malformed
	// document, and Prometheus rejects the duplicate rather than ignoring it.
	var lastName string
	for i, s := range samples {
		if i == 0 || s.Name != lastName {
			fmt.Fprintf(&b, "# HELP %s %s\n", s.Name, s.Help)
			fmt.Fprintf(&b, "# TYPE %s %s\n", s.Name, s.Type)
			lastName = s.Name
		}
		fmt.Fprintf(&b, "%s%s %g\n", s.Name, s.labelPairs(), s.Value)
	}
	return b.String()
}

// Prometheus renders the text exposition format without pulling in a client
// library — the counter set is small and stable enough not to warrant one.
func (m *Metrics) Prometheus(trackedChannels, anomalousChannels int, graphLookups, graphHits, graphErrs uint64) string {
	return renderExposition(m.samples(trackedChannels, anomalousChannels, graphLookups, graphHits, graphErrs))
}
