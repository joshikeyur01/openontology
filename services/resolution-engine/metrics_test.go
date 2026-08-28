package main

import (
	"strings"
	"testing"
)

// renderMetrics exposes the exposition document with the graph and tracker
// arguments zeroed, since none of these tests are about those.
func renderMetrics(m *Metrics) string {
	return m.Prometheus(0, 0, 0, 0, 0)
}

func countLines(document, prefix string) int {
	n := 0
	for _, line := range strings.Split(document, "\n") {
		if strings.HasPrefix(line, prefix) {
			n++
		}
	}
	return n
}

// HELP and TYPE are declared per metric family. Emitting them once per series
// produces a document Prometheus rejects outright, and the labelled transition
// family has eight series, so this is the regression that matters most.
func TestExpositionDeclaresHelpAndTypeOncePerFamily(t *testing.T) {
	m := NewMetrics()
	m.RecordTransition(TransitionRaised, SeverityCritical)
	m.RecordTransition(TransitionCleared, SeverityHigh)

	document := renderMetrics(m)

	const family = "openontology_mutation_transitions_total"
	if got := countLines(document, "# HELP "+family); got != 1 {
		t.Errorf("HELP for %s declared %d times, want exactly 1:\n%s", family, got, document)
	}
	if got := countLines(document, "# TYPE "+family); got != 1 {
		t.Errorf("TYPE for %s declared %d times, want exactly 1", family, got)
	}
}

// Every combination must be present at zero before it first fires. A series
// that appears only on first use makes rate() undefined over the window before
// it, so a dashboard goes blank exactly when something started happening.
func TestEveryTransitionSeriesExistsBeforeItFires(t *testing.T) {
	document := renderMetrics(NewMetrics())

	for _, kind := range transitionKinds {
		for _, severity := range severityKinds {
			series := `openontology_mutation_transitions_total{severity="` +
				string(severity) + `",transition="` + string(kind) + `"} 0`
			if !strings.Contains(document, series) {
				t.Errorf("series missing from a fresh registry: %s\n%s", series, document)
			}
		}
	}
}

func TestRecordTransitionCountsAgainstItsLabels(t *testing.T) {
	m := NewMetrics()
	m.RecordTransition(TransitionRaised, SeverityCritical)
	m.RecordTransition(TransitionRaised, SeverityCritical)
	m.RecordTransition(TransitionRaised, SeverityHigh)

	document := renderMetrics(m)

	want := []string{
		`openontology_mutation_transitions_total{severity="CRITICAL",transition="RAISED"} 2`,
		`openontology_mutation_transitions_total{severity="HIGH",transition="RAISED"} 1`,
		`openontology_mutation_transitions_total{severity="HIGH",transition="CLEARED"} 0`,
	}
	for _, series := range want {
		if !strings.Contains(document, series) {
			t.Errorf("expected series %q in:\n%s", series, document)
		}
	}

	counts := m.TransitionCounts()
	if got := counts["RAISED"]["CRITICAL"]; got != 2 {
		t.Errorf("TransitionCounts RAISED/CRITICAL = %d, want 2", got)
	}
	if got := counts["CLEARED"]["HIGH"]; got != 0 {
		t.Errorf("TransitionCounts CLEARED/HIGH = %d, want 0", got)
	}
}

// The label space must cover every transition the state machine can actually
// emit, paired with the severity that transition carries.
//
// This is the regression that motivated the test. severityKinds originally held
// only HIGH and CRITICAL, which is true of every *breach* — but CLEARED reports
// INFO, because the channel has recovered and there is no severity left. Every
// CLEARED was therefore dropped by RecordTransition's unknown-pair branch, and
// dropped silently, since that branch exists to avoid a concurrent map write.
// The dashboard showed alarms being raised and never resolving.
func TestLabelSpaceCoversEveryEmittableTransition(t *testing.T) {
	// Exactly what StateTracker.Evaluate returns for each edge.
	emittable := []struct {
		kind     TransitionKind
		severity Severity
	}{
		{TransitionRaised, SeverityHigh},
		{TransitionRaised, SeverityCritical},
		{TransitionEscalated, SeverityCritical},
		{TransitionSustained, SeverityHigh},
		{TransitionSustained, SeverityCritical},
		{TransitionCleared, SeverityInfo},
	}

	for _, want := range emittable {
		m := NewMetrics()
		m.RecordTransition(want.kind, want.severity)

		if got := m.TransitionCounts()[string(want.kind)][string(want.severity)]; got != 1 {
			t.Errorf("RecordTransition(%s, %s) recorded %d, want 1 — this pair is "+
				"droppable, so the series would never appear on /metrics",
				want.kind, want.severity, got)
		}
	}
}

// Every Severity constant the package declares must be in the label space.
// Pairing the two by hand is what let INFO go missing; asserting the
// relationship means adding a severity without exporting it fails here.
func TestEverySeverityConstantIsLabelled(t *testing.T) {
	declared := []Severity{SeverityInfo, SeverityHigh, SeverityCritical}

	labelled := make(map[Severity]bool, len(severityKinds))
	for _, severity := range severityKinds {
		labelled[severity] = true
	}

	for _, severity := range declared {
		if !labelled[severity] {
			t.Errorf("severity %q is declared in model.go but missing from "+
				"severityKinds; transitions carrying it will be dropped", severity)
		}
	}
}

// An unknown pair must not panic and must not write to the map, because a
// concurrent map write from a worker goroutine is exactly what this design
// avoids by pre-populating the key space.
func TestRecordTransitionIgnoresUnknownLabels(t *testing.T) {
	m := NewMetrics()
	before := len(m.transitions)

	m.RecordTransition(TransitionKind("INVENTED"), SeverityHigh)
	m.RecordTransition(TransitionRaised, Severity("NOTASEVERITY"))

	if got := len(m.transitions); got != before {
		t.Errorf("transition map grew from %d to %d; the key space must be fixed", before, got)
	}
	if strings.Contains(renderMetrics(m), "INVENTED") {
		t.Error("an unknown transition reached the exposition document")
	}
}

// /stats is keyed on metric name, so the eight transition series would collide
// there and leave one arbitrary survivor. They belong in TransitionCounts.
func TestJSONExcludesLabelledSeries(t *testing.T) {
	m := NewMetrics()
	m.RecordTransition(TransitionRaised, SeverityCritical)

	flat := m.JSON(0, 0, 0, 0, 0)

	if _, present := flat["mutation_transitions_total"]; present {
		t.Error("labelled series leaked into the flat /stats map, where it collides with itself")
	}
	if _, present := flat["mutations_emitted_total"]; !present {
		t.Error("unlabelled counters must still appear in the flat map")
	}
}

func TestLabelValuesAreEscaped(t *testing.T) {
	sample := metricSample{
		Labels: map[string]string{"reason": `a "quoted" \ path` + "\n"},
	}
	got := sample.labelPairs()
	want := `{reason="a \"quoted\" \\ path\n"}`
	if got != want {
		t.Errorf("labelPairs() = %s, want %s", got, want)
	}
}

// Labels are rendered in sorted key order so two scrapes of unchanged state are
// byte-identical rather than reshuffled by map iteration.
//
// The uptime gauge is excluded deliberately — it is supposed to move between
// renders, and comparing the whole document would only ever be testing that.
func TestLabelOrderIsStable(t *testing.T) {
	counters := func(m *Metrics) string {
		var kept []string
		for _, line := range strings.Split(renderMetrics(m), "\n") {
			if strings.HasPrefix(line, "openontology_uptime_seconds") {
				continue
			}
			kept = append(kept, line)
		}
		return strings.Join(kept, "\n")
	}

	m := NewMetrics()
	first := counters(m)
	for i := 0; i < 20; i++ {
		if got := counters(m); got != first {
			t.Fatalf("exposition document is not stable across renders:\n%s\n---\n%s", first, got)
		}
	}
}
