package metrics

import (
	"strings"
	"testing"
	"time"
)

// The comparison of the two ends is served apart from everything else, because
// it costs an exact count of every replicated object on both sides and is
// therefore measured once an hour. Sampling it with the rest would store a
// hundred and twenty copies of each hour's figure.

func TestTheComparisonIsNotOnTheMainExposition(t *testing.T) {
	labels := Labels{"task": "39", "engine": "mongodb", "object": "orders"}
	t.Cleanup(func() { RowCounts.Forget(labels) })

	SetRowCounts(labels, 1000, 998, time.Unix(1788800000, 0))

	var main, rows strings.Builder
	if err := Default.Write(&main); err != nil {
		t.Fatalf("write the main exposition: %v", err)
	}
	if err := RowCounts.Write(&rows); err != nil {
		t.Fatalf("write the comparison: %v", err)
	}

	if strings.Contains(main.String(), SourceRows) {
		t.Errorf("%s is on the main exposition, which is scraped every thirty seconds",
			SourceRows)
	}
	for _, want := range []string{SourceRows, TargetRows, RowCountMeasuredAt, "orders"} {
		if !strings.Contains(rows.String(), want) {
			t.Errorf("the comparison does not carry %s", want)
		}
	}
}

// A count of -1 is what the monitor reports when it could not be taken. It is
// published as it is: "could not count" and "counted zero" are the difference
// the pair exists to show.
func TestAFailureToCountIsPublishedAsMinusOne(t *testing.T) {
	labels := Labels{"task": "41", "engine": "mysql", "object": "Users"}
	t.Cleanup(func() { RowCounts.Forget(labels) })

	SetRowCounts(labels, -1, 65067, time.Now())

	for _, sample := range RowCounts.Snapshot(SourceRows) {
		if sample.Labels.Key() == labels.Key() && sample.Value != -1 {
			t.Errorf("%s = %v, want -1", SourceRows, sample.Value)
		}
	}
}

// The measurement's age is what separates an hour-old number from a stopped
// collector: a gauge nobody updates looks exactly like one that is current.
func TestWhenTheCountWasTakenIsPublished(t *testing.T) {
	labels := Labels{"task": "39", "engine": "mongodb", "object": "carts"}
	t.Cleanup(func() { RowCounts.Forget(labels) })

	at := time.Unix(1788800000, 0)
	SetRowCounts(labels, 5, 5, at)

	var found bool
	for _, sample := range RowCounts.Snapshot(RowCountMeasuredAt) {
		if sample.Labels.Key() == labels.Key() {
			found = true
			if sample.Value != float64(at.Unix()) {
				t.Errorf("%s = %v, want %d", RowCountMeasuredAt, sample.Value, at.Unix())
			}
		}
	}
	if !found {
		t.Errorf("%s was not published", RowCountMeasuredAt)
	}
}

func TestForgettingAnObjectRemovesAllThreeSeries(t *testing.T) {
	labels := Labels{"task": "99", "engine": "mysql", "object": "gone"}
	SetRowCounts(labels, 1, 1, time.Now())
	ForgetRowCounts(labels)

	for _, name := range []string{SourceRows, TargetRows, RowCountMeasuredAt} {
		for _, sample := range RowCounts.Snapshot(name) {
			if sample.Labels.Key() == labels.Key() {
				t.Errorf("%s still carries the forgotten object", name)
			}
		}
	}
}
