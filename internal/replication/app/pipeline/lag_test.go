package pipeline

import (
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// gaugeFor reads one sample of a gauge back.
func readLagOf(t *testing.T, labels metrics.Labels) (float64, bool) {
	t.Helper()
	for _, sample := range metrics.Default.Snapshot(metrics.ReadLagSeconds) {
		if sample.Labels["task"] == labels["task"] {
			return sample.Value, true
		}
	}
	return 0, false
}

// A heartbeat leaves the gauge alone. Measuring its own age made the gauge walk
// up to the heartbeat interval and back on a link with no delay; writing zero
// instead is no better, because heartbeats outnumber events on a quiet source
// and every scrape would land on one.
func TestAHeartbeatDoesNotDisturbTheReadLag(t *testing.T) {
	ns := domain.Namespace{DB: "shop", Object: "orders"}
	stale := time.Now().Add(-30 * time.Second)
	reader := &fakeReader{events: []*domain.Event{
		// A real event read with no delay at all.
		{NS: ns, Op: domain.OpInsert, Key: "1", Bytes: 1, WallTime: time.Now(),
			Pos: domain.Position{Payload: "p1"}, EndsTransaction: true},
		// Then heartbeats carrying a much older clock, which is what a quiet
		// source produces once a second.
		{Heartbeat: true, SourceTime: stale, WallTime: stale, EndsTransaction: true,
			Pos: domain.Position{Payload: "p2"}},
		{Heartbeat: true, SourceTime: stale, WallTime: stale, EndsTransaction: true,
			Pos: domain.Position{Payload: "p3"}},
	}}

	r := newRunner(t, reader, &fakeApplier{}, newStore())
	labels := metrics.Labels{"task": t.Name()}
	r.Opts.Labels = labels
	t.Cleanup(func() { metrics.Default.Forget(labels) })

	if err := runFor(t, r, 300*time.Millisecond); err != nil {
		t.Fatalf("Run: %v", err)
	}

	lag, ok := readLagOf(t, labels)
	if !ok {
		t.Fatal("the read lag was never reported")
	}
	// The real event's reading survives the heartbeats that followed it.
	if lag > 5 {
		t.Errorf("the read lag is %v, so a heartbeat overwrote the measurement", lag)
	}
}

// An ordering clock that counts whole seconds cannot measure a delay shorter
// than one, so a source that reports a wall clock is measured from that.
func TestTheReadLagPrefersTheWallClock(t *testing.T) {
	ns := domain.Namespace{DB: "shop", Object: "orders"}
	now := time.Now()
	reader := &fakeReader{events: []*domain.Event{
		{NS: ns, Op: domain.OpInsert, Key: "1", Bytes: 1,
			// The ordering clock says thirty seconds ago; the wall clock says now.
			SourceTime: now.Add(-30 * time.Second),
			WallTime:   now,
			Pos:        domain.Position{Payload: "p1"}, EndsTransaction: true},
	}}

	r := newRunner(t, reader, &fakeApplier{}, newStore())
	labels := metrics.Labels{"task": t.Name()}
	r.Opts.Labels = labels
	t.Cleanup(func() { metrics.Default.Forget(labels) })

	if err := runFor(t, r, 300*time.Millisecond); err != nil {
		t.Fatalf("Run: %v", err)
	}

	lag, ok := readLagOf(t, labels)
	if !ok {
		t.Fatal("the read lag was never reported")
	}
	if lag > 5 {
		t.Errorf("the read lag is %v, so it was measured from the ordering clock "+
			"rather than the wall clock", lag)
	}
}
