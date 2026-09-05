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

// The read lag used to be measured from whatever the last event carried,
// heartbeats included. A heartbeat is the stream saying it had nothing, so the
// gauge walked from zero up to the heartbeat interval and dropped back on a
// link with no delay at all.
func TestAHeartbeatMeansNothingIsWaitingToBeRead(t *testing.T) {
	ns := domain.Namespace{DB: "shop", Object: "orders"}
	old := time.Now().Add(-30 * time.Second)
	reader := &fakeReader{events: []*domain.Event{
		{NS: ns, Op: domain.OpInsert, Key: "1", Bytes: 1, SourceTime: old,
			Pos: domain.Position{Payload: "p1"}, EndsTransaction: true},
		{Heartbeat: true, SourceTime: old, EndsTransaction: true,
			Pos: domain.Position{Payload: "p2"}},
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
	if lag != 0 {
		t.Errorf("after a heartbeat the read lag is %v, want 0 -- the stream said "+
			"it had nothing, so nothing is waiting", lag)
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
