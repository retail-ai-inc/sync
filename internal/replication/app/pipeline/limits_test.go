package pipeline

import (
	"testing"
	"time"
)

func withStored(t *testing.T, stored Tuning) {
	t.Helper()
	previous := StoredTuning
	StoredTuning = func() Tuning { return stored }
	t.Cleanup(func() { StoredTuning = previous })
}

func TestStoredTuningFillsInWhatAnEngineLeftUnset(t *testing.T) {
	withStored(t, Tuning{
		Limits:                Limits{MaxEvents: 11, MaxBytes: 22, MaxTransactionEvents: 33},
		FlushInterval:         7 * time.Second,
		QueueCapacity:         44,
		QueueBytes:            55,
		SnapshotQueueCapacity: 66,
	})

	got := tuned(Options{})
	for name, pair := range map[string][2]interface{}{
		"MaxEvents":             {got.Limits.MaxEvents, 11},
		"MaxBytes":              {got.Limits.MaxBytes, 22},
		"MaxTransactionEvents":  {got.Limits.MaxTransactionEvents, 33},
		"FlushInterval":         {got.FlushInterval, 7 * time.Second},
		"QueueCapacity":         {got.QueueCapacity, 44},
		"QueueBytes":            {got.QueueBytes, int64(55)},
		"SnapshotQueueCapacity": {got.SnapshotQueueCapacity, 66},
	} {
		if pair[0] != pair[1] {
			t.Errorf("%s = %v, want %v", name, pair[0], pair[1])
		}
	}
}

// A task saying something is more specific than a global default, so what the
// engine set has to survive.
func TestWhatAnEngineSetSurvivesStoredTuning(t *testing.T) {
	withStored(t, Tuning{
		Limits:        Limits{MaxEvents: 11},
		FlushInterval: 7 * time.Second,
	})

	got := tuned(Options{Limits: Limits{MaxEvents: 2}, FlushInterval: time.Second})
	if got.Limits.MaxEvents != 2 {
		t.Errorf("MaxEvents = %d, want 2", got.Limits.MaxEvents)
	}
	if got.FlushInterval != time.Second {
		t.Errorf("FlushInterval = %v, want 1s", got.FlushInterval)
	}
}

func TestNothingStoredLeavesTheOptionsAlone(t *testing.T) {
	previous := StoredTuning
	StoredTuning = nil
	t.Cleanup(func() { StoredTuning = previous })

	if got := tuned(Options{}); got.Limits != (Limits{}) || got.FlushInterval != 0 ||
		got.QueueCapacity != 0 || got.QueueBytes != 0 || got.SnapshotQueueCapacity != 0 {
		t.Errorf("tuned an empty Options into %+v", got)
	}
	if got := CopyBatch(200); got != 200 {
		t.Errorf("CopyBatch(200) = %d, want the fallback", got)
	}
	if got := Await(time.Second); got != time.Second {
		t.Errorf("Await(1s) = %v, want the fallback", got)
	}
}

// The engines each keep their own default, so a deployment that has set
// nothing gets the fallback and one that has set something gets that.
func TestCopyBatchAndAwaitPreferWhatWasStored(t *testing.T) {
	withStored(t, Tuning{CopyBatchRows: 500, StreamAwait: 150 * time.Millisecond})

	if got := CopyBatch(200); got != 500 {
		t.Errorf("CopyBatch(200) = %d, want 500", got)
	}
	if got := Await(time.Second); got != 150*time.Millisecond {
		t.Errorf("Await(1s) = %v, want 150ms", got)
	}

	withStored(t, Tuning{})
	if got := CopyBatch(200); got != 200 {
		t.Errorf("CopyBatch(200) = %d, want the fallback", got)
	}
	if got := Await(time.Second); got != time.Second {
		t.Errorf("Await(1s) = %v, want the fallback", got)
	}
}
