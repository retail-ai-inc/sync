package pipeline

import (
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// Every limit here has a default, and a task's configuration is allowed to
// leave any of them out -- the whole API silently dropped seven task settings
// once, so which value a zero resolves to is worth pinning rather than
// assuming. A default that quietly became zero would mean an unbounded queue
// or a batch that is never cut.

func TestAnUnsetLimitResolvesToItsDefault(t *testing.T) {
	var limits Limits
	if got := limits.maxEvents(); got != defaultMaxEvents {
		t.Errorf("maxEvents() = %d, want %d", got, defaultMaxEvents)
	}
	if got := limits.maxBytes(); got != defaultMaxBytes {
		t.Errorf("maxBytes() = %d, want %d", got, defaultMaxBytes)
	}
	if got := limits.maxTransactionEvents(); got != defaultMaxTransactionEvents {
		t.Errorf("maxTransactionEvents() = %d, want %d", got, defaultMaxTransactionEvents)
	}
}

func TestASetLimitIsUsed(t *testing.T) {
	limits := Limits{MaxEvents: 7, MaxBytes: 1024, MaxTransactionEvents: 3}
	if got := limits.maxEvents(); got != 7 {
		t.Errorf("maxEvents() = %d, want 7", got)
	}
	if got := limits.maxBytes(); got != 1024 {
		t.Errorf("maxBytes() = %d, want 1024", got)
	}
	if got := limits.maxTransactionEvents(); got != 3 {
		t.Errorf("maxTransactionEvents() = %d, want 3", got)
	}
}

func TestAnUnsetOptionResolvesToItsDefault(t *testing.T) {
	var options Options
	for name, got := range map[string]interface{}{
		"flushInterval":         options.flushInterval(),
		"queueBytes":            options.queueBytes(),
		"queueCapacity":         options.queueCapacity(),
		"snapshotQueueCapacity": options.snapshotQueueCapacity(),
		"reportInterval":        options.reportInterval(),
		"shutdownGrace":         options.shutdownGrace(),
	} {
		switch value := got.(type) {
		case time.Duration:
			if value <= 0 {
				t.Errorf("%s() = %v; an unset option must not resolve to zero", name, value)
			}
		case int:
			if value <= 0 {
				t.Errorf("%s() = %d; an unset option must not resolve to zero", name, value)
			}
		case int64:
			if value <= 0 {
				t.Errorf("%s() = %d; an unset option must not resolve to zero", name, value)
			}
		}
	}

	if got := options.flushInterval(); got != defaultFlushInterval {
		t.Errorf("flushInterval() = %v, want %v", got, defaultFlushInterval)
	}
	if got := options.queueBytes(); got != defaultQueueBytes {
		t.Errorf("queueBytes() = %d, want %d", got, defaultQueueBytes)
	}
	if got := options.queueCapacity(); got != defaultQueueCapacity {
		t.Errorf("queueCapacity() = %d, want %d", got, defaultQueueCapacity)
	}
	if got := options.snapshotQueueCapacity(); got != defaultSnapshotQueueCapacity {
		t.Errorf("snapshotQueueCapacity() = %d, want %d", got, defaultSnapshotQueueCapacity)
	}
	if got := options.shutdownGrace(); got != defaultShutdownGrace {
		t.Errorf("shutdownGrace() = %v, want %v", got, defaultShutdownGrace)
	}
}

func TestASetOptionIsUsed(t *testing.T) {
	options := Options{
		FlushInterval:         2 * time.Second,
		QueueBytes:            1 << 20,
		QueueCapacity:         11,
		SnapshotQueueCapacity: 13,
		ReportInterval:        3 * time.Second,
		ShutdownGrace:         17 * time.Second,
	}
	if got := options.flushInterval(); got != 2*time.Second {
		t.Errorf("flushInterval() = %v", got)
	}
	if got := options.queueBytes(); got != 1<<20 {
		t.Errorf("queueBytes() = %d", got)
	}
	if got := options.queueCapacity(); got != 11 {
		t.Errorf("queueCapacity() = %d", got)
	}
	if got := options.snapshotQueueCapacity(); got != 13 {
		t.Errorf("snapshotQueueCapacity() = %d", got)
	}
	if got := options.reportInterval(); got != 3*time.Second {
		t.Errorf("reportInterval() = %v", got)
	}
	if got := options.shutdownGrace(); got != 17*time.Second {
		t.Errorf("shutdownGrace() = %v", got)
	}
}

// TestAShutdownGraceFitsInsideKubernetesTerminationPeriod: a grace longer than
// the pod's thirty seconds means SIGKILL arrives mid-drain, and whatever the
// task had buffered goes with it.
func TestAShutdownGraceFitsInsideKubernetesTerminationPeriod(t *testing.T) {
	if defaultShutdownGrace >= 30*time.Second {
		t.Errorf("defaultShutdownGrace = %v, which is not inside the default "+
			"thirty-second termination grace period", defaultShutdownGrace)
	}
}

func TestAnUnsetResyncChunkSizeResolvesToItsDefault(t *testing.T) {
	resync := &Resync{}
	if got := resync.chunkSize(); got != defaultChunkSize {
		t.Errorf("chunkSize() = %d, want %d", got, defaultChunkSize)
	}
	resync.ChunkSize = 250
	if got := resync.chunkSize(); got != 250 {
		t.Errorf("chunkSize() = %d, want 250", got)
	}
}

// TestAnEmptyBatchIsNotCuttable: cutting an empty batch would commit a position
// nothing had been applied for, so a restart would resume past events the
// target never received.
func TestAnEmptyBatchIsNotCuttable(t *testing.T) {
	if (&batch{}).cuttable() {
		t.Error("an empty batch reported itself cuttable")
	}
}

func TestABatchIsCuttableOnlyAtATransactionBoundary(t *testing.T) {
	mid := &batch{events: []*domain.Event{
		{EndsTransaction: false},
	}}
	if mid.cuttable() {
		t.Error("a batch ending mid-transaction reported itself cuttable")
	}

	whole := &batch{events: []*domain.Event{
		{EndsTransaction: false},
		{EndsTransaction: true},
	}}
	if !whole.cuttable() {
		t.Error("a batch ending on a transaction boundary reported itself not cuttable")
	}
}

// TestHoldsSchemaChangeFindsOneAnywhere covers the check that keeps a schema
// change from sharing a batch with rows. MongoDB's catalogue is not
// transactional, so a batch carrying both cannot be applied atomically.
func TestHoldsSchemaChangeFindsOneAnywhere(t *testing.T) {
	for name, events := range map[string][]*domain.Event{
		"first":  {{Op: domain.OpSchema}, {Op: domain.OpInsert}},
		"last":   {{Op: domain.OpInsert}, {Op: domain.OpSchema}},
		"middle": {{Op: domain.OpInsert}, {Op: domain.OpSchema}, {Op: domain.OpUpdate}},
		"alone":  {{Op: domain.OpSchema}},
	} {
		t.Run(name, func(t *testing.T) {
			if !holdsSchemaChange(events) {
				t.Error("a batch carrying a schema change reported none")
			}
		})
	}

	for name, events := range map[string][]*domain.Event{
		"rows only": {{Op: domain.OpInsert}, {Op: domain.OpUpdate}, {Op: domain.OpDelete}},
		"empty":     {},
		"nil":       nil,
	} {
		t.Run(name, func(t *testing.T) {
			if holdsSchemaChange(events) {
				t.Error("a batch with no schema change reported one")
			}
		})
	}
}
