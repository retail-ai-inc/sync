package app

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/sirupsen/logrus"
)

// capturingLogger returns a logger writing into a buffer, so a test can tell
// which branch of the dispatcher ran from what was logged.
func capturingLogger() (*logrus.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	l := logrus.New()
	l.SetOutput(buf)
	l.SetLevel(logrus.DebugLevel)
	return l, buf
}

// TestCountAndLogTablesIgnoresUnknownEngines records that the dispatcher
// answers an engine it does not know with one debug line and nothing else.
func TestCountAndLogTablesIgnoresUnknownEngines(t *testing.T) {
	for _, engine := range []string{"cassandra", "", "mongo", "MySQL2", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			log, buf := capturingLogger()

			countAndLogTables(context.Background(), config.SyncConfig{ID: 1, Type: engine}, log)

			if !strings.Contains(buf.String(), "not implemented") {
				t.Fatalf("engine %q reached a counter instead of the default branch: %s",
					engine, buf.String())
			}
		})
	}
}

// TestTheDispatcherFoldsCase records that the monitor accepts casings the syncer
// dispatch in cmd/sync rejects: "MongoDB" is monitored while never being
// replicated, because the syncer switch compares the raw string (T-053).
func TestTheDispatcherFoldsCase(t *testing.T) {
	for _, engine := range []string{"MongoDB", "MONGODB", "MySQL", "MariaDB", "PostgreSQL", "REDIS"} {
		t.Run(engine, func(t *testing.T) {
			log, buf := capturingLogger()

			countAndLogTables(context.Background(), config.SyncConfig{ID: 1, Type: engine}, log)

			if strings.Contains(buf.String(), "not implemented") {
				t.Fatalf("engine %q fell through to the default branch; the fold appears "+
					"to be gone, so assert the rejection instead", engine)
			}
		})
	}
}

// TestStartRowCountMonitoringTakesNoMeasurementBeforeTheFirstTick records that
// the ticker fires after one interval, not immediately, so a process that dies
// inside its first monitoring interval records nothing at all.
func TestStartRowCountMonitoringTakesNoMeasurementBeforeTheFirstTick(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &config.Config{SyncConfigs: []config.SyncConfig{
		{ID: 1, Type: "cassandra", Enable: true},
	}}

	StartRowCountMonitoring(ctx, cfg, quietLogger(), time.Hour)

	// With an hour-long interval nothing can have run yet. Cancelling has to be
	// enough to stop both goroutines.
	cancel()
	time.Sleep(50 * time.Millisecond)
}

func TestTheMonitorLoopStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	cfg := &config.Config{SyncConfigs: []config.SyncConfig{
		{ID: 1, Type: "cassandra", Enable: true},
	}}

	StartRowCountMonitoring(ctx, cfg, quietLogger(), 10*time.Millisecond)
	time.Sleep(60 * time.Millisecond) // let a few ticks pass
	cancel()
	time.Sleep(60 * time.Millisecond) // and a few more that must not run
}

// TestDisabledTasksAreNotMeasured records that the loop skips a task whose
// enable flag is clear, so a paused task's row counts stop being recorded and
// its last measurement stays in the monitoring log indefinitely.
func TestDisabledTasksAreNotMeasured(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A disabled task of a recognised engine pointed at a port that refuses
	// connections. If the loop measured it, the tick would block on the dial.
	cfg := &config.Config{SyncConfigs: []config.SyncConfig{
		{ID: 1, Type: "mongodb", Enable: false,
			SourceConnection: "mongodb://127.0.0.1:1/src",
			TargetConnection: "mongodb://127.0.0.1:1/tgt"},
	}}

	StartRowCountMonitoring(ctx, cfg, quietLogger(), 10*time.Millisecond)
	time.Sleep(80 * time.Millisecond)
}

// It printed "Daily summary completed" whatever had happened, so a MySQL task
// with a dateRange condition was missing from a summary that reported itself
// complete.
func TestTheDailySummarySaysWhatItLeftOut(t *testing.T) {
	log, buf := capturingLogger()
	cfg := &config.Config{SyncConfigs: []config.SyncConfig{
		{ID: 1, Type: "mysql", Enable: true},
		{ID: 2, Type: "postgresql", Enable: true},
		{ID: 3, Type: "redis", Enable: true},
		{ID: 4, Type: "cassandra", Enable: true},
		{ID: 5, Type: "mongodb", Enable: false}, // disabled, so skipped entirely
	}}

	logYesterdayDataVolume(context.Background(), cfg, log)

	out := buf.String()
	if strings.Contains(out, "Daily summary completed") {
		t.Errorf("the summary called itself complete while leaving four tasks out: %s", out)
	}
	for _, engine := range []string{"mysql", "postgresql", "redis", "cassandra"} {
		if !strings.Contains(out, engine) {
			t.Errorf("the %s task was left out without being named: %s", engine, out)
		}
	}
	// The disabled MongoDB task is skipped entirely, not reported as missing.
	if strings.Contains(out, "5 (mongodb)") {
		t.Errorf("a disabled task was reported as missing from the summary: %s", out)
	}
}

// TestTheDailySummaryIsScheduledForJSTMidnight records that the summary runs
// at 00:05 JST, computed from the machine's clock at start-up.
func TestTheDailySummaryIsScheduledForJSTMidnight(t *testing.T) {
	jst, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Skipf("the JST database is unavailable: %v", err)
	}

	now := time.Now().In(jst)
	next := time.Date(now.Year(), now.Month(), now.Day(), 0, 5, 0, 0, jst)
	if now.After(next) {
		next = next.AddDate(0, 0, 1)
	}

	if wait := next.Sub(now); wait <= 0 || wait > 24*time.Hour+5*time.Minute {
		t.Errorf("the computed wait is %v, which is outside one day", wait)
	}
}
