package mysql

import (
	"errors"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// TestAPurgedBinlogIsNotSomethingToRetry is the difference between waiting and
// acting.
//
// Almost every stream failure is worth retrying: the network drops, the source
// restarts, the connection comes back. A purged binlog is not — the position no
// longer exists, so every attempt fails identically, and the only way forward
// is a fresh copy that a human has to decide on. Retrying it forever instead
// leaves the task looking alive while nothing replicates.
func TestAPurgedBinlogIsNotSomethingToRetry(t *testing.T) {
	r := &Reader{Labels: metrics.Labels{"task": t.Name()}}
	defer metrics.Default.Forget(r.Labels)

	for _, text := range []string{
		"ERROR 1236 (HY000): Could not find first log file name in binary log index file",
		"Could not find next log; the first event could not be read",
		"Requested MASTER_LOG_FILE mysql-bin.000004 is older than the purged logs",
		"binary log is not available",
	} {
		t.Run(text[:24], func(t *testing.T) {
			err := r.classify(errors.New(text))
			if err == nil {
				t.Fatal("a purged binlog was reported as no failure at all")
			}
			if !domain.IsUnrecoverable(err) {
				t.Errorf("classified as retryable: %v", err)
			}
			if !strings.Contains(err.Error(), "fresh copy") {
				t.Errorf("the error does not say what to do about it: %v", err)
			}
		})
	}
}

// TestAnOrdinaryStreamFailureStaysRetryable. A dropped connection is the
// ordinary case, and turning it into "take a fresh copy" would re-copy a live
// payment database every time the network hiccuped.
func TestAnOrdinaryStreamFailureStaysRetryable(t *testing.T) {
	r := &Reader{Labels: metrics.Labels{"task": t.Name()}}
	defer metrics.Default.Forget(r.Labels)

	err := r.classify(errors.New("connection reset by peer"))
	if err == nil {
		t.Fatal("a stream failure was swallowed")
	}
	if domain.IsUnrecoverable(err) {
		t.Errorf("a dropped connection was classified as unrecoverable: %v", err)
	}
}

// TestClassifyingAFailureCountsItAndMarksTheStreamDown, so a link that keeps
// dropping shows as a rising count rather than only as a silence.
func TestClassifyingAFailureCountsItAndMarksTheStreamDown(t *testing.T) {
	labels := metrics.Labels{"task": t.Name()}
	r := &Reader{Labels: labels}
	defer metrics.Default.Forget(labels)

	metrics.SetConnected(labels, true)
	r.classify(errors.New("connection reset by peer"))

	var connected, disconnects float64
	for _, s := range metrics.Default.Snapshot(metrics.Connected) {
		if s.Labels.Key() == labels.Key() {
			connected = s.Value
		}
	}
	for _, s := range metrics.Default.Snapshot(metrics.DisconnectsTotal) {
		if s.Labels.Key() == labels.Key() {
			disconnects = s.Value
		}
	}
	if connected != 0 {
		t.Errorf("sync_source_connected = %v after a failure, want 0", connected)
	}
	if disconnects != 1 {
		t.Errorf("sync_source_disconnects_total = %v, want 1", disconnects)
	}
}

func TestNoFailureIsNoFailure(t *testing.T) {
	r := &Reader{Labels: metrics.Labels{"task": t.Name()}}
	defer metrics.Default.Forget(r.Labels)

	if err := r.classify(nil); err != nil {
		t.Errorf("classify(nil) = %v, want nil", err)
	}
}

// TestASourceWithoutGTIDsIsWarnedAboutAtStartup, because the alternative is
// finding out during the failover this deployment exists for.
//
// A file-and-offset position only means something on the server that produced
// it. After a failover to a new primary it either cannot be found or points at
// unrelated bytes, and recovering means a fresh copy of a live payment
// database. Replication works either way, so this is a warning and not a
// refusal — but a silent one would be useless.
func TestASourceWithoutGTIDsIsWarnedAboutAtStartup(t *testing.T) {
	warning := describeGTIDMode("OFF", false)
	if warning == "" {
		t.Fatal("gtid_mode=OFF produced no warning")
	}
	if !strings.Contains(warning, "gtid_mode=OFF") {
		t.Errorf("the warning does not name the setting: %q", warning)
	}
	if !strings.Contains(warning, "failover") {
		t.Errorf("the warning does not say when it will hurt: %q", warning)
	}
}

// TestASourceWithGTIDsIsNotWarnedAbout, and neither is MariaDB, which tracks
// them through different settings entirely.
func TestASourceWithGTIDsIsNotWarnedAbout(t *testing.T) {
	for _, c := range []struct {
		mode    string
		mariaDB bool
	}{
		{"ON", false},
		{"on", false},
		{"OFF", true}, // MariaDB: the setting means something else there
		{"", false},   // a server old enough not to have the setting
	} {
		if got := describeGTIDMode(c.mode, c.mariaDB); got != "" {
			t.Errorf("describeGTIDMode(%q, %v) warned: %q", c.mode, c.mariaDB, got)
		}
	}
}

// TestOnlyASettingNameIsInterpolated is the guard between a configuration value
// and a query. The name goes into SHOW GLOBAL VARIABLES, where it cannot be
// parameterised, so anything that is not a bare identifier has to be refused.
func TestOnlyASettingNameIsInterpolated(t *testing.T) {
	for _, name := range []string{"gtid_mode", "binlog_format", "log_bin"} {
		if !settingName(name) {
			t.Errorf("%q was refused as a setting name", name)
		}
	}
	for _, name := range []string{
		"", "gtid mode", "gtid_mode; DROP TABLE users", "gtid-mode", "gtid_mode'",
		"binlog_format=ROW", "1", "gtid_mode\n",
	} {
		if settingName(name) {
			t.Errorf("%q was accepted as a setting name, and it goes straight into "+
				"a query", name)
		}
	}
}
