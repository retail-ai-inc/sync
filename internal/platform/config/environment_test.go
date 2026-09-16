package config

import (
	"strings"
	"testing"
)

// A knob that does nothing is worse than one that is missing: it is set, no
// error appears, and the deployment is believed to be configured the way the
// documentation says. Four variables were documented and read by nothing.
func TestAVariableNothingReadsIsReported(t *testing.T) {
	unknown := UnknownVariables([]string{
		"PATH=/usr/bin",
		"SYNC_DB_PATH=/mnt/state/sync.db",
		"SYNC_MONGO_BUFFER_LIMIT_BYTES=8589934592",
		"SYNC_TYPO_HERE=1",
		"SYNC_VERIFY_INTERVAL=3600",
		"NOT_SYNC_ANYTHING=x",
	})

	if len(unknown) != 2 {
		t.Fatalf("unknown = %v, want the two nothing reads", unknown)
	}
	joined := strings.Join(unknown, ",")
	for _, want := range []string{"SYNC_MONGO_BUFFER_LIMIT_BYTES", "SYNC_TYPO_HERE"} {
		if !strings.Contains(joined, want) {
			t.Errorf("unknown = %v, want it to name %s", unknown, want)
		}
	}
	for _, unwanted := range []string{"SYNC_DB_PATH", "SYNC_VERIFY_INTERVAL", "PATH", "NOT_SYNC"} {
		if strings.Contains(joined, unwanted) {
			t.Errorf("unknown = %v, names %s which is read", unknown, unwanted)
		}
	}
}

// An entry with no "=" is not a variable, and must not be reported as one.
func TestAMalformedEnvironmentEntryIsIgnored(t *testing.T) {
	if got := UnknownVariables([]string{"SYNC_NO_EQUALS", ""}); len(got) != 0 {
		t.Errorf("unknown = %v, want nothing", got)
	}
}
