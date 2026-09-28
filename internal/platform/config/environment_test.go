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

// Kubernetes names these after the service in front of this process, which is
// called sync. Nobody set them, nothing can read them, and reporting seven of
// them at start-up pushed the line that matters -- the one saying the database
// passwords are in the clear -- off the top of what anybody reads.
func TestTheVariablesKubernetesInjectsAreNotReported(t *testing.T) {
	injected := []string{
		"SYNC_SERVICE_HOST=10.60.117.91",
		"SYNC_SERVICE_PORT=8080",
		"SYNC_SERVICE_PORT_HTTP=8080",
		"SYNC_PORT=tcp://10.60.117.91:8080",
		"SYNC_PORT_80_TCP=tcp://10.60.117.91:80",
		"SYNC_PORT_80_TCP_PROTO=tcp",
		"SYNC_PORT_80_TCP_PORT=80",
		"SYNC_PORT_80_TCP_ADDR=10.60.117.91",
		// A second service whose name also begins with sync.
		"SYNC_UI_SERVICE_HOST=10.60.117.92",
		"SYNC_UI_PORT_8080_TCP_ADDR=10.60.117.92",
	}

	if got := UnknownVariables(injected); len(got) != 0 {
		t.Fatalf("reported variables nobody set: %v", got)
	}
}

// The bare form is told apart by its value, because somebody setting
// SYNC_PORT=8080 expects it to change the port this listens on, and it does
// not.
func TestAPortSomebodySetIsStillReported(t *testing.T) {
	got := UnknownVariables([]string{"SYNC_PORT=8080"})

	if len(got) != 1 || got[0] != "SYNC_PORT" {
		t.Fatalf("unknown = %v, want SYNC_PORT reported", got)
	}
}

func TestAMistypedVariableIsStillReportedBesideTheInjectedOnes(t *testing.T) {
	got := UnknownVariables([]string{
		"SYNC_SERVICE_HOST=10.60.117.91",
		"SYNC_VERIFY_INTERVALL=1h",
		"SYNC_PORT_8080_TCP_ADDR=10.60.117.91",
	})

	if len(got) != 1 || got[0] != "SYNC_VERIFY_INTERVALL" {
		t.Fatalf("unknown = %v, want only the mistyped one", got)
	}
}
