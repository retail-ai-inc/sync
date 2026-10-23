package export

import (
	"os/exec"
	"strings"
	"testing"
)

// A recorded outcome of "exit status 1" is not a reason. Every backup job in
// staging failed with exactly that, and what the command had said was in a
// container log that had since been replaced.

func TestAFailingCommandsComplaintReachesTheError(t *testing.T) {
	var complained complaint
	cmd := exec.Command("sh", "-c", "echo 'ERROR 1045: access denied' >&2; exit 1")
	cmd.Stderr = &complained

	err := cmd.Run()
	if err == nil {
		t.Fatal("the command succeeded")
	}
	if !strings.Contains(complained.said(), "access denied") {
		t.Errorf("the error would read %q, which does not say why", complained.said())
	}
}

// A command that fails per row could say megabytes, and this ends up in a
// column of the control database.
func TestOnlyTheTailIsKept(t *testing.T) {
	var complained complaint
	_, _ = complained.Write([]byte(strings.Repeat("a", stderrTailBytes)))
	_, _ = complained.Write([]byte(strings.Repeat("b", stderrTailBytes)))

	kept := complained.String()
	if len(kept) > stderrTailBytes {
		t.Errorf("kept %d bytes, want at most %d", len(kept), stderrTailBytes)
	}
	if strings.Contains(kept, "a") {
		t.Error("kept the beginning; the end is where a command says why it stopped")
	}
}

// A command that failed silently says so, rather than leaving a message that
// trails off after the exit status.
func TestSilenceIsReportedAsSilence(t *testing.T) {
	var complained complaint
	if got := complained.said(); !strings.Contains(got, "said nothing") {
		t.Errorf("said() = %q, want it to report the silence", got)
	}
}
