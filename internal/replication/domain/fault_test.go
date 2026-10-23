package domain

import (
	"errors"
	"fmt"
	"testing"
)

// TestAnUnrecoverableStopIsRecognised is the distinction the supervisor turns
// on: a task that stopped for a reason retrying cannot fix must not be
// restarted, because every attempt fails the same way and the retries bury the
// one message an operator needs to read.
func TestAnUnrecoverableStopIsRecognised(t *testing.T) {
	err := Unrecoverable("the binlog position %s has been purged", "mysql-bin.000042")

	if !IsUnrecoverable(err) {
		t.Error("an unrecoverable stop was not recognised as one")
	}
	if !errors.Is(err, ErrUnrecoverable) {
		t.Error("the error does not wrap ErrUnrecoverable")
	}
}

// TestTheReasonSurvivesTheWrapping matters because the reason is the whole
// point: "cannot continue" without saying what happened leaves the operator
// reading binlogs by hand.
func TestTheReasonSurvivesTheWrapping(t *testing.T) {
	err := Unrecoverable("the oplog no longer holds %d", 7)

	if got := err.Error(); got != ErrUnrecoverable.Error()+": the oplog no longer holds 7" {
		t.Errorf("error = %q", got)
	}
}

// TestAnOrdinaryFailureIsRetried is the case that must not be misread: a source
// briefly unreachable across the region boundary has to come back, and treating
// it as final would mean replication stopping the first time the network
// hiccuped.
func TestAnOrdinaryFailureIsRetried(t *testing.T) {
	for name, err := range map[string]error{
		"a connection reset": errors.New("connection reset by peer"),
		"nothing at all":     nil,
	} {
		t.Run(name, func(t *testing.T) {
			if IsUnrecoverable(err) {
				t.Errorf("%v was reported as unrecoverable", err)
			}
		})
	}
}

// TestAWrappedUnrecoverableStopIsStillOne covers the path it actually takes.
func TestAWrappedUnrecoverableStopIsStillOne(t *testing.T) {
	err := fmt.Errorf("start the mysql task: %w", Unrecoverable("the position is gone"))

	if !IsUnrecoverable(err) {
		t.Error("an unrecoverable stop stopped being one once it was wrapped")
	}
}
