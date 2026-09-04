package resilience

import (
	"errors"
	"fmt"
	"testing"
)

// Whether a failure is worth trying again is the one judgement this package
// makes, and both directions cost: a permanent failure retried spends five
// attempts and half a minute of backoff before saying what was wrong, and a
// transient one reported as permanent stops a task a regional failover would
// have healed on its own.

// TestARetryableMongoCodeIsFoundInAWrappedError covers the text fallback. A
// wrapped error that has lost its type still carries the number, and reading
// only the driver's typed accounting would report a retryable failure as
// permanent -- which is what a failover produces.
func TestARetryableMongoCodeIsFoundInAWrappedError(t *testing.T) {
	for _, code := range retryableMongoCodes {
		wrapped := fmt.Errorf("connect to the source: %w",
			fmt.Errorf("some detail (error code %d) more detail", code))

		if !mongoServerRetries(wrapped) {
			t.Errorf("a wrapped error carrying code %d was treated as permanent", code)
		}
	}
}

func TestAnUnrelatedCodeIsNotRetried(t *testing.T) {
	// 13 is Unauthorized: waiting does not grant permission.
	if mongoServerRetries(errors.New("not authorized (error code 13)")) {
		t.Error("an authorisation failure was treated as retryable, so a bad " +
			"credential is retried for half a minute before being reported")
	}
}

func TestAnErrorWithNoCodeIsNotRetriedOnText(t *testing.T) {
	if mongoServerRetries(errors.New("something went wrong")) {
		t.Error("an error with no code at all was treated as retryable")
	}
}

// TestACodeEmbeddedInAnotherNumberIsNotMatched: the codes are looked for behind
// their "error code" prefix precisely so a byte count or a document id that
// happens to contain one does not read as a retryable failure.
func TestACodeEmbeddedInAnotherNumberIsNotMatched(t *testing.T) {
	code := retryableMongoCodes[0]
	if mongoServerRetries(fmt.Errorf("wrote %d bytes", code)) {
		t.Errorf("a message merely containing %d was treated as retryable", code)
	}
}

// TestRecoveredOfNothingIsNothing. Guard calls this with whatever recover()
// returned, which is nil on every ordinary return -- so a non-nil error here
// would turn every successful goroutine into a failed task.
func TestRecoveredOfNothingIsNothing(t *testing.T) {
	if err := Recovered(nil); err != nil {
		t.Errorf("Recovered(nil) = %v", err)
	}
}
