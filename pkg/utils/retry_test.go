package utils

import (
	"errors"
	"testing"
	"time"
)

func TestRetrySucceedsImmediately(t *testing.T) {
	calls := 0
	start := time.Now()

	err := Retry(5, 50*time.Millisecond, 2.0, func() error {
		calls++
		return nil
	})

	if err != nil {
		t.Errorf("Retry returned %v, want nil", err)
	}
	if calls != 1 {
		t.Errorf("the operation ran %d times, want 1", calls)
	}
	// No sleep happens when the first attempt succeeds.
	if elapsed := time.Since(start); elapsed > 40*time.Millisecond {
		t.Errorf("Retry took %v for an immediate success", elapsed)
	}
}

func TestRetrySucceedsAfterFailures(t *testing.T) {
	calls := 0

	err := Retry(5, time.Millisecond, 2.0, func() error {
		calls++
		if calls < 3 {
			return errors.New("not yet")
		}
		return nil
	})

	if err != nil {
		t.Errorf("Retry returned %v, want nil", err)
	}
	if calls != 3 {
		t.Errorf("the operation ran %d times, want 3", calls)
	}
}

func TestRetryReturnsLastError(t *testing.T) {
	last := errors.New("attempt 4")
	calls := 0

	err := Retry(4, time.Millisecond, 2.0, func() error {
		calls++
		if calls == 4 {
			return last
		}
		return errors.New("earlier failure")
	})

	if !errors.Is(err, last) {
		t.Errorf("Retry returned %v, want the final error", err)
	}
	if calls != 4 {
		t.Errorf("the operation ran %d times, want 4", calls)
	}
}

// TestRetrySleepsAfterTheFinalFailure records wasted time: the loop sleeps
// after every failure including the last one, then falls out and returns. With
// the settings the syncers use — Retry(5, 2s, 2.0) — the delays are 2+4+8+16+32
// seconds, and that final 32-second sleep buys nothing. Connection failures are
// therefore reported half a minute later than necessary.
func TestRetrySleepsAfterTheFinalFailure(t *testing.T) {
	const delay = 40 * time.Millisecond
	start := time.Now()

	_ = Retry(2, delay, 1.0, func() error { return errors.New("always fails") })

	elapsed := time.Since(start)
	// Two attempts, two sleeps. One sleep would be enough.
	if elapsed < 2*delay {
		t.Errorf("Retry took %v; the trailing sleep may have been removed, which "+
			"would be an improvement", elapsed)
	}
}

// TestRetryCannotBeCancelled records that Retry takes no context. A syncer
// waiting inside it keeps sleeping through shutdown, and with the production
// settings that is up to a minute after cancellation. cmd/sync waits ten
// seconds for a graceful stop before exiting anyway.
func TestRetryCannotBeCancelled(t *testing.T) {
	// The signature is the evidence: there is no way to pass a context in.
	var _ func(int, time.Duration, float64, func() error) error = Retry
}

func TestRetryZeroAttempts(t *testing.T) {
	calls := 0

	err := Retry(0, time.Millisecond, 2.0, func() error {
		calls++
		return errors.New("never runs")
	})

	// The loop body never executes, so the operation is not attempted and the
	// caller gets a nil error for work that never happened.
	if calls != 0 {
		t.Errorf("the operation ran %d times, want 0", calls)
	}
	if err != nil {
		t.Errorf("Retry returned %v; with zero attempts it reports success for "+
			"an operation it never ran", err)
	}
}
