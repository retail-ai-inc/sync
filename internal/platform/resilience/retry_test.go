package resilience

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetrySucceedsImmediately(t *testing.T) {
	calls := 0
	start := time.Now()

	err := Retry(t.Context(), 5, 50*time.Millisecond, 2.0, func() error {
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

	err := Retry(t.Context(), 5, time.Millisecond, 2.0, func() error {
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

	err := Retry(t.Context(), 4, time.Millisecond, 2.0, func() error {
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

// The loop used to sleep after every failure including the last, so with the
// settings the syncers use — Retry(5, 2s, 2.0) — an unreachable source was
// reported thirty-two seconds after the last attempt had already failed.
func TestTheLastAttemptIsNotFollowedByASleep(t *testing.T) {
	const delay = 60 * time.Millisecond
	start := time.Now()

	err := Retry(t.Context(), 2, delay, 1.0, func() error { return errors.New("always fails") })

	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("Retry returned nil for an operation that always failed")
	}
	// Two attempts, one sleep between them.
	if elapsed >= 2*delay {
		t.Errorf("Retry took %v for two attempts %v apart; it is still sleeping "+
			"after the final failure", elapsed, delay)
	}
}

// TestCancellingStopsTheWaiting covers shutdown. A syncer waiting on a source
// that is down used to go on sleeping for the best part of a minute, while the
// supervisor gives a task ten seconds to stop before it gives up on it.
func TestCancellingStopsTheWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0

	start := time.Now()
	err := Retry(ctx, 5, time.Minute, 2.0, func() error {
		calls++
		cancel()
		return errors.New("source is down")
	})

	if err == nil {
		t.Fatal("Retry returned nil after being cancelled")
	}
	if calls != 1 {
		t.Errorf("the operation ran %d times, want 1 before the cancellation was noticed", calls)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Retry took %v to notice the cancellation", elapsed)
	}
}

// TestAPermanentFailureIsNotRetried covers the marker the reconnect helpers use
// to say that waiting will not help: a connection string the driver will never
// parse should be reported now, not in sixty-two seconds.
func TestAPermanentFailureIsNotRetried(t *testing.T) {
	calls := 0
	want := errors.New("invalid connection string")

	err := Retry(t.Context(), 5, time.Millisecond, 2.0, func() error {
		calls++
		return permanentFailure{want}
	})

	if !errors.Is(err, want) {
		t.Errorf("Retry returned %v, want the underlying error", err)
	}
	if calls != 1 {
		t.Errorf("the operation ran %d times, want 1", calls)
	}
}

// TestRetryZeroAttempts covers a caller mistake that used to read as success:
// the loop body never ran and nil came back for work never done.
func TestRetryZeroAttempts(t *testing.T) {
	calls := 0

	err := Retry(t.Context(), 0, time.Millisecond, 2.0, func() error {
		calls++
		return errors.New("never runs")
	})

	if calls != 0 {
		t.Errorf("the operation ran %d times, want 0", calls)
	}
	if err == nil {
		t.Error("Retry returned nil for an operation it never ran")
	}
}
