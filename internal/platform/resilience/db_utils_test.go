package resilience

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// quietLogger returns a logger that discards output, so retry tests do not
// flood the test log.
func quietLogger() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l
}

func TestIsConnectionError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"connection refused", errors.New("dial tcp 127.0.0.1:3306: connect: connection refused"), true},
		{"broken pipe", errors.New("write tcp: broken pipe"), true},
		{"reset by peer", errors.New("read tcp: connection reset by peer"), true},
		{"i/o timeout", errors.New("read tcp: i/o timeout"), true},
		{"EOF", io.EOF, true},
		{"network unreachable", errors.New("network is unreachable"), true},
		{"syntax error", errors.New("syntax error near 'SELCT'"), false},
		{"duplicate key", errors.New("Error 1062: Duplicate entry 'a' for key 'PRIMARY'"), false},
		{"permission denied", errors.New("Access denied for user 'root'"), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsConnectionError(tc.err); got != tc.want {
				t.Errorf("IsConnectionError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// A failure that no amount of waiting can fix must not be retried: the backoff
// is spent for nothing and the message somebody needs to read is buried under
// three identical warnings. The old classifier scanned the error text for
// "connection" and "EOF", so a malformed connection string and a truncated
// configuration document both looked transient.
func TestAFailureWaitingCannotFixIsNotRetried(t *testing.T) {
	permanent := []error{
		errors.New(`invalid connection string: missing "@"`),
		errors.New("unexpected EOF while parsing config JSON"),
		errors.New("error parsing uri: scheme must be mongodb:// — bad connection uri"),
		errors.New("syntax error near 'SELCT'"),
		errors.New("Access denied for user 'root'"),
	}

	for _, err := range permanent {
		if IsConnectionError(err) {
			t.Errorf("IsConnectionError(%q) = true, want false", err)
		}
	}
}

// These are what the drivers say while a replica set elects a new primary or a
// managed instance restarts for maintenance — the moments this tool exists to
// survive. None of them matched the old substring list, so each was given up on
// at the first attempt.
func TestTheFailuresOfAFailoverAreRetried(t *testing.T) {
	transient := []error{
		errors.New("server selection error: context deadline exceeded"),
		errors.New("no reachable servers"),
		errors.New("topology is closed"),
		errors.New("database is locked"),
		errors.New("Error 1213: Deadlock found when trying to get lock"),
		errors.New("connection() error occurred during connection handshake"),
		errors.New("socket was unexpectedly closed"),
		errors.New("client is disconnected"),
		errors.New("not primary; the current primary is host:27017"),
		errors.New("Lost connection to MySQL server during query"),
	}

	for _, err := range transient {
		if !IsConnectionError(err) {
			t.Errorf("IsConnectionError(%q) = false, want true", err)
		}
	}
}

// A cancelled context is this process deciding to stop. Retrying through it is
// how a task asked to shut down kept trying to reach a database it was told to
// let go of.
func TestACancelledContextIsNotAConnectionFailure(t *testing.T) {
	if IsConnectionError(context.Canceled) {
		t.Error("IsConnectionError(context.Canceled) = true, want false")
	}
	if IsConnectionError(fmt.Errorf("write row: %w", context.Canceled)) {
		t.Error("a wrapped cancellation was read as a connection failure")
	}
}

func TestRetryDBOperationSucceedsWithoutRetrying(t *testing.T) {
	calls := 0
	err := RetryDBOperation(context.Background(), quietLogger(), "op", func() error {
		calls++
		return nil
	})

	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if calls != 1 {
		t.Errorf("fn called %d times, want 1", calls)
	}
}

func TestRetryDBOperationDoesNotRetryPermanentErrors(t *testing.T) {
	want := errors.New("syntax error")
	calls := 0

	start := time.Now()
	err := RetryDBOperation(context.Background(), quietLogger(), "op", func() error {
		calls++
		return want
	})

	if !errors.Is(err, want) {
		t.Errorf("err = %v, want %v", err, want)
	}
	if calls != 1 {
		t.Errorf("fn called %d times, want 1", calls)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("took %v — a permanent error should not sleep", elapsed)
	}
}

func TestRetryDBOperationRecoversOnASecondAttempt(t *testing.T) {
	calls := 0
	err := RetryDBOperation(context.Background(), quietLogger(), "op", func() error {
		calls++
		if calls == 1 {
			return errors.New("connection refused")
		}
		return nil
	})

	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if calls != 2 {
		t.Errorf("fn called %d times, want 2", calls)
	}
}

func TestRetryDBOperationHonoursContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	calls := 0
	err := RetryDBOperation(ctx, quietLogger(), "op", func() error {
		calls++
		return errors.New("connection refused")
	})

	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Errorf("fn called %d times, want 1 before the cancellation was noticed", calls)
	}
}

func TestRetryMongoOperationDelegatesToRetryDBOperation(t *testing.T) {
	calls := 0
	err := RetryMongoOperation(context.Background(), quietLogger(), "op", func() error {
		calls++
		return errors.New("syntax error")
	})

	if err == nil {
		t.Error("err = nil, want the permanent error")
	}
	if calls != 1 {
		t.Errorf("fn called %d times, want 1", calls)
	}
}

// The backoff used to sleep after every failed attempt including the last, so a
// call that exhausted its three attempts spent 1s + 2s + 4s = 7s waiting. The
// final four seconds bought nothing — the loop was over and the same error came
// back regardless — and during a failover, when every operation is failing,
// each retried call held its goroutine for more than twice as long as it needed
// to.
func TestTheLastAttemptDoesNotWaitBeforeReporting(t *testing.T) {
	calls := 0
	start := time.Now()

	err := RetryDBOperation(context.Background(), quietLogger(), "op", func() error {
		calls++
		return fmt.Errorf("attempt %d: connection refused", calls)
	})

	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("err = nil, want the last connection error")
	}
	if calls != 3 {
		t.Errorf("fn called %d times, want 3", calls)
	}
	// 1s + 2s between the three attempts, and nothing after the third.
	if elapsed > 5*time.Second {
		t.Errorf("took %v, want about 3s — the trailing sleep is still there", elapsed)
	}
}
