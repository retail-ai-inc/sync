package utils

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

// The classifier is a substring scan over the error text, so errors that are
// permanent get retried. "connection" matches a malformed DSN, "EOF" matches a
// truncated JSON document — neither is transient, and both cost three attempts
// and seven seconds of backoff before the original error is returned.
func TestPermanentErrorsAreClassifiedAsTransient(t *testing.T) {
	permanent := []error{
		errors.New(`invalid connection string: missing "@"`),
		errors.New("unexpected EOF while parsing config JSON"),
		errors.New("error parsing uri: scheme must be mongodb:// — bad connection uri"),
		errors.New("unexpected EOF"),
	}

	for _, err := range permanent {
		if !IsConnectionError(err) {
			t.Fatalf("IsConnectionError(%q) = false — the classifier appears to have been narrowed; assert the new classification instead", err)
		}
	}
}

// Conversely, several errors that really are transient do not match any of the
// substrings, so they are returned to the caller on the first attempt with no
// retry at all.
func TestTransientErrorsAreClassifiedAsPermanent(t *testing.T) {
	transient := []error{
		errors.New("server selection error: context deadline exceeded"),
		errors.New("no reachable servers"),
		errors.New("topology is closed"),
		errors.New("database is locked"),
		errors.New("Error 1213: Deadlock found when trying to get lock"),
	}

	for _, err := range transient {
		if IsConnectionError(err) {
			t.Fatalf("IsConnectionError(%q) = true — the classifier appears to have been widened; assert the new classification instead", err)
		}
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

// The backoff sleep runs after every failed attempt including the last, so a
// call that exhausts its three attempts spends 1s + 2s + 4s = 7s in
// time.After. The final four seconds buy nothing: the loop is over and the
// original error is returned regardless. During a failover — exactly when
// every operation is failing — each retried call blocks its goroutine for
// seven seconds instead of three.
func TestTheFinalBackoffSleepIsWasted(t *testing.T) {
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
	if elapsed < 6500*time.Millisecond {
		t.Fatalf("took %v, expected ~7s — the trailing sleep appears to have been removed; assert the new timing instead", elapsed)
	}
}
