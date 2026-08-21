//go:build integration

// Package harness provides shared fixtures for the integration suite: endpoint
// discovery, convergence polling, and sync task construction.
//
// Endpoints default to the stack in docker/docker-compose.test.yml and can be
// overridden per engine with environment variables, so the same tests run
// against a local stack or CI.
package harness

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// Endpoint addresses. Every value is overridable so the suite is not tied to
// one machine's port allocation.
var (
	MongoSource = env("SYNC_TEST_MONGO_SOURCE", "127.0.0.1:27117")
	MongoTarget = env("SYNC_TEST_MONGO_TARGET", "127.0.0.1:27118")
	MySQLSource = env("SYNC_TEST_MYSQL_SOURCE", "127.0.0.1:3306")
	MySQLTarget = env("SYNC_TEST_MYSQL_TARGET", "127.0.0.1:3308")
	RedisSource = env("SYNC_TEST_REDIS_SOURCE", "127.0.0.1:6479")
	RedisTarget = env("SYNC_TEST_REDIS_TARGET", "127.0.0.1:6480")
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// SplitHostPort separates an endpoint into its parts, failing the test rather
// than returning an error, since a malformed endpoint is a configuration
// mistake and not a condition under test.
func SplitHostPort(t *testing.T, endpoint string) (host, port string) {
	t.Helper()

	for i := len(endpoint) - 1; i >= 0; i-- {
		if endpoint[i] == ':' {
			return endpoint[:i], endpoint[i+1:]
		}
	}
	t.Fatalf("endpoint %q has no port", endpoint)
	return "", ""
}

// Eventually polls until cond returns nil or the deadline passes, then reports
// the last error. It replaces the fixed sleeps the previous suite relied on:
// a passing assertion returns as soon as the system converges, and a failing
// one explains what it was still waiting for.
func Eventually(t *testing.T, timeout time.Duration, cond func() error) {
	t.Helper()

	const interval = 100 * time.Millisecond
	deadline := time.Now().Add(timeout)

	var last error
	for {
		last = cond()
		if last == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %v: %v", timeout, last)
		}
		time.Sleep(interval)
	}
}

// WaitFor polls until cond returns nil and reports whether it ever did. Use it
// where the condition is expected *not* to hold — a defect being recorded — so
// the test spends the full window only while the defect is present and returns
// immediately once it is fixed. Eventually is the right call when the
// condition is expected to hold; this one never fails the test by itself.
func WaitFor(timeout time.Duration, cond func() error) bool {
	const interval = 100 * time.Millisecond
	deadline := time.Now().Add(timeout)

	for {
		if cond() == nil {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(interval)
	}
}

// Consistently checks that cond keeps holding for the whole window. Use it to
// assert that something does *not* happen, such as a document that must never
// reach the target.
func Consistently(t *testing.T, window time.Duration, cond func() error) {
	t.Helper()

	const interval = 100 * time.Millisecond
	deadline := time.Now().Add(window)

	for time.Now().Before(deadline) {
		if err := cond(); err != nil {
			t.Fatalf("condition stopped holding after the check began: %v", err)
		}
		time.Sleep(interval)
	}
}

// RunSyncer starts a syncer in the background and returns a stop function that
// cancels it and waits for the goroutine to unwind. Every test must call the
// returned function, normally through t.Cleanup.
func RunSyncer(t *testing.T, start func(context.Context)) (stop func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		start(ctx)
	}()

	var stopped bool
	return func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Log("syncer did not stop within 15s of cancellation")
		}
	}
}

// UniqueName builds a collection or table name unique to one test run, so
// tests can share a database without colliding and without a cleanup step
// having to run before the next one starts.
func UniqueName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}
