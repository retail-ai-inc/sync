package main

import (
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/config"
)

func TestHTTPAddrDefaultsToTheContainerPort(t *testing.T) {
	t.Setenv("SYNC_HTTP_ADDR", "")
	if got := httpAddr(); got != ":8080" {
		t.Errorf("httpAddr() = %q, want :8080", got)
	}
}

func TestHTTPAddrIsTakenFromTheEnvironment(t *testing.T) {
	t.Setenv("SYNC_HTTP_ADDR", "127.0.0.1:9090")
	if got := httpAddr(); got != "127.0.0.1:9090" {
		t.Errorf("httpAddr() = %q", got)
	}
}

// TestHTTPAddrIgnoresSurroundingSpace: it comes from a Kubernetes manifest,
// where a trailing space is easy to leave in and produces a listen address
// that fails at start-up.
func TestHTTPAddrIgnoresSurroundingSpace(t *testing.T) {
	t.Setenv("SYNC_HTTP_ADDR", "  :9090  ")
	if got := httpAddr(); got != ":9090" {
		t.Errorf("httpAddr() = %q, want the trimmed address", got)
	}
	t.Setenv("SYNC_HTTP_ADDR", "   ")
	if got := httpAddr(); got != ":8080" {
		t.Errorf("httpAddr() = %q, want the default for a blank value", got)
	}
}

// TestPurgeCheckpointsForAnEngineWithNoPurge reports rather than pretending.
// A task deleted without its positions being removed leaves them on the target,
// and a new task given the same id resumes from them -- so silence here would
// be the wrong answer.
func TestPurgeCheckpointsForAnEngineWithNoPurge(t *testing.T) {
	for _, engine := range []string{"postgresql", "cassandra", ""} {
		err := purgeCheckpointsFor(config.SyncConfig{ID: 1, Type: engine})
		if err == nil {
			t.Errorf("a %q task reported its positions purged", engine)
		}
	}
}

// TestPurgeCheckpointsForRecognisesEveryEngineWithAPurge. Each of these reaches
// a target and fails without one; what is being checked is that the type is
// dispatched at all, because an unrecognised spelling falls through to the
// refusal above and no position is ever cleaned up.
func TestPurgeCheckpointsForRecognisesEveryEngineWithAPurge(t *testing.T) {
	for _, engine := range []string{"mongodb", "MongoDB", "mysql", "MariaDB", " redis "} {
		err := purgeCheckpointsFor(config.SyncConfig{
			ID: 1, Type: engine,
			TargetConnection: "", SourceConnection: "",
		})
		if err != nil && strings.Contains(err.Error(), "no clean-up is implemented") {
			t.Errorf("%q was not dispatched to an engine: %v", engine, err)
		}
	}
}
