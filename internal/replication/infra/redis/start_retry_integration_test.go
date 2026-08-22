//go:build integration

package redis

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// TestStartStopsWhenItsContextIsCancelled covers shutdown while the source is
// unreachable. The connection retry was five attempts with exponential backoff —
// thirty seconds of waiting — and it consulted no context at all, so a task told
// to stop went on sleeping long after the supervisor had given up on it.
func TestStartStopsWhenItsContextIsCancelled(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	cfg := sampleConfig()
	cfg.SourceConnection = "not a redis url"
	s := NewRedisSyncer(cfg, logger)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	start := time.Now()
	if err := s.Start(ctx); err == nil {
		t.Error("Start returned nil for a source it never reached")
	}

	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Start waited %v after its context was cancelled", elapsed)
	}
}
