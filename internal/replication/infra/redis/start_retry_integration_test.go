//go:build integration

package redis

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// TestStartGivesUpOnAnUnreachableSource records the connection retry budget:
// five attempts with exponential backoff, which cannot be shortened from
// outside and ignores the context entirely.
func TestStartGivesUpOnAnUnreachableSource(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	cfg := sampleConfig()
	cfg.SourceConnection = "not a redis url"
	s := NewRedisSyncer(cfg, logger)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled; the retry loop does not consult it

	start := time.Now()
	s.Start(ctx)

	if elapsed := time.Since(start); elapsed < time.Second {
		t.Fatalf("Start gave up in %v; the retries appear to honour the context "+
			"now, so assert that instead", elapsed)
	}
}
