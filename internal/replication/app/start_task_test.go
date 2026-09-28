package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/sirupsen/logrus"
)

// quietLogger returns a logger that discards output, so the constructors below
// do not flood the test log with connection errors.
func quietLogger() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l
}

func TestTheEngineConstructorsCarryTheirConfiguration(t *testing.T) {
	cfg := config.SyncConfig{ID: 7, Type: "mysql", RedisPositionPath: "/redis"}

	if got := NewMySQLSyncer(cfg, quietLogger()); got == nil {
		t.Error("NewMySQLSyncer returned nil for a well-formed configuration")
	}
	if got := NewPostgreSQLSyncer(cfg, quietLogger()); got == nil {
		t.Error("NewPostgreSQLSyncer returned nil for a well-formed configuration")
	}
	if got := NewRedisSyncer(cfg, quietLogger()); got == nil {
		t.Error("NewRedisSyncer returned nil for a well-formed configuration")
	}
}

// TestThreeOfFourConstructorsNeverFail records that only the MongoDB
// constructor talks to the database.
func TestThreeOfFourConstructorsNeverFail(t *testing.T) {
	unreachable := config.SyncConfig{
		ID:               1,
		Type:             "mysql",
		SourceConnection: "root:root@tcp(127.0.0.1:1)/src",
		TargetConnection: "root:root@tcp(127.0.0.1:1)/tgt",
	}

	if NewMySQLSyncer(unreachable, quietLogger()) == nil {
		t.Error("NewMySQLSyncer returned nil for an unreachable source; it appears to " +
			"connect now, so assert that instead")
	}
	if NewPostgreSQLSyncer(unreachable, quietLogger()) == nil {
		t.Error("NewPostgreSQLSyncer returned nil for an unreachable source")
	}
	if NewRedisSyncer(unreachable, quietLogger()) == nil {
		t.Error("NewRedisSyncer returned nil for an unreachable source")
	}
}

// NewMongoDBSyncer answered a connection string the driver cannot parse by
// logging and returning nil — no retries, no error.
func TestAMalformedMongoURIIsReportedNotPanicked(t *testing.T) {
	cfg := config.SyncConfig{
		ID:               1,
		Type:             "mongodb",
		SourceConnection: "not-a-uri",
		TargetConnection: "not-a-uri",
	}

	syncer := NewMongoDBSyncer(cfg, nil, quietLogger())
	if syncer == nil {
		t.Fatal("NewMongoDBSyncer returned nil, which the caller dereferences")
	}

	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("Start panicked: %v", r)
			}
		}()
		done <- syncer.Start(context.Background())
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Start returned nil for a syncer that never connected")
		}
		if !errors.Is(err, domain.ErrUnrecoverable) {
			t.Errorf("Start returned %v; a URI the driver will not parse is not "+
				"something a restart fixes", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Start neither returned nor panicked")
	}
}

// TestAnEmptyMongoURIIsAlsoReported covers the same path for a task whose
// connection string was never filled in, which is what a configuration document
// that failed to parse leaves behind.
func TestAnEmptyMongoURIIsAlsoReported(t *testing.T) {
	syncer := NewMongoDBSyncer(config.SyncConfig{ID: 1, Type: "mongodb"}, nil, quietLogger())

	if syncer == nil {
		t.Fatal("NewMongoDBSyncer returned nil, which the caller dereferences")
	}
	if err := syncer.Start(context.Background()); err == nil {
		t.Error("Start returned nil for a task with no connection string")
	}
}
