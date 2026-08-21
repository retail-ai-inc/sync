package app

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/config"
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
// constructor talks to the database. The other three build a struct and return
// it, so an unreachable source is not discovered until Start runs.
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

// TestAMalformedMongoURIYieldsANilSyncer records a defect this test suite found.
//
// NewMongoDBSyncer answers a connection string the driver cannot parse by
// logging and returning nil — no retries, no error. The process entry point
// then writes
//
//	replicationapp.NewMongoDBSyncer(sc, cfg, log).Start(ctx)
//
// with no nil check, in a goroutine. Start has a pointer receiver and its first
// statement reads s.sourceClient, so a nil syncer dereferences nil and panics.
// The panic is unrecovered and in a goroutine, so it takes the whole process
// down: every other replication task and the API server with it.
//
// One task configured with a typo in its connection string is therefore enough
// to stop the service from starting.
func TestAMalformedMongoURIYieldsANilSyncer(t *testing.T) {
	cfg := config.SyncConfig{
		ID:               1,
		Type:             "mongodb",
		SourceConnection: "not-a-uri",
		TargetConnection: "not-a-uri",
	}

	got := NewMongoDBSyncer(cfg, nil, quietLogger())
	if got != nil {
		t.Fatalf("NewMongoDBSyncer returned %T for an unparseable URI; it appears to "+
			"report the failure now, so assert that instead", got)
	}

	// What the entry point does with that nil.
	done := make(chan interface{}, 1)
	go func() {
		defer func() { done <- recover() }()
		got.Start(context.Background())
	}()

	select {
	case r := <-done:
		if r == nil {
			t.Fatal("Start on a nil syncer returned without panicking; a nil receiver " +
				"appears to be handled now, so assert that instead")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start on a nil syncer neither returned nor panicked")
	}
}

// TestAnEmptyMongoURIAlsoYieldsNil records the same path for a task whose
// connection string was never filled in, which is what a configuration document
// that failed to parse leaves behind.
func TestAnEmptyMongoURIAlsoYieldsNil(t *testing.T) {
	got := NewMongoDBSyncer(config.SyncConfig{ID: 1, Type: "mongodb"}, nil, quietLogger())

	if got != nil {
		t.Fatalf("NewMongoDBSyncer returned %T for an empty URI; assert the new "+
			"behaviour instead", got)
	}
}
