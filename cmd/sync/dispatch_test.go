package main

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/sirupsen/logrus"
)

// quietLog returns a logger that discards output.
func quietLog() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l
}

func TestTheDispatchKnowsEveryEngine(t *testing.T) {
	for _, engine := range []string{"mongodb", "mysql", "mariadb", "postgresql", "redis"} {
		t.Run(engine, func(t *testing.T) {
			sc := baseTask()
			sc.Type = engine

			if syncerFor(sc, cfgWith(), quietLog()) == nil {
				t.Errorf("no syncer for %q", engine)
			}
		})
	}
}

// TestAnUnknownSyncTypeIsDropped records that an unrecognised engine is logged
// and the task simply does not run. There is no failed state anywhere: the task
// shows as enabled in the API and nothing replicates.
func TestAnUnknownSyncTypeIsDropped(t *testing.T) {
	for _, engine := range []string{"cassandra", ""} {
		sc := baseTask()
		sc.Type = engine

		if syncerFor(sc, cfgWith(), quietLog()) != nil {
			t.Errorf("%q is dispatched now; assert the new behaviour instead", engine)
		}
	}
}

// TestSyncTypeMatchingIsCaseSensitive records that the switch has no case
// folding and no aliases, so a configuration value that differs only in case is
// treated as unknown and the task never runs. The API stores what the UI sends
// and nothing normalises it.
func TestSyncTypeMatchingIsCaseSensitive(t *testing.T) {
	for _, engine := range []string{"MongoDB", "MySQL", "MariaDB", "PostgreSQL", "Redis", "postgres", "mongo"} {
		sc := baseTask()
		sc.Type = engine

		if syncerFor(sc, cfgWith(), quietLog()) != nil {
			t.Errorf("type %q is dispatched now — the switch appears to normalise "+
				"case; assert that instead", engine)
		}
	}
}

// ------------------------------------------------------------ supervision

// stubTask replaces a real syncer with one that blocks until its context is
// cancelled, so the supervisor's lifecycle can be exercised without a database.
func stubTask(started chan<- int) func(config.SyncConfig, *config.Config, *logrus.Logger) func(context.Context) {
	return func(sc config.SyncConfig, _ *config.Config, _ *logrus.Logger) func(context.Context) {
		return func(ctx context.Context) {
			started <- sc.ID
			<-ctx.Done()
		}
	}
}

// runWith drives a supervisor whose tasks are stubs.
func runWith(t *testing.T, build func(config.SyncConfig, *config.Config, *logrus.Logger) func(context.Context)) *supervisor {
	t.Helper()

	s := newSupervisor(quietLog())
	s.build = build
	t.Cleanup(s.stopAll)
	return s
}

// TestOnlyTheChangedTaskIsRestarted is the point of the whole supervisor. A
// change to one task used to cancel the context every syncer shared, so an edit
// to a reporting task stopped the payment tasks too.
func TestOnlyTheChangedTaskIsRestarted(t *testing.T) {
	started := make(chan int, 8)
	s := runWith(t, stubTask(started))
	ctx := context.Background()

	first, second := baseTask(), baseTask()
	second.ID = 2

	s.apply(ctx, cfgWith(first, second))
	drain(t, started, 2)

	// Only the second task changes.
	edited := second
	edited.TaskName = "renamed"
	s.apply(ctx, cfgWith(first, edited))

	select {
	case id := <-started:
		if id != 2 {
			t.Errorf("task %d was restarted, want only task 2", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the changed task was not restarted")
	}
	select {
	case id := <-started:
		t.Errorf("task %d was restarted as well", id)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestAnUnchangedTaskIsLeftAlone(t *testing.T) {
	started := make(chan int, 8)
	s := runWith(t, stubTask(started))
	ctx := context.Background()

	s.apply(ctx, cfgWith(baseTask()))
	drain(t, started, 1)

	s.apply(ctx, cfgWith(baseTask()))

	select {
	case id := <-started:
		t.Errorf("task %d was restarted although nothing changed", id)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestADisabledTaskIsStopped(t *testing.T) {
	started := make(chan int, 8)
	s := runWith(t, stubTask(started))
	ctx := context.Background()

	s.apply(ctx, cfgWith(baseTask()))
	drain(t, started, 1)

	disabled := baseTask()
	disabled.Enable = false
	s.apply(ctx, cfgWith(disabled))

	if len(s.running) != 0 {
		t.Errorf("%d tasks still running after the task was disabled", len(s.running))
	}
}

func TestATaskRemovedFromTheConfigurationIsStopped(t *testing.T) {
	started := make(chan int, 8)
	s := runWith(t, stubTask(started))
	ctx := context.Background()

	s.apply(ctx, cfgWith(baseTask()))
	drain(t, started, 1)

	s.apply(ctx, cfgWith())

	if len(s.running) != 0 {
		t.Errorf("%d tasks still running after the task was removed", len(s.running))
	}
}

func TestADisabledTaskIsNeverStarted(t *testing.T) {
	started := make(chan int, 8)
	s := runWith(t, stubTask(started))

	disabled := baseTask()
	disabled.Enable = false
	s.apply(context.Background(), cfgWith(disabled))

	if len(s.running) != 0 {
		t.Errorf("a disabled task was started: %d running", len(s.running))
	}
}

// TestStopAllWaitsForEveryTask pins the drain: a task is given time to finish
// what it was applying rather than being abandoned mid-write.
func TestStopAllWaitsForEveryTask(t *testing.T) {
	finished := make(chan int, 8)
	s := runWith(t, func(sc config.SyncConfig, _ *config.Config, _ *logrus.Logger) func(context.Context) {
		return func(ctx context.Context) {
			<-ctx.Done()
			time.Sleep(50 * time.Millisecond) // still applying when asked to stop
			finished <- sc.ID
		}
	})

	first, second := baseTask(), baseTask()
	second.ID = 2
	s.apply(context.Background(), cfgWith(first, second))

	s.stopAll()

	if len(finished) != 2 {
		t.Errorf("%d of 2 tasks had finished when stopAll returned", len(finished))
	}
	if len(s.running) != 0 {
		t.Errorf("%d tasks still recorded as running", len(s.running))
	}
}

// drain waits for n tasks to report that they started.
func drain(t *testing.T, started <-chan int, n int) {
	t.Helper()

	for i := 0; i < n; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d tasks started", i, n)
		}
	}
}
