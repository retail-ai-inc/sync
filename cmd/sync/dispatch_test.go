package main

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/sirupsen/logrus"
)

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

// TestTheEngineNameIsMatchedWithoutRegardToCase covers a task that was measured
// but never replicated. The monitoring side folds case and this switch did not,
// so a type stored as "MongoDB" — which is how the interface spells it — had its
// row counts recorded every minute, appeared on the dashboard for both sides,
// and never had a syncer started for it. The API stores what the interface sends
// and nothing normalises it on the way in.
func TestTheEngineNameIsMatchedWithoutRegardToCase(t *testing.T) {
	for _, engine := range []string{
		"MongoDB", "MySQL", "MariaDB", "PostgreSQL", "Redis", " mysql ", "MONGODB",
	} {
		sc := baseTask()
		sc.Type = engine

		if syncerFor(sc, cfgWith(), quietLog()) == nil {
			t.Errorf("type %q was not dispatched", engine)
		}
	}
}

// TestAnEngineThisDoesNotReplicateIsStillUnknown is the other half: folding case
// is not the same as accepting an alias for something that was never supported.
func TestAnEngineThisDoesNotReplicateIsStillUnknown(t *testing.T) {
	for _, engine := range []string{"postgres", "mongo", "cassandra"} {
		sc := baseTask()
		sc.Type = engine

		if syncerFor(sc, cfgWith(), quietLog()) != nil {
			t.Errorf("type %q was dispatched", engine)
		}
	}
}

// ------------------------------------------------------------ supervision

// stubTask replaces a real syncer with one that blocks until its context is
// cancelled, so the supervisor's lifecycle can be exercised without a database.
func stubTask(started chan<- int) func(config.SyncConfig, *config.Config, *logrus.Logger) func(context.Context) error {
	return func(sc config.SyncConfig, _ *config.Config, _ *logrus.Logger) func(context.Context) error {
		return func(ctx context.Context) error {
			started <- sc.ID
			<-ctx.Done()
			return nil
		}
	}
}

func runWith(t *testing.T, build func(config.SyncConfig, *config.Config, *logrus.Logger) func(context.Context) error) *supervisor {
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
	s := runWith(t, func(sc config.SyncConfig, _ *config.Config, _ *logrus.Logger) func(context.Context) error {
		return func(ctx context.Context) error {
			<-ctx.Done()
			time.Sleep(50 * time.Millisecond) // still applying when asked to stop
			finished <- sc.ID
			return nil
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

// ----------------------------------------------------- supervised restart

// exitingTask returns a syncer that stops by itself with the given error, and
// counts how many times it was started.
func exitingTask(starts chan<- int, err error) func(config.SyncConfig, *config.Config, *logrus.Logger) func(context.Context) error {
	return func(sc config.SyncConfig, _ *config.Config, _ *logrus.Logger) func(context.Context) error {
		return func(context.Context) error {
			starts <- sc.ID
			return err
		}
	}
}

// settled waits for the task to be recorded as exited, so the next apply looks
// at a finished goroutine rather than racing it.
func settled(t *testing.T, s *supervisor, id int) *runningTask {
	t.Helper()

	deadline := time.After(2 * time.Second)
	for {
		task, ok := s.running[id]
		if ok && task.exited() {
			return task
		}
		select {
		case <-deadline:
			t.Fatalf("task %d did not finish", id)
		case <-time.After(time.Millisecond):
		}
	}
}

// TestATaskThatStopsByItselfIsRestarted is the gap this closes. A task whose
// goroutine returned stayed in the running map with its fingerprint unchanged,
// so it was never looked at again — and replication was made to stop
// deliberately on a failed write, so one connection reset across the region
// boundary left the task dead until somebody edited it.
func TestATaskThatStopsByItselfIsRestarted(t *testing.T) {
	starts := make(chan int, 8)
	s := runWith(t, exitingTask(starts, errors.New("connection reset by peer")))
	ctx := context.Background()

	s.apply(ctx, cfgWith(baseTask()))
	drain(t, starts, 1)
	task := settled(t, s, 1)
	// The first stop is retried at once; the backoff applies from then on.
	task.nextAttempt = time.Time{}

	s.apply(ctx, cfgWith(baseTask()))

	select {
	case <-starts:
	case <-time.After(2 * time.Second):
		t.Fatal("the stopped task was not restarted")
	}
}

// TestTheFirstRestartIsImmediate keeps a momentary blip from costing the
// backoff: a connection reset should be recovered from at once.
func TestTheFirstRestartIsImmediate(t *testing.T) {
	starts := make(chan int, 16)
	s := runWith(t, exitingTask(starts, errors.New("connection reset by peer")))
	ctx := context.Background()

	s.apply(ctx, cfgWith(baseTask()))
	drain(t, starts, 1)
	settled(t, s, 1)

	s.apply(ctx, cfgWith(baseTask()))
	select {
	case <-starts:
	case <-time.After(2 * time.Second):
		t.Fatal("the first restart did not happen at once")
	}
}

// TestTheRestartBacksOff keeps a task whose source is down from being retried
// as fast as the dial fails.
func TestTheRestartBacksOff(t *testing.T) {
	starts := make(chan int, 16)
	s := runWith(t, exitingTask(starts, errors.New("connection refused")))
	ctx := context.Background()

	s.apply(ctx, cfgWith(baseTask()))
	drain(t, starts, 1)
	settled(t, s, 1)

	// The first restart is immediate, and it is what sets the first wait.
	s.apply(ctx, cfgWith(baseTask()))
	drain(t, starts, 1)
	first := settled(t, s, 1)
	if first.nextAttempt.IsZero() {
		t.Fatal("no retry time was set after the first restart")
	}

	// Still inside the wait, so nothing happens.
	s.apply(ctx, cfgWith(baseTask()))
	select {
	case <-starts:
		t.Error("the task was restarted before its backoff had elapsed")
	case <-time.After(100 * time.Millisecond):
	}

	// Past the wait, it starts again and the next wait is longer.
	first.nextAttempt = time.Now().Add(-time.Second)
	s.apply(ctx, cfgWith(baseTask()))
	drain(t, starts, 1)
	second := settled(t, s, 1)

	if second.attempts <= first.attempts {
		t.Errorf("attempts went from %d to %d, want it to grow", first.attempts, second.attempts)
	}
	if !second.nextAttempt.After(first.nextAttempt) {
		t.Error("the second wait is not longer than the first")
	}
}

func TestTheBackoffIsCapped(t *testing.T) {
	starts := make(chan int, 4)
	s := runWith(t, exitingTask(starts, errors.New("connection refused")))

	s.apply(context.Background(), cfgWith(baseTask()))
	drain(t, starts, 1)
	task := settled(t, s, 1)
	task.attempts = 40 // enough to overflow a naive shift
	task.nextAttempt = time.Time{}

	s.apply(context.Background(), cfgWith(baseTask()))
	drain(t, starts, 1)
	restarted := settled(t, s, 1)

	if wait := time.Until(restarted.nextAttempt); wait > maxRestartBackoff+time.Second {
		t.Errorf("the wait grew to %v, want it capped at %v", wait, maxRestartBackoff)
	}
}

// TestAnUnrecoverableStopIsNotRetried is the other half of the classification. A
// purged binlog or a rolled-over oplog fails identically on every attempt, and
// looping on it buries the one thing somebody needs to be told.
func TestAnUnrecoverableStopIsNotRetried(t *testing.T) {
	starts := make(chan int, 8)
	s := runWith(t, exitingTask(starts, domain.Unrecoverable("the binlog is gone")))
	ctx := context.Background()

	s.apply(ctx, cfgWith(baseTask()))
	drain(t, starts, 1)
	task := settled(t, s, 1)
	task.nextAttempt = time.Time{}

	s.apply(ctx, cfgWith(baseTask()))
	s.apply(ctx, cfgWith(baseTask()))

	select {
	case <-starts:
		t.Error("a task that cannot recover was restarted anyway")
	case <-time.After(200 * time.Millisecond):
	}
	if !s.running[1].blocked {
		t.Error("the task was not recorded as blocked, so nothing can alert on it")
	}
}

// TestAConfigurationChangeUnblocksATask records the way out: an operator who
// has dealt with the cause edits the task, and the changed fingerprint starts it
// again.
func TestAConfigurationChangeUnblocksATask(t *testing.T) {
	starts := make(chan int, 8)
	s := runWith(t, exitingTask(starts, domain.Unrecoverable("the binlog is gone")))
	ctx := context.Background()

	s.apply(ctx, cfgWith(baseTask()))
	drain(t, starts, 1)
	settled(t, s, 1)
	s.apply(ctx, cfgWith(baseTask()))
	if !s.running[1].blocked {
		t.Fatal("the task was not blocked to begin with")
	}

	edited := baseTask()
	edited.TaskName = "fixed"
	s.apply(ctx, cfgWith(edited))

	select {
	case <-starts:
	case <-time.After(2 * time.Second):
		t.Error("editing the task did not start it again")
	}
	if s.running[1].blocked {
		t.Error("the restarted task is still recorded as blocked")
	}
}

// TestARunningTaskIsNotDisturbed guards the common case: reconsider must only
// look at tasks whose goroutine has actually finished.
func TestARunningTaskIsNotDisturbed(t *testing.T) {
	starts := make(chan int, 8)
	s := runWith(t, stubTask(starts))
	ctx := context.Background()

	s.apply(ctx, cfgWith(baseTask()))
	drain(t, starts, 1)

	for i := 0; i < 3; i++ {
		s.apply(ctx, cfgWith(baseTask()))
	}

	select {
	case <-starts:
		t.Error("a running task was started a second time")
	case <-time.After(200 * time.Millisecond):
	}
}
