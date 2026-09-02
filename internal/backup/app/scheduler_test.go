package app

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// schedulerFor builds a scheduler whose clock and run function the test owns,
// so a schedule can be exercised without waiting for the wall clock.
func schedulerFor(t *testing.T, at time.Time) (*scheduler, *[]int) {
	t.Helper()

	var fired []int
	s := &scheduler{
		log: quietBackupLogger(),
		due: map[int]time.Time{},
		now: func() time.Time { return at },
		submit: func(id int) string {
			fired = append(fired, id)
			return fmt.Sprintf("backup_%d_test", id)
		},
	}
	return s, &fired
}

func at(t *testing.T, text string) time.Time {
	t.Helper()

	when, err := time.ParseInLocation("2006-01-02 15:04:05", text, time.Local)
	if err != nil {
		t.Fatalf("parse %q: %v", text, err)
	}
	return when
}

// A restart must not fire every job whose time has passed. The crontab it
// replaces had this for free; a scheduler that ran everything overdue would
// start all seven staging backups at once on every rolling update.
func TestAJobSeenForTheFirstTimeIsNotRunImmediately(t *testing.T) {
	db := useTempJobDB(t)
	insertJob(t, db, 1, `{"name":"nightly","schedule":"20 0 * * *","sourceType":"mysql"}`)

	// 09:00, long past the 00:20 occurrence. Two ticks at the same moment: the
	// first only schedules, and the second must find nothing due — a first look
	// that scheduled the job for the occurrence already behind it would pass the
	// first tick and fire on the second.
	s, fired := schedulerFor(t, at(t, "2026-09-02 09:00:00"))
	s.tick(context.Background())
	s.tick(context.Background())

	if len(*fired) != 0 {
		t.Errorf("the scheduler ran %v without its time having come", *fired)
	}
	when, ok := s.due[1]
	if !ok {
		t.Fatalf("the job was not scheduled: %v", s.due)
	}
	if !when.After(at(t, "2026-09-02 09:00:00")) {
		t.Errorf("next run %s is not in the future", when)
	}
}

// The occurrence itself has to fire, once.
func TestAJobRunsWhenItsTimeArrives(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{"name":"nightly","schedule":"20 0 * * *","sourceType":"mysql"}`)

	now := at(t, "2026-09-02 09:00:00")
	s, fired := schedulerFor(t, now)
	s.tick(context.Background()) // schedules for 00:20 tomorrow

	// Move to just after that occurrence.
	s.now = func() time.Time { return at(t, "2026-09-03 00:20:30") }
	s.tick(context.Background())
	if len(*fired) != 1 || (*fired)[0] != int(id) {
		t.Fatalf("fired = %v, want job %d once", *fired, id)
	}

	// A tick a second later must not run it again.
	s.now = func() time.Time { return at(t, "2026-09-03 00:20:31") }
	s.tick(context.Background())
	if len(*fired) != 1 {
		t.Errorf("fired = %v, want the job to have run once, not per tick", *fired)
	}
}

// A process asleep for a day must not work through the day's occurrences one
// tick at a time: the next time is computed from now, not from what was due.
func TestALongSleepDoesNotReplayEveryMissedOccurrence(t *testing.T) {
	db := useTempJobDB(t)
	insertJob(t, db, 1, `{"name":"hourly","schedule":"0 * * * *","sourceType":"mysql"}`)

	s, fired := schedulerFor(t, at(t, "2026-09-02 00:00:30"))
	s.tick(context.Background())

	s.now = func() time.Time { return at(t, "2026-09-03 00:00:30") } // a day later
	s.tick(context.Background())

	if len(*fired) != 1 {
		t.Errorf("fired %d times after a day asleep, want 1", len(*fired))
	}
}

// A disabled job has no schedule, which is what pause means.
func TestADisabledJobIsNotScheduled(t *testing.T) {
	db := useTempJobDB(t)
	insertJob(t, db, 0, `{"name":"paused","schedule":"* * * * *","sourceType":"mysql"}`)

	s, fired := schedulerFor(t, at(t, "2026-09-02 09:00:00"))
	s.tick(context.Background())
	s.now = func() time.Time { return at(t, "2026-09-02 09:01:30") }
	s.tick(context.Background())

	if len(*fired) != 0 {
		t.Errorf("a disabled job ran: %v", *fired)
	}
	if len(s.due) != 0 {
		t.Errorf("a disabled job was scheduled: %v", s.due)
	}
}

// Re-enabling a job schedules it from that moment. Keeping the old due time
// would fire it at once, which is not what pressing resume asks for. This is
// the part SyncCrontab used to do, and the reason the table is re-read rather
// than being read once at startup.
func TestResumingAJobSchedulesItFromThen(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{"name":"nightly","schedule":"20 0 * * *","sourceType":"mysql"}`)

	s, fired := schedulerFor(t, at(t, "2026-09-02 09:00:00"))
	s.tick(context.Background())

	if _, err := db.Exec(`UPDATE backup_tasks SET enable = 0 WHERE id = ?`, id); err != nil {
		t.Fatalf("pause: %v", err)
	}
	s.tick(context.Background())
	if len(s.due) != 0 {
		t.Fatalf("the paused job is still scheduled: %v", s.due)
	}

	if _, err := db.Exec(`UPDATE backup_tasks SET enable = 1 WHERE id = ?`, id); err != nil {
		t.Fatalf("resume: %v", err)
	}
	s.now = func() time.Time { return at(t, "2026-09-02 09:05:00") }
	s.tick(context.Background())

	if len(*fired) != 0 {
		t.Errorf("resuming ran the job immediately: %v", *fired)
	}
	when, ok := s.due[int(id)]
	if !ok {
		t.Fatal("the resumed job was not scheduled again")
	}
	if !when.After(at(t, "2026-09-02 09:05:00")) {
		t.Errorf("next run %s is not in the future", when)
	}
}

// A schedule that will not parse leaves the job unscheduled and says so, rather
// than taking the whole scheduler down with it.
func TestAnUnusableScheduleDoesNotStopTheOthers(t *testing.T) {
	db := useTempJobDB(t)
	insertJob(t, db, 1, `{"name":"broken","schedule":"not a cron expression","sourceType":"mysql"}`)
	good := insertJob(t, db, 1, `{"name":"fine","schedule":"0 * * * *","sourceType":"mysql"}`)

	s, _ := schedulerFor(t, at(t, "2026-09-02 09:00:00"))
	s.tick(context.Background())

	if _, ok := s.due[int(good)]; !ok {
		t.Error("the usable job was not scheduled alongside the broken one")
	}
	if len(s.due) != 1 {
		t.Errorf("scheduled %v, want only the usable job", s.due)
	}
}

// An unreadable job table must not end the scheduler: the next tick reads it
// again, and giving up would take every backup with it.
func TestAnUnreadableTableIsSurvived(t *testing.T) {
	unopenableDB(t)

	s, fired := schedulerFor(t, at(t, "2026-09-02 09:00:00"))
	s.tick(context.Background()) // must not panic
	if len(*fired) != 0 {
		t.Errorf("something ran without a readable table: %v", *fired)
	}
}

// StartBackupScheduler has to return once its context is cancelled, or a
// shutdown waits for it for ever.
func TestTheSchedulerStopsWithItsContext(t *testing.T) {
	useTempJobDB(t)

	ctx, cancel := context.WithCancel(context.Background())
	stop := StartBackupScheduler(ctx, quietBackupLogger())
	cancel()

	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the scheduler did not stop when its context was cancelled")
	}
}
