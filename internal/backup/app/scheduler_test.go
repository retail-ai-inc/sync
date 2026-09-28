package app

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
)

// schedulerFor builds a scheduler whose clock and run function the test owns,
// so a schedule can be exercised without waiting for the wall clock.
func schedulerFor(t *testing.T, at time.Time) (*scheduler, *[]int) {
	t.Helper()

	var fired []int
	s := &scheduler{
		log: quietBackupLogger(),
		due: map[int]deadline{},
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
	if !when.at.After(at(t, "2026-09-02 09:00:00")) {
		t.Errorf("next run %s is not in the future", when.at)
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
	if !when.at.After(at(t, "2026-09-02 09:05:00")) {
		t.Errorf("next run %s is not in the future", when.at)
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

// TestEditingAScheduleRecomputesTheDeadline covers a cached due time answering
// a question nobody is asking any more.
//
// The deadline is computed once, when the job is first seen. Editing the
// schedule left it standing: a nightly job switched to every minute at nine in
// the morning went on waiting until midnight, and one moved later still ran at
// the time it used to have.
func TestEditingAScheduleRecomputesTheDeadline(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{"name":"nightly","schedule":"20 0 * * *","sourceType":"mysql"}`)

	s, fired := schedulerFor(t, at(t, "2026-09-02 09:00:00"))
	s.tick(context.Background()) // scheduled for 00:20 tomorrow

	if _, err := db.Exec(`UPDATE backup_tasks SET config_json = ? WHERE id = ?`,
		`{"name":"nightly","schedule":"* * * * *","sourceType":"mysql"}`, id); err != nil {
		t.Fatalf("reschedule the job: %v", err)
	}

	// One minute later the new schedule is due; the old one is not.
	s.now = func() time.Time { return at(t, "2026-09-02 09:01:30") }
	s.tick(context.Background()) // notices the edit and schedules from now
	s.now = func() time.Time { return at(t, "2026-09-02 09:02:30") }
	s.tick(context.Background()) // the new occurrence fires

	if len(*fired) != 1 || (*fired)[0] != int(id) {
		t.Fatalf("fired = %v, want the rescheduled job to run once -- the cached "+
			"midnight deadline was kept", *fired)
	}
}

// TestAnUneditedScheduleKeepsItsDeadline: recomputing on every tick would push
// the deadline forward for ever and the job would never fire.
func TestAnUneditedScheduleKeepsItsDeadline(t *testing.T) {
	db := useTempJobDB(t)
	insertJob(t, db, 1, `{"name":"nightly","schedule":"20 0 * * *","sourceType":"mysql"}`)

	s, _ := schedulerFor(t, at(t, "2026-09-02 09:00:00"))
	s.tick(context.Background())
	first := s.due[1]

	s.now = func() time.Time { return at(t, "2026-09-02 09:00:30") }
	s.tick(context.Background())

	if !s.due[1].at.Equal(first.at) {
		t.Errorf("the deadline moved from %s to %s without the schedule changing",
			first.at, s.due[1].at)
	}
}

// A window that passed while nothing was running is made up once.
//
// The deadline lives in this process and the process is replaced daily, so a
// job seen for the first time was always given its NEXT occurrence: staging
// went two nights with no backup at all and nothing anywhere recorded that a
// window had been skipped. The job row already carries the answer -- when it
// last backed up -- and it was never read.
func TestAWindowMissedWhileNothingWasRunningIsMadeUp(t *testing.T) {
	db := useTempJobDB(t)
	// Last backup two days ago; the schedule has come round twice since.
	insertJobLastRun(t, db, 1, `{"name":"nightly","schedule":"20 0 * * *","sourceType":"mysql"}`,
		"2026-08-31 15:20:00") // UTC, which is 2026-09-01 00:20 in the scheduler's clock

	s, fired := schedulerFor(t, at(t, "2026-09-03 09:00:00"))
	s.tick(context.Background())

	if len(*fired) != 1 {
		t.Fatalf("the scheduler ran %v, want the missed window made up exactly once", *fired)
	}

	// Once, not once per occurrence: a second tick at the same moment must not
	// fire again, and the deadline is the next occurrence rather than a replay.
	s.tick(context.Background())
	if len(*fired) != 1 {
		t.Errorf("the scheduler ran %v, want the catch-up not repeated", *fired)
	}
	if when := s.due[1].at; !when.After(at(t, "2026-09-03 09:00:00")) {
		t.Errorf("next due %s, want a future occurrence", when)
	}
}

// And a job that did run inside its last window is left alone, which is the
// case a rolling update at nine in the morning produces for every job.
func TestAJobThatRanInItsLastWindowIsNotRunAgain(t *testing.T) {
	db := useTempJobDB(t)
	// 00:20 this morning in the scheduler's clock.
	insertJobLastRun(t, db, 1, `{"name":"nightly","schedule":"20 0 * * *","sourceType":"mysql"}`,
		at(t, "2026-09-03 00:20:00").UTC().Format("2006-01-02 15:04:05"))

	s, fired := schedulerFor(t, at(t, "2026-09-03 09:00:00"))
	s.tick(context.Background())
	s.tick(context.Background())

	if len(*fired) != 0 {
		t.Errorf("the scheduler ran %v for a job that already backed up this window", *fired)
	}
}

// A job that has never run is still left alone: a fresh deployment would
// otherwise start every job at once, which is what the forward scheduling was
// there to prevent in the first place.
func TestAJobThatHasNeverRunIsNotMadeUp(t *testing.T) {
	db := useTempJobDB(t)
	insertJob(t, db, 1, `{"name":"nightly","schedule":"20 0 * * *","sourceType":"mysql"}`)

	s, fired := schedulerFor(t, at(t, "2026-09-03 09:00:00"))
	s.tick(context.Background())
	s.tick(context.Background())

	if len(*fired) != 0 {
		t.Errorf("the scheduler ran %v for a job that has never run", *fired)
	}
}

// A schedule with no next occurrence answers the zero time, which every tick is after, so it ran every thirty seconds.
func TestAScheduleThatNeverComesIsNeverRun(t *testing.T) {
	db := useTempJobDB(t)
	ran := insertJobLastRun(t, db, 1, `{"name":"feb30","schedule":"0 0 30 2 *","sourceType":"mysql"}`,
		"2026-09-01 15:00:00")
	insertJob(t, db, 1, `{"name":"apr31","schedule":"0 0 31 4 *","sourceType":"mysql"}`)
	missed := func() float64 {
		got, _ := sampleFor(t, metrics.BackupMissedWindows, backupLabels(int(ran)))
		return got
	}
	before := missed()

	s, fired := schedulerFor(t, at(t, "2026-09-02 09:00:00"))
	for _, when := range []string{"2026-09-02 09:00:00", "2026-09-02 09:00:30", "2026-09-02 09:01:00"} {
		s.now = func() time.Time { return at(t, when) }
		s.tick(context.Background())
	}

	if len(*fired) != 0 {
		t.Errorf("the scheduler ran %v for schedules that never come round", *fired)
	}
	if after := missed(); after != before {
		t.Errorf("missed windows went from %v to %v for a schedule with no window", before, after)
	}
}
