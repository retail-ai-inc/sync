package app

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/backup/domain"
	"github.com/retail-ai-inc/sync/internal/backup/infra"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/platform/timex"
)

// The backup schedule, run by this process.
//
// It used to be the system crontab: SyncCrontab wrote one line per enabled job
// calling this process's own HTTP API with curl. That has two failure modes,
// both silent. The crontab is written only by the create, update, pause,
// resume and delete handlers, so a new container starts with none and every
// backup stops until somebody happens to edit a job — and once the API
// required a credential, the crontab's unauthenticated curl got 401 and the
// entries that were there stopped working, with `-s ... >/dev/null 2>&1`
// swallowing the reply.
//
// Running the schedule in the process removes the credential and the crontab
// both. It survives a restart because it is rebuilt from the job table.

// scheduleEvery is how often the job table is re-read, so a job added or paused
// through the API is picked up without a restart — the part SyncCrontab was for.
const scheduleEvery = 30 * time.Second

// StartBackupScheduler runs each enabled job on its own schedule until the
// context is cancelled. The returned function waits for it to stop.
func StartBackupScheduler(ctx context.Context, log logrus.FieldLogger) (stop func()) {
	if log == nil {
		log = logrus.StandardLogger()
	}
	s := &scheduler{log: log, due: map[int]deadline{}, now: time.Now}

	var done sync.WaitGroup
	done.Add(1)
	go func() {
		defer done.Done()
		s.run(ctx)
	}()
	return done.Wait
}

// deadline is when a job fires next and the expression that produced it.
type deadline struct {
	at         time.Time
	expression string
}

type scheduler struct {
	log logrus.FieldLogger
	// due is when each job fires next, keyed by job id. A job seen for the
	// first time is given its next occurrence rather than being run at once:
	// otherwise every restart would fire every overdue job, and seven backups
	// would start together on every rolling update.
	//
	// The expression it was computed from is kept with it. Without that, editing
	// a job's schedule changed nothing until it next fired: a nightly job
	// switched to every minute at nine in the morning went on waiting until
	// midnight, and one moved later still ran at the time it used to have.
	due map[int]deadline
	now func() time.Time
	// submit runs a job. Replaced in tests.
	submit func(int) string
}

func (s *scheduler) run(ctx context.Context) {
	s.log.Infof("[Backup] Scheduling backups in-process, re-reading the job table every %s",
		scheduleEvery)

	ticker := time.NewTicker(scheduleEvery)
	defer ticker.Stop()

	s.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

// tick reads the job table and fires whatever is due.
func (s *scheduler) tick(ctx context.Context) {
	jobs, err := infra.ListJobs()
	if err != nil {
		// Reported rather than fatal: the next tick reads the table again, and a
		// scheduler that gave up on one unreadable read would take every backup
		// with it.
		s.log.Warnf("[Backup] Could not read the job table, so nothing is scheduled "+
			"this round: %v", err)
		return
	}

	now := s.now()
	enabled := map[int]bool{}

	for _, job := range jobs {
		if job.Enable() != 1 {
			continue
		}
		config, err := job.Config()
		if err != nil {
			s.log.Errorf("[Backup] Job %d has a configuration that will not parse, so "+
				"it has NO SCHEDULED BACKUP: %v", job.ID(), err)
			continue
		}
		schedule, err := domain.ParseSchedule(config.Schedule)
		if err != nil {
			s.log.Errorf("[Backup] Job %d has an unusable schedule %q, so it has NO "+
				"SCHEDULED BACKUP: %v", job.ID(), config.Schedule, err)
			continue
		}
		enabled[job.ID()] = true

		held, known := s.due[job.ID()]
		// A schedule that has been edited is as good as unseen: what was cached
		// answers a question nobody is asking any more.
		if known && held.expression != config.Schedule {
			s.log.Infof("[Backup] Job %d (%s) was rescheduled from %q to %q",
				job.ID(), config.Name, held.expression, config.Schedule)
			known = false
		}
		if !known {
			next := deadline{at: schedule.Next(now), expression: config.Schedule}
			s.due[job.ID()] = next
			s.log.Infof("[Backup] Job %d (%s) is scheduled %q, next at %s",
				job.ID(), config.Name, config.Schedule, next.at.Format(time.RFC3339))
			metrics.SetBackupNextDue(backupLabels(job.ID()), next.at)

			// A window missed while nothing was running is invisible otherwise:
			// the deadline lives in this process, the process is replaced daily,
			// and a job seen for the first time is given its NEXT occurrence. Two
			// nights in a row went unbacked-up in staging with no record anywhere
			// that a window had been skipped.
			//
			// One catch-up, not a replay: whether an occurrence has passed since
			// the last run is the whole question, and how many is not.
			if missed, at := s.missedWindow(job, schedule, now); missed {
				metrics.CountBackupMissedWindow(backupLabels(job.ID()))
				s.log.Warnf("[Backup] Job %d (%s) should have run at %s and nothing did, "+
					"so it is running now. Its last backup was %q",
					job.ID(), config.Name, at.Format(time.RFC3339), job.LastBackupTime())
				taskID := s.run1(job.ID())
				s.log.Infof("[Backup] Job %d started as %s to cover the missed window",
					job.ID(), taskID)
			}
			continue
		}
		if now.Before(held.at) {
			continue
		}

		// Next from now, not from the time that was due: a process asleep for an
		// hour must not work through the hour's occurrences one tick at a time.
		next := deadline{at: schedule.Next(now), expression: config.Schedule}
		s.due[job.ID()] = next
		metrics.SetBackupNextDue(backupLabels(job.ID()), next.at)
		taskID := s.run1(job.ID())
		s.log.Infof("[Backup] Job %d started as %s; next at %s",
			job.ID(), taskID, next.at.Format(time.RFC3339))
	}

	// A job that was paused or deleted stops being tracked, so re-enabling it
	// schedules from then rather than firing immediately.
	for id := range s.due {
		if !enabled[id] {
			delete(s.due, id)
		}
	}
	_ = ctx
}

// missedWindow reports whether an occurrence fell between a job's last backup
// and now, which is what a process that was not running through it looks like
// afterwards.
//
// A job that has never run is left alone: a fresh deployment would otherwise
// fire every job at once, which is the reason the first sighting schedules
// forward in the first place.
func (s *scheduler) missedWindow(job domain.BackupJob, schedule domain.Schedule,
	now time.Time) (bool, time.Time) {

	last := strings.TrimSpace(job.LastBackupTime())
	if last == "" {
		return false, time.Time{}
	}
	at, err := timex.ParseDatabaseTimestamp(last)
	if err != nil {
		s.log.Warnf("[Backup] Job %d has a last backup time that will not parse (%q), "+
			"so a missed window cannot be told from a first run: %v", job.ID(), last, err)
		return false, time.Time{}
	}

	// The schedule is read in the same clock the scheduler fires in; the stored
	// time is UTC.
	due := schedule.Next(at.In(now.Location()))
	if due.After(now) {
		return false, time.Time{}
	}
	return true, due
}

func (s *scheduler) run1(id int) string {
	if s.submit != nil {
		return s.submit(id)
	}
	return SubmitRun(id)
}
