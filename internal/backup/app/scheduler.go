package app

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/backup/domain"
	"github.com/retail-ai-inc/sync/internal/backup/infra"
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
	s := &scheduler{log: log, due: map[int]time.Time{}, now: time.Now}

	var done sync.WaitGroup
	done.Add(1)
	go func() {
		defer done.Done()
		s.run(ctx)
	}()
	return done.Wait
}

type scheduler struct {
	log logrus.FieldLogger
	// due is when each job fires next, keyed by job id. A job seen for the
	// first time is given its next occurrence rather than being run at once:
	// otherwise every restart would fire every overdue job, and seven backups
	// would start together on every rolling update.
	due map[int]time.Time
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

		at, known := s.due[job.ID()]
		if !known {
			s.due[job.ID()] = schedule.Next(now)
			s.log.Infof("[Backup] Job %d (%s) is scheduled %q, next at %s",
				job.ID(), config.Name, config.Schedule,
				s.due[job.ID()].Format(time.RFC3339))
			continue
		}
		if now.Before(at) {
			continue
		}

		// Next from now, not from the time that was due: a process asleep for an
		// hour must not work through the hour's occurrences one tick at a time.
		s.due[job.ID()] = schedule.Next(now)
		taskID := s.run1(job.ID())
		s.log.Infof("[Backup] Job %d started as %s; next at %s",
			job.ID(), taskID, s.due[job.ID()].Format(time.RFC3339))
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

func (s *scheduler) run1(id int) string {
	if s.submit != nil {
		return s.submit(id)
	}
	return SubmitRun(id)
}
