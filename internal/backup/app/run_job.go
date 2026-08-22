package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/retail-ai-inc/sync/internal/backup/domain"
	"github.com/retail-ai-inc/sync/internal/backup/infra"
	"github.com/retail-ai-inc/sync/internal/backup/infra/export"
	"github.com/retail-ai-inc/sync/internal/platform/httpx"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
	"github.com/sirupsen/logrus"
)

// The in-memory register of runs.
//
// Nothing used to remove an entry, so it grew for the lifetime of the process:
// one record per backup ever submitted, reclaimed only by a restart. Finished
// runs are dropped once they are older than runRetention, which is long enough
// for a caller to have polled the outcome.
const runRetention = 24 * time.Hour

var (
	runs     = make(map[string]*domain.Run)
	runsLock sync.RWMutex
)

// forgetOldRuns drops finished runs nobody is going to ask about again. The
// caller holds the lock.
func forgetOldRuns() {
	cutoff := time.Now().Add(-runRetention)
	for id, run := range runs {
		if run.CompletedAt != nil && run.CompletedAt.Before(cutoff) {
			delete(runs, id)
		}
	}
}

// RecordRun files a run under its task id. SubmitRun uses it after minting the
// id; it is exported because the status endpoint's tests need to seed a run
// without starting an export.
func RecordRun(taskID string, run *domain.Run) {
	runsLock.Lock()
	defer runsLock.Unlock()

	forgetOldRuns()
	runs[taskID] = run
}

// LookupRun returns a recorded run.
//
// A copy, not the pointer. The lock guards the map, not the Run behind it, and
// the background goroutine writes to that Run as the export proceeds — so the
// status endpoint used to serialise a struct that was being changed underneath
// it, which is a data race and shows up as a status and a message from two
// different moments.
func LookupRun(taskID string) (*domain.Run, bool) {
	runsLock.RLock()
	defer runsLock.RUnlock()

	run, exists := runs[taskID]
	if !exists {
		return nil, false
	}
	snapshot := *run
	return &snapshot, true
}

// AdvanceRun moves a recorded run to a new status. An unknown task id is
// silently ignored.
func AdvanceRun(taskID, status, message string, err error) {
	runsLock.Lock()
	defer runsLock.Unlock()
	if run, exists := runs[taskID]; exists {
		run.Advance(status, message, err)
	}
}

// ForgetRuns drops every recorded run. Nothing in production calls it — the
// register has no eviction (T-115) — but tests need to start from a clean one,
// and the register is unexported.
func ForgetRuns() {
	runsLock.Lock()
	defer runsLock.Unlock()
	runs = make(map[string]*domain.Run)
}

// SubmitRun registers a run for a job and starts it in the background,
// returning the task id the caller can poll.
func SubmitRun(id int) string {
	// The id used to be the job id and the current Unix second, so two
	// submissions of the same job within one second produced the same id and the
	// second overwrote the first — the caller that submitted first then polled
	// somebody else's run.
	taskID := fmt.Sprintf("backup_%d_%d_%s", id, time.Now().UnixNano(), randomSuffix())

	logrus.Infof("[Backup] Submitting backup task: %d with taskID: %s", id, taskID)

	RecordRun(taskID, &domain.Run{
		TaskID:    taskID,
		BackupID:  id,
		Status:    domain.RunPending,
		Message:   "Backup task submitted",
		CreatedAt: time.Now(),
	})

	go execute(taskID, id)
	return taskID
}

// randomSuffix makes a task id unique even against a clock that has not moved.
func randomSuffix() string {
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		return "0"
	}
	return hex.EncodeToString(buf)
}

// execute runs a job to completion and records how it went.
func execute(taskID string, id int) {
	AdvanceRun(taskID, domain.RunRunning, "Backup execution started", nil)

	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		logrus.Errorf("[BackupExecutor] Failed to open database for task %s: %v", taskID, err)
		AdvanceRun(taskID, domain.RunFailed, "Failed to open database", err)
		// Nowhere to record the outcome: the place it would be recorded is the
		// database that could not be opened.
		return
	}
	defer db.Close()

	executor := export.NewBackupExecutor(db)

	// Use background context with 2-hour timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()

	if err := executor.Execute(ctx, id); err != nil {
		logrus.Errorf("[BackupExecutor] Failed to execute backup task %d: %v", id, err)
		// Durable first, then the in-memory register. A caller polling the
		// status endpoint reads the register, so advancing it first would let it
		// see "failed" at a moment when nothing on disk says so yet — and if the
		// process went down in that window, nothing ever would.
		recordOutcome(id, domain.RunFailed, err.Error())
		AdvanceRun(taskID, domain.RunFailed, "Backup execution failed", err)
		return
	}

	nowStr := httpx.TimeNowStr()
	if _, err = db.Exec("UPDATE backup_tasks SET last_backup_time=? WHERE id=?", nowStr, id); err != nil {
		logrus.Warnf("[BackupExecutor] Failed to update last_backup_time: %v", err)
		// Continue execution, don't interrupt response
	}

	recordOutcome(id, domain.RunCompleted, "")
	AdvanceRun(taskID, domain.RunCompleted, "Backup executed successfully", nil)
	logrus.Debugf("[BackupExecutor] Background backup task %s completed successfully", taskID)
}

// recordOutcome writes how a run went to the control database.
//
// The in-memory register is the only other record, and it goes with the
// process. A backup that failed overnight and a restart in the morning left the
// dashboard showing nothing but the timestamp of the last run that worked, so
// "the backup is a few days old" and "the backup has been failing since
// Tuesday" looked identical — the second is the one worth waking up for.
func recordOutcome(id int, status, message string) {
	if err := infra.RecordRunOutcome(id, httpx.TimeNowStr(), status, message); err != nil {
		logrus.Warnf("[BackupExecutor] Failed to record the outcome of task %d: %v", id, err)
	}
}

// ErrJobNotFound means the id names no job.
var ErrJobNotFound = errors.New("no such task")

// StartRun runs a job now, in the background, and reports the task id to poll.
//
// It used to stamp last_backup_time and answer "started successfully" without
// running anything at all: no executor was built, no command ran, nothing was
// written anywhere. So "back this up now" produced a dashboard entry saying the
// job had just succeeded, and an operator checking before a switchover that the
// data was recoverable saw a fresh, successful backup that did not exist. That
// is worse than showing "never backed up".
func StartRun(id string) (taskID string, err error) {
	exists, err := infra.JobExists(id)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", ErrJobNotFound
	}

	numeric, err := strconv.Atoi(id)
	if err != nil {
		return "", fmt.Errorf("%q is not a job id: %w", id, err)
	}

	logrus.Infof("[Backup] Manually triggered backup task: %s", id)
	return SubmitRun(numeric), nil
}

// RunCount reports how many runs the register holds. Nothing in production
// needs it; the tests that record the absence of eviction (T-115) do.
func RunCount() int {
	runsLock.RLock()
	defer runsLock.RUnlock()
	return len(runs)
}
