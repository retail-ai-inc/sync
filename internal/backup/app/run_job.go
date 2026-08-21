package app

import (
	"context"
	"errors"
	"fmt"
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
// Nothing ever removes an entry, so the map grows for the lifetime of the
// process (T-115). Preserved as it stands.
var (
	runs     = make(map[string]*domain.Run)
	runsLock sync.RWMutex
)

// RecordRun files a run under its task id. SubmitRun uses it after minting the
// id; it is exported because the status endpoint's tests need to seed a run
// without starting an export.
func RecordRun(taskID string, run *domain.Run) {
	runsLock.Lock()
	defer runsLock.Unlock()
	runs[taskID] = run
}

// LookupRun returns a recorded run.
func LookupRun(taskID string) (*domain.Run, bool) {
	runsLock.RLock()
	defer runsLock.RUnlock()
	run, exists := runs[taskID]
	return run, exists
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
//
// The task id is the job id and the current Unix second, so two submissions of
// the same job within one second collide and the second overwrites the first
// (T-114). Preserved as it stands.
func SubmitRun(id int) string {
	taskID := fmt.Sprintf("backup_%d_%d", id, time.Now().Unix())

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

// execute runs a job to completion and records how it went.
func execute(taskID string, id int) {
	AdvanceRun(taskID, domain.RunRunning, "Backup execution started", nil)

	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		logrus.Errorf("[BackupExecutor] Failed to open database for task %s: %v", taskID, err)
		AdvanceRun(taskID, domain.RunFailed, "Failed to open database", err)
		return
	}
	defer db.Close()

	executor := export.NewBackupExecutor(db)

	// Use background context with 2-hour timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()

	if err := executor.Execute(ctx, id); err != nil {
		logrus.Errorf("[BackupExecutor] Failed to execute backup task %d: %v", id, err)
		AdvanceRun(taskID, domain.RunFailed, "Backup execution failed", err)
		return
	}

	nowStr := httpx.TimeNowStr()
	if _, err = db.Exec("UPDATE backup_tasks SET last_backup_time=? WHERE id=?", nowStr, id); err != nil {
		logrus.Warnf("[BackupExecutor] Failed to update last_backup_time: %v", err)
		// Continue execution, don't interrupt response
	}

	AdvanceRun(taskID, domain.RunCompleted, "Backup executed successfully", nil)
	logrus.Debugf("[BackupExecutor] Background backup task %s completed successfully", taskID)
}

// ErrJobNotFound means the id names no job.
var ErrJobNotFound = errors.New("no such task")

// MarkRun stamps a job's last backup time and nothing else.
//
// This is the second of two ways to "run a backup" and it does not run one: it
// records the time and answers that the job started. The one that actually
// exports is SubmitRun. Reconciling the two is aggregate work (#59).
func MarkRun(id string) error {
	exists, err := infra.JobExists(id)
	if err != nil {
		return err
	}
	if !exists {
		return ErrJobNotFound
	}
	if err := infra.StampLastBackup(id, httpx.TimeNowStr()); err != nil {
		return err
	}
	logrus.Infof("[Backup] Manually triggered backup task: %s", id)
	return nil
}

// RunCount reports how many runs the register holds. Nothing in production
// needs it; the tests that record the absence of eviction (T-115) do.
func RunCount() int {
	runsLock.RLock()
	defer runsLock.RUnlock()
	return len(runs)
}
