package app

import (
	"context"

	"github.com/retail-ai-inc/sync/internal/backup/domain"
	"github.com/retail-ai-inc/sync/internal/backup/infra"
	"github.com/retail-ai-inc/sync/internal/backup/infra/crontab"
	"github.com/retail-ai-inc/sync/internal/platform/httpx"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
	"github.com/sirupsen/logrus"
)

// CreateJob stores a new job. New jobs are always enabled.
func CreateJob(req domain.Request) (id int64, name, status string, err error) {
	if req.Name == "" {
		req.Name = "Backup Task"
	}
	status = domain.StatusEnabled
	enable := 1

	newID, err := infra.InsertJob(enable, httpx.TimeNowStr(), domain.NextBackupTime(req.Schedule),
		domain.ConfigFrom(req, status))
	if err != nil {
		return 0, "", "", err
	}
	return newID, req.Name, status, nil
}

// UpdateJob replaces a job's configuration.
//
// The status and, when the request omits it, the name are carried over from the
// stored document; everything else comes from the request, so an omitted field
// is stored as its zero value (T-111).
func UpdateJob(id string, req domain.Request) error {
	configJSON, enable, err := infra.ReadJobRow(id)
	if err != nil {
		return err
	}

	oldConfig := domain.ParseStoredConfig(configJSON)
	status := domain.DeriveUpdateStatus(oldConfig, enable)
	if req.Name == "" {
		req.Name = domain.DeriveUpdateName(oldConfig, id)
	}

	return infra.UpdateJob(id, httpx.TimeNowStr(), domain.NextBackupTime(req.Schedule),
		domain.ConfigFrom(req, status))
}

// DeleteJob removes a job.
func DeleteJob(id string) error { return infra.DeleteJob(id) }

// PauseJob disables a job. ResumeJob enables it.
func PauseJob(id string) error  { return infra.SetEnable(id, false, httpx.TimeNowStr()) }
func ResumeJob(id string) error { return infra.SetEnable(id, true, httpx.TimeNowStr()) }

// SyncCrontab rewrites the system crontab from the enabled jobs. Every write
// endpoint calls it after answering, and a failure is logged rather than
// reported, so the response never says the schedule did not take.
func SyncCrontab(ctx context.Context, caller string) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		logrus.Errorf("[CronManager] Failed to open database: %v", err)
		return
	}
	// Ensure API path correctly includes /api prefix
	apiServer := "http://localhost:8080/api" // Should be obtained from configuration
	if err := crontab.NewCronManager(db, apiServer).SyncCrontab(ctx); err != nil {
		logrus.Warnf("[%s] Failed to sync crontab: %v", caller, err)
		// Continue execution, don't interrupt response
	}
}
