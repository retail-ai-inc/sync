package app

import (
	"github.com/retail-ai-inc/sync/internal/backup/domain"
	"github.com/retail-ai-inc/sync/internal/backup/infra"
	"github.com/retail-ai-inc/sync/internal/platform/httpx"
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

// UpdateJob replaces a job's configuration. The status and, when the request
// omits it, the name are carried over from the stored document; everything
// else comes from the request, because an update replaces the configuration
// rather than merging into it.
func UpdateJob(id string, req domain.Request) error {
	if err := req.Complete(); err != nil {
		return err
	}

	configJSON, enable, err := infra.ReadJobRow(id)
	if err != nil {
		return err
	}

	oldConfig := domain.ParseStoredConfig(configJSON)
	// An edit that did not touch the password sends back the mask the list
	// endpoint handed out, and saving that would leave the job authenticating
	// with "********".
	req = domain.CarryStoredPasswords(req, oldConfig)
	status := domain.DeriveUpdateStatus(oldConfig, enable)
	if req.Name == "" {
		req.Name = domain.DeriveUpdateName(oldConfig, id)
	}

	return infra.UpdateJob(id, httpx.TimeNowStr(), domain.NextBackupTime(req.Schedule),
		domain.ConfigFrom(req, status))
}

func DeleteJob(id string) error { return infra.DeleteJob(id) }

// PauseJob disables a job. ResumeJob enables it.
func PauseJob(id string) error  { return infra.SetEnable(id, false, httpx.TimeNowStr()) }
func ResumeJob(id string) error { return infra.SetEnable(id, true, httpx.TimeNowStr()) }
