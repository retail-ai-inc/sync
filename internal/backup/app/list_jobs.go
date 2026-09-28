// Package app holds the backup context's use cases: listing jobs, changing
// them, and running one.
package app

import (
	"github.com/retail-ai-inc/sync/internal/backup/domain"
	"github.com/retail-ai-inc/sync/internal/backup/infra"
	"github.com/sirupsen/logrus"
)

// JobView is one job as the list endpoint reports it: the stored job together
// with the configuration read out of it and the status the two agree on.
type JobView struct {
	Job    domain.BackupJob
	Config domain.Config
	Status string
	Name   string
}

// ListJobs returns every job with its configuration resolved.
//
// A configuration that will not parse is logged and carried through as the zero
// value, so a corrupt row is served as a job with no schedule rather than
// failing the whole request.
func ListJobs() ([]JobView, error) {
	jobs, err := infra.ListJobs()
	if err != nil {
		return nil, err
	}

	var views []JobView
	for _, job := range jobs {
		config, err := job.Config()
		if err != nil {
			logrus.Warnf("[Backup] Failed to parse backup configuration JSON: %v", err)
		}
		views = append(views, JobView{
			Job:    job,
			Config: config,
			Status: job.Status(config),
			Name:   job.DisplayName(config),
		})
	}
	return views, nil
}
