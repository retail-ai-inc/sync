// Package app holds the replication context's use cases: listing tasks,
// changing them, starting and stopping them, and reporting their progress.
package app

import (
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra"
	"github.com/sirupsen/logrus"
)

// TaskView is one task as the list endpoint reports it: the stored task with
// its configuration resolved.
type TaskView struct {
	Task   domain.SyncTask
	Config domain.Config
	Status string
	Name   string
}

// ListTasks returns every task with its configuration resolved. A configuration
// that will not parse is logged and carried through as the zero value.
func ListTasks() ([]TaskView, error) {
	tasks, err := infra.ListTasks()
	if err != nil {
		return nil, err
	}

	var views []TaskView
	for _, task := range tasks {
		config, err := task.Config()
		if err != nil {
			logrus.Warnf("Failed to parse configuration JSON: %v", err)
		}
		views = append(views, TaskView{
			Task:   task,
			Config: config,
			Status: task.Status(config),
			Name:   task.DisplayName(config),
		})
	}
	return views, nil
}
