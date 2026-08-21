// Package app holds the replication context's use cases: listing tasks,
// changing them, starting and stopping them, and reporting their progress.
package app

import (
	"context"
	"time"

	dbconn "github.com/retail-ai-inc/sync/internal/platform/dbconn/mongodb"
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

// TableProgress reports each table's replication progress for today.
//
// For a MongoDB task the row counts are replaced with a live estimate from the
// collection, because the monitoring log lags. A connection failure is logged
// and the logged figures are served instead.
func TableProgress(ctx context.Context, id string, now time.Time) ([]domain.TableStat, error) {
	stats, err := infra.TodayTableStats(id, now)
	if err != nil {
		return nil, err
	}
	if len(stats) == 0 || !domain.IsMongoDB(domain.Config{Type: infra.ReadTaskEngine(id)}) {
		return stats, nil
	}

	client, dbName, err := dbconn.ConnectMongoDBFromTaskID(ctx, id, logrus.StandardLogger())
	if err != nil {
		logrus.Warnf("[SyncTables] Failed to connect to MongoDB: %v", err)
		return stats, nil
	}
	defer client.Disconnect(ctx)

	for i, stat := range stats {
		collection := client.Database(dbName).Collection(stat.TableName)
		count, err := collection.EstimatedDocumentCount(ctx)
		if err != nil {
			logrus.Warnf("[SyncTables] Failed to get count for %s.%s: %v", dbName, stat.TableName, err)
			continue
		}
		stats[i].TotalRows = count
		logrus.Debugf("[SyncTables] Updated %s count to %d", stat.TableName, count)
	}
	return stats, nil
}
