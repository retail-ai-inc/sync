package app

import (
	"github.com/retail-ai-inc/sync/internal/monitoring/domain"
	"github.com/retail-ai-inc/sync/internal/monitoring/infra"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
)

// The use cases behind the monitoring endpoints.
//
// Each one is a read and a rule: the endpoints had both inline, so the only way
// to ask "what does this task's log look like filtered to errors" was to build
// a request.

// ErrNoTask reports that a task id names no row.
var ErrNoTask = infra.ErrNoTask

// TaskStatus reports whether a task is running, in the words the UI shows.
func TaskStatus(taskID string) (string, error) {
	enabled, err := infra.TaskEnabled(taskID)
	if err != nil {
		return "", err
	}
	if enabled {
		return "Running", nil
	}
	return "Stopped", nil
}

// TaskActivity reports what a task has applied and how far behind it is.
//
// A nil means nothing has been recorded for it, which is not the same as zero:
// progress, throughput and delay used to be the constants 85, 500 and 0.2, so a
// task that had never run showed the same healthy figures as one carrying
// payments.
func TaskActivity(taskID string) (applied, lag interface{}) {
	var total float64
	var counted bool
	for _, sample := range metrics.Default.Snapshot(metrics.AppliedTotal) {
		if sample.Labels["task"] == taskID {
			total += sample.Value
			counted = true
		}
	}
	if counted {
		applied = total
	}

	for _, sample := range metrics.Default.Snapshot(metrics.LagSeconds) {
		if sample.Labels["task"] != taskID {
			continue
		}
		// The worst of a task's collections is the one that matters.
		if lag == nil || sample.Value > lag.(float64) {
			lag = sample.Value
		}
	}
	return applied, lag
}

// ChangeStreamStatus reports every change stream's counters and their totals.
func ChangeStreamStatus() (domain.ChangeStreamReport, error) {
	stats, err := infra.ChangeStreamStatistics()
	if err != nil {
		return domain.ChangeStreamReport{}, err
	}
	return domain.SummariseChangeStreams(stats), nil
}
