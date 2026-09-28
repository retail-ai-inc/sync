package infra

import (
	"strconv"
	"time"

	"github.com/retail-ai-inc/sync/internal/monitoring/domain"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
)

// The change stream statistics table has been written to for a year and has
// never held anything but zeroes. The registry the collector read from —
// RegisterChangeStream and the functions that update it — has no callers
// anywhere in the tree, so the map it keeps was always empty: the collector
// asked for a task's streams, got nothing, wrote nothing, and logged that it
// had stored the statistics.

// changeStreamActivity reports what a task's collections have done, built from
// the counters the replication side actually maintains.
func changeStreamActivity(syncTaskID int) map[string]*domain.ChangeStreamInfo {
	streams := map[string]*domain.ChangeStreamInfo{}
	task := strconv.Itoa(syncTaskID)
	now := time.Now()

	// entry finds or creates the record for one collection of this task.
	entry := func(labels metrics.Labels) *domain.ChangeStreamInfo {
		if labels["task"] != task || labels["engine"] != "mongodb" {
			return nil
		}
		collection := labels["collection"]
		if collection == "" {
			// A task-wide series rather than a per-collection one.
			return nil
		}
		database := labels["source"]
		key := database + "." + collection
		if _, held := streams[key]; !held {
			streams[key] = &domain.ChangeStreamInfo{
				SyncTaskID:   syncTaskID,
				Database:     database,
				Collection:   collection,
				Created:      now,
				LastActivity: now,
				Active:       true,
			}
		}
		return streams[key]
	}

	for _, sample := range metrics.Default.Snapshot(metrics.AppliedTotal) {
		if info := entry(sample.Labels); info != nil {
			info.EventCount = int64(sample.Value)
			info.ReceivedEvents = int(sample.Value)
			info.ExecutedEvents = int(sample.Value)
		}
	}
	for _, sample := range metrics.Default.Snapshot(metrics.FailedTotal) {
		if info := entry(sample.Labels); info != nil {
			info.ErrorCount = int(sample.Value)
			// Something that failed was received and not executed.
			info.ReceivedEvents += int(sample.Value)
		}
	}

	// A task that is not running has no live streams, whatever its counters say.
	running := true
	for _, sample := range metrics.Default.Snapshot(metrics.TaskUp) {
		if sample.Labels["task"] == task {
			running = sample.Value != 0
		}
	}
	if !running {
		for _, info := range streams {
			info.Active = false
		}
	}

	return streams
}
