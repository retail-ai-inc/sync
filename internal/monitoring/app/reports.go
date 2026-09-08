package app

import (
	"github.com/retail-ai-inc/sync/internal/monitoring/domain"
	"github.com/retail-ai-inc/sync/internal/monitoring/infra"
)

// The use cases behind the monitoring endpoints.
//
// Each one is a read and a rule: the endpoints had both inline, so the only way
// to ask "what does this task's log look like filtered to errors" was to build
// a request.

// ErrNoTask reports that a task id names no row.
var ErrNoTask = infra.ErrNoTask

// ChangeStreamStatus reports every change stream's counters and their totals.
func ChangeStreamStatus() (domain.ChangeStreamReport, error) {
	stats, err := infra.ChangeStreamStatistics()
	if err != nil {
		return domain.ChangeStreamReport{}, err
	}
	return domain.SummariseChangeStreams(stats), nil
}
