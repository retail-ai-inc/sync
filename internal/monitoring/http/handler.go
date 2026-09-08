package monitoringhttp

import (
	"fmt"
	"net/http"

	"github.com/retail-ai-inc/sync/internal/monitoring/app"
	"github.com/retail-ai-inc/sync/internal/platform/httpx"
)

// These endpoints read the request, ask the use case, and render the answer.
// They used to open the control database and write the statements themselves,
// so the level filter and the changestream totals could only be exercised
// through an HTTP request.

func ChangeStreamsStatusHandler(w http.ResponseWriter, r *http.Request) {
	report, err := app.ChangeStreamStatus()
	if err != nil {
		httpx.ErrorJSON(w, "query changestream_statistics fail", err)
		return
	}

	streams := make([]map[string]interface{}, 0, len(report.Streams))
	for _, stat := range report.Streams {
		streams = append(streams, map[string]interface{}{
			"task_id":  fmt.Sprintf("%d", stat.TaskID),
			"name":     stat.Collection,
			"received": stat.Received,
			"executed": stat.Executed,
			"pending":  stat.Pending,
			"errors":   stat.Errors,
			"operations": map[string]interface{}{
				"inserted": stat.Inserted,
				"updated":  stat.Updated,
				"deleted":  stat.Deleted,
			},
		})
	}

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"summary": map[string]interface{}{
				"total_received": report.TotalReceived,
				"total_executed": report.TotalExecuted,
				"total_pending":  report.TotalPending,
				// A rate needs two readings and a gap between them; the stored
				// counters are cumulative totals with no window, so there is
				// nothing here to divide. The Grafana dashboard computes it
				// from the metrics instead.
				"processing_rate": "N/A",
				"active_streams":  report.ActiveStreams,
			},
			"changestreams": streams,
			"last_updated":  report.LastUpdated,
			"tasks_count":   report.TasksCount,
		},
	})
}
