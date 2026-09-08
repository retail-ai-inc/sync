package monitoringhttp

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/retail-ai-inc/sync/internal/monitoring/app"
	"github.com/retail-ai-inc/sync/internal/platform/httpx"
)

// These endpoints read the request, ask the use case, and render the answer.
// They used to open the control database and write the statements themselves,
// so the level filter and the changestream totals could only be exercised
// through an HTTP request.

// convertToJST converts a time string from UTC to JST (UTC+9)
// It accepts RFC3339 format as input and returns a formatted JST time
func convertToJST(timeStr string) string {
	// Try to parse the time string
	parsedTime, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		// If parsing fails, return the original string
		return timeStr
	}

	jst := time.FixedZone("JST", 9*60*60)
	jstTime := parsedTime.In(jst)

	return jstTime.Format("2006-01-02T15:04+09:00")
}

// GET /api/sync/{id}/monitor => {status, progress, tps, ...}
func SyncMonitorHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	status, err := app.TaskStatus(id)
	switch {
	case errors.Is(err, app.ErrNoTask):
		httpx.WriteJSON(w, map[string]interface{}{"success": false, "data": map[string]interface{}{}})
		return
	case err != nil:
		httpx.ErrorJSON(w, "select fail", err)
		return
	}

	applied, lag := app.TaskActivity(id)

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"applied": applied,
			"delay":   lag,
			"status":  status,
		},
	})
}

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
