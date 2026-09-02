package monitoringhttp

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/retail-ai-inc/sync/internal/platform/httpx"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
)

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

	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		httpx.ErrorJSON(w, "db fail", err)
		return
	}
	defer db.Close()

	var enableInt sql.NullInt32
	err = db.QueryRow(`SELECT enable FROM sync_tasks WHERE id=?`, id).Scan(&enableInt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			httpx.WriteJSON(w, map[string]interface{}{"success": false, "data": map[string]interface{}{}})
			return
		}
		httpx.ErrorJSON(w, "select fail", err)
		return
	}

	status := "Stopped"
	if enableInt.Int32 == 1 {
		status = "Running"
	}

	// progress, tps and delay used to be the constants 85, 500 and 0.2, so a
	// task that had never run showed the same healthy figures as one carrying
	// payments. They come from the counters the syncers keep; a task with no
	// counters yet reports null rather than a number nobody measured.
	applied, lag := taskActivity(id)

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"applied": applied,
			"delay":   lag,
			"status":  status,
		},
	})
}

// taskActivity reports what a task has applied and how far behind it is,
// reading the metrics the syncers maintain. A nil means nothing has been
// recorded for it, which is not the same as zero.
func taskActivity(id string) (applied, lag interface{}) {
	var total float64
	var counted bool
	for _, sample := range metrics.Default.Snapshot(metrics.AppliedTotal) {
		if sample.Labels["task"] == id {
			total += sample.Value
			counted = true
		}
	}
	if counted {
		applied = total
	}

	for _, sample := range metrics.Default.Snapshot(metrics.LagSeconds) {
		if sample.Labels["task"] == id {
			// The worst of a task's collections is the one that matters.
			if lag == nil || sample.Value > lag.(float64) {
				lag = sample.Value
			}
		}
	}
	return applied, lag
}

// GET /api/sync/{id}/metrics
func SyncMetricsHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	rangeStr := r.URL.Query().Get("range")

	sinceTime, err := parseRangeToSince(rangeStr)
	if err != nil {
		httpx.ErrorJSONStatus(w, http.StatusBadRequest, "unknown range", err)
		return
	}

	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		httpx.ErrorJSON(w, "db fail", err)
		return
	}
	defer db.Close()

	// "YYYY-MM-DD HH:MM:SS"
	timeFormat := "2006-01-02 15:04:05"

	var rows *sql.Rows
	var query string
	var queryParams []interface{}

	if id == "0" {
		if !sinceTime.IsZero() {
			query = `
SELECT logged_at, tgt_table, src_row_count, tgt_row_count, sync_task_id
FROM monitoring_log
WHERE logged_at >= ?
ORDER BY logged_at ASC
LIMIT 1000
`
			utcSince := sinceTime.UTC().Format(timeFormat)
			queryParams = []interface{}{utcSince}
		} else {
			query = `
SELECT logged_at, tgt_table, src_row_count, tgt_row_count, sync_task_id
FROM monitoring_log
ORDER BY logged_at ASC
LIMIT 1000
`
		}
	} else {
		if !sinceTime.IsZero() {
			query = `
SELECT logged_at, tgt_table, src_row_count, tgt_row_count, sync_task_id
FROM monitoring_log
WHERE sync_task_id=?
  AND logged_at >= ?
ORDER BY logged_at ASC
LIMIT 1000
`
			utcSince := sinceTime.UTC().Format(timeFormat)
			queryParams = []interface{}{id, utcSince}
		} else {
			query = `
SELECT logged_at, tgt_table, src_row_count, tgt_row_count, sync_task_id
FROM monitoring_log
WHERE sync_task_id=?
ORDER BY logged_at ASC
LIMIT 1000
`
			queryParams = []interface{}{id}
		}
	}

	if len(queryParams) > 0 {
		rows, err = db.Query(query, queryParams...)
	} else {
		rows, err = db.Query(query)
	}

	if err != nil {
		httpx.ErrorJSON(w, "query monitoring_log fail", err)
		return
	}
	defer rows.Close()

	var rowCountTrend []map[string]interface{}
	for rows.Next() {
		var t, tbl string
		var src, tgt int64
		var taskID string
		if err := rows.Scan(&t, &tbl, &src, &tgt, &taskID); err != nil {
			httpx.ErrorJSON(w, "scan monitoring_log fail", err)
			return
		}
		diff := src - tgt
		if diff < 0 {
			diff = -diff
		}

		tableName := tbl
		if id == "0" {
			tableName = "taskID:" + taskID + "_" + tbl
		}

		jstTime := convertToJST(t)

		rowCountTrend = append(rowCountTrend,
			map[string]interface{}{"time": jstTime, "table": tableName, "type": "source", "value": src},
			map[string]interface{}{"time": jstTime, "table": tableName, "type": "target", "value": tgt},
			map[string]interface{}{"time": jstTime, "table": tableName, "type": "diff", "value": diff},
		)
	}

	// A window with nothing in it used to be answered by running the query
	// again without the window and returning the whole history — up to a
	// thousand rows, with nothing in the response to say the window had been
	// abandoned. Somebody asking what happened in the last hour got last month.
	if rowCountTrend == nil {
		rowCountTrend = []map[string]interface{}{}
	}

	if err := rows.Err(); err != nil {
		httpx.ErrorJSON(w, "monitoring_log iteration error", err)
		return
	}

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"rowCountTrend":  rowCountTrend,
			"syncEventStats": []interface{}{},
		},
	})
}

// GET /api/sync/{id}/logs
func SyncLogsHandler(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "id")
	levelParam := r.URL.Query().Get("level")
	search := r.URL.Query().Get("search")
	rangeStr := r.URL.Query().Get("range")

	sinceTime, err := parseRangeToSince(rangeStr)
	if err != nil {
		httpx.ErrorJSONStatus(w, http.StatusBadRequest, "unknown range", err)
		return
	}

	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		httpx.ErrorJSON(w, "open db fail", err)
		return
	}
	defer db.Close()

	var rows *sql.Rows
	var query string
	var queryParams []interface{}

	// "YYYY-MM-DD HH:MM:SS"
	timeFormat := "2006-01-02 15:04:05"

	if !sinceTime.IsZero() {
		query = `
SELECT log_time, level, message
FROM sync_log
WHERE sync_task_id=?
  AND log_time >= ?
ORDER BY log_time DESC
LIMIT 500
`
		utcSince := sinceTime.UTC().Format(timeFormat)
		queryParams = []interface{}{taskID, utcSince}
	} else {
		query = `
SELECT log_time, level, message
FROM sync_log
WHERE sync_task_id=?
ORDER BY log_time DESC
LIMIT 500
`
		queryParams = []interface{}{taskID}
	}

	rows, err = db.Query(query, queryParams...)
	if err != nil {
		httpx.ErrorJSON(w, "query sync_log fail", err)
		return
	}
	defer rows.Close()

	var logs []map[string]interface{}
	for rows.Next() {
		var t, lvl, msg string
		if err := rows.Scan(&t, &lvl, &msg); err != nil {
			httpx.ErrorJSON(w, "scan sync_log fail", err)
			return
		}

		jstTime := convertToJST(t)

		logs = append(logs, map[string]interface{}{
			"time":    jstTime,
			"level":   lvl,
			"message": msg,
		})
	}
	if err := rows.Err(); err != nil {
		httpx.ErrorJSON(w, "sync_log iteration error", err)
		return
	}

	var filtered []map[string]interface{}
	for _, l := range logs {
		if levelParam != "" && !strings.EqualFold(l["level"].(string), levelParam) {
			continue
		}
		if search != "" && !strings.Contains(
			strings.ToLower(l["message"].(string)),
			strings.ToLower(search),
		) {
			continue
		}
		filtered = append(filtered, l)
	}

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data":    filtered,
	})
}

// parseRangeToSince resolves a window like "1h", "12h" or "7d" to the instant
// it starts at. An empty range means no window at all.
func parseRangeToSince(rangeStr string) (since time.Time, err error) {
	trimmed := strings.TrimSpace(rangeStr)
	if trimmed == "" {
		return time.Time{}, nil
	}

	now := time.Now().UTC()
	lower := strings.ToLower(trimmed)

	// Days, which time.ParseDuration does not know.
	if days, found := strings.CutSuffix(lower, "d"); found {
		n, convErr := strconv.Atoi(days)
		if convErr != nil || n < 1 {
			return time.Time{}, fmt.Errorf("%q is not a range", rangeStr)
		}
		return now.AddDate(0, 0, -n), nil
	}

	span, err := time.ParseDuration(lower)
	if err != nil || span <= 0 {
		return time.Time{}, fmt.Errorf("%q is not a range", rangeStr)
	}
	return now.Add(-span), nil
}

// GET /api/changestreams/status
func ChangeStreamsStatusHandler(w http.ResponseWriter, r *http.Request) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		httpx.ErrorJSON(w, "open db fail", err)
		return
	}
	defer db.Close()

	// Query changestream_statistics table directly
	query := `
SELECT 
	task_id,
	collection_name,
	received,
	executed,
	pending,
	errors,
	inserted,
	updated,
	deleted,
	last_updated,
	created_at
FROM changestream_statistics
ORDER BY task_id, collection_name
`

	rows, err := db.Query(query)
	if err != nil {
		httpx.ErrorJSON(w, "query changestream_statistics fail", err)
		return
	}
	defer rows.Close()

	// Aggregated data
	totalReceived := 0
	totalExecuted := 0
	totalPending := 0
	totalErrors := 0
	totalActiveStreams := 0
	allChangeStreams := make([]map[string]interface{}, 0)
	lastUpdated := ""
	taskIDs := make(map[int]bool)

	for rows.Next() {
		var taskID int
		var collectionName string
		var received, executed, pending, errors, inserted, updated, deleted int
		var lastUpdatedTime, createdAt string

		if err := rows.Scan(&taskID, &collectionName, &received, &executed, &pending, &errors, &inserted, &updated, &deleted, &lastUpdatedTime, &createdAt); err != nil {
			continue
		}

		if taskID == 0 {
			continue
		}

		// Track unique task IDs
		taskIDs[taskID] = true

		// Aggregate summary data
		totalReceived += received
		totalExecuted += executed
		totalPending += pending
		totalErrors += errors
		totalActiveStreams++

		// Create changestream detail (maintain original API format)
		csDetail := map[string]interface{}{
			"task_id":  fmt.Sprintf("%d", taskID), // Convert to string format
			"name":     collectionName,            // Use "name" instead of "collection_name"
			"received": received,
			"executed": executed,
			"pending":  pending,
			"errors":   errors,
			"operations": map[string]interface{}{ // Nest operations object
				"inserted": inserted,
				"updated":  updated,
				"deleted":  deleted,
			},
		}

		allChangeStreams = append(allChangeStreams, csDetail)

		if lastUpdated == "" || lastUpdatedTime > lastUpdated {
			lastUpdated = lastUpdatedTime
		}
	}

	if err := rows.Err(); err != nil {
		httpx.ErrorJSON(w, "changestream_statistics iteration error", err)
		return
	}

	// Calculate processing rate based on total received/executed over time
	processingRate := "N/A"
	if totalActiveStreams > 0 && totalExecuted > 0 {
		// Simple calculation: assume data represents recent activity
		// For more accurate rate, we would need time-based windows
		// processingRate = fmt.Sprintf("~%d/min", totalExecuted)
	}

	response := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"summary": map[string]interface{}{
				"total_received":  totalReceived,
				"total_executed":  totalExecuted,
				"total_pending":   totalPending,
				"processing_rate": processingRate,
				"active_streams":  totalActiveStreams,
			},
			"changestreams": allChangeStreams,
			"last_updated":  lastUpdated,
			"tasks_count":   len(taskIDs),
		},
	}

	httpx.WriteJSON(w, response)
}
