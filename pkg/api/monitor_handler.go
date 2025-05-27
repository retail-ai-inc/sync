package api

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	// "github.com/sirupsen/logrus"
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

	// Convert to JST (UTC+9)
	jst := time.FixedZone("JST", 9*60*60)
	jstTime := parsedTime.In(jst)

	// Format the time in the desired format
	return jstTime.Format("2006-01-02T15:04+09:00")
}

// GET /api/sync/{id}/monitor => {status, progress, tps, ...}
func SyncMonitorHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	db, err := openLocalDB()
	if err != nil {
		errorJSON(w, "db fail", err)
		return
	}
	defer db.Close()

	var enableInt sql.NullInt32
	err = db.QueryRow(`SELECT enable FROM sync_tasks WHERE id=?`, id).Scan(&enableInt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, map[string]interface{}{"success": false, "data": map[string]interface{}{}})
			return
		}
		errorJSON(w, "select fail", err)
		return
	}

	status := "Stopped"
	if enableInt.Int32 == 1 {
		status = "Running"
	}

	writeJSON(w, map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"progress": 85,
			"tps":      500,
			"delay":    0.2,
			"status":   status,
		},
	})
}

// GET /api/sync/{id}/metrics
func SyncMetricsHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	rangeStr := r.URL.Query().Get("range")

	sinceTime := parseRangeToSince(rangeStr)

	db, err := openLocalDB()
	if err != nil {
		errorJSON(w, "db fail", err)
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
		errorJSON(w, "query monitoring_log fail", err)
		return
	}
	defer rows.Close()

	var rowCountTrend []map[string]interface{}
	for rows.Next() {
		var t, tbl string
		var src, tgt int64
		var taskID string
		if err := rows.Scan(&t, &tbl, &src, &tgt, &taskID); err != nil {
			errorJSON(w, "scan monitoring_log fail", err)
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

	if len(rowCountTrend) == 0 && !sinceTime.IsZero() {
		rows.Close()

		if id == "0" {
			query = `
SELECT logged_at, tgt_table, src_row_count, tgt_row_count, sync_task_id
FROM monitoring_log
ORDER BY logged_at ASC
LIMIT 1000
`
			rows, err = db.Query(query)
		} else {
			query = `
SELECT logged_at, tgt_table, src_row_count, tgt_row_count, sync_task_id
FROM monitoring_log
WHERE sync_task_id=?
ORDER BY logged_at ASC
LIMIT 1000
`
			rows, err = db.Query(query, id)
		}

		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var t, tbl string
				var src, tgt int64
				var taskID string
				if err := rows.Scan(&t, &tbl, &src, &tgt, &taskID); err != nil {
					continue
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
		}
	}

	if err := rows.Err(); err != nil {
		errorJSON(w, "monitoring_log iteration error", err)
		return
	}

	writeJSON(w, map[string]interface{}{
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

	sinceTime := parseRangeToSince(rangeStr)

	db, err := openLocalDB()
	if err != nil {
		errorJSON(w, "open db fail", err)
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
		errorJSON(w, "query sync_log fail", err)
		return
	}
	defer rows.Close()

	var logs []map[string]interface{}
	for rows.Next() {
		var t, lvl, msg string
		if err := rows.Scan(&t, &lvl, &msg); err != nil {
			errorJSON(w, "scan sync_log fail", err)
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
		errorJSON(w, "sync_log iteration error", err)
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

	writeJSON(w, map[string]interface{}{
		"success": true,
		"data":    filtered,
	})
}

// 1h, 2h, 3h, 6h, 12h, 1d, 2d, 7d，default: -10h
func parseRangeToSince(rangeStr string) (since time.Time) {
	if rangeStr == "" {
		return time.Time{}
	}

	now := time.Now().UTC()
	lower := strings.ToLower(rangeStr)

	switch lower {
	case "1h":
		return now.Add(-1 * time.Hour)
	case "2h":
		return now.Add(-2 * time.Hour)
	case "3h":
		return now.Add(-3 * time.Hour)
	case "6h":
		return now.Add(-6 * time.Hour)
	case "12h":
		return now.Add(-12 * time.Hour)
	case "1d":
		return now.AddDate(0, 0, -1)
	case "2d":
		return now.AddDate(0, 0, -2)
	case "7d":
		return now.AddDate(0, 0, -7)
	default:
		return now.Add(-10 * time.Hour)
	}
}

// GET /api/changestreams/status
func ChangeStreamsStatusHandler(w http.ResponseWriter, r *http.Request) {
	db, err := openLocalDB()
	if err != nil {
		errorJSON(w, "open db fail", err)
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
		errorJSON(w, "query changestream_statistics fail", err)
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

		// Skip Task ID=0 to filter out legacy data
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

		// Update last updated time (take the latest)
		if lastUpdated == "" || lastUpdatedTime > lastUpdated {
			lastUpdated = lastUpdatedTime
		}
	}

	if err := rows.Err(); err != nil {
		errorJSON(w, "changestream_statistics iteration error", err)
		return
	}

	// Calculate processing rate based on total received/executed over time
	processingRate := "N/A"
	if totalActiveStreams > 0 && totalExecuted > 0 {
		// Simple calculation: assume data represents recent activity
		// For more accurate rate, we would need time-based windows
		processingRate = fmt.Sprintf("~%d/min", totalExecuted)
	}

	// Prepare response
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

	writeJSON(w, response)
}
