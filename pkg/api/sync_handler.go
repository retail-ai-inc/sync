package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	// "net/url"
	"os"
	// "regexp"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/sirupsen/logrus"
)


// GET /api/sync => query sync_tasks
func SyncListHandler(w http.ResponseWriter, r *http.Request) {
	db, err := openLocalDB()
	if err != nil {
		errorJSON(w, "open db fail", err)
		return
	}
	defer db.Close()

	rows, err := db.Query(`
SELECT
  id,
  enable,
  COALESCE(last_update_time,''),
  COALESCE(last_run_time,''),
  config_json
FROM sync_tasks
ORDER BY id ASC
`)
	if err != nil {
		errorJSON(w, "query sync_tasks fail", err)
		return
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		var (
			id           int
			enableInt    int
			lastUpdate   string
			lastRun      string
			cfgJSON      string
		)
		if err := rows.Scan(&id, &enableInt, &lastUpdate, &lastRun, &cfgJSON); err != nil {
			errorJSON(w, "scan sync_tasks fail", err)
			return
		}

		// enable => status
		status := "Stopped"
		if enableInt == 1 {
			status = "Running"
		}

		// Deserialize config_json
		var extra struct {
			Type       string            `json:"type"`
			TaskName   string            `json:"taskName"`
			Status     string            `json:"status"`
			SourceConn map[string]string `json:"sourceConn"`
			TargetConn map[string]string `json:"targetConn"`
			Mappings   []map[string]interface{} `json:"mappings"`
		}
		if cfgJSON != "" {
			_ = json.Unmarshal([]byte(cfgJSON), &extra)
		}
		if extra.Status != "" {
			status = extra.Status
		}
		if extra.TaskName == "" {
			extra.TaskName = fmt.Sprintf("Sync Task %d", id)
		}


		item := map[string]interface{}{
			"id":             id,
			"enable":         (enableInt != 0),
			"status":         status,
			"lastUpdateTime": lastUpdate,
			"lastRunTime":    lastRun,

			"taskName":   extra.TaskName,
			"sourceType": extra.Type,
			"sourceConn": extra.SourceConn,
			"targetConn": extra.TargetConn,
			"mappings":   extra.Mappings,

		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		errorJSON(w, "sync_tasks iteration error", err)
		return
	}

	writeJSON(w, map[string]interface{}{
		"success": true,
		"data":    result,
	})
}

// POST /api/sync => create
func SyncCreateHandler(w http.ResponseWriter, r *http.Request) {
	logrus.Infof("SyncCreateHandler => method=%s, URL=%s", r.Method, r.URL.String())

	db, err := openLocalDB()
	if err != nil {
		errorJSON(w, "open db fail", err)
		return
	}
	defer db.Close()

	var req struct {
		TaskName   string            `json:"taskName"`
		Type       string            `json:"sourceType"`
		Status     string            `json:"status"`
		SourceConn map[string]string `json:"sourceConn"`
		TargetConn map[string]string `json:"targetConn"`
		Mappings   []map[string]interface{} `json:"mappings"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorJSON(w, "decode fail", err)
		return
	}
	if req.TaskName == "" {
		req.TaskName = "Sync Task"
	}
	if req.Status == "" {
		req.Status = "Stopped"
	}
	enableVal := 0
	if strings.ToLower(req.Status) == "running" {
		enableVal = 1
	}

	nowStr := time.Now().Format("2006-01-02 15:04:05")

	var cfgJSONStruct = struct {
		Type       string            `json:"type"`
		TaskName   string            `json:"taskName"`
		Status     string            `json:"status"`
		SourceConn map[string]string `json:"sourceConn"`
		TargetConn map[string]string `json:"targetConn"`
		Mappings   []map[string]interface{} `json:"mappings"`
	}{
		Type:       req.Type,
		TaskName:   req.TaskName,
		Status:     req.Status,
		SourceConn: req.SourceConn,
		TargetConn: req.TargetConn,
		Mappings:   req.Mappings,
	}
	cfgBytes, _ := json.Marshal(cfgJSONStruct)

	res, err := db.Exec(`
INSERT INTO sync_tasks(enable, last_update_time, last_run_time, config_json)
VALUES(?, ?, ?, ?)
`, enableVal, nowStr, "", string(cfgBytes))
	if err != nil {
		errorJSON(w, "insert fail", err)
		return
	}
	newID, _ := res.LastInsertId()

	respData := map[string]interface{}{
		"id":             newID,
		"enable":         (enableVal != 0),
		"lastUpdateTime": nowStr,
		"lastRunTime":    "",
		"taskName":       req.TaskName,
		"status":         req.Status,
		"sourceType":     req.Type,
		"sourceConn":     req.SourceConn,
		"targetConn":     req.TargetConn,
		"mappings":       req.Mappings,
	}
	writeJSON(w, map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"msg":      "Added successfully",
			"formData": respData,
		},
	})
}

// PUT /api/sync/{id}/start => enable=1, update config_json.status='Running'
func SyncStartHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	err := updateTaskStatus(id, true)
	if err != nil {
		errorJSON(w, "start fail", err)
		return
	}
	writeJSON(w, map[string]interface{}{
		"success": true,
		"data":    map[string]interface{}{"msg": "Started the sync task: " + id},
	})
}

// PUT /api/sync/{id}/stop => enable=0, Update config_json.status='Stopped'
func SyncStopHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	err := updateTaskStatus(id, false)
	if err != nil {
		errorJSON(w, "stop fail", err)
		return
	}
	writeJSON(w, map[string]interface{}{
		"success": true,
		"data":    map[string]interface{}{"msg": "Stopped the sync task: " + id},
	})
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
// -------------------------
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

    var rows *sql.Rows
    if !sinceTime.IsZero() {
        // "logged_at >= ?"
        rows, err = db.Query(`
SELECT logged_at, src_row_count, tgt_row_count
FROM monitoring_log
WHERE sync_task_id=?
  AND datetime(logged_at) >= datetime(?)
ORDER BY logged_at ASC
LIMIT 1000
`, id, sinceTime.Format("2006-01-02 15:04:05"))
    } else {
        rows, err = db.Query(`
SELECT logged_at, src_row_count, tgt_row_count
FROM monitoring_log
WHERE sync_task_id=?
ORDER BY logged_at ASC
LIMIT 1000
`, id)
    }
	if err != nil {
		errorJSON(w, "query monitoring_log fail", err)
		return
	}
	defer rows.Close()

	var rowCountTrend []map[string]interface{}
	for rows.Next() {
		var t string
		var src, tgt int64
		if err := rows.Scan(&t, &src, &tgt); err != nil {
			errorJSON(w, "scan monitoring_log fail", err)
			return
		}
		diff := src - tgt
		if diff < 0 {
			diff = -diff
		}
		rowCountTrend = append(rowCountTrend,
			map[string]interface{}{"time": t, "value": src, "type": "source"},
			map[string]interface{}{"time": t, "value": tgt, "type": "target"},
			map[string]interface{}{"time": t, "value": diff, "type": "diff"},
		)
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
// -----------------------
func SyncLogsHandler(w http.ResponseWriter, r *http.Request) {
    taskID := chi.URLParam(r, "id")
    levelParam := r.URL.Query().Get("level")
	search := r.URL.Query().Get("search")

    db, err := openLocalDB()
    if err != nil {
        errorJSON(w, "open db fail", err)
        return
    }
    defer db.Close()

    rows, err := db.Query(`
SELECT log_time, level, message
FROM sync_log
WHERE sync_task_id=?
ORDER BY log_time DESC
LIMIT 500
`, taskID)
    if err != nil {
        errorJSON(w, "query sync_log fail", err)
        return
    }
    defer rows.Close()

    var logs []map[string]interface{}
    for rows.Next() {
        var t string
        var lvl, msg string
        if err := rows.Scan(&t, &lvl, &msg); err != nil {
            errorJSON(w, "scan sync_log fail", err)
            return
        }
        logs = append(logs, map[string]interface{}{
            "time":    t,
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
        if search != "" {
            if !strings.Contains(strings.ToLower(l["message"].(string)), strings.ToLower(search)) {
			continue
		}
        }
		filtered = append(filtered, l)
	}

	writeJSON(w, map[string]interface{}{
		"success": true,
		"data":    filtered,
	})
}

// PUT /api/sync/{id} => Update sync_tasks.config_json + enable + last_update_time
func SyncUpdateHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	db, err := openLocalDB()
	if err != nil {
		errorJSON(w, "db fail", err)
		return
	}
	defer db.Close()

	var req struct {
		TaskName   string            `json:"taskName"`
		SourceType string            `json:"sourceType"`
		Status     string            `json:"status"`
		SourceConn map[string]string `json:"sourceConn"`
		TargetConn map[string]string `json:"targetConn"`
		Mappings   []map[string]interface{} `json:"mappings"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorJSON(w, "decode fail", err)
		return
	}
	if req.TaskName == "" {
		req.TaskName = "Sync Task"
	}
	if req.Status == "" {
		req.Status = "Stopped"
	}
	enableVal := 0
	if strings.ToLower(req.Status) == "running" {
		enableVal = 1
	}

	nowStr := time.Now().Format("2006-01-02 15:04:05")

	var cfgJSONStruct = struct {
		Type       string            `json:"type"`
		TaskName   string            `json:"taskName"`
		Status     string            `json:"status"`
		SourceConn map[string]string `json:"sourceConn"`
		TargetConn map[string]string `json:"targetConn"`
		Mappings   []map[string]interface{} `json:"mappings"`
	}{
		Type:       req.SourceType,
		TaskName:   req.TaskName,
		Status:     req.Status,
		SourceConn: req.SourceConn,
		TargetConn: req.TargetConn,
		Mappings:   req.Mappings,
	}
	cfgBytes, _ := json.Marshal(cfgJSONStruct)

	res, err := db.Exec(`
UPDATE sync_tasks
SET enable=?,
    last_update_time=?,
    config_json=?
WHERE id=?
`, enableVal, nowStr, string(cfgBytes), id)
	if err != nil {
		errorJSON(w, "update fail", err)
		return
	}
	ra, _ := res.RowsAffected()
	if ra == 0 {
		errorJSON(w, "no record found", errors.New("no rows affected"))
		return
	}

	writeJSON(w, map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"msg": "Update success",
			"formData": map[string]interface{}{
				"id":         id,
				"taskName":   req.TaskName,
				"sourceType": req.SourceType,
				"status":     req.Status,
			},
		},
	})
}

// DELETE /api/sync/{id} => 
func SyncDeleteHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	db, err := openLocalDB()
	if err != nil {
        errorJSON(w, "open db fail", err)
		return
	}
	defer db.Close()

	res, err := db.Exec(`DELETE FROM sync_tasks WHERE id=?`, id)
	if err != nil {
		errorJSON(w, "delete fail", err)
		return
	}
	ra, _ := res.RowsAffected()
	if ra == 0 {
		writeJSON(w, map[string]interface{}{
			"success": false,
			"data":    map[string]interface{}{"msg": "Deletion failed: no record"},
		})
		return
	}
	writeJSON(w, map[string]interface{}{
		"success": true,
		"data":    map[string]interface{}{"msg": "Deleted successfully"},
	})
}

// --------------------------------------------------------------------
// Helpers

func openLocalDB() (*sql.DB, error) {
	dbPath := os.Getenv("SYNC_DB_PATH")
	if dbPath == "" {
		dbPath = "sync.db"
	}
	return sql.Open("sqlite3", dbPath)
}

func updateTaskStatus(id string, toStart bool) error {
	db, err := openLocalDB()
	if err != nil {
		return err
	}
	defer db.Close()

	var oldCfgJSON string
	var oldEnable sql.NullInt32
	err = db.QueryRow(`SELECT config_json, enable FROM sync_tasks WHERE id=?`, id).
		Scan(&oldCfgJSON, &oldEnable)
	if err != nil {
		return err
	}
	var statusStr string
	var newEnable int
	if toStart {
		statusStr = "Running"
		newEnable = 1
	} else {
		statusStr = "Stopped"
		newEnable = 0
	}

	var data map[string]interface{}
	if err2 := json.Unmarshal([]byte(oldCfgJSON), &data); err2 != nil {
		data = make(map[string]interface{})
	}
	data["status"] = statusStr
	newBytes, _ := json.Marshal(data)

	nowStr := time.Now().Format("2006-01-02 15:04:05")
	_, err = db.Exec(`
UPDATE sync_tasks
SET enable=?,
    last_update_time=?,
    config_json=?
WHERE id=?
`, newEnable, nowStr, string(newBytes), id)
	return err
}

// Support 1h,3h,6h,12h,1d,2d,7d
func parseRangeToSince(rangeStr string) time.Time {
    if rangeStr == "" {
        return time.Time{} 
    }
    now := time.Now()
    lower := strings.ToLower(rangeStr)
    switch lower {
    case "1h":
        return now.Add(-1 * time.Hour)
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
    }
    return time.Time{}
}

func errorJSON(w http.ResponseWriter, msg string, err error) {
	logrus.Errorf("%s => %v", msg, err)
	resp := map[string]interface{}{
		"success": false,
		"error":   msg,
		"detail":  err.Error(),
	}
	writeJSON(w, resp)
}

func writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(data)
}
