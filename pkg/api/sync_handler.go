// pkg/api/sync_handler.go
package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/sirupsen/logrus"
)

// GET /api/sync
// Read sync_configs and return fields similar to mock/sync.ts
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
  type, 
  enable,
  source_connection,
  target_connection,
  COALESCE(task_name, ''),
  COALESCE(status, ''),
  COALESCE(last_update_time, ''),
  COALESCE(last_run_time, '')
FROM sync_configs
ORDER BY id ASC
`)
	if err != nil {
		errorJSON(w, "query sync_configs fail", err)
		return
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		var (
			id       int
			sType    string
			enable   int
			srcConn  string
			tgtConn  string

			taskName       string
			status         string
			lastUpdateTime string
			lastRunTime    string
		)
		if err := rows.Scan(
			&id, &sType, &enable, &srcConn, &tgtConn,
			&taskName, &status, &lastUpdateTime, &lastRunTime,
		); err != nil {
			errorJSON(w, "scan sync_configs fail", err)
			return
		}

		if status == "" {
			if enable == 1 {
				status = "Running"
			} else {
				status = "Stopped"
			}
		}
		if taskName == "" {
			taskName = fmt.Sprintf("Sync Task %d", id)
		}

		item := map[string]interface{}{
			"id":             id,
			"taskName":       taskName,
			"sourceType":     sType,
			"source":         srcConn,
			"target":         tgtConn,
			"status":         status,
			"lastUpdateTime": lastUpdateTime,
			"lastRunTime":    lastRunTime,
			"sourceConn":     parseConnection(srcConn),
			"targetConn":     parseConnection(tgtConn),
			"mappings":       loadMappingsForSync(db, id),
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		errorJSON(w, "sync_configs iteration error", err)
		return
	}

	resp := map[string]interface{}{
		"success": true,
		"data":    result,
	}
	writeJSON(w, resp)
}

// POST /api/sync
// Create a new sync_configs record
func SyncCreateHandler(w http.ResponseWriter, r *http.Request) {
	// Add logging to debug "405 Method Not Allowed" or other issues
	logrus.Infof("SyncCreateHandler => method=%s, URL=%s", r.Method, r.URL.String())

	db, err := openLocalDB()
	if err != nil {
		errorJSON(w, "open db fail", err)
		return
	}
	defer db.Close()

	var req struct {
		Type       string            `json:"sourceType"`
		SourceConn map[string]string `json:"sourceConn"`
		TargetConn map[string]string `json:"targetConn"`
		Mappings   []struct {
			SourceTable string `json:"sourceTable"`
			TargetTable string `json:"targetTable"`
		} `json:"mappings"`
		TaskName string `json:"taskName"`
		Status   string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorJSON(w, "decode request fail", err)
		return
	}

	// Construct DSN
	srcC := buildConnection(req.SourceConn)
	tgtC := buildConnection(req.TargetConn)

	if req.Status == "" {
		req.Status = "Stopped"
	}
	if req.TaskName == "" {
		req.TaskName = "Sync Task"
	}
	nowStr := time.Now().Format("2006-01-02 15:04:05")
	enableVal := 0
	if strings.ToLower(req.Status) == "running" {
		enableVal = 1
	}

	res, err := db.Exec(`
INSERT INTO sync_configs (
  type,
  enable,
  source_connection,
  target_connection,
  task_name,
  status,
  last_update_time,
  last_run_time
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
`,
		req.Type,
		enableVal,
		srcC,
		tgtC,
		req.TaskName,
		req.Status,
		nowStr,
		"",
	)
	if err != nil {
		errorJSON(w, "insert sync_configs fail", err)
		return
	}
	newID, err := res.LastInsertId()
	if err != nil {
		errorJSON(w, "retrieve last insert id fail", err)
		return
	}

	res2, err := db.Exec(`
INSERT INTO database_mappings (sync_config_id, source_database, target_database)
VALUES (?, ?, ?)
`, newID, req.SourceConn["database"], req.TargetConn["database"])
	if err != nil {
		errorJSON(w, "insert database_mappings fail", err)
		return
	}
	mapID, err := res2.LastInsertId()
	if err != nil {
		errorJSON(w, "retrieve mapping last insert id fail", err)
		return
	}

	for _, m := range req.Mappings {
		_, err = db.Exec(`
INSERT INTO table_mappings (database_mapping_id, source_table, target_table)
VALUES (?, ?, ?)
`, mapID, m.SourceTable, m.TargetTable)
		if err != nil {
			errorJSON(w, "insert table_mappings fail", err)
			return
		}
	}

	respData := map[string]interface{}{
		"id":             newID,
		"status":         req.Status,
		"lastUpdateTime": nowStr,
		"lastRunTime":    "",
		"taskName":       req.TaskName,
		"sourceType":     req.Type,
		"sourceConn":     req.SourceConn,
		"targetConn":     req.TargetConn,
		"mappings":       req.Mappings,
		"source":         srcC,
		"target":         tgtC,
	}

	resp := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"msg":      "Added successfully",
			"formData": respData,
		},
	}
	writeJSON(w, resp)
}

// PUT /api/sync/:id/start
func SyncStartHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	logrus.Infof("SyncStartHandler => Attempting to start sync task with id=%s", id)

	db, err := openLocalDB()
	if err != nil {
		errorJSON(w, "open db fail", err)
		return
	}
	defer db.Close()

	nowStr := time.Now().Format("2006-01-02 15:04:05")
	res, err := db.Exec(`
UPDATE sync_configs
SET enable=1,
    status='Running',
    last_run_time=?,
    last_update_time=?
WHERE id=?
`, nowStr, nowStr, id)
	if err != nil {
		errorJSON(w, "update sync_configs start fail", err)
		return
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		errorJSON(w, "retrieve rows affected for start fail", err)
		return
	}
	if rowsAffected == 0 {
		errorJSON(w, "no sync_config found with the given id", errors.New("no rows affected"))
		return
	}

	resp := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"msg": fmt.Sprintf("Started the sync task: %s", id),
		},
	}
	writeJSON(w, resp)
}

// PUT /api/sync/:id/stop
func SyncStopHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	logrus.Infof("SyncStopHandler => Attempting to stop sync task with id=%s", id)

	db, err := openLocalDB()
	if err != nil {
		errorJSON(w, "open db fail", err)
		return
	}
	defer db.Close()

	nowStr := time.Now().Format("2006-01-02 15:04:05")
	res, err := db.Exec(`
UPDATE sync_configs
SET enable=0,
    status='Stopped',
    last_update_time=?
WHERE id=?
`, nowStr, id)
	if err != nil {
		errorJSON(w, "update sync_configs stop fail", err)
		return
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		errorJSON(w, "retrieve rows affected for stop fail", err)
		return
	}
	if rowsAffected == 0 {
		errorJSON(w, "no sync_config found with the given id", errors.New("no rows affected"))
		return
	}

	resp := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"msg": fmt.Sprintf("Stopped the sync task: %s", id),
		},
	}
	writeJSON(w, resp)
}

// GET /api/sync/:id/monitor
func SyncMonitorHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	logrus.Infof("SyncMonitorHandler => Monitoring sync task with id=%s", id)

	db, err := openLocalDB()
	if err != nil {
		errorJSON(w, "open db fail", err)
		return
	}
	defer db.Close()

	var status sql.NullString
	err = db.QueryRow(`SELECT status FROM sync_configs WHERE id=?`, id).Scan(&status)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// If no rows found => return success=false
			resp := map[string]interface{}{
				"success": false,
				"data":    map[string]interface{}{},
			}
			writeJSON(w, resp)
			return
		}
		errorJSON(w, "select sync_configs fail", err)
		return
	}

	st := status.String
	if st == "" {
		st = "Stopped"
	}
	resp := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"progress": 85,
			"tps":      500,
			"delay":    0.2,
			"status":   st,
		},
	}
	writeJSON(w, resp)
}

// GET /api/sync/:id/metrics
// Query from monitoring_log => rowCountTrend
func SyncMetricsHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	logrus.Infof("SyncMetricsHandler => Fetching metrics for sync task id=%s", id)

	db, err := openLocalDB()
	if err != nil {
		errorJSON(w, "open db fail", err)
		return
	}
	defer db.Close()

	// Modify SQL query to only fetch monitoring logs related to the specific sync_config_id
	rows, err := db.Query(`
SELECT ml.logged_at, ml.src_row_count, ml.tgt_row_count
FROM monitoring_log ml
JOIN database_mappings dm ON ml.sync_config_id = dm.sync_config_id
WHERE dm.sync_config_id=?
ORDER BY ml.logged_at ASC
LIMIT 1000
`, id)
	if err != nil {
		errorJSON(w, "query monitoring_log fail", err)
		return
	}
	defer rows.Close()

	var rowCountTrend []map[string]interface{}
	for rows.Next() {
		var (
			loggedAt string
			srcCount int64
			tgtCount int64
		)
		if err := rows.Scan(&loggedAt, &srcCount, &tgtCount); err != nil {
			errorJSON(w, "scan monitoring_log fail", err)
			return
		}
		diffVal := srcCount - tgtCount
		if diffVal < 0 {
			diffVal = -diffVal
		}
		rowCountTrend = append(rowCountTrend,
			map[string]interface{}{
				"time":  loggedAt,
				"value": srcCount,
				"type":  "source",
			},
			map[string]interface{}{
				"time":  loggedAt,
				"value": tgtCount,
				"type":  "target",
			},
			map[string]interface{}{
				"time":  loggedAt,
				"value": diffVal,
				"type":  "diff",
			},
		)
	}
	syncEventStats := []map[string]interface{}{}

	resp := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"rowCountTrend":  rowCountTrend,
			"syncEventStats": syncEventStats,
		},
	}
	writeJSON(w, resp)
}

// GET /api/sync/:id/logs
func SyncLogsHandler(w http.ResponseWriter, r *http.Request) {
	level := r.URL.Query().Get("level")
	search := r.URL.Query().Get("search")

	// Keep it as is, or read logs from the database as needed
	logs := []map[string]interface{}{
		{"time": "2025-01-14 10:01:02", "level": "ERROR", "message": "some error log"},
		{"time": "2025-01-14 09:59:50", "level": "WARN", "message": "some warning"},
		{"time": "2025-01-14 09:59:00", "level": "INFO", "message": "some info message"},
		{"time": "2025-01-14 09:58:30", "level": "INFO", "message": "another info"},
	}
	var filtered []map[string]interface{}
	for _, l := range logs {
		if level != "" && l["level"] != level {
			continue
		}
		msgStr := l["message"].(string)
		if search != "" && !strings.Contains(strings.ToLower(msgStr), strings.ToLower(search)) {
			continue
		}
		filtered = append(filtered, l)
	}
	resp := map[string]interface{}{
		"success": true,
		"data":    filtered,
	}
	writeJSON(w, resp)
}

// PUT /api/sync/:id
func SyncUpdateHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	logrus.Infof("SyncUpdateHandler => Updating sync task with id=%s", id)

	db, err := openLocalDB()
	if err != nil {
		errorJSON(w, "open db fail", err)
		return
	}
	defer db.Close()

	var req struct {
		TaskName   string            `json:"taskName"`
		SourceType string            `json:"sourceType"`
		Status     string            `json:"status"`
		SourceConn map[string]string `json:"sourceConn"`
		TargetConn map[string]string `json:"targetConn"`
		Mappings   []struct {
			SourceTable string `json:"sourceTable"`
			TargetTable string `json:"targetTable"`
		} `json:"mappings"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorJSON(w, "decode request fail", err)
		return
	}

	nowStr := time.Now().Format("2006-01-02 15:04:05")
	enableVal := 0
	if strings.ToLower(req.Status) == "running" {
		enableVal = 1
	}
	if req.Status == "" {
		req.Status = "Stopped"
	}

	srcC := buildConnection(req.SourceConn)
	tgtC := buildConnection(req.TargetConn)

	res, err := db.Exec(`
UPDATE sync_configs
SET type=?,
    source_connection=?,
    target_connection=?,
    task_name=?,
    status=?,
    last_update_time=?,
    enable=?
WHERE id=?
`,
		req.SourceType,
		srcC,
		tgtC,
		req.TaskName,
		req.Status,
		nowStr,
		enableVal,
		id)
	if err != nil {
		errorJSON(w, "update sync_configs fail", err)
		return
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		errorJSON(w, "retrieve rows affected for update fail", err)
		return
	}
	if rowsAffected == 0 {
		errorJSON(w, "no sync_config found with the given id", errors.New("no rows affected"))
		return
	}

	// Rebuild mappings
	_, err = db.Exec(`
DELETE FROM table_mappings 
WHERE database_mapping_id IN (
	SELECT id FROM database_mappings WHERE sync_config_id=?
)`, id)
	if err != nil {
		errorJSON(w, "delete existing table_mappings fail", err)
		return
	}
	_, err = db.Exec(`DELETE FROM database_mappings WHERE sync_config_id=?`, id)
	if err != nil {
		errorJSON(w, "delete existing database_mappings fail", err)
		return
	}

	res2, err := db.Exec(`
INSERT INTO database_mappings (sync_config_id, source_database, target_database)
VALUES (?, ?, ?)
`,
		id,
		req.SourceConn["database"],
		req.TargetConn["database"])
	if err != nil {
		errorJSON(w, "insert new database_mappings fail", err)
		return
	}
	mappingID, err := res2.LastInsertId()
	if err != nil {
		errorJSON(w, "retrieve mapping last insert id fail", err)
		return
	}

	for _, m := range req.Mappings {
		_, err = db.Exec(`
INSERT INTO table_mappings (database_mapping_id, source_table, target_table)
VALUES (?, ?, ?)
`, mappingID, m.SourceTable, m.TargetTable)
		if err != nil {
			errorJSON(w, "insert table_mappings fail", err)
			return
		}
	}

	resp := map[string]interface{}{
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
	}
	writeJSON(w, resp)
}

// DELETE /api/sync/:id
func SyncDeleteHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	logrus.Infof("SyncDeleteHandler => Attempting to delete sync task with id=%s", id)

	db, err := openLocalDB()
	if err != nil {
		errorJSON(w, "open db fail", err)
		return
	}
	defer db.Close()

	// Delete related table_mappings
	res1, err := db.Exec(`
DELETE FROM table_mappings
WHERE database_mapping_id IN (
	SELECT id FROM database_mappings WHERE sync_config_id=?
)
`, id)
	if err != nil {
		errorJSON(w, "delete table_mappings fail", err)
		return
	}
	rowsAffected1, _ := res1.RowsAffected()
	logrus.Infof("Deleted %d table_mappings for sync_config_id=%s", rowsAffected1, id)

	// Delete related database_mappings
	res2, err := db.Exec(`DELETE FROM database_mappings WHERE sync_config_id=?`, id)
	if err != nil {
		errorJSON(w, "delete database_mappings fail", err)
		return
	}
	rowsAffected2, _ := res2.RowsAffected()
	logrus.Infof("Deleted %d database_mappings for sync_config_id=%s", rowsAffected2, id)

	// Delete sync_configs record
	res3, err := db.Exec(`DELETE FROM sync_configs WHERE id=?`, id)
	if err != nil {
		errorJSON(w, "delete sync_configs fail", err)
		return
	}
	rowsAffected3, err := res3.RowsAffected()
	if err != nil {
		errorJSON(w, "retrieve rows affected for delete fail", err)
		return
	}
	logrus.Infof("Deleted %d sync_configs records for id=%s", rowsAffected3, id)

	if rowsAffected3 == 0 {
		resp := map[string]interface{}{
			"success": false,
			"data": map[string]interface{}{
				"msg": "Deletion failed: no record",
			},
		}
		writeJSON(w, resp)
		return
	}
	resp := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"msg": "Deleted successfully",
		},
	}
	writeJSON(w, resp)
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

func loadMappingsForSync(db *sql.DB, syncConfigID int) []map[string]string {
	var result []map[string]string
	rows, err := db.Query(`
SELECT tm.source_table, tm.target_table
FROM table_mappings tm
JOIN database_mappings dm ON dm.id = tm.database_mapping_id
WHERE dm.sync_config_id=?
`, syncConfigID)
	if err != nil {
		logrus.Errorf("loadMappingsForSync query fail: %v", err)
		return result
	}
	defer rows.Close()

	for rows.Next() {
		var st, tt string
		if err := rows.Scan(&st, &tt); err != nil {
			logrus.Errorf("loadMappingsForSync scan fail: %v", err)
			continue
		}
		result = append(result, map[string]string{
			"sourceTable": st,
			"targetTable": tt,
		})
	}
	return result
}

// parseConnection: Simple parsing for mongodb://, mysql DSN, postgres://, redis://, etc.
func parseConnection(raw string) map[string]string {
	res := map[string]string{
		"host":     raw,
		"port":     "",
		"user":     "",
		"password": "",
		"database": "",
	}
	if raw == "" {
		return res
	}

	// mongodb://
	if strings.HasPrefix(strings.ToLower(raw), "mongodb://") {
		uri, err := url.Parse(raw)
		if err == nil {
			res["host"] = uri.Hostname()
			res["port"] = uri.Port()
			pw, _ := uri.User.Password()
			res["user"] = uri.User.Username()
			res["password"] = pw
			dbp := strings.TrimPrefix(uri.Path, "/")
			res["database"] = dbp
		}
		return res
	}

	// mysql => root:pwd@tcp(host:port)/db
	reMy := regexp.MustCompile(`^(?P<User>.*?):(?P<Pass>.*?)@tcp\((?P<Host>.*?):(?P<Port>\d+)\)/(?P<Db>\w+)$`)
	if matches := reMy.FindStringSubmatch(raw); len(matches) == 6 {
		res["user"] = matches[1]
		res["password"] = matches[2]
		res["host"] = matches[3]
		res["port"] = matches[4]
		res["database"] = matches[5]
		return res
	}

	// postgres://user:pwd@localhost:5432/db?sslmode=disable
	if strings.HasPrefix(strings.ToLower(raw), "postgres://") {
		uri, err := url.Parse(raw)
		if err == nil {
			res["host"] = uri.Hostname()
			res["port"] = uri.Port()
			pw, _ := uri.User.Password()
			res["user"] = uri.User.Username()
			res["password"] = pw
			dbp := strings.TrimPrefix(uri.Path, "/")
			res["database"] = dbp
		}
		return res
	}

	// redis://:pwd@host:6379/db
	if strings.HasPrefix(strings.ToLower(raw), "redis://") {
		uri, err := url.Parse(raw)
		if err == nil {
			res["host"] = uri.Hostname()
			res["port"] = uri.Port()
			pw, _ := uri.User.Password()
			res["user"] = uri.User.Username()
			res["password"] = pw
			dbp := strings.TrimPrefix(uri.Path, "/")
			res["database"] = dbp
		}
		return res
	}

	// fallback => return host=raw
	return res
}

// buildConnection: Combine host, port, user, password, database back to DSN
func buildConnection(m map[string]string) string {
	if m == nil {
		return ""
	}
	host := m["host"]
	port := m["port"]
	user := m["user"]
	pwd := m["password"]
	dbn := m["database"]

	// MySQL
	if user != "" && host != "" && port != "" && dbn != "" && strings.Contains(strings.ToLower(m["host"]), "tcp") {
		return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, pwd, host, port, dbn)
	}

	// postgres
	if strings.HasPrefix(strings.ToLower(m["host"]), "postgres://") {
		return host
	}

	// redis
	if strings.HasPrefix(strings.ToLower(m["host"]), "redis://") {
		return host
	}

	// mongodb
	if strings.HasPrefix(strings.ToLower(m["host"]), "mongodb://") {
		return host
	}

	// fallback
	if port != "" && dbn != "" {
		return fmt.Sprintf("%s:%s/%s", host, port, dbn)
	}
	return host
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
