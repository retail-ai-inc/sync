package replication

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	sqlitedb "github.com/retail-ai-inc/sync/internal/platform/db"

	"github.com/go-chi/chi/v5"
	"github.com/retail-ai-inc/sync/internal/platform/dbconn/mongodb"
	"github.com/retail-ai-inc/sync/internal/platform/httpx"
	"github.com/sirupsen/logrus"
)

// GET /api/sync => query sync_tasks
func SyncListHandler(w http.ResponseWriter, r *http.Request) {
	db, err := sqlitedb.OpenSQLiteDB()
	if err != nil {
		httpx.ErrorJSON(w, "open db fail", err)
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
		httpx.ErrorJSON(w, "query sync_tasks fail", err)
		return
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		var (
			id         int
			enableInt  int
			lastUpdate string
			lastRun    string
			cfgJSON    string
		)
		if err := rows.Scan(&id, &enableInt, &lastUpdate, &lastRun, &cfgJSON); err != nil {
			httpx.ErrorJSON(w, "scan sync_tasks fail", err)
			return
		}

		status := "Stopped"
		if enableInt == 1 {
			status = "Running"
		}

		// Add DB-specific fields to the struct:
		var extra struct {
			Type                   string                   `json:"type"`
			TaskName               string                   `json:"taskName"`
			Status                 string                   `json:"status"`
			SourceConn             map[string]string        `json:"sourceConn"`
			TargetConn             map[string]string        `json:"targetConn"`
			Mappings               []map[string]interface{} `json:"mappings"`
			PgReplicationSlot      string                   `json:"pg_replication_slot"`
			PgPlugin               string                   `json:"pg_plugin"`
			PgPositionPath         string                   `json:"pg_position_path"`
			PgPublicationNames     string                   `json:"pg_publication_names"`
			MysqlPositionPath      string                   `json:"mysql_position_path"`
			MongodbResumeTokenPath string                   `json:"mongodb_resume_token_path"`
			RedisPositionPath      string                   `json:"redis_position_path"`
			SecurityEnabled        bool                     `json:"securityEnabled"`
		}

		logrus.Debugf("Configuration JSON from database: %s", cfgJSON)

		if cfgJSON != "" {
			if err := json.Unmarshal([]byte(cfgJSON), &extra); err != nil {
				logrus.Warnf("Failed to parse configuration JSON: %v", err)
			}
			extraJSON, _ := json.Marshal(extra)
			logrus.Debugf("Parsed extra structure: %s", string(extraJSON))
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
			"lastUpdateTime": httpx.ConvertTimeToJST(lastUpdate),
			"lastRunTime":    httpx.ConvertTimeToJST(lastRun),
			"taskName":       extra.TaskName,
			"sourceType":     extra.Type,
			"sourceConn":     extra.SourceConn,
			"targetConn":     extra.TargetConn,
			"mappings":       extra.Mappings,

			// Include the DB-specific fields in the returned item:
			"pg_replication_slot":       extra.PgReplicationSlot,
			"pg_plugin":                 extra.PgPlugin,
			"pg_position_path":          extra.PgPositionPath,
			"pg_publication_names":      extra.PgPublicationNames,
			"mysql_position_path":       extra.MysqlPositionPath,
			"mongodb_resume_token_path": extra.MongodbResumeTokenPath,
			"redis_position_path":       extra.RedisPositionPath,
			"securityEnabled":           extra.SecurityEnabled,
		}

		itemJSON, _ := json.Marshal(item)
		logrus.Debugf("Returned item: %s", string(itemJSON))

		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		httpx.ErrorJSON(w, "sync_tasks iteration error", err)
		return
	}

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data":    result,
	})
}

// POST /api/sync => create
func SyncCreateHandler(w http.ResponseWriter, r *http.Request) {
	logrus.Infof("SyncCreateHandler => method=%s, URL=%s", r.Method, r.URL.String())

	db, err := sqlitedb.OpenSQLiteDB()
	if err != nil {
		httpx.ErrorJSON(w, "open db fail", err)
		return
	}
	defer db.Close()

	// Include all DB-specific fields in the request struct:
	var req struct {
		TaskName               string                   `json:"taskName"`
		Type                   string                   `json:"sourceType"` // Note: front-end might send "sourceType"
		Status                 string                   `json:"status"`
		SourceConn             map[string]string        `json:"sourceConn"`
		TargetConn             map[string]string        `json:"targetConn"`
		Mappings               []map[string]interface{} `json:"mappings"`
		PgReplicationSlot      string                   `json:"pg_replication_slot"`
		PgPlugin               string                   `json:"pg_plugin"`
		PgPositionPath         string                   `json:"pg_position_path"`
		PgPublicationNames     string                   `json:"pg_publication_names"`
		MysqlPositionPath      string                   `json:"mysql_position_path"`
		MongodbResumeTokenPath string                   `json:"mongodb_resume_token_path"`
		RedisPositionPath      string                   `json:"redis_position_path"`
		SecurityEnabled        bool                     `json:"securityEnabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.ErrorJSON(w, "decode fail", err)
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

	nowStr := httpx.TimeNowStr()

	// Build the config JSON struct with all DB-specific fields
	var cfgJSONStruct = struct {
		Type                   string                   `json:"type"`
		TaskName               string                   `json:"taskName"`
		Status                 string                   `json:"status"`
		SourceConn             map[string]string        `json:"sourceConn"`
		TargetConn             map[string]string        `json:"targetConn"`
		Mappings               []map[string]interface{} `json:"mappings"`
		PgReplicationSlot      string                   `json:"pg_replication_slot"`
		PgPlugin               string                   `json:"pg_plugin"`
		PgPositionPath         string                   `json:"pg_position_path"`
		PgPublicationNames     string                   `json:"pg_publication_names"`
		MysqlPositionPath      string                   `json:"mysql_position_path"`
		MongodbResumeTokenPath string                   `json:"mongodb_resume_token_path"`
		RedisPositionPath      string                   `json:"redis_position_path"`
		SecurityEnabled        bool                     `json:"securityEnabled"`
	}{
		Type:                   req.Type,
		TaskName:               req.TaskName,
		Status:                 req.Status,
		SourceConn:             req.SourceConn,
		TargetConn:             req.TargetConn,
		Mappings:               req.Mappings,
		PgReplicationSlot:      req.PgReplicationSlot,
		PgPlugin:               req.PgPlugin,
		PgPositionPath:         req.PgPositionPath,
		PgPublicationNames:     req.PgPublicationNames,
		MysqlPositionPath:      req.MysqlPositionPath,
		MongodbResumeTokenPath: req.MongodbResumeTokenPath,
		RedisPositionPath:      req.RedisPositionPath,
		SecurityEnabled:        req.SecurityEnabled,
	}
	cfgBytes, _ := json.Marshal(cfgJSONStruct)

	res, err := db.Exec(`
INSERT INTO sync_tasks(enable, last_update_time, last_run_time, config_json)
VALUES(?, ?, ?, ?)
`, enableVal, nowStr, "", string(cfgBytes))
	if err != nil {
		httpx.ErrorJSON(w, "insert fail", err)
		return
	}
	newID, _ := res.LastInsertId()

	// Build response data
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

		"pg_replication_slot":       req.PgReplicationSlot,
		"pg_plugin":                 req.PgPlugin,
		"pg_position_path":          req.PgPositionPath,
		"pg_publication_names":      req.PgPublicationNames,
		"mysql_position_path":       req.MysqlPositionPath,
		"mongodb_resume_token_path": req.MongodbResumeTokenPath,
		"redis_position_path":       req.RedisPositionPath,
		"securityEnabled":           req.SecurityEnabled,
	}

	httpx.WriteJSON(w, map[string]interface{}{
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
		httpx.ErrorJSON(w, "start fail", err)
		return
	}
	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data":    map[string]interface{}{"msg": "Started the sync task: " + id},
	})
}

// PUT /api/sync/{id}/stop => enable=0, Update config_json.status='Stopped'
func SyncStopHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	err := updateTaskStatus(id, false)
	if err != nil {
		httpx.ErrorJSON(w, "stop fail", err)
		return
	}
	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data":    map[string]interface{}{"msg": "Stopped the sync task: " + id},
	})
}

// PUT /api/sync/{id} => Update sync_tasks.config_json + enable + last_update_time
func SyncUpdateHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	db, err := sqlitedb.OpenSQLiteDB()
	if err != nil {
		httpx.ErrorJSON(w, "db fail", err)
		return
	}
	defer db.Close()

	// Include all DB-specific fields in the request struct:
	var req struct {
		TaskName               string                   `json:"taskName"`
		SourceType             string                   `json:"sourceType"`
		Status                 string                   `json:"status"`
		SourceConn             map[string]string        `json:"sourceConn"`
		TargetConn             map[string]string        `json:"targetConn"`
		Mappings               []map[string]interface{} `json:"mappings"`
		PgReplicationSlot      string                   `json:"pg_replication_slot"`
		PgPlugin               string                   `json:"pg_plugin"`
		PgPositionPath         string                   `json:"pg_position_path"`
		PgPublicationNames     string                   `json:"pg_publication_names"`
		MysqlPositionPath      string                   `json:"mysql_position_path"`
		MongodbResumeTokenPath string                   `json:"mongodb_resume_token_path"`
		RedisPositionPath      string                   `json:"redis_position_path"`
		SecurityEnabled        bool                     `json:"securityEnabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.ErrorJSON(w, "decode fail", err)
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

	nowStr := httpx.TimeNowStr()

	// Build the config JSON struct with all DB-specific fields
	var cfgJSONStruct = struct {
		Type                   string                   `json:"type"`
		TaskName               string                   `json:"taskName"`
		Status                 string                   `json:"status"`
		SourceConn             map[string]string        `json:"sourceConn"`
		TargetConn             map[string]string        `json:"targetConn"`
		Mappings               []map[string]interface{} `json:"mappings"`
		PgReplicationSlot      string                   `json:"pg_replication_slot"`
		PgPlugin               string                   `json:"pg_plugin"`
		PgPositionPath         string                   `json:"pg_position_path"`
		PgPublicationNames     string                   `json:"pg_publication_names"`
		MysqlPositionPath      string                   `json:"mysql_position_path"`
		MongodbResumeTokenPath string                   `json:"mongodb_resume_token_path"`
		RedisPositionPath      string                   `json:"redis_position_path"`
		SecurityEnabled        bool                     `json:"securityEnabled"`
	}{
		Type:                   req.SourceType,
		TaskName:               req.TaskName,
		Status:                 req.Status,
		SourceConn:             req.SourceConn,
		TargetConn:             req.TargetConn,
		Mappings:               req.Mappings,
		PgReplicationSlot:      req.PgReplicationSlot,
		PgPlugin:               req.PgPlugin,
		PgPositionPath:         req.PgPositionPath,
		PgPublicationNames:     req.PgPublicationNames,
		MysqlPositionPath:      req.MysqlPositionPath,
		MongodbResumeTokenPath: req.MongodbResumeTokenPath,
		RedisPositionPath:      req.RedisPositionPath,
		SecurityEnabled:        req.SecurityEnabled,
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
		httpx.ErrorJSON(w, "update fail", err)
		return
	}
	ra, _ := res.RowsAffected()
	if ra == 0 {
		httpx.ErrorJSON(w, "no record found", errors.New("no rows affected"))
		return
	}

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"msg": "Update success",
			"formData": map[string]interface{}{
				"id":                        id,
				"taskName":                  req.TaskName,
				"sourceType":                req.SourceType,
				"status":                    req.Status,
				"pg_replication_slot":       req.PgReplicationSlot,
				"pg_plugin":                 req.PgPlugin,
				"pg_position_path":          req.PgPositionPath,
				"pg_publication_names":      req.PgPublicationNames,
				"mysql_position_path":       req.MysqlPositionPath,
				"mongodb_resume_token_path": req.MongodbResumeTokenPath,
				"redis_position_path":       req.RedisPositionPath,
				"securityEnabled":           req.SecurityEnabled,
			},
		},
	})
}

// DELETE /api/sync/{id}
func SyncDeleteHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	db, err := sqlitedb.OpenSQLiteDB()
	if err != nil {
		httpx.ErrorJSON(w, "open db fail", err)
		return
	}
	defer db.Close()

	res, err := db.Exec(`DELETE FROM sync_tasks WHERE id=?`, id)
	if err != nil {
		httpx.ErrorJSON(w, "delete fail", err)
		return
	}
	ra, _ := res.RowsAffected()
	if ra == 0 {
		httpx.WriteJSON(w, map[string]interface{}{
			"success": false,
			"data":    map[string]interface{}{"msg": "Deletion failed: no record"},
		})
		return
	}
	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data":    map[string]interface{}{"msg": "Deleted successfully"},
	})
}

// GET /api/sync/{id}/tables => returns tables info and sync stats for today
func SyncTablesHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	logrus.Infof("[SyncTables] Fetching tables data for task: %s", id)

	db, err := sqlitedb.OpenSQLiteDB()
	if err != nil {
		httpx.ErrorJSON(w, "open db fail", err)
		return
	}
	defer db.Close()

	// Get today's date range in UTC
	now := time.Now().UTC()
	todayStart := now.Format("2006-01-02") + " 00:00:00"
	todayEnd := now.Format("2006-01-02") + " 23:59:59"

	// Use a SQL query to get all tables and sync data volume for today
	rows, err := db.Query(`
		SELECT 
			tgt_table,
			MAX(tgt_row_count) - MIN(tgt_row_count) AS synced_today,
			MAX(tgt_row_count) AS total_rows,
			MAX(logged_at) AS last_sync_time
		FROM monitoring_log
		WHERE sync_task_id = ?
		AND logged_at BETWEEN ? AND ?
		GROUP BY tgt_table
	`, id, todayStart, todayEnd)

	if err != nil {
		httpx.ErrorJSON(w, "query monitoring_log fail", err)
		return
	}
	defer rows.Close()

	// Process query results
	tableStats := make([]map[string]interface{}, 0)
	for rows.Next() {
		var (
			tableName    string
			syncedToday  int64
			totalRows    int64
			lastSyncTime string
		)
		if err := rows.Scan(&tableName, &syncedToday, &totalRows, &lastSyncTime); err != nil {
			logrus.Warnf("[SyncTables] Error scanning row: %v", err)
			continue
		}

		// Correct potentially negative sync amounts
		if syncedToday < 0 {
			syncedToday = 0
		}

		tableStats = append(tableStats, map[string]interface{}{
			"tableName":    tableName,
			"syncedToday":  syncedToday,
			"totalRows":    totalRows,
			"lastSyncTime": httpx.ConvertTimeToJST(lastSyncTime),
		})
	}

	var configJSON string
	err = db.QueryRow("SELECT config_json FROM sync_tasks WHERE id = ?", id).Scan(&configJSON)
	if err == nil && configJSON != "" {
		var cfg struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal([]byte(configJSON), &cfg); err == nil {
			if strings.ToLower(cfg.Type) == "mongodb" && len(tableStats) > 0 {
				ctx := r.Context()

				client, dbName, err := mongodb.ConnectMongoDBFromTaskID(ctx, id, logrus.StandardLogger())
				if err != nil {
					logrus.Warnf("[SyncTables] Failed to connect to MongoDB: %v", err)
				} else {
					defer client.Disconnect(ctx)

					for i, stat := range tableStats {
						tableName := stat["tableName"].(string)

						collection := client.Database(dbName).Collection(tableName)
						count, err := collection.EstimatedDocumentCount(ctx)
						if err != nil {
							logrus.Warnf("[SyncTables] Failed to get count for %s.%s: %v", dbName, tableName, err)
						} else {
							tableStats[i]["totalRows"] = count
							logrus.Debugf("[SyncTables] Updated %s count to %d", tableName, count)
						}
					}
				}
			}
		}
	}

	// Get JST date for display purposes only
	jst := time.FixedZone("JST", 9*60*60)
	jstDate := now.In(jst).Format("2006-01-02")

	// Return the results
	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"taskId":     id,
			"tableCount": len(tableStats),
			"syncDate":   jstDate,
			"tables":     tableStats,
		},
	})
}
