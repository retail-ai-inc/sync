package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/retail-ai-inc/sync/pkg/db"
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
			id         int
			enableInt  int
			lastUpdate string
			lastRun    string
			cfgJSON    string
		)
		if err := rows.Scan(&id, &enableInt, &lastUpdate, &lastRun, &cfgJSON); err != nil {
			errorJSON(w, "scan sync_tasks fail", err)
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
			"lastUpdateTime": lastUpdate,
			"lastRunTime":    lastRun,
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

	nowStr := timeNowStr()

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
		errorJSON(w, "insert fail", err)
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

// PUT /api/sync/{id} => Update sync_tasks.config_json + enable + last_update_time
func SyncUpdateHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	db, err := openLocalDB()
	if err != nil {
		errorJSON(w, "db fail", err)
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

	nowStr := timeNowStr()

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

// -------------------------
// Helpers & Common Utilities
// -------------------------

func openLocalDB() (*sql.DB, error) {
	return db.OpenSQLiteDB()
}

func updateTaskStatus(id string, toStart bool) error {
	db, err := openLocalDB()
	if err != nil {
		return err
	}
	defer db.Close()

	var oldCfgJSON string
	err = db.QueryRow(`SELECT config_json FROM sync_tasks WHERE id=?`, id).Scan(&oldCfgJSON)
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

	nowStr := timeNowStr()
	_, err = db.Exec(`
UPDATE sync_tasks
SET enable=?,
    last_update_time=?,
    config_json=?
WHERE id=?
`, newEnable, nowStr, string(newBytes), id)
	return err
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

func timeNowStr() string {
	return time.Now().Format("2006-01-02 15:04:05")
}
