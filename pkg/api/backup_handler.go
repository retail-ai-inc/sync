package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/retail-ai-inc/sync/pkg/backup"
	"github.com/sirupsen/logrus"
)

func BackupListHandler(w http.ResponseWriter, r *http.Request) {
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
  COALESCE(last_backup_time,''),
  COALESCE(next_backup_time,''),
  config_json
FROM backup_tasks
ORDER BY id ASC
`)
	if err != nil {
		errorJSON(w, "query backup_tasks fail", err)
		return
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		var (
			id         int
			enableInt  int
			lastUpdate string
			lastBackup string
			nextBackup string
			cfgJSON    string
		)
		if err := rows.Scan(&id, &enableInt, &lastUpdate, &lastBackup, &nextBackup, &cfgJSON); err != nil {
			errorJSON(w, "scan backup_tasks fail", err)
			return
		}

		status := "disabled"
		if enableInt == 1 {
			status = "enabled"
		}

		var config struct {
			Name            string                 `json:"name"`
			SourceType      string                 `json:"sourceType"`
			Database        map[string]interface{} `json:"database"`
			Destination     map[string]interface{} `json:"destination"`
			Schedule        string                 `json:"schedule"`
			Format          string                 `json:"format"`
			BackupType      string                 `json:"backupType"`
			Query           map[string]interface{} `json:"query"`
			Status          string                 `json:"status"`
			CompressionType string                 `json:"compressionType"`
		}

		if cfgJSON != "" {
			if err := json.Unmarshal([]byte(cfgJSON), &config); err != nil {
				logrus.Warnf("[Backup] Failed to parse backup configuration JSON: %v", err)
			}
		}

		if config.Status != "" {
			status = config.Status
		}

		if config.Name == "" {
			config.Name = fmt.Sprintf("Backup Task %d", id)
		}

		item := map[string]interface{}{
			"id":              id,
			"name":            config.Name,
			"sourceType":      config.SourceType,
			"database":        config.Database,
			"destination":     config.Destination,
			"schedule":        config.Schedule,
			"format":          config.Format,
			"backupType":      config.BackupType,
			"query":           config.Query,
			"status":          status,
			"compressionType": config.CompressionType,
			"lastUpdateTime":  convertTimeToJST(lastUpdate),
			"lastBackupTime":  convertTimeToJST(lastBackup),
			"nextBackupTime":  convertTimeToJST(nextBackup),
		}

		result = append(result, item)
	}

	if err := rows.Err(); err != nil {
		errorJSON(w, "backup_tasks iteration error", err)
		return
	}

	writeJSON(w, map[string]interface{}{
		"success": true,
		"data":    result,
	})
}

func BackupCreateHandler(w http.ResponseWriter, r *http.Request) {
	logrus.Infof("[Backup] BackupCreateHandler => method=%s, URL=%s", r.Method, r.URL.String())

	db, err := openLocalDB()
	if err != nil {
		errorJSON(w, "open db fail", err)
		return
	}
	defer db.Close()

	var req struct {
		Name            string                 `json:"name"`
		SourceType      string                 `json:"sourceType"`
		Database        map[string]interface{} `json:"database"`
		Destination     map[string]interface{} `json:"destination"`
		Schedule        string                 `json:"schedule"`
		Format          string                 `json:"format"`
		BackupType      string                 `json:"backupType"`
		Query           map[string]interface{} `json:"query"`
		CompressionType string                 `json:"compressionType"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorJSON(w, "decode fail", err)
		return
	}

	if req.Name == "" {
		req.Name = "Backup Task"
	}

	status := "enabled"
	enableVal := 1

	nowStr := timeNowStr()

	nextBackupTime := calculateNextBackupTime(req.Schedule)

	type configStruct struct {
		Name            string                 `json:"name"`
		SourceType      string                 `json:"sourceType"`
		Database        map[string]interface{} `json:"database"`
		Destination     map[string]interface{} `json:"destination"`
		Schedule        string                 `json:"schedule"`
		Format          string                 `json:"format"`
		BackupType      string                 `json:"backupType"`
		Query           map[string]interface{} `json:"query"`
		Status          string                 `json:"status"`
		CompressionType string                 `json:"compressionType"`
	}

	cfgJSONStruct := configStruct{
		Name:            req.Name,
		SourceType:      req.SourceType,
		Database:        req.Database,
		Destination:     req.Destination,
		Schedule:        req.Schedule,
		Format:          req.Format,
		BackupType:      req.BackupType,
		Query:           req.Query,
		Status:          status,
		CompressionType: req.CompressionType,
	}

	cfgBytes, _ := json.Marshal(cfgJSONStruct)

	res, err := db.Exec(`
INSERT INTO backup_tasks(enable, last_update_time, last_backup_time, next_backup_time, config_json)
VALUES(?, ?, ?, ?, ?)
`, enableVal, nowStr, "", nextBackupTime, string(cfgBytes))

	if err != nil {
		errorJSON(w, "insert fail", err)
		return
	}

	newID, _ := res.LastInsertId()

	writeJSON(w, map[string]interface{}{
		"success": true,
		"message": "Backup job created successfully",
		"data": map[string]interface{}{
			"id":     newID,
			"name":   req.Name,
			"status": status,
		},
	})

	// After successful creation, sync crontab
	db, cronManager := getDbAndCronManager()
	if err := cronManager.SyncCrontab(r.Context()); err != nil {
		logrus.Warnf("[BackupCreateHandler] Failed to sync crontab: %v", err)
		// Continue execution, don't interrupt response
	}
}

func BackupDeleteHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	logrus.Infof("[Backup] BackupDeleteHandler => taskID=%s", id)

	db, err := openLocalDB()
	if err != nil {
		errorJSON(w, "open db fail", err)
		return
	}
	defer db.Close()

	res, err := db.Exec(`DELETE FROM backup_tasks WHERE id=?`, id)
	if err != nil {
		errorJSON(w, "delete fail", err)
		return
	}

	ra, _ := res.RowsAffected()
	if ra == 0 {
		writeJSON(w, map[string]interface{}{
			"success": false,
			"message": "Backup job deletion failed: no record found",
		})
		return
	}

	writeJSON(w, map[string]interface{}{
		"success": true,
		"message": "Backup job deleted successfully",
	})

	// After successful deletion, sync crontab
	db, cronManager := getDbAndCronManager()
	if err := cronManager.SyncCrontab(r.Context()); err != nil {
		logrus.Warnf("[BackupDeleteHandler] Failed to sync crontab: %v", err)
		// Continue execution, don't interrupt response
	}
}

func BackupPauseHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	logrus.Infof("[Backup] BackupPauseHandler => taskID=%s", id)

	err := updateBackupStatus(id, false)
	if err != nil {
		errorJSON(w, "pause fail", err)
		return
	}

	writeJSON(w, map[string]interface{}{
		"success": true,
		"message": "Backup job paused successfully",
	})

	// After successful pause, sync crontab
	_, cronManager := getDbAndCronManager()
	if err := cronManager.SyncCrontab(r.Context()); err != nil {
		logrus.Warnf("[BackupPauseHandler] Failed to sync crontab: %v", err)
		// Continue execution, don't interrupt response
	}
}

func BackupResumeHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	logrus.Infof("[Backup] BackupResumeHandler => taskID=%s", id)

	err := updateBackupStatus(id, true)
	if err != nil {
		errorJSON(w, "resume fail", err)
		return
	}

	writeJSON(w, map[string]interface{}{
		"success": true,
		"message": "Backup job resumed successfully",
	})

	// After successful resume, sync crontab
	_, cronManager := getDbAndCronManager()
	if err := cronManager.SyncCrontab(r.Context()); err != nil {
		logrus.Warnf("[BackupResumeHandler] Failed to sync crontab: %v", err)
		// Continue execution, don't interrupt response
	}
}

func BackupRunHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	logrus.Infof("[Backup] BackupRunHandler => taskID=%s", id)

	db, err := openLocalDB()
	if err != nil {
		errorJSON(w, "open db fail", err)
		return
	}
	defer db.Close()

	var exists int
	err = db.QueryRow("SELECT COUNT(*) FROM backup_tasks WHERE id=?", id).Scan(&exists)
	if err != nil {
		errorJSON(w, "query fail", err)
		return
	}

	if exists == 0 {
		errorJSON(w, "backup task not found", errors.New("no such task"))
		return
	}

	nowStr := timeNowStr()
	_, err = db.Exec("UPDATE backup_tasks SET last_backup_time=? WHERE id=?", nowStr, id)
	if err != nil {
		errorJSON(w, "update fail", err)
		return
	}

	logrus.Infof("[Backup] Manually triggered backup task: %s", id)

	writeJSON(w, map[string]interface{}{
		"success": true,
		"message": "Backup job started successfully",
	})
}

func BackupUpdateHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	logrus.Infof("[Backup] BackupUpdateHandler => taskID=%s", id)

	db, err := openLocalDB()
	if err != nil {
		errorJSON(w, "db fail", err)
		return
	}
	defer db.Close()

	var req struct {
		Name            string                 `json:"name"`
		SourceType      string                 `json:"sourceType"`
		Database        map[string]interface{} `json:"database"`
		Destination     map[string]interface{} `json:"destination"`
		Schedule        string                 `json:"schedule"`
		Format          string                 `json:"format"`
		BackupType      string                 `json:"backupType"`
		Query           map[string]interface{} `json:"query"`
		CompressionType string                 `json:"compressionType"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorJSON(w, "decode fail", err)
		return
	}

	var oldCfgJSON string
	var enableInt int
	err = db.QueryRow(`SELECT config_json, enable FROM backup_tasks WHERE id=?`, id).Scan(&oldCfgJSON, &enableInt)
	if err != nil {
		errorJSON(w, "fetch existing config fail", err)
		return
	}

	var oldConfig map[string]interface{}
	if err := json.Unmarshal([]byte(oldCfgJSON), &oldConfig); err != nil {
		oldConfig = make(map[string]interface{})
	}

	status := "disabled"
	if val, ok := oldConfig["status"]; ok {
		status = val.(string)
	} else if enableInt == 1 {
		status = "enabled"
	}

	if req.Name == "" {
		if name, ok := oldConfig["name"]; ok {
			req.Name = name.(string)
		} else {
			req.Name = fmt.Sprintf("Backup Task %s", id)
		}
	}

	nowStr := timeNowStr()

	nextBackupTime := calculateNextBackupTime(req.Schedule)

	type configStruct struct {
		Name            string                 `json:"name"`
		SourceType      string                 `json:"sourceType"`
		Database        map[string]interface{} `json:"database"`
		Destination     map[string]interface{} `json:"destination"`
		Schedule        string                 `json:"schedule"`
		Format          string                 `json:"format"`
		BackupType      string                 `json:"backupType"`
		Query           map[string]interface{} `json:"query"`
		Status          string                 `json:"status"`
		CompressionType string                 `json:"compressionType"`
	}

	cfgJSONStruct := configStruct{
		Name:            req.Name,
		SourceType:      req.SourceType,
		Database:        req.Database,
		Destination:     req.Destination,
		Schedule:        req.Schedule,
		Format:          req.Format,
		BackupType:      req.BackupType,
		Query:           req.Query,
		Status:          status,
		CompressionType: req.CompressionType,
	}

	cfgBytes, _ := json.Marshal(cfgJSONStruct)

	res, err := db.Exec(`
UPDATE backup_tasks
SET last_update_time=?,
    next_backup_time=?,
    config_json=?
WHERE id=?
`, nowStr, nextBackupTime, string(cfgBytes), id)

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
		"message": "Backup job updated successfully",
	})

	// After successful update, sync crontab
	db, cronManager := getDbAndCronManager()
	if err := cronManager.SyncCrontab(r.Context()); err != nil {
		logrus.Warnf("[BackupUpdateHandler] Failed to sync crontab: %v", err)
		// Continue execution, don't interrupt response
	}
}

func updateBackupStatus(id string, enable bool) error {
	db, err := openLocalDB()
	if err != nil {
		return err
	}
	defer db.Close()

	var oldCfgJSON string
	err = db.QueryRow(`SELECT config_json FROM backup_tasks WHERE id=?`, id).Scan(&oldCfgJSON)
	if err != nil {
		return err
	}

	var statusStr string
	var newEnable int
	if enable {
		statusStr = "enabled"
		newEnable = 1
	} else {
		statusStr = "disabled"
		newEnable = 0
	}

	var data map[string]interface{}
	if err := json.Unmarshal([]byte(oldCfgJSON), &data); err != nil {
		data = make(map[string]interface{})
	}
	data["status"] = statusStr
	newBytes, _ := json.Marshal(data)

	nowStr := timeNowStr()
	_, err = db.Exec(`
UPDATE backup_tasks
SET enable=?,
    last_update_time=?,
    config_json=?
WHERE id=?
`, newEnable, nowStr, string(newBytes), id)
	return err
}

func calculateNextBackupTime(cronExpr string) string {
	tomorrow := time.Now().UTC().Add(24 * time.Hour)
	return tomorrow.Format("2006-01-02 15:04:05")
}

// getDbAndCronManager Get database connection and crontab manager
func getDbAndCronManager() (*sql.DB, *backup.CronManager) {
	db, err := openLocalDB()
	if err != nil {
		logrus.Errorf("[CronManager] Failed to open database: %v", err)
		return nil, nil
	}
	// Ensure API path correctly includes /api prefix
	apiServer := "http://localhost:8080/api" // Should be obtained from configuration
	return db, backup.NewCronManager(db, apiServer)
}
