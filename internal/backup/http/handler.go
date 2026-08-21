// Package backuphttp adapts the backup use cases to HTTP.
//
// The handlers keep the request parsing and the response writing they have
// always done, including the two endpoints that both claim to run a backup and
// mean different things by it.
package backuphttp

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/retail-ai-inc/sync/internal/backup/app"
	"github.com/retail-ai-inc/sync/internal/backup/domain"
	"github.com/retail-ai-inc/sync/internal/backup/infra"
	"github.com/retail-ai-inc/sync/internal/platform/httpx"
	"github.com/sirupsen/logrus"
)

// fail answers with the plain JSON error envelope, using the message the store
// tagged its failure with when there is one.
func fail(w http.ResponseWriter, fallback string, err error) {
	var fault *infra.Fault
	if errors.As(err, &fault) {
		httpx.ErrorJSON(w, fault.Stage, fault.Err)
		return
	}
	httpx.ErrorJSON(w, fallback, err)
}

// BackupListHandler GET /api/backup
func BackupListHandler(w http.ResponseWriter, r *http.Request) {
	views, err := app.ListJobs()
	if err != nil {
		fail(w, "query backup_tasks fail", err)
		return
	}

	var result []map[string]interface{}
	for _, v := range views {
		result = append(result, map[string]interface{}{
			"id":                 v.Job.ID(),
			"name":               v.Name,
			"sourceType":         v.Config.SourceType,
			"database":           v.Config.Database,
			"destination":        v.Config.Destination,
			"schedule":           v.Config.Schedule,
			"format":             v.Config.Format,
			"backupType":         v.Config.BackupType,
			"query":              v.Config.Query,
			"status":             v.Status,
			"compressionType":    v.Config.CompressionType,
			"tableSelectionMode": v.Config.TableSelectionMode,
			"regexPattern":       v.Config.RegexPattern,
			"lastUpdateTime":     httpx.ConvertTimeToJST(v.Job.LastUpdateTime()),
			"lastBackupTime":     httpx.ConvertTimeToJST(v.Job.LastBackupTime()),
			"nextBackupTime":     httpx.ConvertTimeToJST(v.Job.NextBackupTimeRaw()),
		})
	}

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data":    result,
	})
}

// BackupCreateHandler POST /api/backup
func BackupCreateHandler(w http.ResponseWriter, r *http.Request) {
	logrus.Infof("[Backup] BackupCreateHandler => method=%s, URL=%s", r.Method, r.URL.String())

	var req domain.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.ErrorJSON(w, "decode fail", err)
		return
	}

	newID, name, status, err := app.CreateJob(req)
	if err != nil {
		fail(w, "insert fail", err)
		return
	}

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"message": "Backup job created successfully",
		"data": map[string]interface{}{
			"id":     newID,
			"name":   name,
			"status": status,
		},
	})

	// After successful creation, sync crontab
	app.SyncCrontab(r.Context(), "BackupCreateHandler")
}

// BackupUpdateHandler PUT /api/backup/{id}
func BackupUpdateHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	logrus.Infof("[Backup] BackupUpdateHandler => taskID=%s", id)

	var req domain.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.ErrorJSON(w, "decode fail", err)
		return
	}

	switch err := app.UpdateJob(id, req); {
	case err == nil:
	case errors.Is(err, infra.ErrNoSuchJob):
		httpx.ErrorJSON(w, "no record found", errors.New("no rows affected"))
		return
	default:
		fail(w, "update fail", err)
		return
	}

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"message": "Backup job updated successfully",
	})

	// After successful update, sync crontab
	app.SyncCrontab(r.Context(), "BackupUpdateHandler")
}

// BackupDeleteHandler DELETE /api/backup/{id}
func BackupDeleteHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	logrus.Infof("[Backup] BackupDeleteHandler => taskID=%s", id)

	switch err := app.DeleteJob(id); {
	case err == nil:
	case errors.Is(err, infra.ErrNoSuchJob):
		httpx.WriteJSON(w, map[string]interface{}{
			"success": false,
			"message": "Backup job deletion failed: no record found",
		})
		return
	default:
		fail(w, "delete fail", err)
		return
	}

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"message": "Backup job deleted successfully",
	})

	// After successful deletion, sync crontab
	app.SyncCrontab(r.Context(), "BackupDeleteHandler")
}

// BackupPauseHandler PUT /api/backup/{id}/pause
func BackupPauseHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	logrus.Infof("[Backup] BackupPauseHandler => taskID=%s", id)

	if err := app.PauseJob(id); err != nil {
		httpx.ErrorJSON(w, "pause fail", err)
		return
	}

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"message": "Backup job paused successfully",
	})

	// After successful pause, sync crontab
	app.SyncCrontab(r.Context(), "BackupPauseHandler")
}

// BackupResumeHandler PUT /api/backup/{id}/resume
func BackupResumeHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	logrus.Infof("[Backup] BackupResumeHandler => taskID=%s", id)

	if err := app.ResumeJob(id); err != nil {
		httpx.ErrorJSON(w, "resume fail", err)
		return
	}

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"message": "Backup job resumed successfully",
	})

	// After successful resume, sync crontab
	app.SyncCrontab(r.Context(), "BackupResumeHandler")
}

// BackupRunHandler POST /api/backup/{id}/run
//
// It stamps the last backup time and answers that the job started. It does not
// start one; BackupExecuteHandler does.
func BackupRunHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	logrus.Infof("[Backup] BackupRunHandler => taskID=%s", id)

	switch err := app.MarkRun(id); {
	case err == nil:
	case errors.Is(err, app.ErrJobNotFound):
		httpx.ErrorJSON(w, "backup task not found", errors.New("no such task"))
		return
	default:
		fail(w, "update fail", err)
		return
	}

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"message": "Backup job started successfully",
	})
}

// BackupExecuteHandler POST /api/backup/execute/{id}
//
// It builds an executor and runs the job in the background, returning a task id
// to poll.
func BackupExecuteHandler(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	taskID := app.SubmitRun(id)

	// Return immediate response with task ID
	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"taskId":  taskID,
		"message": "Backup task submitted and running in background",
	})
}

// BackupStatusHandler GET /api/backup/status/{taskId}
func BackupStatusHandler(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskId")
	if taskID == "" {
		http.Error(w, "Task ID is required", http.StatusBadRequest)
		return
	}

	status, exists := app.LookupRun(taskID)
	if !exists {
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	httpx.WriteJSON(w, status)
}
