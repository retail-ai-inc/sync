package api

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/retail-ai-inc/sync/pkg/backup"
	"github.com/sirupsen/logrus"
)

// BackupExecuteHandler Execute the backup task with the specified ID
// POST /api/backup/execute/{id}
func BackupExecuteHandler(w http.ResponseWriter, r *http.Request) {
	// Get ID parameter
	idStr := chi.URLParam(r, "id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	logrus.Infof("[Backup] Executing backup task: %d", id)

	// Get database connection
	db, err := openLocalDB()
	if err != nil {
		errorJSON(w, "Failed to open database", err)
		return
	}
	defer db.Close()

	// Create executor and perform backup
	executor := backup.NewBackupExecutor(db)
	if err := executor.Execute(r.Context(), id); err != nil {
		logrus.Errorf("[BackupExecutor] Failed to execute backup task %d: %v", id, err)
		errorJSON(w, "Failed to execute backup", err)
		return
	}

	// Update last backup time
	nowStr := timeNowStr()
	_, err = db.Exec("UPDATE backup_tasks SET last_backup_time=? WHERE id=?", nowStr, id)
	if err != nil {
		logrus.Warnf("[BackupExecutor] Failed to update last_backup_time: %v", err)
		// Continue execution, don't interrupt response
	}

	// Return success response
	writeJSON(w, map[string]interface{}{
		"success": true,
		"message": "Backup executed successfully",
	})
}
