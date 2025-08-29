package api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/retail-ai-inc/sync/pkg/backup"
	"github.com/sirupsen/logrus"
)

// BackupTaskStatus represents the status of a background backup task
type BackupTaskStatus struct {
	TaskID      string     `json:"taskId"`
	BackupID    int        `json:"backupId"`
	Status      string     `json:"status"` // pending, running, completed, failed
	Message     string     `json:"message"`
	CreatedAt   time.Time  `json:"createdAt"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
	Error       string     `json:"error,omitempty"`
}

// In-memory task status storage
var (
	taskStatusMap   = make(map[string]*BackupTaskStatus)
	taskStatusMutex sync.RWMutex
)

// setTaskStatus sets the task status in memory
func setTaskStatus(taskID string, status *BackupTaskStatus) {
	taskStatusMutex.Lock()
	defer taskStatusMutex.Unlock()
	taskStatusMap[taskID] = status
}

// getTaskStatus retrieves the task status from memory
func getTaskStatus(taskID string) (*BackupTaskStatus, bool) {
	taskStatusMutex.RLock()
	defer taskStatusMutex.RUnlock()
	status, exists := taskStatusMap[taskID]
	return status, exists
}

// updateBackupTaskStatus updates the backup task status in memory
func updateBackupTaskStatus(taskID string, status string, message string, err error) {
	taskStatusMutex.Lock()
	defer taskStatusMutex.Unlock()
	if task, exists := taskStatusMap[taskID]; exists {
		task.Status = status
		task.Message = message
		if err != nil {
			task.Error = err.Error()
		}
		if status == "completed" || status == "failed" {
			now := time.Now()
			task.CompletedAt = &now
		}
	}
}

// BackupExecuteHandler Execute the backup task with the specified ID (async)
// POST /api/backup/execute/{id}
func BackupExecuteHandler(w http.ResponseWriter, r *http.Request) {
	// Get ID parameter
	idStr := chi.URLParam(r, "id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	// Generate unique task ID
	taskID := fmt.Sprintf("backup_%d_%d", id, time.Now().Unix())

	logrus.Infof("[Backup] Submitting backup task: %d with taskID: %s", id, taskID)

	// Create task status
	taskStatus := &BackupTaskStatus{
		TaskID:    taskID,
		BackupID:  id,
		Status:    "pending",
		Message:   "Backup task submitted",
		CreatedAt: time.Now(),
	}
	setTaskStatus(taskID, taskStatus)

	// Start background backup
	go func() {
		// Update status to running
		updateBackupTaskStatus(taskID, "running", "Backup execution started", nil)

		// Get database connection
		db, err := openLocalDB()
		if err != nil {
			logrus.Errorf("[BackupExecutor] Failed to open database for task %s: %v", taskID, err)
			updateBackupTaskStatus(taskID, "failed", "Failed to open database", err)
			return
		}
		defer db.Close()

		// Create executor and perform backup
		executor := backup.NewBackupExecutor(db)

		// Use background context with 2-hour timeout
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
		defer cancel()

		if err := executor.Execute(ctx, id); err != nil {
			logrus.Errorf("[BackupExecutor] Failed to execute backup task %d: %v", id, err)
			updateBackupTaskStatus(taskID, "failed", "Backup execution failed", err)
			return
		}

		// Update last backup time
		nowStr := timeNowStr()
		_, err = db.Exec("UPDATE backup_tasks SET last_backup_time=? WHERE id=?", nowStr, id)
		if err != nil {
			logrus.Warnf("[BackupExecutor] Failed to update last_backup_time: %v", err)
			// Continue execution, don't interrupt response
		}

		// Update task status to completed
		updateBackupTaskStatus(taskID, "completed", "Backup executed successfully", nil)
		logrus.Debugf("[BackupExecutor] Background backup task %s completed successfully", taskID)
	}()

	// Return immediate response with task ID
	writeJSON(w, map[string]interface{}{
		"success": true,
		"taskId":  taskID,
		"message": "Backup task submitted and running in background",
	})
}

// BackupStatusHandler Get the status of a background backup task
// GET /api/backup/status/{taskId}
func BackupStatusHandler(w http.ResponseWriter, r *http.Request) {
	// Get task ID parameter
	taskID := chi.URLParam(r, "taskId")
	if taskID == "" {
		http.Error(w, "Task ID is required", http.StatusBadRequest)
		return
	}

	// Get task status
	status, exists := getTaskStatus(taskID)
	if !exists {
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	// Return task status
	writeJSON(w, status)
}
