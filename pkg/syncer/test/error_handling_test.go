package test

import (
	"github.com/retail-ai-inc/sync/pkg/config"
	"testing"
	"time"
)

func testTC20ErrorHandling(t *testing.T, syncConfigs []config.SyncConfig) {
	// Network disconnection test
	// t.Run("NetworkDisconnection", func(t *testing.T) {
	// 	syncConfig := syncConfigs[0]
	// 	// Wait for syncer to start
	// 	time.Sleep(2 * time.Second)

	// 	oldConn := syncConfig.SourceConnection
	// 	syncConfig.SourceConnection = "invalid_connection_string"
	// 	// Update config in database
	// 	updateSyncConfig(t, syncConfig)
	// 	time.Sleep(3 * time.Second)

	// 	validateErrorLogs(t, syncConfig.ID, "connection refused")
	// 	// Restore connection
	// 	syncConfig.SourceConnection = oldConn
	// 	updateSyncConfig(t, syncConfig)
	// })

	// Permission error test
	// t.Run("PermissionDenied", func(t *testing.T) {
	//     ...
	// })

	// Auto reconnect test
	t.Run("AutoReconnect", func(t *testing.T) {
		if len(syncConfigs) == 0 {
			t.Skip("No sync configurations available for auto reconnect test")
			return
		}
		syncConfig := syncConfigs[0]
		// Simulate database restart
		restartDatabase(t, syncConfig)
		time.Sleep(10 * time.Second)

		// Validate successful reconnection
		validateSyncerStatus(t, syncConfig)
	})
}

func validateErrorLogs(t *testing.T, taskID int, expectedError string) {
	db, err := openLocalDB()
	if err != nil {
		t.Fatalf("open db fail: %v", err)
	}
	defer db.Close()

	var count int
	err = db.QueryRow(`
		SELECT COUNT(*) FROM sync_log 
		WHERE sync_task_id = ? 
		AND level = 'error' 
		AND message LIKE ?`,
		taskID, "%"+expectedError+"%").Scan(&count)

	if err != nil {
		t.Fatalf("query error logs fail: %v", err)
	}

	if count == 0 {
		t.Errorf("expected error log with message containing '%s' not found", expectedError)
	}
}
