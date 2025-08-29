package test

import (
	"context"
	"github.com/retail-ai-inc/sync/pkg/config"
	"testing"
	"time"
)

func testTC23ConfigurationChanges(t *testing.T, syncConfigs []config.SyncConfig) {
	// Dynamic configuration update test
	// t.Run("DynamicConfigUpdate", func(t *testing.T) {
	// 	syncConfig := syncConfigs[0]
	// 	// Wait for syncer to start
	// 	time.Sleep(2 * time.Second)

	// 	// Backup original config
	// 	oldConfig := syncConfig

	// 	// Modify config
	// 	newConfig := syncConfig
	// 	newConfig.Mappings = append(newConfig.Mappings, config.DatabaseMapping{
	// 		SourceDatabase: "new_db",
	// 		TargetDatabase: "new_db",
	// 		Tables: []config.TableMapping{
	// 			{SourceTable: "new_table", TargetTable: "new_table"},
	// 		},
	// 	})

	// 	// Update config
	// 	updateSyncConfig(t, newConfig)
	// 	// Wait for config to take effect
	// 	time.Sleep(5 * time.Second)

	// 	validateConfigUpdate(t, newConfig)

	// 	// Restore original config
	// 	updateSyncConfig(t, oldConfig)
	// })

	t.Run("SchemaChange", func(t *testing.T) {
		if len(syncConfigs) == 0 {
			t.Skip("No sync configurations available for schema change test")
			return
		}
		syncConfig := syncConfigs[0]
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		syncer := createSyncer(syncConfig)
		go syncer.Start(ctx)

		alterSourceSchema(t, syncConfig)

		validateSchemaSync(t, syncConfig)
	})
}

func updateSyncConfig(t *testing.T, newConfig config.SyncConfig) {
	db, err := openLocalDB()
	if err != nil {
		t.Fatalf("open db fail: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		UPDATE sync_tasks 
		SET config_json = ?, 
		    last_update_time = CURRENT_TIMESTAMP 
		WHERE id = ?`,
		newConfig.ID)

	if err != nil {
		t.Fatalf("update config fail: %v", err)
	}
}

func alterSourceSchema(t *testing.T, syncConfig config.SyncConfig) {
}

func validateSchemaSync(t *testing.T, syncConfig config.SyncConfig) {
}
