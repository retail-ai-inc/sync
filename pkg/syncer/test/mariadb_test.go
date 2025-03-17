package test

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/retail-ai-inc/sync/pkg/config"
	"strings"
	"testing"
	"time"
)

func testTC05MariaDBSync(t *testing.T, syncConfigs []config.SyncConfig) {
	var mariadbTasks []config.SyncConfig
	for _, syncConfig := range syncConfigs {
		if strings.ToLower(syncConfig.Type) == "mariadb" && syncConfig.Enable {
			mariadbTasks = append(mariadbTasks, syncConfig)
		}
	}
	if len(mariadbTasks) == 0 {
		t.Skip("[TC05] no enabled MariaDB tasks found.")
	}

	syncConfig := mariadbTasks[0]
	srcDSN := syncConfig.SourceConnection
	tgtDSN := syncConfig.TargetConnection

	if len(syncConfig.Mappings) == 0 || len(syncConfig.Mappings[0].Tables) == 0 {
		t.Skip("[TC05] no table mapping.")
	}
	sourceTable := syncConfig.Mappings[0].Tables[0].SourceTable
	targetTable := syncConfig.Mappings[0].Tables[0].TargetTable

	srcDB, err := sql.Open("mysql", srcDSN)
	if err != nil {
		t.Fatalf("[TC05] open src(mariadb) fail: %v", err)
	}
	defer srcDB.Close()

	tgtDB, err := sql.Open("mysql", tgtDSN)
	if err != nil {
		t.Fatalf("[TC05] open tgt(mariadb) fail: %v", err)
	}
	defer tgtDB.Close()

	time.Sleep(1 * time.Second)

	// Insert rows
	for i := 1; i <= 1; i++ {
		id := getNextID(srcDB, sourceTable)
		q := fmt.Sprintf("INSERT INTO %s (id, name, email) VALUES(%d, 'User_%d', 'user%d@mail.com')", sourceTable, id, id, id)
		mustExec(t, srcDB, q)
	}
	time.Sleep(2 * time.Second)

	// Validate data consistency
	compareDataConsistency(t, srcDB, tgtDB, sourceTable, targetTable, syncConfig.Mappings)

	mustExec(t, srcDB, fmt.Sprintf("INSERT INTO %s (id, name, email) VALUES(%d, 'Extra', 'extra@mail.com')", sourceTable, getNextID(srcDB, sourceTable)))
	time.Sleep(2 * time.Second)
	mustExec(t, srcDB, fmt.Sprintf("UPDATE %s SET name='UpdatedMaria' WHERE name='Extra'", sourceTable))
	time.Sleep(2 * time.Second)
	mustExec(t, srcDB, fmt.Sprintf("DELETE FROM %s WHERE name='UpdatedMaria'", sourceTable)) // Remove a row
	time.Sleep(2 * time.Second)

	// Final data consistency check
	compareDataConsistency(t, srcDB, tgtDB, sourceTable, targetTable, syncConfig.Mappings)
}
