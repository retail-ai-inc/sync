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

func testTC04MySQLSync(t *testing.T, syncConfigs []config.SyncConfig) {
	var mysTasks []config.SyncConfig
	for _, syncConfig := range syncConfigs {
		if strings.ToLower(syncConfig.Type) == "mysql" && syncConfig.Enable {
			mysTasks = append(mysTasks, syncConfig)
		}
	}
	if len(mysTasks) == 0 {
		t.Skip("[TC04] no enabled MySQL tasks found.")
	}

	syncConfig := mysTasks[0]
	srcDSN := syncConfig.SourceConnection
	tgtDSN := syncConfig.TargetConnection
	if len(syncConfig.Mappings) == 0 || len(syncConfig.Mappings[0].Tables) == 0 {
		t.Skip("[TC04] no table mapping.")
	}
	sourceTable := syncConfig.Mappings[0].Tables[0].SourceTable
	targetTable := syncConfig.Mappings[0].Tables[0].TargetTable

	srcDB, err := sql.Open("mysql", srcDSN)
	if err != nil {
		t.Fatalf("[TC04] open src fail: %v", err)
	}
	defer srcDB.Close()

	tgtDB, err := sql.Open("mysql", tgtDSN)
	if err != nil {
		t.Fatalf("[TC04] open tgt fail: %v", err)
	}
	defer tgtDB.Close()

	time.Sleep(1 * time.Second)
	// Insert data into the source table dynamically based on existing rows
	for i := 1; i <= 5; i++ {
		id := getNextID(srcDB, sourceTable)
		q := fmt.Sprintf("INSERT INTO %s (id, name, email) VALUES(%d, 'User_%d', 'user%d@mail.com')", sourceTable, id, id, id)
		mustExec(t, srcDB, q)
	}
	time.Sleep(5 * time.Second)

	// Validate the data consistency by comparing rows between source and target
	compareDataConsistency(t, srcDB, tgtDB, sourceTable, targetTable)

	// Update, insert, and delete operations
	mustExec(t, srcDB, fmt.Sprintf("INSERT INTO %s (id, name, email) VALUES(%d, 'Extra', 'extra@mail.com')", sourceTable, getNextID(srcDB, sourceTable)))
	time.Sleep(2 * time.Second)
	mustExec(t, srcDB, fmt.Sprintf("UPDATE %s SET name='UpdatedName' WHERE name='Extra'", sourceTable))
	time.Sleep(2 * time.Second)
	mustExec(t, srcDB, fmt.Sprintf("DELETE FROM %s WHERE name='UpdatedName'", sourceTable)) // Remove a row
	time.Sleep(2 * time.Second)

	// Validate the final data consistency
	compareDataConsistency(t, srcDB, tgtDB, sourceTable, targetTable)
}
