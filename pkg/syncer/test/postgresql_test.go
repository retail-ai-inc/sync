package test

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/retail-ai-inc/sync/pkg/config"
	"strings"
	"testing"
	"time"
)

func testTC06PostgreSQLSync(t *testing.T, syncConfigs []config.SyncConfig) {
	var pgTasks []config.SyncConfig
	for _, syncConfig := range syncConfigs {
		if strings.ToLower(syncConfig.Type) == "postgresql" && syncConfig.Enable {
			pgTasks = append(pgTasks, syncConfig)
		}
	}

	if len(pgTasks) == 0 {
		t.Skip("[TC06] no enabled PostgreSQL tasks found.")
	}

	syncConfig := pgTasks[0]
	srcDSN := syncConfig.SourceConnection
	tgtDSN := syncConfig.TargetConnection
	if len(syncConfig.Mappings) == 0 || len(syncConfig.Mappings[0].Tables) == 0 {
		t.Skip("[TC06] no table mapping.")
	}
	sourceTable := syncConfig.Mappings[0].Tables[0].SourceTable
	targetTable := syncConfig.Mappings[0].Tables[0].TargetTable

	srcDB, err := sql.Open("postgres", srcDSN)
	if err != nil {
		t.Fatalf("[TC06] open src(pg) fail: %v", err)
	}
	defer srcDB.Close()

	tgtDB, err := sql.Open("postgres", tgtDSN)
	if err != nil {
		t.Fatalf("[TC06] open tgt(pg) fail: %v", err)
	}
	defer tgtDB.Close()

	time.Sleep(1 * time.Second)

	// Insert rows into source
	for i := 1; i <= 4; i++ {
		id := getNextID(srcDB, sourceTable)
		q := fmt.Sprintf("INSERT INTO public.%s (id, name, email) VALUES (%d, 'PgUser_%d', 'pguser%d@mail.com')", sourceTable, id, id, id)
		mustExec(t, srcDB, q)
	}
	time.Sleep(5 * time.Second)

	// Validate data consistency
	compareDataConsistency(t, srcDB, tgtDB, sourceTable, targetTable)

	mustExec(t, srcDB, fmt.Sprintf("INSERT INTO public.%s (id, name, email) VALUES(%d, 'ExtraPg', 'extrapg@mail.com')", sourceTable, getNextID(srcDB, sourceTable)))
	time.Sleep(2 * time.Second)
	mustExec(t, srcDB, fmt.Sprintf("UPDATE public.%s SET name='UpdatedPg' WHERE name='ExtraPg'", sourceTable))
	time.Sleep(2 * time.Second)
	mustExec(t, srcDB, fmt.Sprintf("DELETE FROM public.%s WHERE name='UpdatedPg'", sourceTable)) // Remove a row
	time.Sleep(2 * time.Second)

	// Final data consistency check
	compareDataConsistency(t, srcDB, tgtDB, sourceTable, targetTable)
}
