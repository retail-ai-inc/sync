//go:build integration

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
	"github.com/retail-ai-inc/sync/test/harness"
)

func mysqlDSN(endpoint, database string) string {
	return fmt.Sprintf("root:root@tcp(%s)/%s", endpoint, database)
}

// seedTable creates a table with a known number of rows and removes it again,
// so the counts under test are the only thing the numbers can come from.
func seedTable(t *testing.T, dataSource, table string, rows int) {
	t.Helper()
	db, err := sql.Open("mysql", dataSource)
	if err != nil {
		t.Fatalf("open %s: %v", dataSource, err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec("DROP TABLE IF EXISTS `" + table + "`")
		_ = db.Close()
	})
	if _, err := db.Exec("DROP TABLE IF EXISTS `" + table + "`"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE `" + table + "` (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < rows; i++ {
		if _, err := db.Exec("INSERT INTO `"+table+"` (id) VALUES (?)", i); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
}

func uniqueTable(t *testing.T, prefix string) string {
	t.Helper()
	return strings.ReplaceAll(harness.UniqueName(prefix), "-", "_")
}

func TestRowCountsCountsBothSidesOfEveryMapping(t *testing.T) {
	table := uniqueTable(t, "counted")
	other := uniqueTable(t, "matching")
	sourceDSN := mysqlDSN(harness.MySQLSource, "source_db")
	targetDSN := mysqlDSN(harness.MySQLTarget, "target_db")

	seedTable(t, sourceDSN, table, 6)
	seedTable(t, targetDSN, table, 4)
	seedTable(t, sourceDSN, other, 2)
	seedTable(t, targetDSN, other, 2)

	counts, err := RowCounts(context.Background(), config.SyncConfig{
		ID: 9301, Type: "mysql",
		SourceConnection: sourceDSN, TargetConnection: targetDSN,
		Mappings: []config.DatabaseMapping{{
			SourceDatabase: "source_db", TargetDatabase: "target_db",
			Tables: []config.TableMapping{
				{SourceTable: table, TargetTable: table},
				{SourceTable: other},
			},
		}},
	})
	if err != nil {
		t.Fatalf("RowCounts: %v", err)
	}
	if counts.Discovered {
		t.Error("a task that names its tables was reported as discovered")
	}

	got := map[string][2]int64{}
	for _, object := range counts.Objects {
		got[object.Source] = [2]int64{object.SourceRows, object.TargetRows}
	}
	if got[table] != [2]int64{6, 4} {
		t.Errorf("%s counted %v, want [6 4]", table, got[table])
	}
	if got[other] != [2]int64{2, 2} {
		t.Errorf("%s counted %v, want [2 2]", other, got[other])
	}
}

func TestRowCountsDiscoversTheWholeDatabaseWhenNothingIsNamed(t *testing.T) {
	table := uniqueTable(t, "discovered")
	sourceDSN := mysqlDSN(harness.MySQLSource, "source_db")
	targetDSN := mysqlDSN(harness.MySQLTarget, "target_db")
	seedTable(t, sourceDSN, table, 5)

	counts, err := RowCounts(context.Background(), config.SyncConfig{
		ID: 9302, Type: "mysql",
		SourceConnection: sourceDSN, TargetConnection: targetDSN,
	})
	if err != nil {
		t.Fatalf("RowCounts: %v", err)
	}
	if !counts.Discovered {
		t.Error("a whole-database task was not reported as discovered")
	}

	var found bool
	for _, object := range counts.Objects {
		if object.Source != table {
			continue
		}
		found = true
		if object.SourceRows != 5 {
			t.Errorf("%s counted %d on the source, want 5", table, object.SourceRows)
		}
		// The target does not have it. A note rather than a count is what says
		// the object is missing instead of empty.
		if object.TargetRows != -1 || object.Note == "" {
			t.Errorf("a table the target lacks read back as %+v", object)
		}
	}
	if !found {
		t.Errorf("%s was not discovered on the source", table)
	}
}

func TestRowCountsWillNotConnectToNowhere(t *testing.T) {
	_, err := RowCounts(context.Background(), config.SyncConfig{
		ID: 9303, Type: "mysql",
		SourceConnection: "root:root@tcp(127.0.0.1:1)/nothing",
		TargetConnection: "root:root@tcp(127.0.0.1:1)/nothing",
	})
	if err == nil {
		t.Error("RowCounts reported on a source that does not exist")
	}
}

func TestProgressComparesTheSourceAgainstWhatWasStored(t *testing.T) {
	report, err := Progress(context.Background(), config.SyncConfig{
		ID: 9304, Type: "mysql",
		SourceConnection: mysqlDSN(harness.MySQLSource, "source_db"),
		TargetConnection: mysqlDSN(harness.MySQLTarget, "target_db"),
	})
	if err != nil {
		t.Fatalf("Progress: %v", err)
	}
	if report.Engine != "mysql" {
		t.Errorf("engine = %q, want mysql", report.Engine)
	}
	if len(report.Shards) != 1 {
		t.Fatalf("a single server reported %d shards, want 1", len(report.Shards))
	}
	// Nothing has been applied for this task id, so it must say so rather than
	// compare against a position it does not have.
	if shard := report.Shards[0]; shard.Comparable {
		t.Errorf("a task that never ran was reported as comparable: %+v", shard)
	} else if shard.Note == "" {
		t.Error("a shard that cannot be compared gave no reason")
	}
}

func TestPurgingATaskLeavesTheOthersAlone(t *testing.T) {
	targetDSN := mysqlDSN(harness.MySQLTarget, "target_db")
	ctx := context.Background()

	db, err := sql.Open("mysql", targetDSN)
	if err != nil {
		t.Fatalf("open the target: %v", err)
	}
	defer db.Close()

	const mine, theirs = 9305, 9306
	stores := map[int]*checkpoint.SQLStore{}
	for _, taskID := range []int{mine, theirs} {
		store := &checkpoint.SQLStore{DB: db, Schema: "target_db", TaskID: taskID}
		if err := store.Save(ctx, "", `{"file":"binlog.000001","pos":4}`); err != nil {
			t.Fatalf("store a position for %d: %v", taskID, err)
		}
		stores[taskID] = store
		t.Cleanup(func() { _ = store.Purge(context.Background()) })
	}

	if err := PurgeCheckpoints(ctx, config.SyncConfig{
		ID: mine, Type: "mysql", TargetConnection: targetDSN,
	}); err != nil {
		t.Fatalf("PurgeCheckpoints: %v", err)
	}

	if payload, err := stores[mine].Load(ctx, ""); err != nil || payload != "" {
		t.Errorf("the purged task still has a stored position (%q, %v)", payload, err)
	}
	if payload, err := stores[theirs].Load(ctx, ""); err != nil || payload == "" {
		t.Errorf("another task's position was purged with this one (%q, %v)", payload, err)
	}
}

// Best effort by design: a target that cannot be reached must not make a task
// undeletable, so this reports success. Pinned rather than endorsed -- it means
// a purge against a target that is merely down leaves the positions in place,
// which is the state the purge exists to prevent.
func TestPurgingATargetThatIsNotThereReportsSuccess(t *testing.T) {
	err := PurgeCheckpoints(context.Background(), config.SyncConfig{
		ID: 9307, Type: "mysql",
		TargetConnection: "root:root@tcp(127.0.0.1:1)/nothing",
	})
	if err != nil {
		t.Errorf("purging an unreachable target reported %v, which would make the "+
			"task undeletable", err)
	}
}
