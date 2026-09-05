//go:build integration

package app

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/test/harness"
)

// captureAppLog returns a logger writing into a buffer, which is where these
// report what they could not do.
func captureAppLog() (*logrus.Logger, *bytes.Buffer) {
	var out bytes.Buffer
	logger := logrus.New()
	logger.SetOutput(&out)
	return logger, &out
}

func consistencySourceDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql",
		fmt.Sprintf("root:root@tcp(%s)/source_db", harness.MySQLSource))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func makeTable(t *testing.T, db *sql.DB, prefix string) string {
	t.Helper()
	name := strings.ReplaceAll(harness.UniqueName(prefix), "-", "_")
	if _, err := db.Exec("CREATE TABLE `" + name +
		"` (id INT, part INT, value TEXT, PRIMARY KEY (id, part))"); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS `" + name + "`") })
	return name
}

func TestATaskThatNamesNoTablesComparesWhatTheSourceHolds(t *testing.T) {
	db := consistencySourceDB(t)
	table := makeTable(t, db, "discovered")
	logger, _ := captureAppLog()

	pairs := sqlTablePairs(context.Background(),
		config.SyncConfig{ID: 9401}, db, "source_db", logger)

	var found bool
	for _, pair := range pairs {
		if pair.source == table {
			found = true
			// Discovered tables are compared against the same name, because
			// nothing said otherwise.
			if pair.target != table {
				t.Errorf("%s is compared against %q", table, pair.target)
			}
		}
	}
	if !found {
		t.Errorf("%s was not discovered", table)
	}
}

func TestATaskThatNamesItsTablesIsNotDiscovered(t *testing.T) {
	db := consistencySourceDB(t)
	logger, _ := captureAppLog()

	pairs := sqlTablePairs(context.Background(), config.SyncConfig{
		ID: 9402,
		Mappings: []config.DatabaseMapping{{
			Tables: []config.TableMapping{{SourceTable: "orders", TargetTable: "orders_copy"}},
		}},
	}, db, "source_db", logger)

	if len(pairs) != 1 || pairs[0].source != "orders" || pairs[0].target != "orders_copy" {
		t.Errorf("a task that names one table produced %+v", pairs)
	}
}

func TestNothingCanBeDiscoveredThroughADatabaseThatIsNotThere(t *testing.T) {
	db, err := sql.Open("mysql", "root:root@tcp(127.0.0.1:1)/nothing")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	logger, out := captureAppLog()

	if pairs := sqlTablePairs(context.Background(),
		config.SyncConfig{ID: 9403}, db, "nothing", logger); pairs != nil {
		t.Errorf("a source that cannot be read produced %+v", pairs)
	}
	// It reports rather than comparing nothing in silence.
	if !strings.Contains(out.String(), "9403") {
		t.Error("the failure was not logged against the task")
	}
}

func TestAPrimaryKeyIsReturnedWhole(t *testing.T) {
	db := consistencySourceDB(t)
	table := makeTable(t, db, "composite")

	columns, err := primaryKey(context.Background(), db, "source_db", table)
	if err != nil {
		t.Fatalf("primaryKey: %v", err)
	}
	// In order, and both parts: comparing on part of a composite key would
	// report every row sharing the first column as a duplicate.
	if len(columns) != 2 || columns[0] != "id" || columns[1] != "part" {
		t.Errorf("primaryKey = %v, want [id part]", columns)
	}

	// A table whose rows cannot be addressed is refused rather than compared on
	// nothing, which would report every row as a duplicate of every other.
	if _, err := primaryKey(context.Background(), db, "source_db", "no_such_table"); err == nil {
		t.Error("a table with no primary key was accepted for comparison")
	}
}
