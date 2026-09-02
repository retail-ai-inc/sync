package discovery

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// informationSchema stands in for MySQL's. The query is the same; SQLite has no
// information_schema of its own, so the table is created with the two columns
// the query reads.
func informationSchema(t *testing.T, rows ...[3]string) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "schema.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(`CREATE TABLE information_schema_tables (
		table_schema TEXT, table_name TEXT, table_type TEXT)`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	for _, r := range rows {
		if _, err := db.Exec(
			`INSERT INTO information_schema_tables VALUES (?, ?, ?)`, r[0], r[1], r[2]); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	return db
}

// schemaQuerier rewrites the information_schema.tables reference to the table
// the fixture created, so the production query text is exercised unchanged apart
// from that one name.
type schemaQuerier struct{ db *sql.DB }

func (q schemaQuerier) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return q.db.QueryContext(ctx,
		strings.ReplaceAll(query, "information_schema.tables", "information_schema_tables"), args...)
}

// ------------------------------------------------------------------ MySQL

func TestTheBaseTablesOfOneDatabaseAreListed(t *testing.T) {
	db := informationSchema(t,
		[3]string{"shop", "orders", "BASE TABLE"},
		[3]string{"shop", "customers", "BASE TABLE"},
		[3]string{"other", "unrelated", "BASE TABLE"},
	)

	got, err := MySQLTables(context.Background(), schemaQuerier{db}, "shop")
	if err != nil {
		t.Fatalf("MySQLTables: %v", err)
	}
	if len(got) != 2 || got[0] != "customers" || got[1] != "orders" {
		t.Errorf("tables = %v, want customers and orders in order", got)
	}
}

// TestAViewIsNotReplicated records why the type is filtered: a view has no rows
// of its own and the binlog carries no events for it, so replicating one would
// produce an empty table on the target that looks like data loss.
func TestAViewIsNotReplicated(t *testing.T) {
	db := informationSchema(t,
		[3]string{"shop", "orders", "BASE TABLE"},
		[3]string{"shop", "daily_totals", "VIEW"},
	)

	got, err := MySQLTables(context.Background(), schemaQuerier{db}, "shop")
	if err != nil {
		t.Fatalf("MySQLTables: %v", err)
	}
	if len(got) != 1 || got[0] != "orders" {
		t.Errorf("tables = %v, want only the base table", got)
	}
}

// TestTheDirectionLockIsNotReplicated is the reason internal names are filtered
// at all: copying the lock table would tell the target it is a source, which is
// exactly the state the lock exists to detect.
func TestTheDirectionLockIsNotReplicated(t *testing.T) {
	db := informationSchema(t,
		[3]string{"shop", "orders", "BASE TABLE"},
		[3]string{"shop", "_sync_direction_lock", "BASE TABLE"},
	)

	got, err := MySQLTables(context.Background(), schemaQuerier{db}, "shop")
	if err != nil {
		t.Fatalf("MySQLTables: %v", err)
	}
	for _, name := range got {
		if name == "_sync_direction_lock" {
			t.Error("the direction lock would be replicated")
		}
	}
}

func TestADatabaseWithNoTables(t *testing.T) {
	got, err := MySQLTables(context.Background(), schemaQuerier{informationSchema(t)}, "shop")
	if err != nil {
		t.Fatalf("MySQLTables: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("tables = %v, want none", got)
	}
}

func TestAnUnreadableSchemaIsReported(t *testing.T) {
	db := informationSchema(t)
	_ = db.Close()

	if _, err := MySQLTables(context.Background(), schemaQuerier{db}, "shop"); err == nil {
		t.Error("MySQLTables on a closed database returned no error")
	}
}

// ----------------------------------------------------------------- naming

func TestTheSyncersOwnNamesAreRecognised(t *testing.T) {
	for name, want := range map[string]bool{
		"_sync_direction_lock": true,
		"_sync_anything":       true,
		"system.profile":       true,
		"system.views":         true,
		"orders":               false,
		"sync_tasks":           false,
		"customers":            false,
		"":                     false,
	} {
		t.Run(name, func(t *testing.T) {
			if got := IsInternal(name); got != want {
				t.Errorf("IsInternal(%q) = %v, want %v", name, got, want)
			}
		})
	}
}

// ------------------------------------------------------------------ added

// TestOnlyTheNewNamesAreReported is what turns a periodic rescan into "start
// replicating the collections that have appeared" rather than "start them all
// again".
func TestOnlyTheNewNamesAreReported(t *testing.T) {
	known := map[string]bool{"orders": true, "customers": true}

	got := Added(known, []string{"orders", "customers", "refunds", "payouts"})

	if len(got) != 2 || got[0] != "refunds" || got[1] != "payouts" {
		t.Errorf("added = %v, want refunds and payouts", got)
	}
}

func TestNothingNewIsReportedTwice(t *testing.T) {
	known := map[string]bool{"orders": true}

	if got := Added(known, []string{"orders"}); len(got) != 0 {
		t.Errorf("added = %v, want none", got)
	}
	if got := Added(map[string]bool{}, nil); len(got) != 0 {
		t.Errorf("added = %v for an empty source, want none", got)
	}
}

// The task replicates what it names, which is the point of naming — but a
// table added at the source afterwards is then absent from the replica, and a
// failover is a bad time to discover that.
func TestUnlistedReportsWhatATaskDoesNotCarry(t *testing.T) {
	listed := map[string]bool{"orders": true, "payments": true}
	reported := map[string]bool{}

	got := Unlisted(listed, reported, []string{"orders", "payments", "refunds", "ledger"})

	if len(got) != 2 || got[0] != "refunds" || got[1] != "ledger" {
		t.Errorf("Unlisted = %v, want the two tables the task does not name", got)
	}
}

// TestUnlistedReportsEachNameOnce keeps a five-minute scan from logging the same
// warning for ever, which is how a warning stops being read.
func TestUnlistedReportsEachNameOnce(t *testing.T) {
	reported := map[string]bool{}
	names := []string{"refunds"}

	first := Unlisted(map[string]bool{}, reported, names)
	second := Unlisted(map[string]bool{}, reported, names)

	if len(first) != 1 {
		t.Fatalf("the first scan reported %v", first)
	}
	if len(second) != 0 {
		t.Errorf("the second scan reported %v again", second)
	}
}

// TestUnlistedIgnoresTheSyncersOwnTables matters because reporting the
// checkpoint and the direction lock as unreplicated is noise, and noise trains
// people to ignore the warning that is not.
func TestUnlistedIgnoresTheSyncersOwnTables(t *testing.T) {
	got := Unlisted(map[string]bool{}, map[string]bool{},
		[]string{"_sync_checkpoint", "_sync_direction_lock", "orders"})

	if len(got) != 1 || got[0] != "orders" {
		t.Errorf("Unlisted = %v, want only the real table", got)
	}
}

// TestUnlistedFoldsCase covers a task naming a table in different case from the
// server, which MySQL allows and which would otherwise report a table the task
// does carry.
func TestUnlistedFoldsCase(t *testing.T) {
	got := Unlisted(map[string]bool{"orders": true}, map[string]bool{},
		[]string{"Orders", "ORDERS"})

	if len(got) != 0 {
		t.Errorf("Unlisted = %v, want nothing: the task names that table", got)
	}
}

func TestUnlistedOnAnEmptySource(t *testing.T) {
	if got := Unlisted(map[string]bool{"orders": true}, map[string]bool{}, nil); got != nil {
		t.Errorf("Unlisted = %v", got)
	}
}
