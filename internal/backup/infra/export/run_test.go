package export

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// taskDB opens an empty SQLite database carrying the one table the executor
// reads. The executor is written against MySQL but only ever issues portable
// SQL here, so SQLite stands in for the control database.
func taskDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "tasks.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`CREATE TABLE backup_tasks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		enable INTEGER,
		last_update_time TEXT,
		last_backup_time TEXT,
		next_backup_time TEXT,
		config_json TEXT
	)`)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return db
}

func insertBackupTask(t *testing.T, db *sql.DB, enable int, cfg string) int {
	t.Helper()

	res, err := db.Exec(`INSERT INTO backup_tasks
		(enable, last_update_time, last_backup_time, next_backup_time, config_json)
		VALUES (?, '2026-08-21 10:00:00', '2026-08-20 03:00:00', '2026-08-22 03:00:00', ?)`,
		enable, cfg)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	id, _ := res.LastInsertId()
	return int(id)
}

func TestGetBackupTaskReadsEveryColumn(t *testing.T) {
	db := taskDB(t)
	id := insertBackupTask(t, db, 1, `{"name":"nightly"}`)

	task, err := NewBackupExecutor(db).getBackupTask(context.Background(), id)
	if err != nil {
		t.Fatalf("getBackupTask: %v", err)
	}

	if task.ID != id || task.Enable != 1 || task.ConfigJSON != `{"name":"nightly"}` {
		t.Errorf("task = %+v", task)
	}
	if got := task.LastUpdateTime.Format("2006-01-02 15:04:05"); got != "2026-08-21 10:00:00" {
		t.Errorf("LastUpdateTime = %q", got)
	}
	if got := task.NextBackupTime.Format("2006-01-02 15:04:05"); got != "2026-08-22 03:00:00" {
		t.Errorf("NextBackupTime = %q", got)
	}
}

// TestTheStoredTimestampsAreReadAsUTC records that the executor parses the
// stored strings with no zone, so they come back as UTC whatever zone the
// scheduler wrote them in. Every writer in the tree uses UTC, so the round trip
// holds — but nothing in the schema enforces it.
func TestTheStoredTimestampsAreReadAsUTC(t *testing.T) {
	db := taskDB(t)
	id := insertBackupTask(t, db, 1, `{}`)

	task, err := NewBackupExecutor(db).getBackupTask(context.Background(), id)
	if err != nil {
		t.Fatalf("getBackupTask: %v", err)
	}
	if loc := task.LastBackupTime.Location(); loc != nil && loc.String() != "UTC" {
		t.Errorf("location = %q, want UTC", loc)
	}
}

// TestAnUnparseableTimestampBecomesTheZeroTime records that a malformed stored
// timestamp is swallowed: the parse error is discarded and the field is left at
// the zero time, so the scheduler treats the task as never having run rather
// than reporting the corrupt row.
func TestAnUnparseableTimestampBecomesTheZeroTime(t *testing.T) {
	db := taskDB(t)
	res, err := db.Exec(`INSERT INTO backup_tasks
		(enable, last_update_time, last_backup_time, next_backup_time, config_json)
		VALUES (1, 'not a time', NULL, '', '{}')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	id, _ := res.LastInsertId()

	task, err := NewBackupExecutor(db).getBackupTask(context.Background(), int(id))
	if err != nil {
		t.Fatalf("getBackupTask returned an error for a corrupt timestamp: %v — the "+
			"failure appears to be reported now, so assert that instead", err)
	}
	if !task.LastUpdateTime.IsZero() || !task.NextBackupTime.IsZero() {
		t.Errorf("times = %v / %v, want the zero time",
			task.LastUpdateTime, task.NextBackupTime)
	}
}

func TestGetBackupTaskReportsAMissingRow(t *testing.T) {
	db := taskDB(t)

	if _, err := NewBackupExecutor(db).getBackupTask(context.Background(), 404); err == nil {
		t.Fatal("getBackupTask on a missing id returned no error")
	}
}

func TestExecuteReportsAMissingTask(t *testing.T) {
	db := taskDB(t)

	err := NewBackupExecutor(db).Execute(context.Background(), 404)
	if err == nil || !strings.Contains(err.Error(), "failed to get backup task") {
		t.Fatalf("err = %v, want a lookup failure", err)
	}
}

func TestExecuteReportsAnUnparseableConfiguration(t *testing.T) {
	db := taskDB(t)
	id := insertBackupTask(t, db, 1, `{"name":`)

	err := NewBackupExecutor(db).Execute(context.Background(), id)
	if err == nil || !strings.Contains(err.Error(), "failed to parse config") {
		t.Fatalf("err = %v, want a parse failure", err)
	}
}

// TestExecuteSucceedsWithNothingSelected records that a task naming no tables is
// not an error: the group map comes back empty, the loop body never runs and the
// call reports success. A misconfigured task therefore looks like a clean backup
// to the scheduler, which goes on to stamp last_backup_time.
func TestExecuteSucceedsWithNothingSelected(t *testing.T) {
	db := taskDB(t)
	id := insertBackupTask(t, db, 1, `{"name":"empty","sourceType":"mysql"}`)

	if err := NewBackupExecutor(db).Execute(context.Background(), id); err != nil {
		t.Fatalf("Execute with no tables: %v — an empty selection appears to be "+
			"rejected now, so assert that instead", err)
	}
}

// TestExecuteSwallowsAnUnsupportedEngine records that an unknown sourceType does
// not fail the call. The per-group error is logged and the loop continues, so
// Execute returns nil and the task is recorded as backed up.
func TestExecuteSwallowsAnUnsupportedEngine(t *testing.T) {
	db := taskDB(t)
	id := insertBackupTask(t, db, 1,
		`{"name":"x","sourceType":"cassandra","database":{"tables":["orders"]}}`)

	if err := NewBackupExecutor(db).Execute(context.Background(), id); err != nil {
		t.Fatalf("Execute with an unsupported engine returned %v; the per-group "+
			"failure appears to be propagated now, so assert that instead", err)
	}
}

// TestExecuteRejectsRegexModeForAnUnsupportedEngine records the one place where
// a bad engine is fatal: pattern expansion happens before the group loop, and
// its error is returned.
func TestExecuteRejectsRegexModeForAnUnsupportedEngine(t *testing.T) {
	db := taskDB(t)
	id := insertBackupTask(t, db, 1,
		`{"sourceType":"redis","tableSelectionMode":"regex","regexPattern":"^o"}`)

	err := NewBackupExecutor(db).Execute(context.Background(), id)
	if err == nil || !strings.Contains(err.Error(), "regex mode not supported") {
		t.Fatalf("err = %v, want a rejected engine", err)
	}
}

// TestExecuteRunsTheMySQLWorkflowAndCleansUp drives the whole path with stubbed
// binaries: expansion picks the one table, the MySQL branch dumps, compresses
// and uploads, and the temporary directory is removed afterwards.
func TestExecuteRunsTheMySQLWorkflowAndCleansUp(t *testing.T) {
	dir := stubPATH(t)
	stubBin(t, dir, "mysqldump", "touch \"$3\"", 0)
	linkRealBinary(t, dir, "zip")
	stubBin(t, dir, "gsutil", "", 0)

	before := tempDirCount(t)

	db := taskDB(t)
	id := insertBackupTask(t, db, 1, `{
		"name":"nightly","sourceType":"mysql","format":"sql","compressionType":"zip",
		"database":{"url":"127.0.0.1:3306","database":"shop","tables":["orders"]},
		"destination":{"gcsPath":"gs://bucket/x"}
	}`)

	if err := NewBackupExecutor(db).Execute(context.Background(), id); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	args := stubArgs(t, dir, "gsutil")
	if len(args) == 0 || !strings.Contains(strings.Join(args, " "), "gs://bucket/x/") {
		t.Errorf("gsutil args = %v, want the configured destination", args)
	}
	if after := tempDirCount(t); after != before {
		t.Errorf("temp directory count %d -> %d, want the working directory removed",
			before, after)
	}
}

// TestAFailedExportStillLeavesNoTempDirectory records that the failure branch
// removes its working directory too, so a repeatedly failing task does not fill
// the disk.
func TestAFailedExportStillLeavesNoTempDirectory(t *testing.T) {
	dir := stubPATH(t)
	stubBin(t, dir, "mysqldump", "", 1)

	before := tempDirCount(t)

	db := taskDB(t)
	id := insertBackupTask(t, db, 1, `{
		"name":"nightly","sourceType":"mysql","format":"sql",
		"database":{"url":"127.0.0.1:3306","database":"shop","tables":["orders"]},
		"destination":{"gcsPath":"gs://bucket/x"}
	}`)

	if err := NewBackupExecutor(db).Execute(context.Background(), id); err != nil {
		t.Fatalf("Execute returned %v for a failing dump; the failure appears to be "+
			"propagated now, so assert that instead", err)
	}
	if after := tempDirCount(t); after != before {
		t.Errorf("temp directory count %d -> %d after a failure", before, after)
	}
}

// tempDirCount counts the executor's working directories left under the system
// temporary directory.
func tempDirCount(t *testing.T) int {
	t.Helper()

	entries, err := os.ReadDir(os.TempDir())
	if err != nil {
		t.Fatalf("read temp dir: %v", err)
	}
	n := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "backup_") {
			n++
		}
	}
	return n
}

func groupKeys(groups map[string][]string) []string {
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func TestExpandAndGroupTablesTreatsUnrelatedTablesIndividually(t *testing.T) {
	cfg := ExecutorBackupConfig{}
	cfg.Database.Tables = []string{"orders", "customers", "products"}

	groups, err := newExecutor().ExpandAndGroupTables(context.Background(), &cfg)
	if err != nil {
		t.Fatalf("ExpandAndGroupTables: %v", err)
	}
	want := []string{"customers", "orders", "products"}
	if got := groupKeys(groups); !reflect.DeepEqual(got, want) {
		t.Errorf("groups = %v, want %v", got, want)
	}
	if !reflect.DeepEqual(groups["orders"], []string{"orders"}) {
		t.Errorf("orders group = %v", groups["orders"])
	}
}

// TestExpandAndGroupTablesMergesASharedPrefix records the merge rule: monthly
// shards sharing one prefix collapse into a single group, which the export
// branch then writes as one file.
func TestExpandAndGroupTablesMergesASharedPrefix(t *testing.T) {
	cfg := ExecutorBackupConfig{}
	cfg.Database.Tables = []string{"orders_202601", "orders_202602", "orders_202603"}

	groups, err := newExecutor().ExpandAndGroupTables(context.Background(), &cfg)
	if err != nil {
		t.Fatalf("ExpandAndGroupTables: %v", err)
	}
	if got := groupKeys(groups); !reflect.DeepEqual(got, []string{"orders"}) {
		t.Fatalf("groups = %v, want one \"orders\" group", got)
	}
	if len(groups["orders"]) != 3 {
		t.Errorf("orders group = %v, want all three shards", groups["orders"])
	}
}

// TestASingleShardIsNotMerged records the asymmetry in the merge branch: with
// one group holding one table, the group is keyed by the table name rather than
// the prefix, so a lone "orders_202601" is exported under its own name.
func TestASingleShardIsNotMerged(t *testing.T) {
	cfg := ExecutorBackupConfig{}
	cfg.Database.Tables = []string{"orders_202601"}

	groups, err := newExecutor().ExpandAndGroupTables(context.Background(), &cfg)
	if err != nil {
		t.Fatalf("ExpandAndGroupTables: %v", err)
	}
	if got := groupKeys(groups); !reflect.DeepEqual(got, []string{"orders_202601"}) {
		t.Errorf("groups = %v, want the table's own name", got)
	}
}

// TestATimeRangeDropsIrrelevantShards records that the query's time range is
// applied during expansion, so shards outside it never reach the export branch.
func TestATimeRangeDropsIrrelevantShards(t *testing.T) {
	// The query map is keyed by table, and each entry is keyed by field, so the
	// range object sits two levels down.
	cfg := ExecutorBackupConfig{
		Query: map[string]map[string]interface{}{
			"orders": {
				"created_at": map[string]interface{}{
					"type":        "daily",
					"startOffset": float64(-40),
					"endOffset":   float64(0),
				},
			},
		},
	}
	cfg.Database.Tables = []string{"orders_202001", "orders_202002"}

	groups, err := newExecutor().ExpandAndGroupTables(context.Background(), &cfg)
	if err != nil {
		t.Fatalf("ExpandAndGroupTables: %v", err)
	}
	// Both shards are from 2020 and the range covers the last forty days, so the
	// filter keeps only its fallback: the first table.
	if tables := groups["orders"]; len(tables) != 1 || tables[0] != "orders_202001" {
		t.Errorf("orders group = %v, want just the fallback shard", tables)
	}
}

// TestExpandAndGroupTablesReportsAnUnreachableMySQL records that regex mode for
// a MySQL task needs a live connection, so the pattern cannot be expanded when
// the source is down and the whole backup fails.
func TestExpandAndGroupTablesReportsAnUnreachableMySQL(t *testing.T) {
	cfg := ExecutorBackupConfig{
		SourceType:         "mysql",
		TableSelectionMode: "regex",
		RegexPattern:       "^orders",
	}
	cfg.Database.URL = "127.0.0.1:1"
	cfg.Database.Database = "shop"

	_, err := newExecutor().ExpandAndGroupTables(context.Background(), &cfg)
	if err == nil || !strings.Contains(err.Error(), "failed to get MySQL tables") {
		t.Fatalf("err = %v, want an expansion failure", err)
	}
}

// TestRegexModeNeedsAPattern records that the mode flag alone does nothing: with
// an empty pattern the manual branch runs instead, quietly backing up whatever
// the table list happens to hold.
func TestRegexModeNeedsAPattern(t *testing.T) {
	cfg := ExecutorBackupConfig{SourceType: "mysql", TableSelectionMode: "regex"}
	cfg.Database.Tables = []string{"orders"}

	groups, err := newExecutor().ExpandAndGroupTables(context.Background(), &cfg)
	if err != nil {
		t.Fatalf("ExpandAndGroupTables: %v", err)
	}
	if got := groupKeys(groups); !reflect.DeepEqual(got, []string{"orders"}) {
		t.Errorf("groups = %v, want the manual selection", got)
	}
}
