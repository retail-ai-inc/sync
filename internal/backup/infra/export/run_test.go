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
// reads.
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
// scheduler wrote them in.
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

// The group map came back empty, the loop body never ran and the call reported
// success, so a misconfigured task looked like a clean backup to the scheduler
// — which then stamped last_backup_time on a backup that does not exist.
func TestSelectingNothingIsNotASuccessfulBackup(t *testing.T) {
	db := taskDB(t)
	id := insertBackupTask(t, db, 1, `{"name":"empty","sourceType":"mysql"}`)

	if err := NewBackupExecutor(db).Execute(context.Background(), id); err == nil {
		t.Error("Execute = nil for a task that selected no tables")
	}
}

// An unknown sourceType — like every other failure inside the group loop —
// used to be logged and stepped over, so Execute returned nil and the task was
// recorded as backed up.
func TestAnUnsupportedEngineFailsTheBackup(t *testing.T) {
	db := taskDB(t)
	id := insertBackupTask(t, db, 1,
		`{"name":"x","sourceType":"cassandra","database":{"tables":["orders"]}}`)

	err := NewBackupExecutor(db).Execute(context.Background(), id)
	if err == nil {
		t.Fatal("Execute = nil for an engine it cannot export")
	}
	if !strings.Contains(err.Error(), "cassandra") {
		t.Errorf("err = %v, want it to name the engine", err)
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
	stubBin(t, dir, "mysqldump", "echo '-- dump'", 0)
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

// TestAFailedExportIsReportedAndLeavesNoTempDirectory covers both halves of a
// failing dump: the caller is told, and the working directory is removed so a
// task that keeps failing does not fill the disk.
func TestAFailedExportIsReportedAndLeavesNoTempDirectory(t *testing.T) {
	dir := stubPATH(t)
	stubBin(t, dir, "mysqldump", "", 1)

	before := tempDirCount(t)

	db := taskDB(t)
	id := insertBackupTask(t, db, 1, `{
		"name":"nightly","sourceType":"mysql","format":"sql",
		"database":{"url":"127.0.0.1:3306","database":"shop","tables":["orders"]},
		"destination":{"gcsPath":"gs://bucket/x"}
	}`)

	if err := NewBackupExecutor(db).Execute(context.Background(), id); err == nil {
		t.Error("Execute = nil for a dump that failed")
	}
	if after := tempDirCount(t); after != before {
		t.Errorf("temp directory count %d -> %d after a failure", before, after)
	}
}

// A merged group and a single table in one job each reach the bucket as their own object.
func TestEachPrefixIsUploadedToItsOwnObject(t *testing.T) {
	dir := stubPATH(t)
	stubBin(t, dir, "mysqldump", "echo '-- dump'", 0)
	linkRealBinary(t, dir, "zip")
	stubBin(t, dir, "gsutil", "", 0)

	db := taskDB(t)
	id := insertBackupTask(t, db, 1, `{
		"name":"nightly","sourceType":"mysql","format":"sql","compressionType":"zip",
		"database":{"url":"127.0.0.1:3306","database":"shop",
			"tables":["orders_202607","orders_202608","customers"]},
		"destination":{"gcsPath":"gs://bucket/x"}
	}`)

	if err := NewBackupExecutor(db).Execute(context.Background(), id); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	args := stubArgs(t, dir, "gsutil")
	var objects []string
	for i, a := range args {
		if a == "cp" && i+2 < len(args) {
			objects = append(objects, args[i+2])
		}
	}
	sort.Strings(objects)
	want := []string{
		"gs://bucket/x/customers-" + yesterdayStamp() + ".zip",
		"gs://bucket/x/orders-" + yesterdayStamp() + ".zip",
	}
	if !reflect.DeepEqual(objects, want) {
		t.Errorf("uploaded objects = %v, want %v", objects, want)
	}
	dumped := strings.Join(stubArgs(t, dir, "mysqldump"), " ")
	for _, table := range []string{"orders_202607", "orders_202608", "customers"} {
		if !strings.Contains(dumped, table) {
			t.Errorf("mysqldump arguments = %q, want %s among them", dumped, table)
		}
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

// Every exporter names its file after the prefix of the group's first table.
func TestTablesSharingAPrefixAreMergedEvenBesideOtherGroups(t *testing.T) {
	cfg := ExecutorBackupConfig{}
	cfg.Database.Tables = []string{"orders_202607", "orders_202608", "customers"}

	e := newExecutor()
	groups, err := e.ExpandAndGroupTables(context.Background(), &cfg)
	if err != nil {
		t.Fatalf("ExpandAndGroupTables: %v", err)
	}
	want := map[string][]string{
		"orders":    {"orders_202607", "orders_202608"},
		"customers": {"customers"},
	}
	if !reflect.DeepEqual(groups, want) {
		t.Errorf("groups = %v, want %v", groups, want)
	}
	written := map[string]string{}
	for group, tables := range groups {
		name := e.extractTablePrefix(tables[0])
		if other, taken := written[name]; taken {
			t.Errorf("groups %q and %q both write %s-<date>.zip", other, group, name)
		}
		written[name] = group
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
	// Both shards are from 2020 and the range covers the last forty days, so
	// neither is selected.
	if tables := groups["orders"]; len(tables) != 0 {
		t.Errorf("orders group = %v, want nothing", tables)
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

// The mode flag alone used to fall through to the manual branch, quietly
// backing up whatever tables happened to be left in the list — which is not
// what the job says it does.
func TestRegexModeNeedsAPattern(t *testing.T) {
	cfg := ExecutorBackupConfig{SourceType: "mysql", TableSelectionMode: "regex"}
	cfg.Database.Tables = []string{"orders"}

	if _, err := newExecutor().ExpandAndGroupTables(context.Background(), &cfg); err == nil {
		t.Error("ExpandAndGroupTables = nil for a pattern-mode job with no pattern")
	}
}
