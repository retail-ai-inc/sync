package app

import (
	"context"
	"database/sql"
	"github.com/sirupsen/logrus"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/infra/verify"
)

func TestConsistencyCheckingIsOffUnlessAskedFor(t *testing.T) {
	if got := verifyInterval(); got != 0 {
		t.Errorf("interval = %v with nothing configured, want off", got)
	}

	t.Setenv("SYNC_VERIFY_INTERVAL", "30m")
	if got := verifyInterval(); got != 30*time.Minute {
		t.Errorf("interval = %v, want 30m", got)
	}

	for _, bad := range []string{"not a duration", "0", "-1h"} {
		t.Setenv("SYNC_VERIFY_INTERVAL", bad)
		if got := verifyInterval(); got != 0 {
			t.Errorf("interval = %v for %q, want off", got, bad)
		}
	}
}

// TestRepairIsOptIn records the judgement: correcting a divergence nobody has
// looked at means writing to the target from the source automatically, which is
// an operator's decision to make.
func TestRepairIsOptIn(t *testing.T) {
	if repairEnabled() {
		t.Error("repair is on with nothing configured")
	}

	t.Setenv("SYNC_VERIFY_REPAIR", "true")
	if !repairEnabled() {
		t.Error("repair is off although it was asked for")
	}

	t.Setenv("SYNC_VERIFY_REPAIR", "TRUE")
	if !repairEnabled() {
		t.Error("the value is compared case-sensitively")
	}

	t.Setenv("SYNC_VERIFY_REPAIR", "yes")
	if repairEnabled() {
		t.Error(`"yes" turned repair on; only "true" should`)
	}
}

// TestTheOutcomeIsRecordedAsAMetric is what makes a divergence something to
// graph and alert on rather than a line in a log nobody reads.
func TestTheOutcomeIsRecordedAsAMetric(t *testing.T) {
	task := config.SyncConfig{
		ID: 42, Type: "mysql",
		SourceConnection: "u:p@tcp(tokyo:3306)/shop",
		TargetConnection: "u:p@tcp(osaka:3306)/shop",
	}
	labels := metrics.Labels{
		"task": "42", "engine": "mysql", "table": "orders",
		"source": "tokyo:3306/shop", "target": "osaka:3306/shop",
	}
	t.Cleanup(func() { metrics.Default.Forget(labels) })

	report(context.Background(), nil, quiet(), task, "orders", verify.Result{
		SourceRows: 10, TargetRows: 9, Missing: 1,
		Sample: []verify.Difference{{Key: "7", Kind: verify.Missing}},
	})

	var b strings.Builder
	if err := metrics.Default.Write(&b); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := b.String()
	if !strings.Contains(got, `sync_verify_differences{engine="mysql",source="tokyo:3306/shop",table="orders",target="osaka:3306/shop",task="42"} 1`) {
		t.Errorf("exposition =\n%s", got)
	}
	if !strings.Contains(got, "sync_verify_last_run_timestamp_seconds") {
		t.Error("the run time was not recorded, so a comparison that stopped happening looks the same as one that found nothing")
	}
}

func TestAnIdenticalPairIsReportedAsZero(t *testing.T) {
	task := config.SyncConfig{ID: 43, Type: "mysql"}
	labels := metrics.Labels{"task": "43", "engine": "mysql", "table": "orders", "source": "", "target": ""}
	t.Cleanup(func() { metrics.Default.Forget(labels) })

	report(context.Background(), nil, quiet(), task, "orders", verify.Result{SourceRows: 10, TargetRows: 10})

	var b strings.Builder
	_ = metrics.Default.Write(&b)
	if !strings.Contains(b.String(), `sync_verify_differences{engine="mysql",source="",table="orders",target="",task="43"} 0`) {
		t.Errorf("exposition =\n%s", b.String())
	}
}

func TestADivergenceIsAnnounced(t *testing.T) {
	n := &recordingNotifier{configured: true}
	task := config.SyncConfig{
		ID: 44, Type: "mysql",
		SourceConnection: "u:p@tcp(tokyo:3306)/shop",
		TargetConnection: "u:p@tcp(osaka:3306)/shop",
	}
	t.Cleanup(func() {
		metrics.Default.Forget(metrics.Labels{"task": "44", "engine": "mysql", "table": "orders",
			"source": "tokyo:3306/shop", "target": "osaka:3306/shop"})
	})

	report(context.Background(), n, quiet(), task, "orders", verify.Result{
		SourceRows: 10, TargetRows: 8, Missing: 2,
		Sample: []verify.Difference{
			{Key: "7", Kind: verify.Missing}, {Key: "8", Kind: verify.Missing}},
	})

	if len(n.messages) != 1 {
		t.Fatalf("%d messages were sent", len(n.messages))
	}
	message := n.messages[0]
	for _, want := range []string{"orders", "tokyo:3306/shop", "osaka:3306/shop", "2 differences", "missing 7"} {
		if !strings.Contains(message, want) {
			t.Errorf("the alert does not mention %q:\n%s", want, message)
		}
	}
	if strings.Contains(message, ":p@") {
		t.Errorf("the alert leaked the credentials:\n%s", message)
	}
}

func TestAnIdenticalPairIsNotAnnounced(t *testing.T) {
	n := &recordingNotifier{configured: true}
	task := config.SyncConfig{ID: 45, Type: "mysql"}
	t.Cleanup(func() {
		metrics.Default.Forget(metrics.Labels{"task": "45", "engine": "mysql", "table": "orders",
			"source": "", "target": ""})
	})

	report(context.Background(), n, quiet(), task, "orders", verify.Result{SourceRows: 5, TargetRows: 5})

	if len(n.messages) != 0 {
		t.Errorf("a message was sent for an identical pair: %v", n.messages)
	}
}

func TestTheDifferenceSummaryIsShortEnoughToRead(t *testing.T) {
	var sample []verify.Difference
	for i := 0; i < 50; i++ {
		sample = append(sample, verify.Difference{Key: strings.Repeat("k", 40), Kind: verify.Missing})
	}

	got := describe(sample)

	if strings.Count(got, "missing") != 5 {
		t.Errorf("the summary names %d differences, want five:\n%s",
			strings.Count(got, "missing"), got)
	}
	if strings.Contains(got, strings.Repeat("k", 40)) {
		t.Errorf("a long key was not shortened:\n%s", got)
	}
	if describe(nil) != "none" {
		t.Errorf("describe(nil) = %q", describe(nil))
	}
}

// TestTheRepairStatementIsAnUpsert pins what a repair writes: a plain insert
// would fail on every row that exists but differs, which is the case the repair
// is most often for.
func TestTheRepairStatementIsAnUpsert(t *testing.T) {
	got := mysqlUpsert("shop", "orders", []string{"id", "amount"})

	want := "INSERT INTO shop.orders (id, amount) VALUES (?, ?) " +
		"ON DUPLICATE KEY UPDATE id = VALUES(id), amount = VALUES(amount)"
	if got != want {
		t.Errorf("statement =\n%s\nwant\n%s", got, want)
	}
	if unqualified := mysqlUpsert("", "orders", []string{"id"}); !strings.Contains(unqualified, "INTO orders ") {
		t.Errorf("statement = %q for an unqualified table", unqualified)
	}
}

func keyUsage(t *testing.T, rows ...[4]string) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "schema.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(`CREATE TABLE key_column_usage (
		table_schema TEXT, table_name TEXT, constraint_name TEXT,
		column_name TEXT, ordinal_position INTEGER)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i, r := range rows {
		if _, err := db.Exec(
			`INSERT INTO key_column_usage VALUES (?, ?, ?, ?, ?)`,
			r[0], r[1], r[2], r[3], i+1); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	return db
}

// primaryKeyOf runs the production query against the fixture, rewriting only
// the schema-qualified table name SQLite has no equivalent of.
func primaryKeyOf(t *testing.T, db *sql.DB, schema, table string) (string, error) {
	t.Helper()

	rows, err := db.Query(
		`SELECT column_name FROM key_column_usage
		 WHERE table_schema = ? AND table_name = ? AND constraint_name = 'PRIMARY'
		 ORDER BY ordinal_position`, schema, table)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return "", err
		}
		columns = append(columns, name)
	}
	switch len(columns) {
	case 0:
		return "", errNoPrimaryKey
	case 1:
		return columns[0], nil
	default:
		return "", errCompositeKey
	}
}

var (
	errNoPrimaryKey = &keyError{"it has no primary key"}
	errCompositeKey = &keyError{"its primary key spans several columns"}
)

type keyError struct{ msg string }

func (e *keyError) Error() string { return e.msg }

func TestASingleColumnPrimaryKeyIsUsed(t *testing.T) {
	db := keyUsage(t, [4]string{"shop", "orders", "PRIMARY", "id"})

	got, err := primaryKeyOf(t, db, "shop", "orders")
	if err != nil {
		t.Fatalf("primaryKey: %v", err)
	}
	if got != "id" {
		t.Errorf("key = %q", got)
	}
}

// TestACompositeKeyIsRefusedRatherThanGuessed matters because comparing on part
// of a composite key would report differences that are not there — every row
// sharing the first column would look like a duplicate.
func TestACompositeKeyIsRefusedRatherThanGuessed(t *testing.T) {
	db := keyUsage(t,
		[4]string{"shop", "ledger", "PRIMARY", "account"},
		[4]string{"shop", "ledger", "PRIMARY", "entry"})

	if _, err := primaryKeyOf(t, db, "shop", "ledger"); err == nil {
		t.Error("a composite key was accepted")
	}
}

func TestATableWithNoPrimaryKeyIsRefused(t *testing.T) {
	db := keyUsage(t, [4]string{"shop", "log", "idx_time", "logged_at"})

	if _, err := primaryKeyOf(t, db, "shop", "log"); err == nil {
		t.Error("a table with no primary key was accepted")
	}
}

func TestStartConsistencyChecksIsANoOpWhenOff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Nothing configured, so this must return without starting anything.
	StartConsistencyChecks(ctx, nil, quiet(), func() []config.SyncConfig { return nil })
}

func TestRunningTheChecksWithNoConfigurationIsSafe(t *testing.T) {
	runConsistencyChecks(context.Background(), tasksOf(nil), nil, nil, quiet())
}

// TestADisabledTaskIsNotCompared keeps a comparison from loading a source the
// operator has deliberately stopped replicating.
func TestADisabledTaskIsNotCompared(t *testing.T) {
	cfg := &config.Config{SyncConfigs: []config.SyncConfig{
		{ID: 1, Type: "mysql", Enable: false, SourceConnection: "u:p@tcp(127.0.0.1:1)/shop"},
	}}

	// With the task disabled nothing dials anything, so this returns at once
	// rather than waiting for a connection to port 1 to fail.
	done := make(chan struct{})
	go func() {
		defer close(done)
		runConsistencyChecks(context.Background(), tasksOf(cfg), cfg, nil, quiet())
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a disabled task was compared anyway")
	}
}

// tasksOf reports a configuration's task list, which the checker now takes
// separately so it reads the current one rather than one captured at start-up.
func tasksOf(cfg *config.Config) []config.SyncConfig {
	if cfg == nil {
		return nil
	}
	return cfg.SyncConfigs
}

// TestTheChecksWalkTheTaskListTheyAreGiven pins the wiring the live task list
// depends on.
//
// The checker used to read cfg.SyncConfigs, which is the list captured when the
// monitors started. Reading it again through the argument is what lets a task
// added, disabled or deleted after start-up be picked up -- and, with repairs
// enabled, what stops a task taken out of service from going on being written
// to. The configuration here carries a different list from the argument, so a
// checker that reached for the wrong one compares the wrong tasks.
func TestTheChecksWalkTheTaskListTheyAreGiven(t *testing.T) {
	log := &recordingConsistencyLogger{}

	captured := &config.Config{SyncConfigs: []config.SyncConfig{
		{ID: 111, Type: "mysql", Enable: true, SourceConnection: "not a dsn at all"},
	}}
	current := []config.SyncConfig{
		{ID: 222, Type: "mysql", Enable: true, SourceConnection: "also not a dsn"},
	}

	runConsistencyChecks(context.Background(), current, captured, nil, log.logger())

	if log.saw("Task 111") {
		t.Error("the checker walked the captured list, so a task removed from the " +
			"current one is still being compared")
	}
	if !log.saw("Task 222") {
		t.Errorf("the checker did not walk the list it was given: %v", log.lines)
	}
}

// TestADisabledTaskIsNotChecked covers the rule that makes removal effective:
// the list may still hold a task, and being disabled is enough to leave it out.
func TestADisabledTaskIsNotChecked(t *testing.T) {
	log := &recordingConsistencyLogger{}

	runConsistencyChecks(context.Background(),
		[]config.SyncConfig{{ID: 333, Type: "mysql", Enable: false,
			SourceConnection: "not a dsn"}},
		&config.Config{}, nil, log.logger())

	if log.saw("Task 333") {
		t.Error("a disabled task was compared, and with repairs on that means " +
			"written to")
	}
}

type recordingConsistencyLogger struct {
	lines []string
}

func (r *recordingConsistencyLogger) logger() *logrus.Logger {
	log := logrus.New()
	log.SetOutput(r)
	log.SetLevel(logrus.DebugLevel)
	return log
}

func (r *recordingConsistencyLogger) Write(p []byte) (int, error) {
	r.lines = append(r.lines, string(p))
	return len(p), nil
}

func (r *recordingConsistencyLogger) saw(substring string) bool {
	for _, line := range r.lines {
		if strings.Contains(line, substring) {
			return true
		}
	}
	return false
}
