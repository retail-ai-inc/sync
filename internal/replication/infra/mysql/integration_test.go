//go:build integration

package mysql

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/dsn"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/test/harness"
)

const (
	sourceDB = "source_db"
	targetDB = "target_db"
)

func open(t *testing.T, endpoint, database string) *sql.DB {
	t.Helper()

	host, port := harness.SplitHostPort(t, endpoint)
	dsn := dsn.BuildDSNByType("mysql", map[string]string{
		"user": "root", "password": "root", "host": host, "port": port, "database": database,
	})

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", endpoint, err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping %s: %v", endpoint, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func mustExec(t *testing.T, db *sql.DB, query string, args ...interface{}) {
	t.Helper()

	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func countRows(t *testing.T, db *sql.DB, table, where string, args ...interface{}) int {
	t.Helper()

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	if where != "" {
		query += " WHERE " + where
	}
	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		return -1 // the table may not exist yet; callers poll on the value
	}
	return n
}

func syncTask(t *testing.T, table string, tables ...config.TableMapping) config.SyncConfig {
	t.Helper()

	srcHost, srcPort := harness.SplitHostPort(t, harness.MySQLSource)
	tgtHost, tgtPort := harness.SplitHostPort(t, harness.MySQLTarget)

	if len(tables) == 0 {
		tables = []config.TableMapping{{SourceTable: table, TargetTable: table}}
	}

	return config.SyncConfig{
		ID:     harness.UniqueTaskID(),
		Enable: true,
		Type:   "mysql",
		SourceConnection: dsn.BuildDSNByType("mysql", map[string]string{
			"user": "root", "password": "root", "host": srcHost, "port": srcPort, "database": sourceDB,
		}),
		TargetConnection: dsn.BuildDSNByType("mysql", map[string]string{
			"user": "root", "password": "root", "host": tgtHost, "port": tgtPort, "database": targetDB,
		}),
		MySQLPositionPath: t.TempDir() + "/binlog.pos",
		Mappings:          []config.DatabaseMapping{{Tables: tables}},
	}
}

func startSyncer(t *testing.T, cfg config.SyncConfig) (stop func()) {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	// NewSyncer, not NewMySQLSyncer: the first is what cmd/sync starts.
	syncer := NewSyncer(cfg, logger)
	if syncer == nil {
		t.Fatal("NewSyncer returned nil")
	}
	stop = harness.RunSyncer(t, syncer.Start)
	t.Cleanup(stop)
	return stop
}

// createSourceTable makes a uniquely named table on the source and removes both
// copies afterwards.
func createSourceTable(t *testing.T, src, tgt *sql.DB, table string) {
	t.Helper()

	mustExec(t, src, fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(100))`, table))
	t.Cleanup(func() {
		_, _ = src.Exec("DROP TABLE IF EXISTS " + table)
		_, _ = tgt.Exec("DROP TABLE IF EXISTS " + table)
	})
}

func TestInitialSyncCreatesTargetTableAndCopiesRows(t *testing.T) {
	table := harness.UniqueName("initial")
	src, tgt := open(t, harness.MySQLSource, sourceDB), open(t, harness.MySQLTarget, targetDB)

	createSourceTable(t, src, tgt, table)
	for i := 1; i <= 20; i++ {
		mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (?, ?)", table), i, fmt.Sprintf("user_%d", i))
	}

	startSyncer(t, syncTask(t, table))

	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, ""); n != 20 {
			return fmt.Errorf("target holds %d rows, want 20", n)
		}
		return nil
	})
}

func TestIncrementalSyncAppliesInsertUpdateDelete(t *testing.T) {
	table := harness.UniqueName("incremental")
	src, tgt := open(t, harness.MySQLSource, sourceDB), open(t, harness.MySQLTarget, targetDB)

	createSourceTable(t, src, tgt, table)
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'seed')", table))

	startSyncer(t, syncTask(t, table))
	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, ""); n != 1 {
			return fmt.Errorf("initial sync has not landed: %d rows", n)
		}
		return nil
	})

	t.Run("insert", func(t *testing.T) {
		mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (2, 'inserted')", table))
		harness.Eventually(t, 20*time.Second, func() error {
			if n := countRows(t, tgt, table, "id = 2"); n != 1 {
				return fmt.Errorf("insert has not arrived")
			}
			return nil
		})
	})

	t.Run("update", func(t *testing.T) {
		mustExec(t, src, fmt.Sprintf("UPDATE %s SET name = 'updated' WHERE id = 2", table))
		harness.Eventually(t, 20*time.Second, func() error {
			if n := countRows(t, tgt, table, "id = 2 AND name = 'updated'"); n != 1 {
				return fmt.Errorf("update has not arrived")
			}
			return nil
		})
	})

	t.Run("delete", func(t *testing.T) {
		mustExec(t, src, fmt.Sprintf("DELETE FROM %s WHERE id = 2", table))
		harness.Eventually(t, 20*time.Second, func() error {
			if n := countRows(t, tgt, table, "id = 2"); n != 0 {
				return fmt.Errorf("delete has not arrived")
			}
			return nil
		})
	})
}

// TestDDLIsPropagated exercises F-044.
func TestDDLIsPropagated(t *testing.T) {
	table := harness.UniqueName("ddl")
	src, tgt := open(t, harness.MySQLSource, sourceDB), open(t, harness.MySQLTarget, targetDB)

	createSourceTable(t, src, tgt, table)
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'before')", table))

	startSyncer(t, syncTask(t, table))
	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, ""); n != 1 {
			return fmt.Errorf("initial sync has not landed: %d rows", n)
		}
		return nil
	})

	// A schema change of the kind any live service eventually performs.
	mustExec(t, src, fmt.Sprintf("ALTER TABLE %s ADD COLUMN email VARCHAR(100)", table))
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name, email) VALUES (2, 'after', 'a@b.com')", table))

	emailColumns := func() int {
		t.Helper()
		var n int
		if err := tgt.QueryRow(`
			SELECT COUNT(*) FROM information_schema.columns
			WHERE table_schema = ? AND table_name = ? AND column_name = 'email'`,
			targetDB, table).Scan(&n); err != nil {
			t.Fatalf("inspect target schema: %v", err)
		}
		return n
	}

	harness.Eventually(t, 30*time.Second, func() error {
		if emailColumns() == 0 {
			return fmt.Errorf("the column has not been propagated")
		}
		if countRows(t, tgt, table, "id = 2") != 1 {
			return fmt.Errorf("the row written after the ALTER has not arrived")
		}
		return nil
	})

	var email string
	if err := tgt.QueryRow(fmt.Sprintf("SELECT email FROM %s WHERE id = 2", table)).Scan(&email); err != nil {
		t.Fatalf("read the new column: %v", err)
	}
	if email != "a@b.com" {
		t.Errorf("email = %q, want the replicated value", email)
	}
}

// A DROP at the source is not applied to the disaster-recovery copy, because
// that copy is what a mistaken DROP would be recovered from.
func TestADroppedTableStopsReplication(t *testing.T) {
	table := harness.UniqueName("ddldrop")
	src, tgt := open(t, harness.MySQLSource, sourceDB), open(t, harness.MySQLTarget, targetDB)

	createSourceTable(t, src, tgt, table)
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'keep me')", table))

	startSyncer(t, syncTask(t, table))
	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, ""); n != 1 {
			return fmt.Errorf("initial sync has not landed: %d rows", n)
		}
		return nil
	})

	mustExec(t, src, "DROP TABLE "+table)

	harness.WaitFor(10*time.Second, func() error {
		if countRows(t, tgt, table, "") < 0 {
			return nil // the table is gone, which is the failure this guards
		}
		return fmt.Errorf("still there")
	})

	if n := countRows(t, tgt, table, ""); n != 1 {
		t.Errorf("the target table holds %d rows; the DROP was replicated to the "+
			"disaster-recovery copy", n)
	}
}

// TestSecurityPolicyIsAppliedToMySQL is the counterpart to the MongoDB case:
// the same configuration that MongoDB ignores does take effect here, which is
// what makes the gap a per-engine inconsistency rather than a missing feature.
func TestSecurityPolicyIsAppliedToMySQL(t *testing.T) {
	table := harness.UniqueName("security")
	src, tgt := open(t, harness.MySQLSource, sourceDB), open(t, harness.MySQLTarget, targetDB)

	mustExec(t, src, fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100))`, table))
	t.Cleanup(func() {
		_, _ = src.Exec("DROP TABLE IF EXISTS " + table)
		_, _ = tgt.Exec("DROP TABLE IF EXISTS " + table)
	})
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, email) VALUES (1, 'jack@example.com')", table))

	cfg := syncTask(t, table, config.TableMapping{
		SourceTable:     table,
		TargetTable:     table,
		SecurityEnabled: true,
		FieldSecurity: []interface{}{
			map[string]interface{}{"field": "email", "securityType": "masked"},
		},
	})
	startSyncer(t, cfg)

	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, ""); n != 1 {
			return fmt.Errorf("initial sync has not landed: %d rows", n)
		}
		return nil
	})

	var email string
	if err := tgt.QueryRow(fmt.Sprintf("SELECT email FROM %s WHERE id = 1", table)).Scan(&email); err != nil {
		t.Fatalf("read target: %v", err)
	}
	if email == "jack@example.com" {
		t.Fatalf("the target holds the address in the clear; masking has stopped " +
			"working for MySQL as well")
	}
	t.Logf("MySQL masked the value to %q, while MongoDB leaves it in the clear", email)
}

// TestWritesDuringInitialSyncAreNotLost exercises F-040.
func TestWritesDuringInitialSyncAreNotLost(t *testing.T) {
	table := harness.UniqueName("snapshotgap")
	src, tgt := open(t, harness.MySQLSource, sourceDB), open(t, harness.MySQLTarget, targetDB)

	createSourceTable(t, src, tgt, table)

	// Large enough that the batched copy takes seconds rather than milliseconds.
	const seeded = 20000
	mustExec(t, src, "SET autocommit = 0")
	for i := 1; i <= seeded; i += 500 {
		values := ""
		for j := i; j < i+500 && j <= seeded; j++ {
			if values != "" {
				values += ","
			}
			values += fmt.Sprintf("(%d,'user_%d')", j, j)
		}
		mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES %s", table, values))
	}
	mustExec(t, src, "COMMIT")
	mustExec(t, src, "SET autocommit = 1")

	startSyncer(t, syncTask(t, table))

	markers := 0
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		copied := countRows(t, tgt, table, "")
		if copied >= seeded {
			break
		}
		mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (?, 'marker')", table), 900000+markers)
		markers++
		time.Sleep(150 * time.Millisecond)
	}
	if markers == 0 {
		t.Skip("the copy finished too quickly to write a marker; raise the seed size")
	}
	t.Logf("wrote %d markers while the initial copy was running", markers)

	harness.Eventually(t, 120*time.Second, func() error {
		if n := countRows(t, tgt, table, "id <= ?", seeded); n != seeded {
			return fmt.Errorf("copy still running: %d of %d seeded rows", n, seeded)
		}
		return nil
	})

	// The seeded rows arrive through the copy; the markers arrive afterwards.
	harness.Eventually(t, 60*time.Second, func() error {
		if arrived := countRows(t, tgt, table, "name = 'marker'"); arrived != markers {
			return fmt.Errorf("%d of %d rows written during the initial copy have "+
				"reached the target; writes in the window between the copy and the "+
				"stream starting are lost (F-040)", arrived, markers)
		}
		return nil
	})
}

// TestResumeFromStoredBinlogPosition checks that a restarted syncer replays
// what it missed.
func TestResumeFromStoredBinlogPosition(t *testing.T) {
	table := harness.UniqueName("resume")
	src, tgt := open(t, harness.MySQLSource, sourceDB), open(t, harness.MySQLTarget, targetDB)

	createSourceTable(t, src, tgt, table)
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'seed')", table))

	positionPath := t.TempDir() + "/binlog.pos"
	// One configuration, reused: the two runs have to be the same task, because
	// the checkpoint is keyed by task id.
	base := syncTask(t, table)
	base.MySQLPositionPath = positionPath
	newCfg := func() config.SyncConfig { return base }

	stop := startSyncer(t, newCfg())
	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, ""); n != 1 {
			return fmt.Errorf("initial sync has not landed: %d rows", n)
		}
		return nil
	})

	// One change while running, so a position is definitely persisted.
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (2, 'while running')", table))
	harness.Eventually(t, 20*time.Second, func() error {
		if n := countRows(t, tgt, table, "id = 2"); n != 1 {
			return fmt.Errorf("the first change has not arrived")
		}
		return nil
	})

	stop()
	time.Sleep(2 * time.Second)

	// Written while nothing is reading the binlog.
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (3, 'while stopped')", table))

	startSyncer(t, newCfg())

	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, "id = 3"); n != 1 {
			return fmt.Errorf("the row written while the syncer was stopped has not " +
				"been replayed; resuming from the stored binlog position is not working")
		}
		return nil
	})
}

// TestTransactionBoundariesAreObserved exercises F-045.
func TestTransactionBoundariesAreObserved(t *testing.T) {
	table := harness.UniqueName("txn")
	src, tgt := open(t, harness.MySQLSource, sourceDB), open(t, harness.MySQLTarget, targetDB)

	mustExec(t, src, fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, balance INT NOT NULL)`, table))
	t.Cleanup(func() {
		_, _ = src.Exec("DROP TABLE IF EXISTS " + table)
		_, _ = tgt.Exec("DROP TABLE IF EXISTS " + table)
	})
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, balance) VALUES (1, 1000), (2, 1000)", table))

	const total = 2000

	startSyncer(t, syncTask(t, table))
	harness.Eventually(t, 45*time.Second, func() error {
		var sum int
		if err := tgt.QueryRow(fmt.Sprintf("SELECT COALESCE(SUM(balance), -1) FROM %s", table)).Scan(&sum); err != nil {
			return fmt.Errorf("target not ready: %w", err)
		}
		if sum != total {
			return fmt.Errorf("initial sync has not landed: sum is %d", sum)
		}
		return nil
	})

	// Poll the target while transfers run and record any sum that breaks the
	// invariant.
	stopPolling := make(chan struct{})
	violations := make(chan int, 64)
	go func() {
		defer close(violations)
		for {
			select {
			case <-stopPolling:
				return
			default:
				var sum int
				if err := tgt.QueryRow(fmt.Sprintf("SELECT COALESCE(SUM(balance), %d) FROM %s", total, table)).Scan(&sum); err == nil && sum != total {
					select {
					case violations <- sum:
					default:
					}
				}
			}
		}
	}()

	for i := 0; i < 200; i++ {
		tx, err := src.Begin()
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if _, err := tx.Exec(fmt.Sprintf("UPDATE %s SET balance = balance - 100 WHERE id = 1", table)); err != nil {
			t.Fatalf("debit: %v", err)
		}
		if _, err := tx.Exec(fmt.Sprintf("UPDATE %s SET balance = balance + 100 WHERE id = 2", table)); err != nil {
			t.Fatalf("credit: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
		// Reverse it so the balances stay in range.
		tx, err = src.Begin()
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if _, err := tx.Exec(fmt.Sprintf("UPDATE %s SET balance = balance + 100 WHERE id = 1", table)); err != nil {
			t.Fatalf("credit back: %v", err)
		}
		if _, err := tx.Exec(fmt.Sprintf("UPDATE %s SET balance = balance - 100 WHERE id = 2", table)); err != nil {
			t.Fatalf("debit back: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	// Let replication drain, then stop polling.
	time.Sleep(5 * time.Second)
	close(stopPolling)

	var observed []int
	for sum := range violations {
		observed = append(observed, sum)
		if len(observed) >= 5 {
			break
		}
	}

	// The end state must be correct regardless.
	var finalSum int
	if err := tgt.QueryRow(fmt.Sprintf("SELECT SUM(balance) FROM %s", table)).Scan(&finalSum); err != nil {
		t.Fatalf("read final sum: %v", err)
	}
	if finalSum != total {
		t.Errorf("the target settled on a sum of %d, want %d", finalSum, total)
	}

	// Zero observations is the pass.
	if len(observed) > 0 {
		t.Errorf("the target exposed %d intermediate sums such as %v, none of which "+
			"ever existed at the source: the row events of one transaction are not "+
			"being applied together (F-045)",
			len(observed), observed[:min(len(observed), 3)])
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// The reader used to be left running when the task's context was cancelled:
// the supervisor restarting a task then ran a second reader beside the first,
// and a task edited to point at a different target went on writing to the old
// one for the life of the process.
func TestAStoppedTaskStopsWriting(t *testing.T) {
	table := harness.UniqueName("stopped")
	src, tgt := open(t, harness.MySQLSource, sourceDB), open(t, harness.MySQLTarget, targetDB)

	createSourceTable(t, src, tgt, table)
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'seed')", table))

	stop := startSyncer(t, syncTask(t, table))
	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, ""); n != 1 {
			return fmt.Errorf("initial sync has not landed: %d rows", n)
		}
		return nil
	})

	stop()

	// Written after the task stopped. Nothing should carry it across.
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (2, 'after the stop')", table))

	harness.Consistently(t, 10*time.Second, func() error {
		if n := countRows(t, tgt, table, "id = 2"); n != 0 {
			return fmt.Errorf("a row written after the task stopped reached the "+
				"target: the reader is still running (%d rows)", n)
		}
		return nil
	})
}

// TestTheFinalPositionIsRecordedOnACleanStop pins the other half of stopping.
func TestTheFinalPositionIsRecordedOnACleanStop(t *testing.T) {
	table := harness.UniqueName("finalpos")
	src, tgt := open(t, harness.MySQLSource, sourceDB), open(t, harness.MySQLTarget, targetDB)

	createSourceTable(t, src, tgt, table)
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'seed')", table))

	base := syncTask(t, table)
	stop := startSyncer(t, base)
	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, ""); n != 1 {
			return fmt.Errorf("initial sync has not landed: %d rows", n)
		}
		return nil
	})

	// A change, then an immediate stop: the throttle is still holding the
	// position this produced.
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (2, 'last')", table))
	harness.Eventually(t, 20*time.Second, func() error {
		if n := countRows(t, tgt, table, "id = 2"); n != 1 {
			return fmt.Errorf("the change has not been applied yet")
		}
		return nil
	})
	stop()

	var payload string
	if err := tgt.QueryRow(
		"SELECT payload FROM _sync_checkpoint WHERE task_id = ?", base.ID).Scan(&payload); err != nil {
		t.Fatalf("read the recorded position: %v", err)
	}
	if payload == "" {
		t.Fatal("no position was recorded")
	}
	t.Logf("recorded position: %s", payload)
}

// A task that names its tables replicates those and no more, which is the
// point of naming them — but a table added at the source afterwards is then
// missing from the replica, and "the disaster-recovery copy does not have that
// table" is not something to find out during a failover.
func TestATableTheTaskDoesNotListIsReported(t *testing.T) {
	listed := harness.UniqueName("listed")
	unlisted := harness.UniqueName("unlisted")
	src, tgt := open(t, harness.MySQLSource, sourceDB), open(t, harness.MySQLTarget, targetDB)

	createSourceTable(t, src, tgt, listed)
	createSourceTable(t, src, tgt, unlisted)
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'seed')", listed))
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'seed')", unlisted))

	var recorded bytes.Buffer
	logger := logrus.New()
	logger.SetOutput(&recorded)
	logger.SetLevel(logrus.WarnLevel)

	cfg := syncTask(t, listed)
	// Through NewSyncer, which is what the supervisor starts.
	stop := harness.RunSyncer(t, NewSyncer(cfg, logger).Start)
	t.Cleanup(stop)

	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, listed, ""); n != 1 {
			return fmt.Errorf("the listed table has not been copied")
		}
		return nil
	})

	// The unlisted table is not replicated, which is correct.
	if n := countRows(t, tgt, unlisted, ""); n > 0 {
		t.Errorf("the unlisted table was replicated (%d rows); a named task should "+
			"replicate only what it names", n)
	}

	// But it is reported.
	harness.Eventually(t, 30*time.Second, func() error {
		if !strings.Contains(recorded.String(), unlisted) {
			return fmt.Errorf("nothing warned about %s", unlisted)
		}
		return nil
	})

	labels := metrics.Labels{"task": fmt.Sprint(cfg.ID), "engine": "mysql"}
	var reported bool
	for _, sample := range metrics.Default.Snapshot(metrics.Unreplicated) {
		if sample.Labels.Key() == labels.Key() && sample.Value > 0 {
			reported = true
		}
	}
	if !reported {
		t.Error("the unreplicated table count was not recorded")
	}
}

// TestATimestampArrivesAsTheSourceShowsIt covers the value a TIMESTAMP column
// holds on the target, which is not what a row count can see.
//
// A TIMESTAMP is stored as an instant and rendered in the reader's zone. The
// stream used to render it in this process's zone, so a syncer running in JST
// wrote every TIMESTAMP nine hours ahead of the source while the first copy
// wrote the same rows correctly — the two sides agreed on every count and
// disagreed on the values.
func TestATimestampArrivesAsTheSourceShowsIt(t *testing.T) {
	table := harness.UniqueName("tstz")
	src, tgt := open(t, harness.MySQLSource, sourceDB), open(t, harness.MySQLTarget, targetDB)

	mustExec(t, src, fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, seen TIMESTAMP NOT NULL, name VARCHAR(40))`, table))
	t.Cleanup(func() {
		_, _ = src.Exec("DROP TABLE IF EXISTS " + table)
		_, _ = tgt.Exec("DROP TABLE IF EXISTS " + table)
	})
	// One row through the first copy, to compare the two paths against each
	// other as well as against the source.
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, seen, name) VALUES (1, ?, 'copied')", table),
		"2026-03-01 04:05:06")

	startSyncer(t, syncTask(t, table))
	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, ""); n != 1 {
			return fmt.Errorf("the first copy has not landed: %d rows", n)
		}
		return nil
	})

	// And one through the stream.
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, seen, name) VALUES (2, ?, 'streamed')", table),
		"2026-03-01 04:05:06")
	harness.Eventually(t, 30*time.Second, func() error {
		if n := countRows(t, tgt, table, "id = 2"); n != 1 {
			return fmt.Errorf("the streamed row has not arrived")
		}
		return nil
	})

	for _, id := range []int{1, 2} {
		var source, target string
		if err := src.QueryRow(fmt.Sprintf("SELECT seen FROM %s WHERE id = ?", table), id).
			Scan(&source); err != nil {
			t.Fatalf("read the source row %d: %v", id, err)
		}
		if err := tgt.QueryRow(fmt.Sprintf("SELECT seen FROM %s WHERE id = ?", table), id).
			Scan(&target); err != nil {
			t.Fatalf("read the target row %d: %v", id, err)
		}
		if source != target {
			t.Errorf("row %d reads %q on the source and %q on the target", id, source, target)
		}
	}
}

// TestATimestampSurvivesAServerInAnotherZone is the half the 2026-09-14 fix
// left open.
//
// That fix pinned how the binlog's instants are rendered into text. What the
// target then makes of that text is decided by the target's own session zone,
// and nothing pinned it: a pair of servers set to different zones shifted every
// replicated TIMESTAMP by the difference, with the row counts still agreeing.
// Both DSNs carry time_zone='+00:00' now, so the session the task writes
// through is the same one it reads through whatever the servers are set to.
func TestATimestampSurvivesAServerInAnotherZone(t *testing.T) {
	table := harness.UniqueName("tszone")
	src, tgt := open(t, harness.MySQLSource, sourceDB), open(t, harness.MySQLTarget, targetDB)

	mustExec(t, src, fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, seen TIMESTAMP NOT NULL)`, table))
	t.Cleanup(func() {
		_, _ = src.Exec("DROP TABLE IF EXISTS " + table)
		_, _ = tgt.Exec("DROP TABLE IF EXISTS " + table)
	})
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, seen) VALUES (1, ?)", table),
		"2026-03-01 04:05:06")

	startSyncer(t, syncTask(t, table))
	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, ""); n != 1 {
			return fmt.Errorf("the first copy has not landed")
		}
		return nil
	})
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, seen) VALUES (2, ?)", table),
		"2026-03-01 04:05:06")
	harness.Eventually(t, 30*time.Second, func() error {
		if n := countRows(t, tgt, table, "id = 2"); n != 1 {
			return fmt.Errorf("the streamed row has not arrived")
		}
		return nil
	})

	// Read both sides through a session pinned the same way, which is what the
	// task itself does: the instant has to be the same on both.
	for _, id := range []int{1, 2} {
		var source, target int64
		if err := src.QueryRow(fmt.Sprintf(
			"SELECT UNIX_TIMESTAMP(seen) FROM %s WHERE id = ?", table), id).Scan(&source); err != nil {
			t.Fatalf("read the source row %d: %v", id, err)
		}
		if err := tgt.QueryRow(fmt.Sprintf(
			"SELECT UNIX_TIMESTAMP(seen) FROM %s WHERE id = ?", table), id).Scan(&target); err != nil {
			t.Fatalf("read the target row %d: %v", id, err)
		}
		if source != target {
			t.Errorf("row %d is %d on the source and %d on the target, %d seconds apart",
				id, source, target, target-source)
		}
	}
}
