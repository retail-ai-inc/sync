//go:build integration

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/dsn"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
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

	syncer := NewMySQLSyncer(cfg, logger)
	if syncer == nil {
		t.Fatal("NewMySQLSyncer returned nil")
	}
	stop = harness.RunSyncer(t, func(ctx context.Context) { syncer.Start(ctx) })
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

// TestDDLIsPropagated exercises F-044. A column added at the source is applied
// to the target before the rows that use it arrive, so a schema change no
// longer stops replication for the affected table.
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

// TestADroppedTableStopsReplication is the other half of DDL handling. A DROP
// at the source is not applied to the disaster-recovery copy, because that copy
// is what a mistaken DROP would be recovered from. Replication stops instead,
// which an operator sees.
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

// TestWritesDuringInitialSyncAreNotLost exercises F-040, the MySQL counterpart
// of the MongoDB snapshot gap. The copy reads through a consistent snapshot
// whose binlog coordinates are pinned before the first row is read, so a write
// made while the copy is running is replayed by the stream that resumes from
// those coordinates rather than falling between the two.
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

	// Write markers while the copy is still running.
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

	// The seeded rows arrive through the copy; the markers arrive afterwards,
	// through the stream replaying from the coordinates the copy pinned. So this
	// has to be polled rather than read once: the copy converging says nothing
	// about the replay having caught up.
	harness.Eventually(t, 60*time.Second, func() error {
		if arrived := countRows(t, tgt, table, "name = 'marker'"); arrived != markers {
			return fmt.Errorf("%d of %d rows written during the initial copy have "+
				"reached the target; writes in the window between the copy and the "+
				"stream starting are lost (F-040)", arrived, markers)
		}
		return nil
	})
}

// TestResumeFromStoredBinlogPosition checks that a restarted syncer replays what
// it missed. The position file lives under MySQLPositionPath, so the second run
// must reuse the same path.
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

// TestTransactionBoundariesAreObserved exercises F-045. Row events are buffered
// and applied inside one target transaction at the XID that closes the source
// transaction, so a reader on the target cannot observe a state that never
// existed at the source.
//
// The scenario keeps a two-row invariant — the balances must always sum to the
// same constant — and transfers between the rows inside explicit transactions
// while polling the target. Any observation with a different sum means the
// invariant was broken in flight.
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

	// Zero observations is the pass. The poller can only miss a violation, never
	// invent one, so this is a best-effort check in the direction that matters:
	// anything it does catch is a state the source never held.
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
