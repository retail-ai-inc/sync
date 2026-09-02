//go:build integration

package postgresql

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/test/harness"
)

// The compose stack starts the source with wal_level=logical, which is what
// logical replication needs and what makes these tests possible at all.
const (
	sourceDatabase = "source_db"
	targetDatabase = "target_db"
)

func connString(endpoint, database string) string {
	return fmt.Sprintf("postgres://root:root@%s/%s?sslmode=disable", endpoint, database)
}

func open(t *testing.T, endpoint, database string) *sql.DB {
	t.Helper()

	db, err := sql.Open("postgres", connString(endpoint, database))
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

// countRows answers -1 rather than failing when the table is not there yet, so
// callers can poll on the value while the initial copy is still running.
func countRows(t *testing.T, db *sql.DB, table, where string, args ...interface{}) int {
	t.Helper()

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	if where != "" {
		query += " WHERE " + where
	}
	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		return -1
	}
	return n
}

// sourceTable creates a table on the source, publishes it, and removes the
// table, the publication and the replication slot afterwards. A slot left
// behind is not harmless: PostgreSQL keeps every WAL segment the slot has not
// consumed, so an abandoned one fills the disk of the server it was made on.
func sourceTable(t *testing.T, src, tgt *sql.DB, table, publication, slot string) {
	t.Helper()

	mustExec(t, src, fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, name TEXT)`, table))
	mustExec(t, src, fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE %s`, publication, table))

	t.Cleanup(func() {
		_, _ = src.Exec("DROP PUBLICATION IF EXISTS " + publication)
		_, _ = src.Exec("DROP TABLE IF EXISTS " + table)
		_, _ = tgt.Exec("DROP TABLE IF EXISTS " + table)
		// The syncer holds the slot while it runs; the cleanup order puts the
		// syncer's own stop first, so by here it is free.
		_, _ = src.Exec(`SELECT pg_drop_replication_slot($1)
			WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, slot)
	})
}

func syncTask(t *testing.T, table, publication, slot string, tables ...config.TableMapping) config.SyncConfig {
	t.Helper()

	if len(tables) == 0 {
		tables = []config.TableMapping{{SourceTable: table, TargetTable: table}}
	}

	return config.SyncConfig{
		ID:                    harness.UniqueTaskID(),
		Enable:                true,
		Type:                  "postgresql",
		SourceConnection:      connString(harness.PostgresSource, sourceDatabase),
		TargetConnection:      connString(harness.PostgresTarget, targetDatabase),
		PGReplicationSlotName: slot,
		PGPluginName:          "pgoutput",
		PGPublicationNames:    publication,
		PGPositionPath:        t.TempDir() + "/pg.pos",
		Mappings:              []config.DatabaseMapping{{Tables: tables}},
	}
}

func startSyncer(t *testing.T, cfg config.SyncConfig) (stop func()) {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	syncer := NewPostgreSQLSyncer(cfg, logger)
	if syncer == nil {
		t.Fatal("NewPostgreSQLSyncer returned nil")
	}
	stop = harness.RunSyncer(t, syncer.Start)
	t.Cleanup(stop)
	return stop
}

func names(t *testing.T, prefix string) (table, publication, slot string) {
	t.Helper()

	table = harness.UniqueName(prefix)
	return table, "pub_" + table, "slot_" + table
}

// TestInitialSyncCreatesTargetTableAndCopiesRows covers the copy a new region
// starts with: the target has no table at all until the syncer builds one from
// the source's own column definitions.
func TestInitialSyncCreatesTargetTableAndCopiesRows(t *testing.T) {
	table, publication, slot := names(t, "pg_initial")
	src, tgt := open(t, harness.PostgresSource, sourceDatabase), open(t, harness.PostgresTarget, targetDatabase)

	sourceTable(t, src, tgt, table, publication, slot)
	for i := 1; i <= 20; i++ {
		mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES ($1, $2)", table),
			i, fmt.Sprintf("user_%d", i))
	}

	startSyncer(t, syncTask(t, table, publication, slot))

	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, ""); n != 20 {
			return fmt.Errorf("target holds %d rows, want 20", n)
		}
		return nil
	})
}

// TestIncrementalSyncAppliesInsertUpdateDelete covers the stream. The update and
// the delete are the ones worth having: they used to be written with a WHERE
// naming every column, so a row the target had drifted on was updated nowhere
// and the statement still reported success.
func TestIncrementalSyncAppliesInsertUpdateDelete(t *testing.T) {
	table, publication, slot := names(t, "pg_incremental")
	src, tgt := open(t, harness.PostgresSource, sourceDatabase), open(t, harness.PostgresTarget, targetDatabase)

	sourceTable(t, src, tgt, table, publication, slot)
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'seed')", table))

	startSyncer(t, syncTask(t, table, publication, slot))
	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, ""); n != 1 {
			return fmt.Errorf("the initial copy has not landed: %d rows", n)
		}
		return nil
	})

	t.Run("insert", func(t *testing.T) {
		mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (2, 'inserted')", table))
		harness.Eventually(t, 20*time.Second, func() error {
			if n := countRows(t, tgt, table, "id = 2"); n != 1 {
				return fmt.Errorf("the insert has not arrived")
			}
			return nil
		})
	})

	t.Run("update", func(t *testing.T) {
		mustExec(t, src, fmt.Sprintf("UPDATE %s SET name = 'updated' WHERE id = 2", table))
		harness.Eventually(t, 20*time.Second, func() error {
			if n := countRows(t, tgt, table, "id = 2 AND name = 'updated'"); n != 1 {
				return fmt.Errorf("the update has not arrived")
			}
			return nil
		})
	})

	t.Run("delete", func(t *testing.T) {
		mustExec(t, src, fmt.Sprintf("DELETE FROM %s WHERE id = 2", table))
		harness.Eventually(t, 20*time.Second, func() error {
			if n := countRows(t, tgt, table, "id = 2"); n != 0 {
				return fmt.Errorf("the delete has not arrived: %d rows still there", n)
			}
			return nil
		})
	})
}

// TestReplicationResumesFromTheStoredPosition covers what a restart in the other
// region depends on. The position is recorded on the target, so a syncer that
// comes back finds it there and replays from it rather than from the beginning
// or from nothing.
func TestReplicationResumesFromTheStoredPosition(t *testing.T) {
	table, publication, slot := names(t, "pg_resume")
	src, tgt := open(t, harness.PostgresSource, sourceDatabase), open(t, harness.PostgresTarget, targetDatabase)

	sourceTable(t, src, tgt, table, publication, slot)
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'before')", table))

	cfg := syncTask(t, table, publication, slot)
	stop := startSyncer(t, cfg)
	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, ""); n != 1 {
			return fmt.Errorf("the initial copy has not landed: %d rows", n)
		}
		return nil
	})
	stop()

	// Written while nothing is replicating. The slot holds the WAL, so this is
	// what the restarted syncer has to pick up.
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (2, 'while stopped')", table))

	startSyncer(t, cfg)
	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, "id = 2"); n != 1 {
			return fmt.Errorf("the row written while the syncer was down never arrived")
		}
		return nil
	})

	// And the row that was already there was not copied twice.
	if n := countRows(t, tgt, table, ""); n != 2 {
		t.Errorf("target holds %d rows, want 2", n)
	}
}

// TestATaskWithNoSlotStopsForGood records that a task missing its replication
// slot or plugin is reported as unrecoverable rather than retried forever. There
// is nothing to read changes from and no amount of waiting produces one.
func TestATaskWithNoSlotStopsForGood(t *testing.T) {
	table, publication, slot := names(t, "pg_noslot")
	src, tgt := open(t, harness.PostgresSource, sourceDatabase), open(t, harness.PostgresTarget, targetDatabase)
	sourceTable(t, src, tgt, table, publication, slot)

	cfg := syncTask(t, table, publication, slot)
	cfg.PGReplicationSlotName = ""

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	err := NewPostgreSQLSyncer(cfg, logger).Start(t.Context())
	if err == nil {
		t.Fatal("Start succeeded with no replication slot configured")
	}
	if !domain.IsUnrecoverable(err) {
		t.Errorf("err = %v, want it marked unrecoverable so the supervisor stops retrying", err)
	}
}
