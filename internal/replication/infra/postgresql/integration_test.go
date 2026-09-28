//go:build integration

package postgresql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
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
// table, the publication and the replication slot afterwards.
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

// The update and the delete are the ones worth having: they used to be written
// with a WHERE naming every column, so a row the target had drifted on was
// updated nowhere and the statement still reported success.
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

// The position is recorded on the target, so a syncer that comes back finds it
// there and replays from it rather than from the beginning or from nothing.
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

	// Written while nothing is replicating.
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
// slot or plugin is reported as unrecoverable rather than retried forever.
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

// storePosition records a position for the task as though a run against source had written it.
func storePosition(t *testing.T, tgt *sql.DB, taskID int, source string) {
	t.Helper()

	store := &checkpoint.SQLStore{DB: tgt, TaskID: taskID, NumberedPlaceholders: true}
	payload, err := encodeLSN(pglogrepl.LSN(1<<32), source)
	if err != nil {
		t.Fatalf("encodeLSN: %v", err)
	}
	if err := store.Save(t.Context(), "", payload); err != nil {
		t.Fatalf("store the position: %v", err)
	}
	t.Cleanup(func() { _ = store.Purge(context.Background()) })
}

const foreignSource = "10.0.0.9:5432/" + sourceDatabase

// A slot made here now would start after the changes the target is missing.
func TestAStoredPositionWithNoSlotStopsWithoutCreatingOne(t *testing.T) {
	for _, tt := range []struct {
		name, source string
	}{
		{"another server", foreignSource},
		{"this server", endpointOf(connString(harness.PostgresSource, sourceDatabase))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			table, publication, slot := names(t, "pg_stored_noslot")
			src, tgt := open(t, harness.PostgresSource, sourceDatabase), open(t, harness.PostgresTarget, targetDatabase)
			sourceTable(t, src, tgt, table, publication, slot)

			cfg := syncTask(t, table, publication, slot)
			storePosition(t, tgt, cfg.ID, tt.source)

			logger := logrus.New()
			logger.SetLevel(logrus.ErrorLevel)

			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			err := NewPostgreSQLSyncer(cfg, logger).Start(ctx)
			if ctx.Err() != nil {
				t.Fatalf("Start was still running after the timeout: %v", err)
			}
			if !domain.IsUnrecoverable(err) {
				t.Fatalf("err = %v, want it marked unrecoverable", err)
			}
			for _, named := range []string{tt.source, slot, "1/0"} {
				if !strings.Contains(err.Error(), named) {
					t.Errorf("the refusal does not name %s: %v", named, err)
				}
			}
			if n := countRows(t, src, "pg_replication_slots", "slot_name = $1", slot); n != 0 {
				t.Errorf("the refused start left %d replication slot(s) %s behind", n, slot)
			}
		})
	}
}

// A foreign position with the slot already here resumes from the slot.
func TestAForeignPositionResumesFromTheSlotHere(t *testing.T) {
	table, publication, slot := names(t, "pg_foreign_slot")
	src, tgt := open(t, harness.PostgresSource, sourceDatabase), open(t, harness.PostgresTarget, targetDatabase)
	sourceTable(t, src, tgt, table, publication, slot)
	mustExec(t, src, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slot)

	cfg := syncTask(t, table, publication, slot)
	storePosition(t, tgt, cfg.ID, foreignSource)

	// Written while nothing is replicating, so only the slot's retained WAL carries it.
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'while stopped')", table))

	startSyncer(t, cfg)
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (2, 'after the start')", table))
	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, "id IN (1, 2)"); n != 2 {
			return fmt.Errorf("the target holds %d of the 2 rows written after the move", n)
		}
		return nil
	})
}

// slotReleased waits until no connection holds slot, so a restart can take it.
func slotReleased(t *testing.T, src *sql.DB, slot string) {
	t.Helper()

	harness.Eventually(t, 20*time.Second, func() error {
		if n := countRows(t, src, "pg_replication_slots", "slot_name = $1 AND NOT active", slot); n != 1 {
			return fmt.Errorf("replication slot %s is still held", slot)
		}
		return nil
	})
}

// A failure means a restart applied again the transaction its stored position ends at.
func TestAStreamedTransactionIsNotReplayedAfterARestart(t *testing.T) {
	for _, tt := range []struct {
		name, columns string
		fullIdentity  bool
	}{
		{"with a primary key", "id INT PRIMARY KEY, name TEXT", false},
		{"with no primary key", "id INT, name TEXT", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			table, publication, slot := names(t, "pg_replay")
			src, tgt := open(t, harness.PostgresSource, sourceDatabase), open(t, harness.PostgresTarget, targetDatabase)

			mustExec(t, src, fmt.Sprintf("CREATE TABLE %s (%s)", table, tt.columns))
			if tt.fullIdentity {
				mustExec(t, src, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", table))
			}
			mustExec(t, src, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", publication, table))
			t.Cleanup(func() {
				_, _ = src.Exec("DROP PUBLICATION IF EXISTS " + publication)
				_, _ = src.Exec("DROP TABLE IF EXISTS " + table)
				_, _ = tgt.Exec("DROP TABLE IF EXISTS " + table)
				_, _ = src.Exec(`SELECT pg_drop_replication_slot($1)
					WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, slot)
			})
			mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'copied')", table))

			cfg := syncTask(t, table, publication, slot)
			stop := startSyncer(t, cfg)
			harness.Eventually(t, 45*time.Second, func() error {
				if n := countRows(t, tgt, table, "id = 1"); n != 1 {
					return fmt.Errorf("the initial copy has not landed: %d rows", n)
				}
				return nil
			})
			mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (2, 'streamed')", table))
			harness.Eventually(t, 20*time.Second, func() error {
				if n := countRows(t, tgt, table, "id = 2"); n != 1 {
					return fmt.Errorf("the streamed insert has not arrived")
				}
				return nil
			})
			stop()
			slotReleased(t, src, slot)

			startSyncer(t, cfg)
			mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (3, 'after the restart')", table))
			harness.Eventually(t, 45*time.Second, func() error {
				if n := countRows(t, tgt, table, "id = 3"); n != 1 {
					return fmt.Errorf("the insert made after the restart has not arrived, so the stream stopped")
				}
				return nil
			})
			if want, got := rowsOf(t, src, table), rowsOf(t, tgt, table); !reflect.DeepEqual(got, want) {
				t.Errorf("target holds %v, source holds %v", got, want)
			}
		})
	}
}

// A failure means a write the target refuses for good was retried for ever instead of stopping the task.
func TestADuplicateOnTheTargetStopsTheTask(t *testing.T) {
	table, publication, slot := names(t, "pg_target_duplicate")
	src, tgt := open(t, harness.PostgresSource, sourceDatabase), open(t, harness.PostgresTarget, targetDatabase)
	sourceTable(t, src, tgt, table, publication, slot)
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'copied')", table))

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- NewPostgreSQLSyncer(syncTask(t, table, publication, slot), logger).Start(ctx) }()
	stopped := false
	t.Cleanup(func() {
		cancel()
		if !stopped {
			<-done
		}
	})

	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, "id = 1"); n != 1 {
			return fmt.Errorf("the initial copy has not landed: %d rows", n)
		}
		return nil
	})
	mustExec(t, tgt, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (5, 'on the target only')", table))
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (5, 'from the source')", table))

	select {
	case err := <-done:
		stopped = true
		if !domain.IsUnrecoverable(err) {
			t.Fatalf("err = %v, want it marked unrecoverable", err)
		}
		if !strings.Contains(err.Error(), "23505") {
			t.Errorf("the refusal does not carry the target's SQLSTATE 23505: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the task was still retrying the refused write 30s after it was made")
	}
}

// A failure means a streamed change went to a table named after the source rather than the one its mapping names.
func TestAStreamedRowLandsInTheMappedTargetTable(t *testing.T) {
	for _, tt := range []struct {
		name         string
		targetSchema string
		targets      []string
	}{
		{"renamed", "", []string{"_dr"}},
		{"in another schema", "dr", []string{"_dr"}},
		{"to two tables", "", []string{"_a", "_b"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			table, publication, slot := names(t, "pg_mapped")
			src, tgt := open(t, harness.PostgresSource, sourceDatabase), open(t, harness.PostgresTarget, targetDatabase)
			sourceTable(t, src, tgt, table, publication, slot)

			schema := "public"
			if tt.targetSchema != "" {
				schema = tt.targetSchema + "_" + table
				mustExec(t, tgt, "CREATE SCHEMA "+schema)
				t.Cleanup(func() { _, _ = tgt.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE") })
			}
			var mapped []config.TableMapping
			var targets []string
			for _, suffix := range tt.targets {
				target := table + suffix
				mapped = append(mapped, config.TableMapping{SourceTable: table, TargetTable: target})
				targets = append(targets, schema+"."+target)
				t.Cleanup(func() { _, _ = tgt.Exec("DROP TABLE IF EXISTS " + schema + "." + target) })
			}
			mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'copied')", table))

			cfg := syncTask(t, table, publication, slot, mapped...)
			if tt.targetSchema != "" {
				cfg.Mappings[0].TargetSchema = schema
			}
			startSyncer(t, cfg)
			harness.Eventually(t, 45*time.Second, func() error {
				for _, target := range targets {
					if n := countRows(t, tgt, target, "id = 1"); n != 1 {
						return fmt.Errorf("the initial copy has not landed in %s: %d rows", target, n)
					}
				}
				return nil
			})

			mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (2, 'streamed')", table))
			mustExec(t, src, fmt.Sprintf("UPDATE %s SET name = 'updated' WHERE id = 1", table))
			harness.Eventually(t, 20*time.Second, func() error {
				for _, target := range targets {
					if n := countRows(t, tgt, target, "(id = 1 AND name = 'updated') OR id = 2"); n != 2 {
						return fmt.Errorf("the streamed changes have not arrived in %s", target)
					}
				}
				return nil
			})
			if n := countRows(t, tgt, "information_schema.tables", "table_name = $1", table); n != 0 {
				t.Errorf("the target has %d table(s) named after the source table %s", n, table)
			}
		})
	}
}

// A failure means a row shaped by a column the target lacks was retried, or skipped, instead of stopping the task.
func TestAColumnAddedOnTheSourceStopsTheTask(t *testing.T) {
	table, publication, slot := names(t, "pg_added_column")
	src, tgt := open(t, harness.PostgresSource, sourceDatabase), open(t, harness.PostgresTarget, targetDatabase)
	sourceTable(t, src, tgt, table, publication, slot)
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'copied')", table))

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- NewPostgreSQLSyncer(syncTask(t, table, publication, slot), logger).Start(ctx) }()
	stopped := false
	t.Cleanup(func() {
		cancel()
		if !stopped {
			<-done
		}
	})

	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, "id = 1"); n != 1 {
			return fmt.Errorf("the initial copy has not landed: %d rows", n)
		}
		return nil
	})
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (2, 'streamed')", table))
	harness.Eventually(t, 20*time.Second, func() error {
		if n := countRows(t, tgt, table, "id = 2"); n != 1 {
			return fmt.Errorf("the stream has not carried a row before the change")
		}
		return nil
	})

	mustExec(t, src, fmt.Sprintf("ALTER TABLE %s ADD COLUMN note TEXT", table))
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name, note) VALUES (3, 'after', 'new')", table))

	select {
	case err := <-done:
		stopped = true
		if !domain.IsUnrecoverable(err) {
			t.Fatalf("err = %v, want it marked unrecoverable", err)
		}
		if !strings.Contains(err.Error(), "42703") {
			t.Errorf("the refusal does not carry the target's SQLSTATE 42703: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the task was still running 30s after a row with a column the target lacks")
	}
	if n := countRows(t, tgt, table, "id = 3"); n != 0 {
		t.Errorf("the row with the added column reached the target without it")
	}
}
