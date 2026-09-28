//go:build integration

package postgresql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lib/pq"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/test/harness"
)

// copyRows keeps the second table's copy streaming for several seconds after the first has landed.
const copyRows = 20000

// A failure means an update decoded while the copy held the source connection matched nothing on the target.
func TestAnUpdateMadeDuringTheCopyArrives(t *testing.T) {
	table, publication, slot := names(t, "pg_busy")
	large := table + "_large"
	src, tgt := open(t, harness.PostgresSource, sourceDatabase), open(t, harness.PostgresTarget, targetDatabase)

	mustExec(t, src, fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, name TEXT)`, table))
	mustExec(t, src, fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, name TEXT)`, large))
	mustExec(t, src, fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE %s, %s`, publication, table, large))
	t.Cleanup(func() {
		_, _ = src.Exec("DROP PUBLICATION IF EXISTS " + publication)
		for _, name := range []string{table, large} {
			_, _ = src.Exec("DROP TABLE IF EXISTS " + name)
			_, _ = tgt.Exec("DROP TABLE IF EXISTS " + name)
		}
		_, _ = src.Exec(`SELECT pg_drop_replication_slot($1)
			WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, slot)
	})
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (7, 'before')", table))
	mustExec(t, src, fmt.Sprintf(
		"INSERT INTO %s (id, name) SELECT g, 'row ' || g FROM generate_series(1, %d) g", large, copyRows))

	startSyncer(t, syncTask(t, table, publication, slot,
		config.TableMapping{SourceTable: table, TargetTable: table},
		config.TableMapping{SourceTable: large, TargetTable: large}))
	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, ""); n != 1 {
			return fmt.Errorf("the first table's copy has not landed: %d rows", n)
		}
		return nil
	})
	if n := countRows(t, tgt, large, ""); n == copyRows {
		t.Fatalf("the second table's copy finished before the update was made, so nothing ran alongside it")
	}

	mustExec(t, src, fmt.Sprintf("UPDATE %s SET name = 'after' WHERE id = 7", table))

	harness.Eventually(t, 90*time.Second, func() error {
		if n := countRows(t, tgt, large, ""); n != copyRows {
			return fmt.Errorf("the second table's copy has not landed: %d rows", n)
		}
		if n := countRows(t, tgt, table, "id = 7 AND name = 'after'"); n != 1 {
			return fmt.Errorf("the update made during the copy has not arrived")
		}
		return nil
	})
}

// A failure means a table whose name needs quoting could not have its key read.
func TestTheKeyOfAMixedCaseTableIsRead(t *testing.T) {
	table := "Mixed_" + harness.UniqueName("pg_keys")
	src := open(t, harness.PostgresSource, sourceDatabase)
	mustExec(t, src, fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, name TEXT)`, pq.QuoteIdentifier(table)))
	t.Cleanup(func() { _, _ = src.Exec("DROP TABLE IF EXISTS " + pq.QuoteIdentifier(table)) })

	conn, err := pgx.Connect(t.Context(), connString(harness.PostgresSource, sourceDatabase))
	if err != nil {
		t.Fatalf("connect to the source: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	keys, err := (&schemaWork{Source: conn, Logger: quiet()}).primaryKey("public", table)
	if err != nil {
		t.Fatalf("primaryKey(public, %s): %v", table, err)
	}
	if len(keys) != 1 || keys[0] != "id" {
		t.Errorf("keys = %v, want [id]", keys)
	}
}
