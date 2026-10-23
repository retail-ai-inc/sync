//go:build integration

package postgresql

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/test/harness"
)

// A failure means a change to one key was applied ahead of an earlier change to another.
func TestABatchIsAppliedInTheOrderTheWALHeldIt(t *testing.T) {
	table, publication, slot := names(t, "pg_ordering")
	src, tgt := open(t, harness.PostgresSource, sourceDatabase), open(t, harness.PostgresTarget, targetDatabase)

	sourceTable(t, src, tgt, table, publication, slot)
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (10, 'seed')", table))

	startSyncer(t, syncTask(t, table, publication, slot))
	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, ""); n != 1 {
			return fmt.Errorf("the initial copy has not landed: %d rows", n)
		}
		return nil
	})

	tx, err := src.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	for _, statement := range []string{
		fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'a')", table),
		fmt.Sprintf("DELETE FROM %s WHERE id = 1", table),
		fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'b')", table),
		fmt.Sprintf("UPDATE %s SET id = 2 WHERE id = 1", table),
	} {
		if _, err := tx.Exec(statement); err != nil {
			t.Fatalf("exec %q: %v", statement, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, "id = 2"); n != 1 {
			return fmt.Errorf("the transaction has not arrived")
		}
		return nil
	})

	want, got := rowsOf(t, src, table), rowsOf(t, tgt, table)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("target holds %v, source holds %v", got, want)
	}
}

func rowsOf(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()

	rows, err := db.Query(fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", table))
	if err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()

	var out []string
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("scan %s: %v", table, err)
		}
		out = append(out, fmt.Sprintf("(%d, %s)", id, name))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	return out
}
