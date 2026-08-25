//go:build integration

package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/test/harness"
)

// TestAUniqueValueHandedFromOneRowToAnother is why the statements of a batch go
// to the target in the order the binlog held them.
//
// The pipeline can split a batch into runs that may be applied independently,
// and it decides independence by the primary key: two changes to one row keep
// their order, everything else may move. That is not enough. Two rows are not
// independent when a unique index relates them — and handing a unique value from
// one row to another is an ordinary thing for an application to do.
//
// Here the split moves the INSERT that takes the value ahead of the UPDATE that
// frees it. The INSERT is an upsert, because replication is replayed, so it does
// not fail: ON DUPLICATE KEY UPDATE fires on the unique index instead of the
// primary key and rewrites the row that still holds the value, changing its
// primary key. The UPDATE that follows then matches nothing. Two rows on the
// source, one on the target, no error anywhere.
func TestAUniqueValueHandedFromOneRowToAnother(t *testing.T) {
	src, tgt := open(t, harness.MySQLSource, sourceDB), open(t, harness.MySQLTarget, targetDB)

	table := harness.UniqueName("handover")
	mustExec(t, src, fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(80) UNIQUE, n INT)`, table))
	t.Cleanup(func() {
		_, _ = src.Exec("DROP TABLE IF EXISTS " + table)
		_, _ = tgt.Exec("DROP TABLE IF EXISTS " + table)
	})
	mustExec(t, src, fmt.Sprintf(
		`INSERT INTO %s (id, email, n) VALUES (1, 'a@b', 0)`, table))

	cfg := syncTask(t, table, config.TableMapping{SourceTable: table, TargetTable: table})
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- NewSyncer(cfg, logger).Start(ctx) }()

	// Wait for the copy to land before writing, so the three statements below
	// arrive on the stream rather than in the snapshot.
	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, ""); n != 1 {
			return fmt.Errorf("target holds %d rows, want the copy's 1", n)
		}
		return nil
	})

	// One transaction, so the three land in one batch: a batch is only ever cut
	// on a transaction boundary.
	tx, err := src.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	for _, statement := range []string{
		fmt.Sprintf("UPDATE %s SET n = 1 WHERE id = 1", table),
		fmt.Sprintf("UPDATE %s SET email = 'freed' WHERE id = 1", table),
		fmt.Sprintf("INSERT INTO %s (id, email, n) VALUES (2, 'a@b', 0)", table),
	} {
		if _, err := tx.Exec(statement); err != nil {
			t.Fatalf("exec %q: %v", statement, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, ""); n != 2 {
			return fmt.Errorf("target holds %d rows, want 2", n)
		}
		return nil
	})

	// Both rows, each with the value the source left it holding.
	for _, want := range []struct {
		id    int
		email string
		n     int
	}{
		{1, "freed", 1},
		{2, "a@b", 0},
	} {
		var email string
		var n int
		err := tgt.QueryRow(fmt.Sprintf("SELECT email, n FROM %s WHERE id = ?", table), want.id).
			Scan(&email, &n)
		if err != nil {
			t.Errorf("row %d is not on the target: %v", want.id, err)
			continue
		}
		if email != want.email || n != want.n {
			t.Errorf("row %d = (%q, %d), want (%q, %d)", want.id, email, n, want.email, want.n)
		}
	}

	cancel()
	<-done
}
