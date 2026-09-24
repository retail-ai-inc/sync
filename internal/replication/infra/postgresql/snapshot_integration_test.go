//go:build integration

package postgresql

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/test/harness"
)

// A failure means a row committed after the slot was created was both copied and streamed.
func TestARowWrittenAfterTheSlotAndBeforeTheCopyLandsOnce(t *testing.T) {
	for _, tt := range []struct{ name, columns string }{
		{"with a primary key", "id INT PRIMARY KEY, name TEXT"},
		{"with no primary key", "id INT, name TEXT"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			table, publication, slot := names(t, "pg_overlap")
			src, tgt := open(t, harness.PostgresSource, sourceDatabase), open(t, harness.PostgresTarget, targetDatabase)

			mustExec(t, src, fmt.Sprintf("CREATE TABLE %s (%s)", table, tt.columns))
			mustExec(t, src, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", publication, table))
			mustExec(t, tgt, fmt.Sprintf("CREATE TABLE %s (%s)", table, tt.columns))
			t.Cleanup(func() {
				_, _ = src.Exec("DROP PUBLICATION IF EXISTS " + publication)
				_, _ = src.Exec("DROP TABLE IF EXISTS " + table)
				_, _ = tgt.Exec("DROP TABLE IF EXISTS " + table)
				_, _ = src.Exec(`SELECT pg_drop_replication_slot($1)
					WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, slot)
			})
			mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'before the slot')", table))

			hold, err := tgt.Begin()
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			t.Cleanup(func() { _ = hold.Rollback() })
			if _, err := hold.Exec("LOCK TABLE " + table + " IN ACCESS EXCLUSIVE MODE"); err != nil {
				t.Fatalf("lock the target table: %v", err)
			}

			startSyncer(t, syncTask(t, table, publication, slot))
			harness.Eventually(t, 45*time.Second, func() error {
				if n := countRows(t, tgt, "pg_stat_activity", "wait_event_type = 'Lock' AND query LIKE $1",
					"%FROM public."+table+"%"); n < 1 {
					return fmt.Errorf("the copy is not waiting on the target table yet")
				}
				return nil
			})
			mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (2, 'after the slot')", table))
			if err := hold.Rollback(); err != nil {
				t.Fatalf("release the target table: %v", err)
			}
			harness.Eventually(t, 45*time.Second, func() error {
				if n := countRows(t, tgt, table, "id = 1"); n < 1 {
					return fmt.Errorf("the copy has not landed")
				}
				return nil
			})

			mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (3, 'after the copy')", table))
			harness.Eventually(t, 45*time.Second, func() error {
				if n := countRows(t, tgt, table, "id = 3"); n < 1 {
					return fmt.Errorf("the row written after the copy has not arrived, so the stream stopped")
				}
				return nil
			})
			if want, got := rowsOf(t, src, table), rowsOf(t, tgt, table); !reflect.DeepEqual(got, want) {
				t.Errorf("target holds %v, source holds %v", got, want)
			}
		})
	}
}

// A failure means a leftover slot was streamed from with a copy that cannot be aligned with it.
func TestASlotLeftWithoutAStoredPositionStopsTheTask(t *testing.T) {
	table, publication, slot := names(t, "pg_leftover_slot")
	src, tgt := open(t, harness.PostgresSource, sourceDatabase), open(t, harness.PostgresTarget, targetDatabase)
	sourceTable(t, src, tgt, table, publication, slot)
	mustExec(t, src, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slot)
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'after the slot')", table))

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	err := NewPostgreSQLSyncer(syncTask(t, table, publication, slot), logger).Start(ctx)
	if ctx.Err() != nil {
		t.Fatalf("Start was still running after the timeout: %v", err)
	}
	if !domain.IsUnrecoverable(err) {
		t.Fatalf("err = %v, want it marked unrecoverable", err)
	}
	if !strings.Contains(err.Error(), slot) {
		t.Errorf("the refusal does not name the slot %s: %v", slot, err)
	}
	if n := countRows(t, src, "pg_replication_slots", "slot_name = $1", slot); n != 1 {
		t.Errorf("the refused start left %d replication slot(s) %s, want the one it found", n, slot)
	}
}
