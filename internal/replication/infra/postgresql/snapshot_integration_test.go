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

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
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
	if n := countRows(t, tgt, table, ""); n > 0 {
		t.Errorf("the refused start copied %d row(s) to the target before refusing", n)
	}
}

// A failure means the first copy wrote a protected field to the target as the source holds it.
func TestTheFirstCopyMasksAndEncryptsTheFieldsATableProtects(t *testing.T) {
	t.Setenv("SYNC_FIELD_KEY", testFieldKey)
	table, publication, slot := names(t, "pg_copy_secured")
	src, tgt := open(t, harness.PostgresSource, sourceDatabase), open(t, harness.PostgresTarget, targetDatabase)

	mustExec(t, src, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email TEXT, card TEXT, score INT)", table))
	mustExec(t, src, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", publication, table))
	t.Cleanup(func() {
		_, _ = src.Exec("DROP PUBLICATION IF EXISTS " + publication)
		_, _ = src.Exec("DROP TABLE IF EXISTS " + table)
		_, _ = tgt.Exec("DROP TABLE IF EXISTS " + table)
		_, _ = src.Exec(`SELECT pg_drop_replication_slot($1)
			WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, slot)
	})
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s VALUES (1, 'ada@example.com', '4111111111111111', 7), "+
		"(2, NULL, NULL, NULL)", table))

	startSyncer(t, syncTask(t, table, publication, slot, config.TableMapping{
		SourceTable: table, TargetTable: table, SecurityEnabled: true,
		FieldSecurity: []interface{}{
			map[string]interface{}{"field": "email", "securityType": "masked"},
			map[string]interface{}{"field": "card", "securityType": "encrypted"},
			map[string]interface{}{"field": "score", "securityType": "masked"},
		},
	}))
	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, ""); n != 2 {
			return fmt.Errorf("target holds %d rows, want 2", n)
		}
		return nil
	})

	var email, card sql.NullString
	var score sql.NullInt64
	read := func(id int) {
		t.Helper()
		if err := tgt.QueryRow(fmt.Sprintf("SELECT email, card, score FROM %s WHERE id = $1", table), id).
			Scan(&email, &card, &score); err != nil {
			t.Fatalf("read row %d: %v", id, err)
		}
	}

	read(1)
	if want := strings.Repeat("*", len("ada@example.com")); email.String != want {
		t.Errorf("email = %q, want %q", email.String, want)
	}
	if plain, err := opened(card.String); err != nil || plain != "4111111111111111" {
		t.Errorf("card = %q, want the source's value encrypted: %v", card.String, err)
	}
	if !score.Valid || score.Int64 != 0 {
		t.Errorf("score = %v, want it masked to 0", score)
	}

	read(2)
	if email.Valid || card.Valid || score.Valid {
		t.Errorf("email = %v, card = %v, score = %v, want all NULL", email, card, score)
	}
}

// A failure means the first copy could not carry a column type the stream carries.
func TestTheCopyCarriesNonTextTypes(t *testing.T) {
	table, publication, slot := names(t, "pg_copy_types")
	src, tgt := open(t, harness.PostgresSource, sourceDatabase), open(t, harness.PostgresTarget, targetDatabase)

	const columns = "u, j, s, a, ip, m, b, n"
	shape := fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, u UUID, j JSONB, s JSONB,
		a INT[], ip INET, m MACADDR, b BYTEA, n NUMERIC)`, table)
	mustExec(t, src, shape)
	mustExec(t, src, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", publication, table))
	mustExec(t, tgt, shape)
	t.Cleanup(func() {
		_, _ = src.Exec("DROP PUBLICATION IF EXISTS " + publication)
		_, _ = src.Exec("DROP TABLE IF EXISTS " + table)
		_, _ = tgt.Exec("DROP TABLE IF EXISTS " + table)
		_, _ = src.Exec(`SELECT pg_drop_replication_slot($1)
			WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, slot)
	})
	mustExec(t, src, fmt.Sprintf(`INSERT INTO %s (id, %s) VALUES
		(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"k": [1, 2], "big": 12345678901234567890}',
			'"a json string"', '{1,NULL,3}', '192.168.0.1/24', '08:00:2b:01:02:03', '\x00ff', 1.10),
		(2, NULL, 'null', NULL, '{}', NULL, NULL, NULL, NULL)`, table, columns))

	startSyncer(t, syncTask(t, table, publication, slot))
	harness.Eventually(t, 45*time.Second, func() error {
		if n := countRows(t, tgt, table, ""); n != 2 {
			return fmt.Errorf("target holds %d rows, want 2", n)
		}
		return nil
	})

	if want, got := textRowsOf(t, src, table, columns), textRowsOf(t, tgt, table, columns); !reflect.DeepEqual(got, want) {
		t.Errorf("target holds %v, source holds %v", got, want)
	}
}

// textRowsOf reads columns of table as PostgreSQL prints them, NULL apart from any text.
func textRowsOf(t *testing.T, db *sql.DB, table, columns string) []string {
	t.Helper()

	names := strings.Split(columns, ", ")
	casts := make([]string, len(names))
	for i, name := range names {
		casts[i] = fmt.Sprintf("COALESCE(%s::text, '<NULL>')", name)
	}
	rows, err := db.Query(fmt.Sprintf("SELECT id, %s FROM %s ORDER BY id", strings.Join(casts, ", "), table))
	if err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()

	var out []string
	for rows.Next() {
		values := make([]string, len(names)+1)
		dest := make([]any, len(values))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			t.Fatalf("scan %s: %v", table, err)
		}
		out = append(out, strings.Join(values, " | "))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	return out
}
