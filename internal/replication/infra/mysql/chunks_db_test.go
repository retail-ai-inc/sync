package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// Walking a table in primary key order, for a re-copy that runs alongside the
// stream. This is the documented remedy for several of the errors that stop a
// task, so it has to work on a table it has never seen.

func oneColumn(match string, values ...string) reply {
	rows := make([][]driver.Value, 0, len(values))
	for _, v := range values {
		rows = append(rows, []driver.Value{v})
	}
	return reply{match: match, columns: []string{"COLUMN_NAME"}, rows: rows}
}

func chunkSource(t *testing.T, replies ...reply) (*Chunks, *fakeDB) {
	t.Helper()
	fake := &fakeDB{replies: replies}
	return &Chunks{
		Source:         fake.open(t),
		Database:       "tenant_trial_naviee",
		TargetDatabase: "tenant_trial_naviee_bk",
	}, fake
}

func clockReply(seconds int64) reply {
	return reply{match: "UNIX_TIMESTAMP", columns: []string{"UNIX_TIMESTAMP()"},
		rows: [][]driver.Value{{seconds}}}
}

// TestTheSourcesClockIsReadBeforeTheRows is the ordering the comment states and
// nothing checked. Taken the other way round, a row changed between the read
// and the clock looks older than it is, and the pipeline could apply it over a
// newer value.
func TestTheSourcesClockIsReadBeforeTheRows(t *testing.T) {
	chunks, fake := chunkSource(t,
		clockReply(1757000000),
		oneColumn("KEY_COLUMN_USAGE", "id"),
		oneColumn("information_schema.COLUMNS", "id", "amount"),
		reply{match: "FROM tenant_trial_naviee.orders",
			columns: []string{"id", "amount"},
			rows:    [][]driver.Value{{int64(1), int64(100)}}},
	)

	chunk, err := chunks.NextChunk(context.Background(),
		domain.Namespace{Object: "orders"}, "", 10)
	if err != nil {
		t.Fatalf("NextChunk: %v", err)
	}
	if chunk.ReadAt.Unix() != 1757000000 {
		t.Errorf("ReadAt = %v, want the source's clock", chunk.ReadAt)
	}

	statements := fake.statements()
	clockAt, rowsAt := -1, -1
	for i, statement := range statements {
		if strings.Contains(statement, "UNIX_TIMESTAMP") {
			clockAt = i
		}
		if strings.Contains(statement, "FROM tenant_trial_naviee.orders") {
			rowsAt = i
		}
	}
	if clockAt < 0 || rowsAt < 0 {
		t.Fatalf("statements: %v", statements)
	}
	if clockAt > rowsAt {
		t.Error("the clock was read after the rows, so a row changed in between " +
			"would look older than it is")
	}
}

func TestAChunkCarriesTheRowsAsUpserts(t *testing.T) {
	chunks, _ := chunkSource(t,
		clockReply(1757000000),
		oneColumn("KEY_COLUMN_USAGE", "id"),
		oneColumn("information_schema.COLUMNS", "id", "amount"),
		reply{match: "FROM tenant_trial_naviee.orders",
			columns: []string{"id", "amount"},
			rows: [][]driver.Value{
				{int64(1), int64(100)},
				{int64(2), int64(250)},
			}},
	)

	chunk, err := chunks.NextChunk(context.Background(),
		domain.Namespace{Object: "orders"}, "", 10)
	if err != nil {
		t.Fatalf("NextChunk: %v", err)
	}
	if len(chunk.Events) != 2 {
		t.Fatalf("the chunk carries %d events, want 2", len(chunk.Events))
	}
	for _, event := range chunk.Events {
		if event.Op != domain.OpInsert {
			t.Errorf("a re-copied row is a %v, want an insert that replaces", event.Op)
		}
		written, ok := event.Payload.(statement)
		if !ok {
			t.Fatalf("the payload is %T", event.Payload)
		}
		if !strings.Contains(written.query, "tenant_trial_naviee_bk") {
			t.Errorf("the row is written to %q, not the target database", written.query)
		}
	}
	if chunk.After != "2" {
		t.Errorf("After = %q, want the last key read", chunk.After)
	}
}

// TestAFullChunkIsNotTheLast: Done is how the pipeline knows to ask again, and
// a chunk that filled the limit almost certainly has more behind it.
func TestAFullChunkIsNotTheLast(t *testing.T) {
	chunks, _ := chunkSource(t,
		clockReply(1),
		oneColumn("KEY_COLUMN_USAGE", "id"),
		oneColumn("information_schema.COLUMNS", "id"),
		reply{match: "FROM tenant_trial_naviee.orders", columns: []string{"id"},
			rows: [][]driver.Value{{int64(1)}, {int64(2)}}},
	)

	chunk, err := chunks.NextChunk(context.Background(),
		domain.Namespace{Object: "orders"}, "", 2)
	if err != nil {
		t.Fatalf("NextChunk: %v", err)
	}
	if chunk.Done {
		t.Error("a chunk that filled the limit reported itself the last one")
	}
}

func TestAShortChunkIsTheLast(t *testing.T) {
	chunks, _ := chunkSource(t,
		clockReply(1),
		oneColumn("KEY_COLUMN_USAGE", "id"),
		oneColumn("information_schema.COLUMNS", "id"),
		reply{match: "FROM tenant_trial_naviee.orders", columns: []string{"id"},
			rows: [][]driver.Value{{int64(1)}}},
	)

	chunk, err := chunks.NextChunk(context.Background(),
		domain.Namespace{Object: "orders"}, "", 10)
	if err != nil {
		t.Fatalf("NextChunk: %v", err)
	}
	if !chunk.Done {
		t.Error("a chunk shorter than the limit did not report itself the last one")
	}
}

// TestTheKeyIsBoundNotInterpolated: the resume point comes from a stored
// position, and the walk continues past it with a bound parameter.
func TestTheKeyIsBoundNotInterpolated(t *testing.T) {
	chunks, fake := chunkSource(t,
		clockReply(1),
		oneColumn("KEY_COLUMN_USAGE", "id"),
		oneColumn("information_schema.COLUMNS", "id"),
		reply{match: "FROM tenant_trial_naviee.orders", columns: []string{"id"}},
	)

	if _, err := chunks.NextChunk(context.Background(),
		domain.Namespace{Object: "orders"}, "1000", 10); err != nil {
		t.Fatalf("NextChunk: %v", err)
	}

	for _, statement := range fake.statements() {
		if strings.Contains(statement, "FROM tenant_trial_naviee.orders") {
			if strings.Contains(statement, "1000") {
				t.Errorf("the resume key was interpolated: %q", statement)
			}
			if !strings.Contains(statement, "> ?") {
				t.Errorf("the resume key was not bound: %q", statement)
			}
		}
	}
}

// TestATableWithNoSingleColumnKeyIsRefusedUnrecoverably. A re-copy walks in key
// order, and there is no order to walk without one; the task has to say so
// rather than retry, because retrying will get the same answer for ever.
func TestATableWithNoSingleColumnKeyIsRefusedUnrecoverably(t *testing.T) {
	for name, key := range map[string]reply{
		"no key":    oneColumn("KEY_COLUMN_USAGE"),
		"composite": oneColumn("KEY_COLUMN_USAGE", "tenant_id", "id"),
	} {
		t.Run(name, func(t *testing.T) {
			chunks, _ := chunkSource(t, clockReply(1), key)

			_, err := chunks.NextChunk(context.Background(),
				domain.Namespace{Object: "orders"}, "", 10)
			if err == nil {
				t.Fatal("a table with no single-column key was walked anyway")
			}
			if !domain.IsUnrecoverable(err) {
				t.Errorf("the refusal is retryable, so the task would ask for ever: %v", err)
			}
		})
	}
}

func TestATableWithNoColumnsIsReported(t *testing.T) {
	chunks, _ := chunkSource(t,
		clockReply(1),
		oneColumn("KEY_COLUMN_USAGE", "id"),
		oneColumn("information_schema.COLUMNS"),
	)

	if _, err := chunks.NextChunk(context.Background(),
		domain.Namespace{Object: "orders"}, "", 10); err == nil {
		t.Fatal("a table with no columns was walked anyway")
	}
}

func TestAClockThatCannotBeReadStopsTheChunk(t *testing.T) {
	chunks, _ := chunkSource(t,
		reply{match: "UNIX_TIMESTAMP", err: errors.New("the server went away")})

	if _, err := chunks.NextChunk(context.Background(),
		domain.Namespace{Object: "orders"}, "", 10); err == nil {
		t.Fatal("a chunk was returned with no clock, so the pipeline could not " +
			"tell whether it was safe to apply")
	}
}

// TestTheTargetTableIsResolved covers a mapping that renames: the rows are read
// from the source's name and written to the target's.
func TestTheTargetTableIsResolved(t *testing.T) {
	chunks, _ := chunkSource(t,
		clockReply(1),
		oneColumn("KEY_COLUMN_USAGE", "id"),
		oneColumn("information_schema.COLUMNS", "id"),
		reply{match: "FROM tenant_trial_naviee.orders", columns: []string{"id"},
			rows: [][]driver.Value{{int64(1)}}},
	)
	chunks.TargetOf = func(string) string { return "orders_archive" }

	chunk, err := chunks.NextChunk(context.Background(),
		domain.Namespace{Object: "orders"}, "", 10)
	if err != nil {
		t.Fatalf("NextChunk: %v", err)
	}
	written := chunk.Events[0].Payload.(statement)
	if !strings.Contains(written.query, "orders_archive") {
		t.Errorf("the row was not written to the mapped table: %q", written.query)
	}
}

// TestGetTableColumnsLeavesOutGeneratedColumns exercises the discovery itself
// rather than the predicate under it. A generated column is computed by the
// server, which refuses a write that supplies one -- sending every column the
// source had made any table holding one impossible to copy, and with it every
// table whose foreign key pointed at it.
func TestGetTableColumnsLeavesOutGeneratedColumns(t *testing.T) {
	fake := &fakeDB{replies: []reply{{
		match:   "SHOW COLUMNS FROM",
		columns: []string{"Field", "Type", "Null", "Key", "Default", "Extra"},
		rows: [][]driver.Value{
			{"id", "bigint", "NO", "PRI", nil, "auto_increment"},
			{"total", "decimal(10,2)", "YES", "", nil, "VIRTUAL GENERATED"},
			{"amount", "decimal(10,2)", "YES", "", nil, ""},
			{"tax", "decimal(10,2)", "YES", "", nil, "STORED GENERATED"},
			{"created_at", "timestamp", "NO", "", "CURRENT_TIMESTAMP", "DEFAULT_GENERATED"},
		},
	}}}
	db := fake.open(t)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("take a connection: %v", err)
	}
	defer conn.Close()

	syncer := &MySQLSyncer{}
	columns, err := syncer.getTableColumns(context.Background(), conn, "db", "orders")
	if err != nil {
		t.Fatalf("getTableColumns: %v", err)
	}

	want := []string{"id", "amount", "created_at"}
	if len(columns) != len(want) {
		t.Fatalf("got %v, want %v", columns, want)
	}
	for i := range want {
		if columns[i] != want[i] {
			t.Errorf("got %v, want %v", columns, want)
			break
		}
	}
}

// TestAColumnWithNoNameIsReported: a row from SHOW COLUMNS with a null Field
// cannot be part of a statement, and silently dropping it would write a row
// missing a column.
func TestAColumnWithNoNameIsReported(t *testing.T) {
	fake := &fakeDB{replies: []reply{{
		match:   "SHOW COLUMNS FROM",
		columns: []string{"Field", "Type", "Null", "Key", "Default", "Extra"},
		rows:    [][]driver.Value{{nil, "bigint", "NO", "", nil, ""}},
	}}}
	db := fake.open(t)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("take a connection: %v", err)
	}
	defer conn.Close()

	syncer := &MySQLSyncer{}
	if _, err := syncer.getTableColumns(context.Background(), conn, "db", "orders"); err == nil {
		t.Fatal("a column with no name was accepted")
	}
}
