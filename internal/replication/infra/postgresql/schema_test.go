package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/retail-ai-inc/sync/internal/platform/config"
)

// Reading the source's shape and preparing the target's.
//
// These reached straight for a *pgx.Conn, which is a concrete type with no
// interface behind it, so none of it could be exercised without a PostgreSQL
// and none of it was. They now ask one method of the source, which is enough to
// stand in for.

// cannedRows answers a query with prepared columns and rows.
type cannedRows struct {
	columns []string
	rows    [][]any
	at      int
	err     error
}

func (c *cannedRows) Close()                        {}
func (c *cannedRows) Err() error                    { return c.err }
func (c *cannedRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (c *cannedRows) RawValues() [][]byte           { return nil }
func (c *cannedRows) Conn() *pgx.Conn               { return nil }

func (c *cannedRows) FieldDescriptions() []pgconn.FieldDescription {
	fields := make([]pgconn.FieldDescription, len(c.columns))
	for i, name := range c.columns {
		fields[i] = pgconn.FieldDescription{Name: name}
	}
	return fields
}

func (c *cannedRows) Next() bool {
	if c.at >= len(c.rows) {
		return false
	}
	c.at++
	return true
}

func (c *cannedRows) Values() ([]any, error) { return c.rows[c.at-1], nil }

func (c *cannedRows) Scan(dest ...any) error {
	row := c.rows[c.at-1]
	for i := range dest {
		if i >= len(row) {
			break
		}
		switch target := dest[i].(type) {
		case *string:
			if row[i] == nil {
				*target = ""
				continue
			}
			*target = row[i].(string)
		case *sql.NullString:
			if row[i] == nil {
				*target = sql.NullString{}
				continue
			}
			*target = sql.NullString{String: row[i].(string), Valid: true}
		case *sql.NullInt64:
			if row[i] == nil {
				*target = sql.NullInt64{}
				continue
			}
			*target = sql.NullInt64{Int64: row[i].(int64), Valid: true}
		}
	}
	return nil
}

// answering is a source that replies to queries matched by substring, in order.
type answering struct {
	replies []sourceReply
	asked   []string
}

type sourceReply struct {
	match   string
	columns []string
	rows    [][]any
	err     error
}

func (a *answering) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	a.asked = append(a.asked, sql)
	for _, reply := range a.replies {
		if strings.Contains(sql, reply.match) {
			if reply.err != nil {
				return nil, reply.err
			}
			return &cannedRows{columns: reply.columns, rows: reply.rows}, nil
		}
	}
	return nil, errors.New("the fake source was not told how to answer: " + sql)
}

func TestThePrimaryKeyIsReadInOrder(t *testing.T) {
	source := &answering{replies: []sourceReply{{
		match:   "indisprimary",
		columns: []string{"attname"},
		rows:    [][]any{{"account"}, {"entry"}},
	}}}
	work := &schemaWork{Source: source, Logger: quiet()}

	keys, err := work.primaryKey("public", "entries")
	if err != nil {
		t.Fatalf("primaryKey: %v", err)
	}
	if len(keys) != 2 || keys[0] != "account" || keys[1] != "entry" {
		t.Errorf("keys = %v, want the composite key in order", keys)
	}
}

// TestATableWithNoKeyReadsAsNone rather than as an error: a row is then
// addressed by every column, which still finds it.
func TestATableWithNoKeyReadsAsNone(t *testing.T) {
	source := &answering{replies: []sourceReply{{
		match: "indisprimary", columns: []string{"attname"},
	}}}
	work := &schemaWork{Source: source, Logger: quiet()}

	keys, err := work.primaryKey("public", "events")
	if err != nil {
		t.Fatalf("primaryKey: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("keys = %v, want none", keys)
	}
}

func TestASourceThatWillNotAnswerAboutKeysIsReported(t *testing.T) {
	source := &answering{replies: []sourceReply{{
		match: "indisprimary", err: errors.New("permission denied"),
	}}}
	work := &schemaWork{Source: source, Logger: quiet()}

	if _, err := work.primaryKey("public", "orders"); err == nil {
		t.Error("a source that refused was reported as a table with no key, so " +
			"every row would be addressed by every column without saying why")
	}
}

// TestTheCreateStatementCarriesTheColumnTypes covers the shape the target is
// given when a table is missing from it.
func TestTheCreateStatementCarriesTheColumnTypes(t *testing.T) {
	source := &answering{replies: []sourceReply{{
		match:   "information_schema.columns",
		columns: []string{"column_name", "data_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale"},
		rows: [][]any{
			{"id", "integer", "NO", "nextval('orders_id_seq'::regclass)", nil, nil, nil},
			{"name", "character varying", "YES", nil, nil, nil, nil},
			{"amount", "numeric", "NO", nil, nil, nil, nil},
		},
	}}}
	work := &schemaWork{Source: source, Logger: quiet()}

	create, sequences, err := work.createTableSQL(context.Background(),
		"public", "orders", "public", "orders")
	if err != nil {
		t.Fatalf("createTableSQL: %v", err)
	}

	for _, want := range []string{`"public"."orders"`, "id integer", "name character varying", "amount numeric"} {
		if !strings.Contains(create, want) {
			t.Errorf("the statement is missing %q: %s", want, create)
		}
	}
	if !strings.Contains(create, "NOT NULL") {
		t.Errorf("a NOT NULL column was made nullable: %s", create)
	}
	if len(sequences) != 1 || !strings.Contains(sequences[0], "orders_id_seq") {
		t.Errorf("sequences = %v, want the one the default draws from -- without it "+
			"the create fails on a column whose default names a sequence that is "+
			"not there", sequences)
	}
}

func TestExtractSequenceNameReadsTheDefault(t *testing.T) {
	for in, want := range map[string]string{
		"nextval('orders_id_seq'::regclass)":        "orders_id_seq",
		"nextval('public.orders_id_seq'::regclass)": "public.orders_id_seq",
		"now()":        "",
		"":             "",
		"nextval('x')": "",
	} {
		if got := extractSequenceName(in); got != want {
			t.Errorf("extractSequenceName(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestTheCopyLeavesAPopulatedTableAlone. Skipping is what makes a restarted
// task cheap; it is also why the copy is not a repair, and a table with one row
// in it is left as it is.
func TestTheCopyLeavesAPopulatedTableAlone(t *testing.T) {
	target := schemaTargetDB(t)
	if _, err := target.Exec(`INSERT INTO public.orders VALUES ('1','100')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	source := &answering{}

	snap := snapshotOver(source, target, [2]string{"orders", "orders"})
	if err := snap.Copy(context.Background()); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if len(source.asked) != 0 {
		t.Errorf("the source was read for a table that already holds rows: %v", source.asked)
	}
}

func TestTheCopyFillsAnEmptyTable(t *testing.T) {
	target := schemaTargetDB(t)
	source := &answering{replies: []sourceReply{{
		match:   "SELECT * FROM public.orders",
		columns: []string{"id", "amount"},
		rows:    [][]any{{"1", "100"}, {"2", "200"}},
	}}}

	snap := snapshotOver(source, target, [2]string{"orders", "orders"})
	if err := snap.Copy(context.Background()); err != nil {
		t.Fatalf("Copy: %v", err)
	}

	var count int
	if err := target.QueryRow(`SELECT COUNT(*) FROM public.orders`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Errorf("%d rows were copied, want 2", count)
	}
}

// TestACopyOfNothingSaysSo: a task that names no tables replicates nothing, and
// finishing quietly makes that look like success.
func TestACopyOfNothingSaysSo(t *testing.T) {
	snap := &Snapshotter{
		Schema: &schemaWork{Logger: quiet()},
		Config: config.SyncConfig{},
		Logger: quiet(),
	}
	if err := snap.Copy(context.Background()); err != nil {
		t.Fatalf("Copy with no tables: %v", err)
	}
	if len(snap.pairs()) != 0 {
		t.Error("pairs were found where the task names none")
	}
}

func TestAMappingWithNoSchemaMeansPublic(t *testing.T) {
	snap := &Snapshotter{Config: config.SyncConfig{
		Mappings: []config.DatabaseMapping{{
			Tables: []config.TableMapping{{SourceTable: "orders"}},
		}},
	}}

	pairs := snap.pairs()
	if len(pairs) != 1 {
		t.Fatalf("pairs = %v", pairs)
	}
	if pairs[0].source() != "public.orders" || pairs[0].target() != "public.orders" {
		t.Errorf("pair = %s -> %s, want public on both sides and the source's name "+
			"carried over", pairs[0].source(), pairs[0].target())
	}
}

// TestPinIsTheSlotsConsistentPoint: the copy and the stream meet there, so
// every change made while the copy ran is still in the log.
func TestPinIsTheSlotsConsistentPoint(t *testing.T) {
	snap := &Snapshotter{ConsistentPoint: 1 << 32, Source: "tokyo:5432/shop"}

	pos, err := snap.Pin(context.Background())
	if err != nil {
		t.Fatalf("Pin: %v", err)
	}
	lsn, _, err := decodeLSN(pos.Payload, "tokyo:5432/shop")
	if err != nil {
		t.Fatalf("the pinned position does not read back: %v", err)
	}
	if lsn != 1<<32 {
		t.Errorf("pinned %s, want the consistent point", lsn)
	}
}

// TestAnExistingSlotPinsNothing. Its own position is where the stream resumes,
// and taking the server's current one instead would skip everything committed
// while the task was down.
func TestAnExistingSlotPinsNothing(t *testing.T) {
	pos, err := (&Snapshotter{}).Pin(context.Background())
	if err != nil {
		t.Fatalf("Pin: %v", err)
	}
	if !pos.IsZero() {
		t.Errorf("Pin returned %v for a slot that was already there", pos)
	}
}

// schemaTargetDB gives SQLite a database attached as "public", so the
// schema-qualified names the copy builds resolve the way they do on PostgreSQL.
func schemaTargetDB(t *testing.T) *sql.DB {
	t.Helper()

	db := targetDB(t, "")
	if _, err := db.Exec(`ATTACH DATABASE ':memory:' AS public`); err != nil {
		t.Fatalf("attach a schema: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE public.orders (id TEXT, amount TEXT)`); err != nil {
		t.Fatalf("create the table: %v", err)
	}
	return db
}

func snapshotOver(source sourceQuerier, target *sql.DB, tables ...[2]string) *Snapshotter {
	mapped := make([]config.TableMapping, 0, len(tables))
	for _, pair := range tables {
		mapped = append(mapped, config.TableMapping{SourceTable: pair[0], TargetTable: pair[1]})
	}
	work := &schemaWork{Source: source, Target: target, Logger: quiet()}
	return &Snapshotter{
		Schema: work,
		Config: config.SyncConfig{Mappings: []config.DatabaseMapping{{Tables: mapped}}},
		Logger: quiet(),
	}
}

// TestATargetTableThatCannotBeCountedStopsTheCopy covers a silent skip that is
// now a stop.
//
// It used to warn and carry on, so a table missing from the target -- because
// the schema preparation failed, or because nobody created it -- left the copy
// doing nothing and reporting success. The link then ran with a table that had
// never been filled, and only a full comparison would ever have said so.
func TestATargetTableThatCannotBeCountedStopsTheCopy(t *testing.T) {
	target := targetDB(t, "") // no tables at all
	source := &answering{}

	err := snapshotOver(source, target, [2]string{"orders", "orders"}).
		Copy(context.Background())
	if err == nil {
		t.Fatal("a table missing from the target was skipped and the copy " +
			"reported success")
	}
	if !strings.Contains(err.Error(), "orders") {
		t.Errorf("the error does not name the table: %v", err)
	}
}
