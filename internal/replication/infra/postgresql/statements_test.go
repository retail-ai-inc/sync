package postgresql

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/retail-ai-inc/sync/internal/platform/config"
)

const ordersSchema = `CREATE TABLE orders (id TEXT, customer TEXT, email TEXT)`

// rows returns every row of the target table as pipe-joined text, so a test can
// assert on what the generated statement actually did.
func rows(t *testing.T, db *sql.DB) []string {
	t.Helper()

	res, err := db.Query(`SELECT COALESCE(id,'<null>'), COALESCE(customer,'<null>'),
		COALESCE(email,'<null>') FROM orders ORDER BY rowid`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer res.Close()

	var out []string
	for res.Next() {
		var a, b, c string
		if err := res.Scan(&a, &b, &c); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, strings.Join([]string{a, b, c}, "|"))
	}
	return out
}

func insertMessage(relID uint32, tup *pglogrepl.TupleData) *pglogrepl.InsertMessageV2 {
	msg := &pglogrepl.InsertMessageV2{}
	msg.RelationID = relID
	msg.Tuple = tup
	return msg
}

func updateMessage(relID uint32, oldTup, newTup *pglogrepl.TupleData) *pglogrepl.UpdateMessageV2 {
	msg := &pglogrepl.UpdateMessageV2{}
	msg.RelationID = relID
	msg.OldTuple = oldTup
	msg.NewTuple = newTup
	return msg
}

func deleteMessage(relID uint32, oldTup *pglogrepl.TupleData) *pglogrepl.DeleteMessageV2 {
	msg := &pglogrepl.DeleteMessageV2{}
	msg.RelationID = relID
	msg.OldTuple = oldTup
	return msg
}

// ------------------------------------------------------------------- insert

func TestHandleInsertWritesTheRow(t *testing.T) {
	db := targetDB(t, ordersSchema)
	rel := relation(1, "main", "orders", "id", "customer", "email")
	st := stateWith(db, rel)

	commit, err := newSyncer(t, config.SyncConfig{}).handleInsert(
		insertMessage(1, tuple(text("1"), text("Ada"), text("ada@example.com"))), st)
	if err != nil {
		t.Fatalf("handleInsert: %v", err)
	}
	if commit {
		t.Error("handleInsert reported a commit")
	}
	if got := rows(t, db); len(got) != 1 || got[0] != "1|Ada|ada@example.com" {
		t.Errorf("rows = %v", got)
	}
}

func TestHandleInsertWritesNullForANullColumn(t *testing.T) {
	db := targetDB(t, ordersSchema)
	st := stateWith(db, relation(1, "main", "orders", "id", "customer", "email"))

	if _, err := newSyncer(t, config.SyncConfig{}).handleInsert(
		insertMessage(1, tuple(text("1"), nil, text("x"))), st); err != nil {
		t.Fatalf("handleInsert: %v", err)
	}
	if got := rows(t, db); len(got) != 1 || got[0] != "1|<null>|x" {
		t.Errorf("rows = %v", got)
	}
}

// TestAnUnchangedToastedValueBecomesNull records a data-loss path: the 'u'
// column type means "unchanged TOASTed value, not sent", and the insert builder
// maps it to NULL along with every other unrecognised type. A large text column
// that PostgreSQL declined to resend is therefore replicated as NULL rather than
// left alone.
func TestAnUnchangedToastedValueBecomesNull(t *testing.T) {
	db := targetDB(t, ordersSchema)
	st := stateWith(db, relation(1, "main", "orders", "id", "customer", "email"))

	tup := tuple(text("1"), text("Ada"), text("x"))
	tup.Columns[2] = &pglogrepl.TupleDataColumn{DataType: 'u'}

	if _, err := newSyncer(t, config.SyncConfig{}).handleInsert(
		insertMessage(1, tup), st); err != nil {
		t.Fatalf("handleInsert: %v", err)
	}
	if got := rows(t, db); len(got) != 1 || got[0] != "1|Ada|<null>" {
		t.Errorf("rows = %v; an unchanged TOASTed value appears to be handled now, "+
			"so assert that instead", got)
	}
}

// TestABinaryColumnBecomesNull records the same gap for 'b': the plugin can send
// binary-formatted values and the builder discards them.
func TestABinaryColumnBecomesNull(t *testing.T) {
	db := targetDB(t, ordersSchema)
	st := stateWith(db, relation(1, "main", "orders", "id", "customer", "email"))

	tup := tuple(text("1"), text("Ada"), text("x"))
	tup.Columns[2] = &pglogrepl.TupleDataColumn{DataType: 'b', Data: []byte{0x01, 0x02}}

	if _, err := newSyncer(t, config.SyncConfig{}).handleInsert(
		insertMessage(1, tup), st); err != nil {
		t.Fatalf("handleInsert: %v", err)
	}
	if got := rows(t, db); got[0] != "1|Ada|<null>" {
		t.Errorf("rows = %v, want the binary column dropped", got)
	}
}

// TestHandleInsertEscapesQuotes is the injection guard: values are interpolated
// into the statement text rather than bound, so the doubling of single quotes is
// the only thing standing between replicated data and arbitrary SQL.
func TestHandleInsertEscapesQuotes(t *testing.T) {
	db := targetDB(t, ordersSchema)
	st := stateWith(db, relation(1, "main", "orders", "id", "customer", "email"))

	name := `O'Brien'); DROP TABLE orders; --`
	if _, err := newSyncer(t, config.SyncConfig{}).handleInsert(
		insertMessage(1, tuple(text("1"), text(name), text("x"))), st); err != nil {
		t.Fatalf("handleInsert: %v", err)
	}

	got := rows(t, db)
	if len(got) != 1 || !strings.Contains(got[0], name) {
		t.Errorf("rows = %v, want the value stored verbatim", got)
	}
}

func TestHandleInsertSkipsAnUnknownRelation(t *testing.T) {
	db := targetDB(t, ordersSchema)
	st := stateWith(db)

	commit, err := newSyncer(t, config.SyncConfig{}).handleInsert(
		insertMessage(99, tuple(text("1"))), st)
	if err != nil || commit {
		t.Fatalf("handleInsert = %v, %v; an unknown relation should be skipped "+
			"silently", commit, err)
	}
	if got := rows(t, db); len(got) != 0 {
		t.Errorf("rows = %v", got)
	}
}

func TestHandleInsertSkipsAnEmptyTuple(t *testing.T) {
	db := targetDB(t, ordersSchema)
	st := stateWith(db, relation(1, "main", "orders", "id"))

	if _, err := newSyncer(t, config.SyncConfig{}).handleInsert(
		insertMessage(1, nil), st); err != nil {
		t.Fatalf("handleInsert: %v", err)
	}
	if got := rows(t, db); len(got) != 0 {
		t.Errorf("rows = %v", got)
	}
}

// TestExtraTupleColumnsAreDroppedButStillCounted records the shape bug in the
// insert builder: it sizes its name and value slices from the tuple, then skips
// the columns the relation does not describe. The skipped positions stay as
// empty strings, so the generated statement carries a trailing ", " pair and
// fails to parse. A relation message that arrives after a schema change — more
// columns in the tuple than in the last relation seen — therefore breaks
// replication rather than dropping the extra column.
func TestExtraTupleColumnsAreDroppedButStillCounted(t *testing.T) {
	db := targetDB(t, ordersSchema)
	st := stateWith(db, relation(1, "main", "orders", "id", "customer"))

	_, err := newSyncer(t, config.SyncConfig{}).handleInsert(
		insertMessage(1, tuple(text("1"), text("Ada"), text("extra"))), st)
	if err == nil {
		t.Fatalf("handleInsert accepted a wider tuple; the mismatch appears to be " +
			"handled now, so assert that instead")
	}
	if got := rows(t, db); len(got) != 0 {
		t.Errorf("rows = %v", got)
	}
}

// TestHandleInsertMasksASecuredField records that field masking is applied on
// the way out, so the target holds the masked text and the original never
// reaches it.
func TestHandleInsertMasksASecuredField(t *testing.T) {
	db := targetDB(t, ordersSchema)
	st := stateWith(db, relation(1, "main", "orders", "id", "customer", "email"))
	cfg := config.SyncConfig{Mappings: mappingWithSecurity("orders", "email")}

	if _, err := newSyncer(t, cfg).handleInsert(
		insertMessage(1, tuple(text("1"), text("Ada"), text("ada@example.com"))), st); err != nil {
		t.Fatalf("handleInsert: %v", err)
	}

	got := rows(t, db)
	if len(got) != 1 {
		t.Fatalf("rows = %v", got)
	}
	if strings.Contains(got[0], "ada@example.com") {
		t.Errorf("row = %q, want the address masked", got[0])
	}
	if !strings.HasPrefix(got[0], "1|Ada|") {
		t.Errorf("row = %q, want the other columns untouched", got[0])
	}
}

// TestMaskingIsNotAppliedToANullColumn records that a NULL stays NULL: the
// masking call sits inside the text branch only, so a secured field that is
// null is replicated as null rather than as the masked form of the empty
// string. That is the right answer, and it means null is distinguishable from
// masked in the target.
func TestMaskingIsNotAppliedToANullColumn(t *testing.T) {
	db := targetDB(t, ordersSchema)
	st := stateWith(db, relation(1, "main", "orders", "id", "customer", "email"))
	cfg := config.SyncConfig{Mappings: mappingWithSecurity("orders", "email")}

	if _, err := newSyncer(t, cfg).handleInsert(
		insertMessage(1, tuple(text("1"), text("Ada"), nil)), st); err != nil {
		t.Fatalf("handleInsert: %v", err)
	}
	if got := rows(t, db); got[0] != "1|Ada|<null>" {
		t.Errorf("rows = %v", got)
	}
}

// ------------------------------------------------------------------- update

func TestHandleUpdateRewritesTheRow(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','ada@example.com')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	st := stateWith(db, relation(1, "main", "orders", "id", "customer", "email"))

	commit, err := newSyncer(t, config.SyncConfig{}).handleUpdate(
		updateMessage(1,
			tuple(text("1"), text("Ada"), text("ada@example.com")),
			tuple(text("1"), text("Grace"), text("grace@example.com"))), st)
	if err != nil {
		t.Fatalf("handleUpdate: %v", err)
	}
	if commit {
		t.Error("handleUpdate reported a commit")
	}
	if got := rows(t, db); len(got) != 1 || got[0] != "1|Grace|grace@example.com" {
		t.Errorf("rows = %v", got)
	}
}

// TestTheUpdateWhereClauseUsesEveryOldColumn records that the WHERE clause is
// built from the whole old tuple, not from the primary key: "buildWhereClauses
// FromPK" reads every column it is given. So an update only lands if every old
// value still matches — the target row must be byte-identical to what the
// source had before the change.
func TestTheUpdateWhereClauseUsesEveryOldColumn(t *testing.T) {
	db := targetDB(t, ordersSchema)
	// The target has drifted: the customer name differs from the source's old
	// value, so the generated WHERE matches nothing.
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Drifted','ada@example.com')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	st := stateWith(db, relation(1, "main", "orders", "id", "customer", "email"))

	if _, err := newSyncer(t, config.SyncConfig{}).handleUpdate(
		updateMessage(1,
			tuple(text("1"), text("Ada"), text("ada@example.com")),
			tuple(text("1"), text("Grace"), text("grace@example.com"))), st); err != nil {
		t.Fatalf("handleUpdate: %v", err)
	}

	got := rows(t, db)
	if len(got) != 1 || got[0] != "1|Drifted|ada@example.com" {
		t.Fatalf("rows = %v; the clause appears to use the key only now, so assert "+
			"that instead", got)
	}
	// And the silent no-op is not reported: the call returned nil above.
}

// TestAnUpdateWithNoOldTupleMatchesOnTheNewValues records the fallback when the
// table has no REPLICA IDENTITY FULL: the WHERE clause is built from the *new*
// tuple, so the statement looks for the row it is about to write. On a target
// that has not yet got that row the update silently affects nothing.
func TestAnUpdateWithNoOldTupleMatchesOnTheNewValues(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','ada@example.com')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	st := stateWith(db, relation(1, "main", "orders", "id", "customer", "email"))

	if _, err := newSyncer(t, config.SyncConfig{}).handleUpdate(
		updateMessage(1, nil,
			tuple(text("1"), text("Grace"), text("grace@example.com"))), st); err != nil {
		t.Fatalf("handleUpdate: %v", err)
	}
	if got := rows(t, db); got[0] != "1|Ada|ada@example.com" {
		t.Errorf("rows = %v; the row appears to be matched by key now, so assert "+
			"that instead", got)
	}
}

func TestHandleUpdateSkipsAnUnknownRelation(t *testing.T) {
	db := targetDB(t, ordersSchema)

	if _, err := newSyncer(t, config.SyncConfig{}).handleUpdate(
		updateMessage(99, nil, tuple(text("1"))), stateWith(db)); err != nil {
		t.Fatalf("handleUpdate: %v", err)
	}
}

func TestHandleUpdateSkipsAnEmptyNewTuple(t *testing.T) {
	db := targetDB(t, ordersSchema)
	st := stateWith(db, relation(1, "main", "orders", "id"))

	if _, err := newSyncer(t, config.SyncConfig{}).handleUpdate(
		updateMessage(1, tuple(text("1")), nil), st); err != nil {
		t.Fatalf("handleUpdate: %v", err)
	}
}

// TestHandleUpdateSkipsWhenNoClauseCanBeBuilt records the guard against an
// unqualified UPDATE: with no columns to match on, the statement would rewrite
// every row, so the handler skips instead.
func TestHandleUpdateSkipsWhenNoClauseCanBeBuilt(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// A relation with no columns leaves both the SET and WHERE lists empty.
	st := stateWith(db, relation(1, "main", "orders"))

	if _, err := newSyncer(t, config.SyncConfig{}).handleUpdate(
		updateMessage(1, tuple(text("1")), tuple(text("2"))), st); err != nil {
		t.Fatalf("handleUpdate: %v", err)
	}
	if got := rows(t, db); got[0] != "1|Ada|x" {
		t.Errorf("rows = %v, want the row untouched", got)
	}
}

// TestUpdateIsNotSubjectToFieldMasking records an asymmetry: handleInsert masks
// secured fields and handleUpdate does not. So the first copy of a row is
// masked in the target and every later revision overwrites it with the raw
// value.
func TestUpdateIsNotSubjectToFieldMasking(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','masked')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	st := stateWith(db, relation(1, "main", "orders", "id", "customer", "email"))
	cfg := config.SyncConfig{Mappings: mappingWithSecurity("orders", "email")}

	if _, err := newSyncer(t, cfg).handleUpdate(
		updateMessage(1,
			tuple(text("1"), text("Ada"), text("masked")),
			tuple(text("1"), text("Ada"), text("grace@example.com"))), st); err != nil {
		t.Fatalf("handleUpdate: %v", err)
	}

	if got := rows(t, db); got[0] != "1|Ada|grace@example.com" {
		t.Fatalf("rows = %v; the update path appears to mask now, so assert that "+
			"instead", got)
	}
}

// ------------------------------------------------------------------- delete

func TestHandleDeleteWithAllColumnsRemovesTheRow(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x'), ('2','Grace','y')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rel := relation(1, "main", "orders", "id", "customer", "email")
	st := stateWith(db, rel)

	commit, err := newSyncer(t, config.SyncConfig{}).handleDeleteWithAllColumns(
		deleteMessage(1, tuple(text("1"), text("Ada"), text("x"))), st, rel)
	if err != nil {
		t.Fatalf("handleDeleteWithAllColumns: %v", err)
	}
	if commit {
		t.Error("the handler reported a commit")
	}
	if got := rows(t, db); len(got) != 1 || got[0] != "2|Grace|y" {
		t.Errorf("rows = %v", got)
	}
}

func TestTheAllColumnsDeleteMatchesNullsToo(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1',NULL,'x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rel := relation(1, "main", "orders", "id", "customer", "email")

	if _, err := newSyncer(t, config.SyncConfig{}).handleDeleteWithAllColumns(
		deleteMessage(1, tuple(text("1"), nil, text("x"))), stateWith(db, rel), rel); err != nil {
		t.Fatalf("handleDeleteWithAllColumns: %v", err)
	}
	if got := rows(t, db); len(got) != 0 {
		t.Errorf("rows = %v, want the row removed", got)
	}
}

// TestTheAllColumnsDeleteSkipsAnEmptyClauseList records the guard against an
// unqualified DELETE, which would empty the target table.
func TestTheAllColumnsDeleteSkipsAnEmptyClauseList(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rel := relation(1, "main", "orders") // no columns described

	if _, err := newSyncer(t, config.SyncConfig{}).handleDeleteWithAllColumns(
		deleteMessage(1, tuple(text("1"))), stateWith(db, rel), rel); err != nil {
		t.Fatalf("handleDeleteWithAllColumns: %v", err)
	}
	if got := rows(t, db); len(got) != 1 {
		t.Errorf("rows = %v, want the table untouched", got)
	}
}

// TestHandleDeleteNeedsTheSourceConnection records that the delete path cannot
// run without a live source: it asks PostgreSQL for the primary key columns
// before building the statement, and with no source connection the call panics
// rather than falling back to the all-columns form. The fallback exists for a
// query *error*, not for a missing connection.
func TestHandleDeleteNeedsTheSourceConnection(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("handleDelete with no source connection did not panic; the " +
				"missing connection appears to be handled now, so assert that instead")
		}
	}()

	db := targetDB(t, ordersSchema)
	rel := relation(1, "main", "orders", "id")
	_, _ = newSyncer(t, config.SyncConfig{}).handleDelete(
		deleteMessage(1, tuple(text("1"))), stateWith(db, rel))
}

func TestHandleDeleteSkipsAnUnknownRelation(t *testing.T) {
	db := targetDB(t, ordersSchema)

	if _, err := newSyncer(t, config.SyncConfig{}).handleDelete(
		deleteMessage(99, tuple(text("1"))), stateWith(db)); err != nil {
		t.Fatalf("handleDelete: %v", err)
	}
}

func TestHandleDeleteSkipsAnEmptyOldTuple(t *testing.T) {
	db := targetDB(t, ordersSchema)
	st := stateWith(db, relation(1, "main", "orders", "id"))

	if _, err := newSyncer(t, config.SyncConfig{}).handleDelete(
		deleteMessage(1, nil), st); err != nil {
		t.Fatalf("handleDelete: %v", err)
	}
}

// ---------------------------------------------------------- where clauses

func TestBuildWhereClausesFromPK(t *testing.T) {
	s := newSyncer(t, config.SyncConfig{})
	rel := relation(1, "main", "orders", "id", "customer")

	got := s.buildWhereClausesFromPK(rel,
		tuple(text("1"), text("O'Brien")).Columns)

	want := []string{"id='1'", "customer='O''Brien'"}
	if len(got) != len(want) {
		t.Fatalf("clauses = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("clause %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestBuildWhereClausesFromPKOnANilRelation(t *testing.T) {
	s := newSyncer(t, config.SyncConfig{})

	if got := s.buildWhereClausesFromPK(nil, tuple(text("1")).Columns); got != nil {
		t.Errorf("clauses = %v, want none", got)
	}
	if got := s.buildWhereClausesFromPK(relation(1, "main", "orders"),
		tuple(text("1")).Columns); got != nil {
		t.Errorf("clauses for a column-less relation = %v, want none", got)
	}
}

// TestAnUnchangedColumnBecomesAnIsNullClause records that the 'u' and 'b' types
// fall into the same default branch here as in the insert builder, so an
// unchanged TOASTed value in the old tuple turns into "col IS NULL" — a clause
// that matches nothing, silently dropping the update or delete.
func TestAnUnchangedColumnBecomesAnIsNullClause(t *testing.T) {
	s := newSyncer(t, config.SyncConfig{})
	rel := relation(1, "main", "orders", "id", "body")

	tup := tuple(text("1"), text("large"))
	tup.Columns[1] = &pglogrepl.TupleDataColumn{DataType: 'u'}

	got := s.buildWhereClausesFromPK(rel, tup.Columns)
	if len(got) != 2 || got[1] != "body IS NULL" {
		t.Errorf("clauses = %v, want the unchanged column matched as NULL", got)
	}
}

func TestExtraColumnsAreIgnoredWhenBuildingClauses(t *testing.T) {
	s := newSyncer(t, config.SyncConfig{})
	rel := relation(1, "main", "orders", "id")

	got := s.buildWhereClausesFromPK(rel, tuple(text("1"), text("extra")).Columns)
	if len(got) != 1 || got[0] != "id='1'" {
		t.Errorf("clauses = %v, want just the described column", got)
	}
}

// -------------------------------------------------------------- dispatch

func TestProcessMessageReportsUnparseableWAL(t *testing.T) {
	st := stateWith(nil)

	_, err := newSyncer(t, config.SyncConfig{}).processMessage(
		pglogrepl.XLogData{WALData: []byte("not a pgoutput message")}, st)
	if err == nil || !strings.Contains(err.Error(), "ParseV2") {
		t.Fatalf("err = %v, want a parse failure", err)
	}
}

// TestProcessMessageRecordsTheReceivedLSNBeforeParsing records the ordering: on
// a parse failure the received LSN is *not* advanced, because the assignment
// comes after the early return. A message the decoder cannot read therefore
// leaves the position where it was and the stream retries it.
func TestProcessMessageRecordsTheReceivedLSNBeforeParsing(t *testing.T) {
	st := stateWith(nil)

	if _, err := newSyncer(t, config.SyncConfig{}).processMessage(
		pglogrepl.XLogData{ServerWALEnd: 99, WALData: []byte("bad")}, st); err == nil {
		t.Fatal("want a parse failure")
	}
	if st.lastReceivedLSN != 0 {
		t.Errorf("lastReceivedLSN = %s, want it left alone on a parse failure",
			st.lastReceivedLSN)
	}
}
