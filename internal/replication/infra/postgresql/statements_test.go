package postgresql

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/infra/security"
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

// The 'u' column type means "unchanged TOASTed value, deliberately not sent",
// and it used to fall into the same branch as every unrecognised type and be
// written as NULL — so an update to one column of a row emptied a large text
// column of the same row that nobody had touched.
func TestAnUnchangedToastedValueIsLeftAlone(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','ada@example.com')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	st := stateWith(db, relation(1, "main", "orders", "id", "customer", "email"))

	// The customer changes; the email is TOASTed and unchanged, so the server
	// does not resend it.
	newTup := tuple(text("1"), text("Grace"), nil)
	newTup.Columns[2] = &pglogrepl.TupleDataColumn{DataType: 'u'}

	if _, err := newSyncer(t, config.SyncConfig{}).handleUpdate(
		updateMessage(1, tuple(text("1"), text("Ada"), text("ada@example.com")), newTup), st); err != nil {
		t.Fatalf("handleUpdate: %v", err)
	}
	if got := rows(t, db); len(got) != 1 || got[0] != "1|Grace|ada@example.com" {
		t.Errorf("rows = %v, want the untouched email still there", got)
	}
}

// TestABinaryColumnIsReportedNotDropped covers the other format the plugin can
// send. A binary value used to be written as NULL, which loses the column
// without saying anything.
func TestABinaryColumnIsReportedNotDropped(t *testing.T) {
	db := targetDB(t, ordersSchema)
	st := stateWith(db, relation(1, "main", "orders", "id", "customer", "email"))

	tup := tuple(text("1"), text("Ada"), text("x"))
	tup.Columns[2] = &pglogrepl.TupleDataColumn{DataType: 'b', Data: []byte{0x01, 0x02}}

	_, err := newSyncer(t, config.SyncConfig{}).handleInsert(insertMessage(1, tup), st)
	if err == nil {
		t.Fatal("handleInsert accepted a binary column")
	}
	if !strings.Contains(err.Error(), "email") {
		t.Errorf("err = %v, want it to name the column", err)
	}
	if got := rows(t, db); len(got) != 0 {
		t.Errorf("rows = %v, want nothing written", got)
	}
}

// Every value in a replicated row comes from the source database, and they
// used to be pasted into the SQL text with a doubled single quote as the only
// escaping — so the correctness of the target depended on a setting of the
// source that nothing here checks.
func TestAValueCannotReachTheStatementText(t *testing.T) {
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

// TestATupleWiderThanItsRelationIsReported covers a row that arrives with more
// columns than the last relation message described, which is what a source
// that has added a column and not re-announced the table sends.
func TestATupleWiderThanItsRelationIsReported(t *testing.T) {
	db := targetDB(t, ordersSchema)
	st := stateWith(db, relation(1, "main", "orders", "id", "customer"))

	_, err := newSyncer(t, config.SyncConfig{}).handleInsert(
		insertMessage(1, tuple(text("1"), text("Ada"), text("extra"))), st)
	if err == nil {
		t.Fatal("handleInsert accepted a tuple wider than its relation")
	}
	if !strings.Contains(err.Error(), "3 columns") {
		t.Errorf("err = %v, want it to say what did not line up", err)
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
// string.
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

// The WHERE clause used to be built from every column of the old tuple, so a
// target row that differed anywhere — drifted once, missed an earlier update,
// or had a field masked on the way in — matched nothing, and the update was a
// silent no-op that nothing reported.
func TestAnUpdateAddressesTheRowByItsKey(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Drifted','ada@example.com')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rel := relation(1, "main", "orders", "id", "customer", "email")
	st := stateWith(db, rel)

	query, args, err := buildUpdate(rel,
		tuple(text("1"), text("Ada"), text("ada@example.com")),
		tuple(text("1"), text("Grace"), text("grace@example.com")),
		[]string{"id"}, security.TableSecurity{})
	if err != nil {
		t.Fatalf("buildUpdate: %v", err)
	}
	if err := newSyncer(t, config.SyncConfig{}).replicateQuery(
		st.replicaConn, query, args, "UPDATE", "main.orders"); err != nil {
		t.Fatalf("replicateQuery: %v", err)
	}

	if got := rows(t, db); len(got) != 1 || got[0] != "1|Grace|grace@example.com" {
		t.Errorf("rows = %v, want the drifted row updated", got)
	}
}

// TestAnUpdateWithNoOldTupleUsesTheKeyFromTheNewOne covers REPLICA IDENTITY
// DEFAULT, where the old tuple is only sent when the key itself changed.
func TestAnUpdateWithNoOldTupleUsesTheKeyFromTheNewOne(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','ada@example.com')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rel := relation(1, "main", "orders", "id", "customer", "email")
	st := stateWith(db, rel)

	query, args, err := buildUpdate(rel, nil,
		tuple(text("1"), text("Grace"), text("grace@example.com")),
		[]string{"id"}, security.TableSecurity{})
	if err != nil {
		t.Fatalf("buildUpdate: %v", err)
	}
	if err := newSyncer(t, config.SyncConfig{}).replicateQuery(
		st.replicaConn, query, args, "UPDATE", "main.orders"); err != nil {
		t.Fatalf("replicateQuery: %v", err)
	}

	if got := rows(t, db); got[0] != "1|Grace|grace@example.com" {
		t.Errorf("rows = %v, want the row updated", got)
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

// TestAnUpdateThatCannotBeBuiltWritesNothing is the guard against an unqualified
// UPDATE, which would rewrite every row of the table. A relation with no columns
// cannot describe the row, so the statement is refused rather than issued.
func TestAnUpdateThatCannotBeBuiltWritesNothing(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	st := stateWith(db, relation(1, "main", "orders"))

	if _, err := newSyncer(t, config.SyncConfig{}).handleUpdate(
		updateMessage(1, tuple(text("1")), tuple(text("2"))), st); err == nil {
		t.Error("handleUpdate accepted a row it could not name a single column of")
	}
	if got := rows(t, db); got[0] != "1|Ada|x" {
		t.Errorf("rows = %v, want the row untouched", got)
	}
}

// TestAnUpdateMasksTheSameFieldsAnInsertDoes covers an asymmetry that undid
// the masking entirely: the insert path masked secured fields and the update
// path did not, so a row arrived masked and then the first change to it
// overwrote the masked value with the one from the source.
func TestAnUpdateMasksTheSameFieldsAnInsertDoes(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','masked')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rel := relation(1, "main", "orders", "id", "customer", "email")
	st := stateWith(db, rel)
	table := security.FindTableSecurityFromMappings("orders", mappingWithSecurity("orders", "email"))

	query, args, err := buildUpdate(rel,
		tuple(text("1"), text("Ada"), text("masked")),
		tuple(text("1"), text("Ada"), text("grace@example.com")),
		[]string{"id"}, table)
	if err != nil {
		t.Fatalf("buildUpdate: %v", err)
	}
	if err := newSyncer(t, config.SyncConfig{}).replicateQuery(
		st.replicaConn, query, args, "UPDATE", "main.orders"); err != nil {
		t.Fatalf("replicateQuery: %v", err)
	}

	got := rows(t, db)
	if strings.Contains(got[0], "grace@example.com") {
		t.Errorf("row = %q, want the address masked", got[0])
	}
	if !strings.HasPrefix(got[0], "1|Ada|") {
		t.Errorf("row = %q, want the other columns written", got[0])
	}
}

// ------------------------------------------------------------------- delete

func TestADeleteWithNoKeyMatchesOnEveryColumn(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x'), ('2','Grace','y')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rel := relation(1, "main", "orders", "id", "customer", "email")
	st := stateWith(db, rel)

	query, args, err := buildDelete(rel, tuple(text("1"), text("Ada"), text("x")), nil)
	if err != nil {
		t.Fatalf("buildDelete: %v", err)
	}
	if err := newSyncer(t, config.SyncConfig{}).replicateQuery(
		st.replicaConn, query, args, "DELETE", "main.orders"); err != nil {
		t.Fatalf("replicateQuery: %v", err)
	}

	if got := rows(t, db); len(got) != 1 || got[0] != "2|Grace|y" {
		t.Errorf("rows = %v", got)
	}
}

// TestADeleteAddressesTheRowByItsKey covers the same drift as the update: with a
// key, only the key has to match.
func TestADeleteAddressesTheRowByItsKey(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Drifted','x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rel := relation(1, "main", "orders", "id", "customer", "email")
	st := stateWith(db, rel)

	query, args, err := buildDelete(rel, tuple(text("1"), text("Ada"), text("x")), []string{"id"})
	if err != nil {
		t.Fatalf("buildDelete: %v", err)
	}
	if err := newSyncer(t, config.SyncConfig{}).replicateQuery(
		st.replicaConn, query, args, "DELETE", "main.orders"); err != nil {
		t.Fatalf("replicateQuery: %v", err)
	}

	if got := rows(t, db); len(got) != 0 {
		t.Errorf("rows = %v, want the row removed", got)
	}
}

func TestTheAllColumnsDeleteMatchesNullsToo(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1',NULL,'x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rel := relation(1, "main", "orders", "id", "customer", "email")
	st := stateWith(db, rel)

	query, args, err := buildDelete(rel, tuple(text("1"), nil, text("x")), nil)
	if err != nil {
		t.Fatalf("buildDelete: %v", err)
	}
	if err := newSyncer(t, config.SyncConfig{}).replicateQuery(
		st.replicaConn, query, args, "DELETE", "main.orders"); err != nil {
		t.Fatalf("replicateQuery: %v", err)
	}

	if got := rows(t, db); len(got) != 0 {
		t.Errorf("rows = %v, want the row removed", got)
	}
}

// TestADeleteWithNothingToMatchOnIsRefused is the guard against an unqualified
// DELETE, which would empty the target table.
func TestADeleteWithNothingToMatchOnIsRefused(t *testing.T) {
	rel := relation(1, "main", "orders") // no columns described

	if _, _, err := buildDelete(rel, tuple(), nil); err == nil {
		t.Error("buildDelete produced a statement with no WHERE clause")
	}
}

// TestAnUpdateWithNothingToMatchOnIsRefused is the same guard for UPDATE, which
// would otherwise rewrite every row of the table.
func TestAnUpdateWithNothingToMatchOnIsRefused(t *testing.T) {
	rel := relation(1, "main", "orders", "id", "customer")

	// The key names a column the row does not carry.
	if _, _, err := buildUpdate(rel,
		tuple(text("1"), text("Ada")), tuple(text("1"), text("Grace")),
		[]string{"missing"}, security.TableSecurity{}); err == nil {
		t.Error("buildUpdate produced a statement with no WHERE clause")
	}
}

// The handler asked PostgreSQL for the table's primary key without checking
// that the connection was there, so one delete during an outage dereferenced
// nil and took the whole process down.
func TestADeleteDoesNotNeedTheSourceConnection(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rel := relation(1, "main", "orders", "id", "customer", "email")

	if _, err := newSyncer(t, config.SyncConfig{}).handleDelete(
		deleteMessage(1, tuple(text("1"), text("Ada"), text("x"))),
		stateWith(db, rel)); err != nil {
		t.Fatalf("handleDelete: %v", err)
	}
	if got := rows(t, db); len(got) != 0 {
		t.Errorf("rows = %v, want the row removed", got)
	}
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

func TestTheClauseBindsItsValues(t *testing.T) {
	rel := relation(1, "main", "orders", "id", "customer")

	query, args, err := buildDelete(rel, tuple(text("1"), text("O'Brien")), nil)
	if err != nil {
		t.Fatalf("buildDelete: %v", err)
	}

	if want := `DELETE FROM "main"."orders" WHERE "id" = $1 AND "customer" = $2`; query != want {
		t.Errorf("query = %q, want %q", query, want)
	}
	if len(args) != 2 || args[0] != "1" || args[1] != "O'Brien" {
		t.Errorf("args = %v, want the two values bound", args)
	}
}

// TestAnUnchangedColumnIsNotMatchedAsNull covers the WHERE clause's half of the
// TOAST problem: an unchanged value used to become "col IS NULL", a clause that
// matches nothing, so the update or delete was silently dropped.
func TestAnUnchangedColumnIsNotMatchedAsNull(t *testing.T) {
	rel := relation(1, "main", "orders", "id", "body")

	tup := tuple(text("1"), text("large"))
	tup.Columns[1] = &pglogrepl.TupleDataColumn{DataType: 'u'}

	query, args, err := buildDelete(rel, tup, nil)
	if err != nil {
		t.Fatalf("buildDelete: %v", err)
	}
	if strings.Contains(query, "IS NULL") {
		t.Errorf("query = %q, want the unchanged column left out", query)
	}
	if len(args) != 1 {
		t.Errorf("args = %v, want just the id", args)
	}
}

// TestAKeyThatIsNullIsMatchedAsNull is the other side: a column that really is
// NULL still has to be compared as one, because = never matches NULL.
func TestAKeyThatIsNullIsMatchedAsNull(t *testing.T) {
	rel := relation(1, "main", "orders", "id", "customer")

	query, _, err := buildDelete(rel, tuple(text("1"), nil), nil)
	if err != nil {
		t.Fatalf("buildDelete: %v", err)
	}
	if !strings.Contains(query, `"customer" IS NULL`) {
		t.Errorf("query = %q, want the null column matched as NULL", query)
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

// TestProcessMessageRecordsTheReceivedLSNBeforeParsing records the ordering:
// on a parse failure the received LSN is *not* advanced, because the
// assignment comes after the early return.
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
