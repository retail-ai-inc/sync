package postgresql

import (
	"encoding/binary"
	"sync/atomic"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/retail-ai-inc/sync/internal/platform/config"
)

// The encoders below produce the pgoutput wire format the decoder expects, so
// the dispatch can be driven with the same bytes PostgreSQL would send.

func u32(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func u64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func u16(v uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return b
}

// beginBytes encodes a BEGIN carrying the transaction's final LSN.
func beginBytes(finalLSN uint64) []byte {
	out := []byte{'B'}
	out = append(out, u64(finalLSN)...)
	out = append(out, u64(0)...) // commit timestamp
	out = append(out, u32(7)...) // xid
	return out
}

// commitBytes encodes a COMMIT.
func commitBytes(commitLSN, endLSN uint64) []byte {
	out := []byte{'C', 0}
	out = append(out, u64(commitLSN)...)
	out = append(out, u64(endLSN)...)
	out = append(out, u64(0)...) // commit timestamp
	return out
}

// relationBytes encodes a RELATION describing a table and its columns.
func relationBytes(relID uint32, namespace, name string, columns ...string) []byte {
	out := []byte{'R'}
	out = append(out, u32(relID)...)
	out = append(out, append([]byte(namespace), 0)...)
	out = append(out, append([]byte(name), 0)...)
	out = append(out, 'd') // replica identity: default
	out = append(out, u16(uint16(len(columns)))...)
	for _, c := range columns {
		out = append(out, 1) // flagged as part of the key
		out = append(out, append([]byte(c), 0)...)
		out = append(out, u32(25)...) // text
		out = append(out, u32(0xFFFFFFFF)...)
	}
	return out
}

// tupleBytes encodes a tuple body: text columns for non-nil values, NULL
// otherwise.
func tupleBytes(values ...*string) []byte {
	out := u16(uint16(len(values)))
	for _, v := range values {
		if v == nil {
			out = append(out, 'n')
			continue
		}
		out = append(out, 't')
		out = append(out, u32(uint32(len(*v)))...)
		out = append(out, []byte(*v)...)
	}
	return out
}

func insertBytes(relID uint32, values ...*string) []byte {
	out := []byte{'I'}
	out = append(out, u32(relID)...)
	out = append(out, 'N')
	out = append(out, tupleBytes(values...)...)
	return out
}

func deleteBytes(relID uint32, values ...*string) []byte {
	out := []byte{'D'}
	out = append(out, u32(relID)...)
	out = append(out, 'O') // old tuple, full row
	out = append(out, tupleBytes(values...)...)
	return out
}

func updateBytes(relID uint32, oldValues, newValues []*string) []byte {
	out := []byte{'U'}
	out = append(out, u32(relID)...)
	if oldValues != nil {
		out = append(out, 'O')
		out = append(out, tupleBytes(oldValues...)...)
	}
	out = append(out, 'N')
	out = append(out, tupleBytes(newValues...)...)
	return out
}

// feed pushes one encoded message through the dispatch.
func feed(t *testing.T, s *PostgreSQLSyncer, st *replicationState, walEnd uint64, data []byte) bool {
	t.Helper()

	commit, err := s.processMessage(
		pglogrepl.XLogData{ServerWALEnd: pglogrepl.LSN(walEnd), WALData: data}, st)
	if err != nil {
		t.Fatalf("processMessage: %v", err)
	}
	return commit
}

// TestTheEncoderMatchesTheDecoder guards the fixtures themselves: if the wire
// format assumed here were wrong every other test in this file would fail for
// the wrong reason.
func TestTheEncoderMatchesTheDecoder(t *testing.T) {
	msg, err := pglogrepl.ParseV2(relationBytes(1, "public", "orders", "id"), false)
	if err != nil {
		t.Fatalf("ParseV2: %v", err)
	}
	rel, ok := msg.(*pglogrepl.RelationMessageV2)
	if !ok {
		t.Fatalf("ParseV2 returned %T", msg)
	}
	if rel.RelationID != 1 || rel.Namespace != "public" || rel.RelationName != "orders" {
		t.Errorf("relation = %+v", rel.RelationMessage)
	}
	if len(rel.Columns) != 1 || rel.Columns[0].Name != "id" {
		t.Errorf("columns = %+v", rel.Columns)
	}
}

func TestARelationMessageIsRemembered(t *testing.T) {
	st := stateWith(nil)
	s := newSyncer(t, config.SyncConfig{})

	feed(t, s, st, 10, relationBytes(42, "public", "orders", "id", "customer"))

	rel, ok := st.relations[42]
	if !ok {
		t.Fatal("the relation was not recorded")
	}
	if rel.RelationName != "orders" || len(rel.Columns) != 2 {
		t.Errorf("relation = %+v", rel.RelationMessage)
	}
	if st.lastReceivedLSN != 10 {
		t.Errorf("lastReceivedLSN = %s, want 10", st.lastReceivedLSN)
	}
}

func TestABeginOpensTheTransaction(t *testing.T) {
	st := stateWith(nil)
	s := newSyncer(t, config.SyncConfig{})

	if commit := feed(t, s, st, 20, beginBytes(100)); commit {
		t.Error("a BEGIN reported a commit")
	}
	if !st.processMessages {
		t.Error("processMessages is false after a BEGIN")
	}
	if st.currentTxLSN != 100 {
		t.Errorf("currentTxLSN = %s, want 100", st.currentTxLSN)
	}
}

// TestAStaleBeginIsSkipped records the resume guard: a transaction whose final
// LSN is at or below the last written position is replayed by the server after
// a restart, and the whole transaction is dropped rather than applied twice.
func TestAStaleBeginIsSkipped(t *testing.T) {
	st := stateWith(nil)
	st.lastWrittenLSN = 200
	s := newSyncer(t, config.SyncConfig{})

	if commit := feed(t, s, st, 20, beginBytes(100)); commit {
		t.Error("a stale BEGIN reported a commit")
	}
	if st.processMessages {
		t.Error("processMessages is true after a stale BEGIN")
	}
}

// TestABeginAtExactlyTheWrittenLSNIsApplied records where the boundary sits:
// the comparison is strictly greater-than, so a transaction whose final LSN
// equals the last written position is applied again. The last transaction before
// a restart is therefore replayed — safe for an idempotent DELETE, a duplicate
// for an INSERT.
func TestABeginAtExactlyTheWrittenLSNIsApplied(t *testing.T) {
	st := stateWith(nil)
	st.lastWrittenLSN = 100
	s := newSyncer(t, config.SyncConfig{})

	feed(t, s, st, 20, beginBytes(100))
	if !st.processMessages {
		t.Error("a BEGIN at the written LSN was skipped; the comparison appears to " +
			"be inclusive now, so assert that instead")
	}
}

func TestACommitClosesTheTransaction(t *testing.T) {
	st := stateWith(nil)
	s := newSyncer(t, config.SyncConfig{})

	feed(t, s, st, 20, beginBytes(100))
	commit := feed(t, s, st, 30, commitBytes(100, 101))

	if !commit {
		t.Error("a COMMIT did not report a commit")
	}
	if st.processMessages {
		t.Error("processMessages is still true after a COMMIT")
	}
}

// TestTheCommitLSNIsNotRecorded records that the dispatch itself never advances
// the written position, and that the COMMIT's own TransactionEndLSN is read and
// discarded. The caller does the advancing, and it uses the LSN the BEGIN
// carried rather than the one the COMMIT reports.
func TestTheCommitLSNIsNotRecorded(t *testing.T) {
	st := stateWith(nil)
	s := newSyncer(t, config.SyncConfig{})

	feed(t, s, st, 20, beginBytes(100))
	feed(t, s, st, 30, commitBytes(100, 999))

	if st.lastReceivedLSN != 30 {
		t.Errorf("lastReceivedLSN = %s, want the stream's 30", st.lastReceivedLSN)
	}
	if st.lastWrittenLSN != 0 {
		t.Errorf("lastWrittenLSN = %s; the commit appears to advance it now",
			st.lastWrittenLSN)
	}
}

func TestAnInsertInsideATransactionIsApplied(t *testing.T) {
	db := targetDB(t, ordersSchema)
	st := stateWith(db)
	s := newSyncer(t, config.SyncConfig{})

	feed(t, s, st, 10, relationBytes(1, "main", "orders", "id", "customer", "email"))
	feed(t, s, st, 20, beginBytes(100))
	feed(t, s, st, 30, insertBytes(1, text("1"), text("Ada"), nil))
	feed(t, s, st, 40, commitBytes(100, 101))

	if got := rows(t, db); len(got) != 1 || got[0] != "1|Ada|<null>" {
		t.Errorf("rows = %v", got)
	}
}

// TestAnInsertOutsideATransactionIsDropped records that every row change is
// gated on having seen a BEGIN. A stream that starts mid-transaction — the
// server resuming from a slot after a restart — loses the changes that arrive
// before the next BEGIN.
func TestAnInsertOutsideATransactionIsDropped(t *testing.T) {
	db := targetDB(t, ordersSchema)
	st := stateWith(db)
	s := newSyncer(t, config.SyncConfig{})

	feed(t, s, st, 10, relationBytes(1, "main", "orders", "id", "customer", "email"))
	feed(t, s, st, 20, insertBytes(1, text("1"), text("Ada"), text("x")))

	if got := rows(t, db); len(got) != 0 {
		t.Errorf("rows = %v, want the change dropped", got)
	}
}

func TestAnUpdateInsideATransactionIsApplied(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	st := stateWith(db)
	s := newSyncer(t, config.SyncConfig{})

	feed(t, s, st, 10, relationBytes(1, "main", "orders", "id", "customer", "email"))
	feed(t, s, st, 20, beginBytes(100))
	feed(t, s, st, 30, updateBytes(1,
		[]*string{text("1"), text("Ada"), text("x")},
		[]*string{text("1"), text("Grace"), text("y")}))

	if got := rows(t, db); len(got) != 1 || got[0] != "1|Grace|y" {
		t.Errorf("rows = %v", got)
	}
}

func TestAnUpdateOutsideATransactionIsDropped(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	st := stateWith(db)
	s := newSyncer(t, config.SyncConfig{})

	feed(t, s, st, 10, relationBytes(1, "main", "orders", "id", "customer", "email"))
	feed(t, s, st, 20, updateBytes(1, nil, []*string{text("1"), text("Grace"), text("y")}))

	if got := rows(t, db); got[0] != "1|Ada|x" {
		t.Errorf("rows = %v, want the change dropped", got)
	}
}

func TestADeleteOutsideATransactionIsDropped(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	st := stateWith(db)
	s := newSyncer(t, config.SyncConfig{})

	feed(t, s, st, 10, relationBytes(1, "main", "orders", "id", "customer", "email"))
	feed(t, s, st, 20, deleteBytes(1, text("1"), text("Ada"), text("x")))

	if got := rows(t, db); len(got) != 1 {
		t.Errorf("rows = %v, want the change dropped", got)
	}
}

// TestAnUnhandledMessageTypeIsIgnored records that message kinds the syncer does
// not implement — TRUNCATE among them — are logged at debug level and skipped.
// A TRUNCATE on the source is therefore never replicated, and the target keeps
// rows the source no longer has.
func TestAnUnhandledMessageTypeIsIgnored(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	st := stateWith(db)
	s := newSyncer(t, config.SyncConfig{})

	feed(t, s, st, 10, relationBytes(1, "main", "orders", "id", "customer", "email"))
	feed(t, s, st, 20, beginBytes(100))

	// TRUNCATE: relation count, options, then the relation ids.
	truncate := []byte{'T'}
	truncate = append(truncate, u32(1)...)
	truncate = append(truncate, 0)
	truncate = append(truncate, u32(1)...)
	feed(t, s, st, 30, truncate)

	if got := rows(t, db); len(got) != 1 {
		t.Errorf("rows = %v; TRUNCATE appears to be replicated now, so assert that "+
			"instead", got)
	}
}

// TestAnInsertForAnUnannouncedRelationIsDropped records the dependency on the
// relation cache: without a RELATION message first, the row cannot be named and
// is skipped. The cache lives only in memory, so a restart mid-stream relies on
// PostgreSQL resending the relation.
func TestAnInsertForAnUnannouncedRelationIsDropped(t *testing.T) {
	db := targetDB(t, ordersSchema)
	st := stateWith(db)
	s := newSyncer(t, config.SyncConfig{})

	feed(t, s, st, 20, beginBytes(100))
	feed(t, s, st, 30, insertBytes(9, text("1"), text("Ada"), text("x")))

	if got := rows(t, db); len(got) != 0 {
		t.Errorf("rows = %v, want the change dropped", got)
	}
}

// TestTheReceivedLSNAdvancesOnEveryMessage records what the flush position is
// built from: the stream's WAL end, updated for every message the decoder
// accepts, including ones the syncer then ignores.
func TestTheReceivedLSNAdvancesOnEveryMessage(t *testing.T) {
	st := stateWith(nil)
	s := newSyncer(t, config.SyncConfig{})

	feed(t, s, st, 10, relationBytes(1, "public", "orders", "id"))
	feed(t, s, st, 25, beginBytes(100))

	if st.lastReceivedLSN != 25 {
		t.Errorf("lastReceivedLSN = %s, want 25", st.lastReceivedLSN)
	}
}

// TestOneSuccessfulStatementClearsTheErrorFlag records the reason a failed row
// can be lost for good. The syncer keeps a single "last statement failed" flag,
// and the commit path only writes the LSN when it reads zero. But the flag is
// reset by *any* later successful statement, including one in the same
// transaction — so a transaction where the first row fails and the second
// succeeds is recorded as fully applied, the LSN moves past it, and the failed
// row is never retried.
func TestOneSuccessfulStatementClearsTheErrorFlag(t *testing.T) {
	db := targetDB(t, ordersSchema)
	st := stateWith(db)
	s := newSyncer(t, config.SyncConfig{})

	// A relation naming a column the target does not have, so its statements fail.
	feed(t, s, st, 10, relationBytes(1, "main", "orders", "id", "missing_column"))
	feed(t, s, st, 11, relationBytes(2, "main", "orders", "id", "customer", "email"))
	feed(t, s, st, 20, beginBytes(100))

	if _, err := s.processMessage(pglogrepl.XLogData{ServerWALEnd: 30,
		WALData: insertBytes(1, text("1"), text("x"))}, st); err == nil {
		t.Fatal("the failing insert reported no error")
	}
	if atomic.LoadInt32(&s.lastExecError) != 1 {
		t.Fatal("the error flag was not raised")
	}

	feed(t, s, st, 40, insertBytes(2, text("2"), text("Ada"), text("y")))

	if atomic.LoadInt32(&s.lastExecError) != 0 {
		t.Fatalf("the error flag survived a later success; the flag appears to be " +
			"per-transaction now, so assert that instead")
	}
	// The first row never reached the target, and nothing records that.
	if got := rows(t, db); len(got) != 1 || got[0] != "2|Ada|y" {
		t.Errorf("rows = %v", got)
	}
}
