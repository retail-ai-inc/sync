package postgresql

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// Decoding the write-ahead log into events.
//
// None of this could be tested before: the decode and the write were one method
// on a struct holding a *pgx.Conn, so exercising the first meant having a
// PostgreSQL for the second. The reader now takes the stream as an interface,
// and these drive it with the bytes the server would send.

// readAll drains the reader until it has produced the wanted number of
// non-heartbeat events, or the context gives up.
func readAll(t *testing.T, r *Reader, want int) []*domain.Event {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var events []*domain.Event
	for len(events) < want {
		event, err := r.Next(ctx)
		if err != nil {
			t.Fatalf("Next after %d events: %v", len(events), err)
		}
		if event.Heartbeat {
			continue
		}
		events = append(events, event)
	}
	return events
}

// TestATransactionArrivesWhole covers the unit the pipeline works in: the
// events of one transaction, with the position on the last of them.
func TestATransactionArrivesWhole(t *testing.T) {
	r := readerFor(t, config.SyncConfig{},
		wal(10, relationBytes(1, "public", "orders", "id", "amount")),
		wal(11, beginBytes(40)),
		wal(12, insertBytes(1, text("1"), text("100"))),
		wal(13, insertBytes(1, text("2"), text("200"))),
		wal(14, commitBytes(40, 41)),
	)
	if err := r.Open(context.Background(), domain.Position{}); err != nil {
		t.Fatalf("Open: %v", err)
	}

	events := readAll(t, r, 2)

	if events[0].EndsTransaction {
		t.Error("the first of two events ended the transaction, so a batch could " +
			"be cut inside it and the target would see half")
	}
	if !events[1].EndsTransaction {
		t.Error("the last event did not end the transaction, so the batch would be " +
			"held for a boundary that never comes")
	}
	if !events[0].Pos.IsZero() {
		t.Error("a position was carried before the commit, so it could be recorded " +
			"ahead of the rows it describes")
	}
	if events[1].Pos.IsZero() {
		t.Fatal("the commit carried no position, so nothing would be recorded")
	}

	lsn, _, err := decodeLSN(events[1].Pos.Payload, "")
	if err != nil {
		t.Fatalf("the position does not read back: %v", err)
	}
	if lsn != pglogrepl.LSN(40) {
		t.Errorf("position = %s, want the commit's own LSN", lsn)
	}
}

func TestEachOperationIsNamed(t *testing.T) {
	r := readerFor(t, config.SyncConfig{},
		wal(10, relationBytes(1, "public", "orders", "id", "amount")),
		wal(11, beginBytes(40)),
		wal(12, insertBytes(1, text("1"), text("100"))),
		wal(13, updateBytes(1, nil, []*string{text("1"), text("150")})),
		wal(14, deleteBytes(1, text("1"), text("150"))),
		wal(15, commitBytes(40, 41)),
	)
	if err := r.Open(context.Background(), domain.Position{}); err != nil {
		t.Fatalf("Open: %v", err)
	}

	events := readAll(t, r, 3)
	for i, want := range []domain.Op{domain.OpInsert, domain.OpUpdate, domain.OpDelete} {
		if events[i].Op != want {
			t.Errorf("event %d is a %v, want %v", i, events[i].Op, want)
		}
		if events[i].NS.DB != "public" || events[i].NS.Object != "orders" {
			t.Errorf("event %d names %v", i, events[i].NS)
		}
		if _, ok := events[i].Payload.(statement); !ok {
			t.Errorf("event %d carries a %T rather than a statement", i, events[i].Payload)
		}
	}
}

// A failure means a streamed row was addressed to the source's table rather than to the ones its mappings name.
func TestARowIsWrittenToEveryTableItsMappingsName(t *testing.T) {
	r := readerFor(t, config.SyncConfig{Mappings: []config.DatabaseMapping{
		{TargetSchema: "dr", Tables: []config.TableMapping{{SourceTable: "orders", TargetTable: "orders_dr"}}},
		{SourceSchema: "archive", Tables: []config.TableMapping{{SourceTable: "orders", TargetTable: "archived"}}},
		{Tables: []config.TableMapping{{SourceTable: "Orders"}}},
		{TargetSchema: "Ledger", Tables: []config.TableMapping{{SourceTable: "orders", TargetTable: "ORDERS"}}},
	}},
		wal(10, relationBytes(1, "public", "orders", "id", "amount")),
		wal(11, beginBytes(40)),
		wal(12, insertBytes(1, text("1"), text("100"))),
		wal(13, updateBytes(1, nil, []*string{text("1"), text("150")})),
		wal(14, deleteBytes(1, text("1"), text("150"))),
		wal(15, commitBytes(40, 41)),
	)
	if err := r.Open(context.Background(), domain.Position{}); err != nil {
		t.Fatalf("Open: %v", err)
	}

	addressed := regexp.MustCompile(`^(INSERT INTO|UPDATE|DELETE FROM) "[^"]*"\."[^"]*"`)
	var got []string
	for _, event := range readAll(t, r, 9) {
		got = append(got, addressed.FindString(event.Payload.(statement).query))
	}
	want := []string{
		`INSERT INTO "dr"."orders_dr"`, `INSERT INTO "public"."orders"`, `INSERT INTO "ledger"."orders"`,
		`UPDATE "dr"."orders_dr"`, `UPDATE "public"."orders"`, `UPDATE "ledger"."orders"`,
		`DELETE FROM "dr"."orders_dr"`, `DELETE FROM "public"."orders"`, `DELETE FROM "ledger"."orders"`,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("statements address %q, want %q", got, want)
	}
}

// TestATransactionAlreadyAppliedIsNotReplayed covers the resumed case. A
// transaction whose end is at or before what has been applied is on the target
// in full; replaying it re-inserts rows that are there and re-deletes rows that
// are not, so the comparison is >= and not >.
func TestATransactionAlreadyAppliedIsNotReplayed(t *testing.T) {
	r := readerFor(t, config.SyncConfig{},
		wal(10, relationBytes(1, "public", "orders", "id")),
		wal(11, beginBytes(40)), // exactly the applied position
		wal(12, insertBytes(1, text("1"))),
		wal(13, commitBytes(40, 41)),
		wal(14, beginBytes(50)), // past it, so this one is carried
		wal(15, insertBytes(1, text("2"))),
		wal(16, commitBytes(50, 51)),
	)
	if err := r.Open(context.Background(), domain.Position{}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	r.Applied(pglogrepl.LSN(40))

	events := readAll(t, r, 1)
	written := events[0].Payload.(statement)
	if len(written.args) == 0 || written.args[0] != "2" {
		t.Errorf("the replayed transaction was carried again: %v", written.args)
	}
}

// TestARowOutsideATransactionIsIgnored: after a stale begin the rows that
// follow belong to it, and carrying them would apply half of a transaction the
// target already has.
func TestARowOutsideATransactionIsIgnored(t *testing.T) {
	r := knownRelation(t)
	r.inTransaction = false

	if err := r.decode(mustParse(t, insertBytes(1, text("1")))); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(r.open) != 0 || len(r.ready) != 0 {
		t.Error("a row outside a transaction produced an event")
	}
}

// TestATruncateStopsTheTask. The disaster-recovery copy is the only thing left
// to recover from, so a mistaken truncate must not be carried onto it -- and
// ignoring it silently would leave the target holding rows the source no longer
// has, with nothing to say so.
func TestATruncateStopsTheTask(t *testing.T) {
	r := knownRelation(t)

	err := r.decode(&pglogrepl.TruncateMessageV2{})
	if err == nil {
		t.Fatal("a truncate was carried or ignored")
	}
	if !domain.IsUnrecoverable(err) {
		t.Errorf("err = %v; a truncate needs somebody to decide, so retrying is wrong", err)
	}
}

// TestAnErrorOnTheStreamStopsTheTask: the source has said it cannot go on, and
// retrying the same slot produces the same answer.
func TestAnErrorOnTheStreamStopsTheTask(t *testing.T) {
	r := &Reader{
		Source: &failingStream{message: &pgproto3.ErrorResponse{Message: "slot is gone"}},
		Config: config.SyncConfig{},
		Logger: quiet(),
	}
	if err := r.Open(context.Background(), domain.Position{}); err != nil {
		t.Fatalf("Open: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := r.Next(ctx)
	if err == nil {
		t.Fatal("an error from the source was read past")
	}
	if !domain.IsUnrecoverable(err) {
		t.Errorf("err = %v, want the task stopped", err)
	}
}

// TestAQuietStreamStillReportsItIsAlive. Silence and a stopped stream look the
// same otherwise, and every lag figure freezes at whatever it last was.
func TestAQuietStreamStillReportsItIsAlive(t *testing.T) {
	r := readerFor(t, config.SyncConfig{})
	if err := r.Open(context.Background(), domain.Position{}); err != nil {
		t.Fatalf("Open: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	event, err := r.Next(ctx)
	if err != nil {
		t.Fatalf("Next on a quiet stream: %v", err)
	}
	if !event.Heartbeat {
		t.Errorf("a quiet stream produced a %v rather than a heartbeat", event.Op)
	}
	if !event.EndsTransaction {
		t.Error("the heartbeat did not end its block")
	}
}

// TestTheAppliedPositionIsNeverAheadOfWhatArrived. The source discards WAL a
// standby says it has flushed, so reporting more than has arrived would let it
// recycle segments the target has not been given.
func TestTheAppliedPositionIsNeverAheadOfWhatArrived(t *testing.T) {
	r := readerFor(t, config.SyncConfig{})
	r.received.Store(100)
	r.Applied(pglogrepl.LSN(200))

	received, applied := r.Positions()
	if received != 100 {
		t.Errorf("received = %s", received)
	}
	if applied > received {
		t.Errorf("applied = %s, which is past what arrived (%s)", applied, received)
	}
}

func TestTheAppliedPositionOnlyMovesForward(t *testing.T) {
	r := readerFor(t, config.SyncConfig{})
	r.received.Store(1000)

	r.Applied(pglogrepl.LSN(500))
	r.Applied(pglogrepl.LSN(200))

	if _, applied := r.Positions(); applied != 500 {
		t.Errorf("applied = %s, want it left at the furthest point reached", applied)
	}
}

func TestPositionsCanBeReadAndAppliedWhileTheStreamIsDecoded(t *testing.T) {
	r := readerFor(t, config.SyncConfig{})
	// Applied runs on the applier's goroutine and Positions on the confirmer's.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for lsn := pglogrepl.LSN(1); lsn <= 100; lsn++ {
			r.Applied(lsn)
			r.Positions()
		}
	}()
	for lsn := pglogrepl.LSN(1); lsn <= 100; lsn++ {
		if err := r.decode(&pglogrepl.BeginMessage{FinalLSN: lsn}); err != nil {
			t.Fatalf("decode a begin: %v", err)
		}
	}
	<-done
}

// TestAKeepaliveIsAnswvered covers the reply the source asks for. Without it the
// source waits, and on some configurations drops the connection.
func TestAKeepaliveIsAnswered(t *testing.T) {
	answered := 0
	r := readerFor(t, config.SyncConfig{})
	r.Confirm = func(context.Context) error { answered++; return nil }

	if err := r.keepalive(context.Background(), keepaliveBytes(77, true)); err != nil {
		t.Fatalf("keepalive: %v", err)
	}
	if answered != 1 {
		t.Errorf("the source asked for a reply and got %d", answered)
	}
	if received, _ := r.Positions(); received != 77 {
		t.Errorf("received = %s, want the keepalive's position", received)
	}

	if err := r.keepalive(context.Background(), keepaliveBytes(88, false)); err != nil {
		t.Fatalf("keepalive: %v", err)
	}
	if answered != 1 {
		t.Error("a keepalive that asked for no reply was answered anyway")
	}
}

// knownRelation returns a reader that has been told about one table and is
// inside a transaction, which is the state a row message arrives in.
func knownRelation(t *testing.T) *Reader {
	t.Helper()
	return stateWith(t, nil, relation(1, "public", "orders", "id")).reader
}

func mustParse(t *testing.T, body []byte) pglogrepl.Message {
	t.Helper()
	message, err := pglogrepl.ParseV2(body, false)
	if err != nil {
		t.Fatalf("ParseV2: %v", err)
	}
	return message
}

// failingStream hands out one message and then refuses.
type failingStream struct {
	message pgproto3.BackendMessage
	given   bool
}

func (f *failingStream) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	if f.given {
		return nil, errors.New("the connection is gone")
	}
	f.given = true
	return f.message, nil
}

// keepaliveBytes is the payload of a primary keepalive, without its type byte.
func keepaliveBytes(walEnd uint64, replyRequested bool) []byte {
	out := u64(walEnd)
	out = append(out, u64(0)...) // server time
	if replyRequested {
		return append(out, 1)
	}
	return append(out, 0)
}
