package postgresql

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// refusesOnce fails its first query the way a connection busy with another is refused.
type refusesOnce struct {
	refused bool
	next    sourceQuerier
}

func (r *refusesOnce) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if !r.refused {
		r.refused = true
		return nil, errors.New("conn busy")
	}
	return r.next.Query(ctx, sql, args...)
}

// A failure means one refused lookup left the table addressed by every column, so its updates match nothing.
func TestAFailedKeyLookupIsNotRemembered(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('7','Ada','ada@example.com')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	source := &refusesOnce{next: &answering{replies: []sourceReply{{
		match: "indisprimary", columns: []string{"attname"}, rows: [][]any{{"id"}},
	}}}}
	st := stateWith(t, db, relation(1, "main", "orders", "id", "customer", "email"))
	st.reader.Keys = (&Syncer{logger: quiet()}).keyLookup(context.Background(),
		&schemaWork{Source: source, Logger: quiet()})

	update := updateMessage(1, nil, tuple(text("7"), text("Grace"), text("grace@example.com")))

	if _, err := st.handleUpdate(update); err == nil {
		t.Error("a key lookup that failed was carried as a table with no key: the " +
			"update was addressed by the values it sets, which the target does not hold yet")
	}
	if _, err := st.handleUpdate(update); err != nil {
		t.Fatalf("the update after the source answered again: %v", err)
	}
	if got := rows(t, db); len(got) != 1 || got[0] != "7|Grace|grace@example.com" {
		t.Errorf("rows = %v, want the update applied once the key could be read", got)
	}
}

// standbyStatus is what the source read from one confirm: write, flush and apply.
type standbyStatus struct{ write, flush, apply pglogrepl.LSN }

// confirmOverPipe runs the syncer's confirm for reader and decodes what reached the source.
func confirmOverPipe(t *testing.T, reader *Reader) standbyStatus {
	t.Helper()

	data := sentOverPipe(t, func(stream *pgconn.PgConn) error {
		return confirmer(stream, reader)(context.Background())
	})
	if len(data) < 25 || data[0] != pglogrepl.StandbyStatusUpdateByteID {
		t.Fatalf("the source was sent %x, want a standby status update", data)
	}
	at := func(i int) pglogrepl.LSN { return pglogrepl.LSN(binary.BigEndian.Uint64(data[1+8*i:])) }
	return standbyStatus{write: at(0), flush: at(1), apply: at(2)}
}

// sentOverPipe runs send against a stream whose far end is the test, and returns the copy data it sent.
func sentOverPipe(t *testing.T, send func(*pgconn.PgConn) error) []byte {
	t.Helper()

	client, server := net.Pipe()
	t.Cleanup(func() { client.Close(); server.Close() })
	config, err := pgconn.ParseConfig("host=localhost user=sync")
	if err != nil {
		t.Fatalf("parse a config: %v", err)
	}
	stream, err := pgconn.Construct(&pgconn.HijackedConn{Conn: client, Config: config})
	if err != nil {
		t.Fatalf("construct the stream: %v", err)
	}

	sent := make(chan error, 1)
	go func() { sent <- send(stream) }()

	message, err := pgproto3.NewBackend(server, server).Receive()
	if err != nil {
		t.Fatalf("read what the source was sent: %v", err)
	}
	if err := <-sent; err != nil {
		t.Fatalf("send: %v", err)
	}
	data, ok := message.(*pgproto3.CopyData)
	if !ok {
		t.Fatalf("the source was sent %#v, want copy data", message)
	}
	return append([]byte(nil), data.Data...)
}

// A failure means the source was told the target holds changes it has only received, so a crash loses them.
func TestTheSourceIsToldTheTargetHoldsOnlyWhatWasApplied(t *testing.T) {
	reader := &Reader{Logger: quiet()}
	raise(&reader.received, 200)
	reader.Applied(100)

	got := confirmOverPipe(t, reader)
	if want := (standbyStatus{write: 200, flush: 100, apply: 100}); got != want {
		t.Errorf("the source was told write/flush/apply %s/%s/%s, want %s/%s/%s",
			got.write, got.flush, got.apply, want.write, want.flush, want.apply)
	}
}

// A failure means progress is reported only when the source asks, or one failed report ends the reporting.
func TestProgressIsReportedOnATimerAndAFailedReportDoesNotEndIt(t *testing.T) {
	calls := make(chan struct{}, 2)
	reader := &Reader{Logger: quiet()}
	reader.Confirm = func(context.Context) error {
		calls <- struct{}{}
		return errors.New("connection reset")
	}

	stop := (&Syncer{logger: quiet()}).confirmPeriodically(context.Background(), reader, time.Millisecond)
	defer stop()

	for i := 0; i < 2; i++ {
		select {
		case <-calls:
		case <-time.After(10 * time.Second):
			t.Fatalf("%d reports reached the source, want 2", i)
		}
	}
}

// A failure means a task resuming from the slot's own position confirmed everything received before writing any of it.
func TestNothingAppliedIsReportedAsNothingFlushed(t *testing.T) {
	reader := &Reader{Logger: quiet()}
	raise(&reader.received, 200)

	got := confirmOverPipe(t, reader)
	if want := (standbyStatus{write: 200}); got != want {
		t.Errorf("the source was told write/flush/apply %s/%s/%s, want %s/%s/%s",
			got.write, got.flush, got.apply, want.write, want.flush, want.apply)
	}
}

// A failure means the status update no longer reads as the one the driver sends, so the server misreads it.
func TestTheStandbyStatusIsWhatTheDriverWouldSendForTheSamePositions(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 123456000, time.UTC)
	ours := sentOverPipe(t, func(stream *pgconn.PgConn) error {
		return sendStandbyStatus(stream, 200, 100, now)
	})
	driver := sentOverPipe(t, func(stream *pgconn.PgConn) error {
		return pglogrepl.SendStandbyStatusUpdate(context.Background(), stream, pglogrepl.StandbyStatusUpdate{
			WALWritePosition: 200, WALFlushPosition: 100, WALApplyPosition: 100, ClientTime: now,
		})
	})
	if string(ours) != string(driver) {
		t.Errorf("sent %x, the driver sends %x", ours, driver)
	}
}
