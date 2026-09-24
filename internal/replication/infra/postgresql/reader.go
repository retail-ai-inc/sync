package postgresql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/security"
)

// Reading the source's write-ahead log as a stream of events.
//
// This used to be a loop that decoded a message and wrote it to the target in
// the same breath, one row per statement, with the position recorded per
// commit. That is the shape the other three engines were moved out of: no
// batching, so a busy source cost one round trip per row; no shared accounting,
// so the lag, the queue, the retention window and the byte budget were all
// missing; and nothing between the decode and the write, so neither half could
// be tested without a PostgreSQL to hand.
//
// It now produces events and lets the pipeline decide when to write them.

// walSource is the replication connection, narrowed to what the reader takes
// from it. Narrowed because the concrete type is a *pgconn.PgConn, which cannot
// be stood in for -- which is why none of this had a test.
type walSource interface {
	ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error)
}

// receiveTimeout bounds one wait for a message, so the reader can notice the
// context between messages on a quiet source.
const receiveTimeout = time.Second

// idleHeartbeat is how long the stream may say nothing before the reader
// reports it is alive anyway. Silence looks exactly like being up to date, and
// every lag figure resolves to this while the source is quiet -- see the
// MongoDB reader's copy of this reasoning.
const idleHeartbeat = time.Second

type Reader struct {
	Source walSource
	// Keys reports a table's primary key columns, so a row can be addressed by
	// its key rather than by every column it holds.
	Keys func(schema, table string) ([]string, error)
	// Confirm tells the source how far this task has got. Called on a keepalive
	// that asks for a reply, and by the applier as positions are recorded.
	Confirm func(ctx context.Context) error

	Config config.SyncConfig
	Logger logrus.FieldLogger
	Labels metrics.Labels

	// relations are the table shapes the source has sent. A row message carries
	// only a relation id, so a stream that has not described a table yet cannot
	// be decoded at all.
	relations map[uint32]*pglogrepl.RelationMessageV2

	// open holds the events of a transaction being decoded, and ready the ones
	// waiting to be handed over. A transaction is only released whole: the
	// pipeline may not cut a batch inside one, and it is the commit that carries
	// the position.
	//
	// inTransaction says whether one is being decoded at all, rather than an
	// empty open standing for it: a transaction that has begun and sent no rows
	// yet and one that is not being carried are different states, and telling
	// them apart by nil against empty is the kind of distinction that survives
	// exactly until somebody writes open = nil meaning "clear".
	inTransaction bool
	open          []*domain.Event
	ready         []*domain.Event

	// applied is the position everything up to has been written at, and received
	// the furthest the source has sent. They are different numbers and reporting
	// one for the other is what let the server recycle WAL carrying changes the
	// target had not seen.
	applied  pglogrepl.LSN
	received pglogrepl.LSN

	lastHeard time.Time
}

// Open starts the stream. The connection and the slot are made by the caller,
// which is where the credentials and the retry policy live.
func (r *Reader) Open(_ context.Context, _ domain.Position) error {
	if r.Source == nil {
		return fmt.Errorf("no replication connection to read from")
	}
	r.relations = map[uint32]*pglogrepl.RelationMessageV2{}
	r.inTransaction = false
	r.open = nil
	r.ready = nil
	r.lastHeard = time.Now()
	metrics.SetConnected(r.Labels, true)
	return nil
}

func (r *Reader) Next(ctx context.Context) (*domain.Event, error) {
	for {
		if len(r.ready) > 0 {
			event := r.ready[0]
			r.ready = r.ready[1:]
			return event, nil
		}

		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := r.receive(ctx); err != nil {
			return nil, err
		}
	}
}

func (r *Reader) Close() error {
	metrics.SetConnected(r.Labels, false)
	return nil
}

// receive takes one message from the source and turns it into whatever it is
// worth: nothing, a heartbeat, or a transaction's events.
func (r *Reader) receive(ctx context.Context) error {
	waitCtx, giveUp := context.WithTimeout(ctx, receiveTimeout)
	message, err := r.Source.ReceiveMessage(waitCtx)
	giveUp()

	if err != nil {
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case pgconn.Timeout(err), errors.Is(err, context.DeadlineExceeded):
			// Nothing was sent, which on a quiet source is the ordinary case.
			r.heartbeatIfDue()
			return nil
		case errors.Is(err, io.EOF):
			metrics.SetConnected(r.Labels, false)
			metrics.CountDisconnect(r.Labels)
			return fmt.Errorf("the replication connection closed: %w", err)
		}
		metrics.SetConnected(r.Labels, false)
		metrics.CountDisconnect(r.Labels)
		return fmt.Errorf("read the replication stream: %w", err)
	}

	r.lastHeard = time.Now()

	if failure, ok := message.(*pgproto3.ErrorResponse); ok {
		return domain.Unrecoverable("the source reported an error on the "+
			"replication stream: %s", failure.Message)
	}

	data, ok := message.(*pgproto3.CopyData)
	if !ok || len(data.Data) == 0 {
		return nil
	}

	switch data.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		return r.keepalive(ctx, data.Data[1:])
	case pglogrepl.XLogDataByteID:
		return r.wal(data.Data[1:])
	default:
		r.Logger.Debugf("[PostgreSQL] Ignoring message type %d", data.Data[0])
		return nil
	}
}

// keepalive records how far the source has written and answers when asked.
func (r *Reader) keepalive(ctx context.Context, payload []byte) error {
	message, err := pglogrepl.ParsePrimaryKeepaliveMessage(payload)
	if err != nil {
		r.Logger.Warnf("[PostgreSQL] Could not read a keepalive: %v", err)
		return nil
	}
	if message.ServerWALEnd > r.received {
		r.received = message.ServerWALEnd
	}
	if message.ReplyRequested && r.Confirm != nil {
		if err := r.Confirm(ctx); err != nil {
			r.Logger.Warnf("[PostgreSQL] Could not answer a keepalive: %v", err)
		}
	}
	r.heartbeatIfDue()
	return nil
}

// heartbeatIfDue reports that the link is alive when the source has said
// nothing for a while. Without it a quiet source and a stopped stream look the
// same, and every lag figure freezes at whatever it last was.
func (r *Reader) heartbeatIfDue() {
	if time.Since(r.lastHeard) < idleHeartbeat && len(r.ready) > 0 {
		return
	}
	r.lastHeard = time.Now()
	r.ready = append(r.ready, &domain.Event{
		Heartbeat:       true,
		EndsTransaction: true,
		SourceTime:      time.Now(),
	})
}

// wal decodes one chunk of the log.
func (r *Reader) wal(payload []byte) error {
	data, err := pglogrepl.ParseXLogData(payload)
	if err != nil {
		return fmt.Errorf("read a WAL record: %w", err)
	}
	if data.ServerWALEnd > r.received {
		r.received = data.ServerWALEnd
	}

	message, err := pglogrepl.ParseV2(data.WALData, false)
	if err != nil {
		return fmt.Errorf("read a replication message: %w", err)
	}
	return r.decode(message)
}

// decode turns one logical message into events.
func (r *Reader) decode(message pglogrepl.Message) error {
	switch typed := message.(type) {
	case *pglogrepl.RelationMessageV2:
		r.relations[typed.RelationID] = typed
		return nil

	case *pglogrepl.BeginMessage:
		// A transaction whose end is at or before what has been applied is
		// already on the target in full. Replaying it re-inserts rows that are
		// there and re-deletes rows that are not, so the comparison is >= and
		// not >.
		if r.applied >= typed.FinalLSN {
			r.inTransaction = false
			r.open = nil
			return nil
		}
		r.inTransaction = true
		r.open = nil
		return nil

	case *pglogrepl.CommitMessage:
		return r.commit(typed)

	case *pglogrepl.InsertMessageV2:
		return r.row(typed.RelationID, insert, typed.Tuple, nil)

	case *pglogrepl.UpdateMessageV2:
		return r.row(typed.RelationID, update, typed.NewTuple, typed.OldTuple)

	case *pglogrepl.DeleteMessageV2:
		return r.row(typed.RelationID, remove, nil, typed.OldTuple)

	case *pglogrepl.TruncateMessageV2:
		// Not carried, for the same reason a DROP is not: the disaster-recovery
		// copy is the only thing left to recover from, and a mistaken truncate
		// would take it too. Ignoring it silently is not an option either -- the
		// target would go on holding rows the source no longer has, with nothing
		// to say so.
		return domain.Unrecoverable("the source truncated a replicated table "+
			"(relations %v). Replication has stopped: truncating the target is not "+
			"something this will do on its own. Truncate it by hand and restart the "+
			"task, or make the copy again.", typed.RelationIDs)

	default:
		r.Logger.Debugf("[PostgreSQL] Ignoring message %T", typed)
		return nil
	}
}

// operation names what a row message does, so one path can build all three.
type operation int

const (
	insert operation = iota
	update
	remove
)

// row builds the statement one row message asks for.
func (r *Reader) row(relationID uint32, op operation, newTuple, oldTuple *pglogrepl.TupleData) error {
	if !r.inTransaction {
		// Outside a transaction this task is carrying, which is the resumed case:
		// the rows belong to one the target already has in full.
		return nil
	}

	rel, known := r.relations[relationID]
	if !known || rel == nil {
		// The source describes a table before sending its rows, so this means the
		// stream began mid-transaction. Applying a row whose shape is unknown is
		// not possible, and skipping it silently loses it.
		return domain.Unrecoverable("the source sent a row for relation %d before "+
			"describing it, so the stream started part way through a transaction. "+
			"Clear this task's position to copy the source again.", relationID)
	}

	policy := security.FindTableSecurityFromMappings(rel.RelationName, r.Config.Mappings)
	keys, err := r.keyColumns(rel)
	if err != nil {
		return err
	}

	var query string
	var args []interface{}
	switch op {
	case insert:
		if newTuple == nil {
			return nil
		}
		query, args, err = buildInsert(rel, newTuple, policy)
	case update:
		if newTuple == nil {
			return nil
		}
		query, args, err = buildUpdate(rel, oldTuple, newTuple, keys, policy)
	case remove:
		if oldTuple == nil {
			// Without REPLICA IDENTITY FULL or a key, the source sends no old
			// row and the delete cannot be addressed at all.
			return domain.Unrecoverable("the source sent a delete for %s with no "+
				"old row, so there is nothing to identify what to delete. Set "+
				"REPLICA IDENTITY on the table, or the delete cannot be carried.",
				qualified(rel))
		}
		query, args, err = buildDelete(rel, oldTuple, keys)
	}
	if err != nil {
		return err
	}

	r.open = append(r.open, &domain.Event{
		NS:      domain.Namespace{DB: rel.Namespace, Object: rel.RelationName},
		Op:      opOf(op),
		Key:     rowKey(rel, keys, newTuple, oldTuple),
		Payload: statement{query: query, args: args},
		Bytes:   len(query) + argBytes(args),
	})
	return nil
}

// commit releases the transaction's events, carrying the position on the last
// of them so it is never recorded ahead of the rows it describes.
func (r *Reader) commit(message *pglogrepl.CommitMessage) error {
	events := r.open
	r.open = nil
	r.inTransaction = false
	if len(events) == 0 {
		return nil
	}

	at := message.CommitTime
	if at.IsZero() {
		at = time.Now()
	}
	position, err := encodeLSN(message.CommitLSN, r.sourceEndpoint())
	if err != nil {
		return err
	}

	last := events[len(events)-1]
	last.EndsTransaction = true
	last.Pos = domain.Position{Payload: position}
	for _, event := range events {
		event.SourceTime = at
	}

	r.ready = append(r.ready, events...)
	return nil
}

// keyColumns reports the columns a row is addressed by, empty when the table
// has no key. Addressing by every column still finds the row; it is slower and
// it cannot tell two identical rows apart.
func (r *Reader) keyColumns(rel *pglogrepl.RelationMessageV2) ([]string, error) {
	if r.Keys == nil {
		return nil, nil
	}
	return r.Keys(rel.Namespace, rel.RelationName)
}

func (r *Reader) sourceEndpoint() string {
	return endpointOf(r.Config.SourceConnection)
}

// Applied records how far the target has been written, which is what the reader
// reports to the source and what decides whether a resumed transaction is stale.
func (r *Reader) Applied(lsn pglogrepl.LSN) {
	if lsn > r.applied {
		r.applied = lsn
	}
}

// Positions reports what to tell the source: how much has been received, and
// how much has been applied. They are deliberately separate -- the server
// discards WAL the standby says it has flushed, so confirming everything
// received would let it recycle segments carrying changes the target has not
// seen.
func (r *Reader) Positions() (received, applied pglogrepl.LSN) {
	applied = r.applied
	if applied > r.received {
		applied = r.received
	}
	return r.received, applied
}

func opOf(op operation) domain.Op {
	switch op {
	case insert:
		return domain.OpInsert
	case update:
		return domain.OpUpdate
	default:
		return domain.OpDelete
	}
}

// rowKey identifies the row for ordering, so two changes to one row are applied
// in the order they were made. Built from the key columns when the table has
// them, and from the whole row otherwise -- which orders identical rows
// arbitrarily but never reorders distinguishable ones.
func rowKey(rel *pglogrepl.RelationMessageV2, keys []string, tuples ...*pglogrepl.TupleData) string {
	for _, tuple := range tuples {
		if tuple == nil {
			continue
		}
		cols, err := readTuple(rel, tuple.Columns)
		if err != nil {
			continue
		}
		var parts []string
		for _, col := range keyed(cols, keys) {
			parts = append(parts, col.Name+"=")
			if col.Value != nil {
				parts = append(parts, fmt.Sprint(col.Value))
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, "\x00")
		}
	}
	return ""
}

func argBytes(args []interface{}) int {
	total := 0
	for _, arg := range args {
		switch typed := arg.(type) {
		case string:
			total += len(typed)
		case []byte:
			total += len(typed)
		default:
			total += 8
		}
	}
	return total
}
