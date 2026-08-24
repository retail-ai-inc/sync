package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// Reader turns one MongoDB deployment's change stream into a stream of events.
//
// One stream, opened on the client rather than on a collection, covers every
// collection the task maps. A change stream per collection made the server scan
// the oplog once for each of them — and the oplog has no index, which is why
// MongoDB's own guidance is to avoid opening a high number of narrowly targeted
// change streams. On a sharded cluster the streams are merged by mongos in
// cluster time order, so one stream also gives a single global ordering, which
// is what a payment ledger spread over several collections needs.
type Reader struct {
	Client *mongo.Client
	Config config.SyncConfig
	Logger logrus.FieldLogger
	Labels metrics.Labels

	// conv renders events into write models. It needs the task's mappings for
	// field security and nothing else.
	conv *MongoDBSyncer

	stream *mongo.ChangeStream
	// mapped names the collections to replicate, empty when the task lists none
	// and everything is replicated.
	mapped map[string]bool

	// tx buffers the events of one source transaction. A change stream reports
	// every event of a multi-document transaction with the same lsid and
	// txnNumber, so they can be handed over together and a batch can never be
	// cut inside one.
	tx     []*domain.Event
	txID   string
	lastAt time.Time

	closeOnce sync.Once
}

// idleHeartbeat is how long the stream may return nothing before the reader
// reports that it is nonetheless alive.
//
// A stream delivering nothing looks exactly like one that is up to date. The
// change stream's postBatchResumeToken advances on empty batches, so liveness is
// already being reported by the server; turning it into an event makes it
// visible.
const idleHeartbeat = 10 * time.Second

// Open starts the change stream at a position, or at the current end when there
// is none.
func (r *Reader) Open(ctx context.Context, from domain.Position) error {
	if r.Client == nil {
		return fmt.Errorf("no source connection")
	}
	r.conv = &MongoDBSyncer{cfg: r.Config, logger: r.Logger}
	r.mapped = r.mappedCollections()

	// The schema changes are asked for as well as the rows. An index added at
	// the source used never to reach the target — indexes were copied once, by
	// the snapshot — so the moment the target most needed an index was exactly
	// the moment it did not have it.
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.M{"$in": watchedOperations}},
		}}},
	}

	opts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup).
		// MongoDB 6.0 and later report DDL on a stream that asks for it.
		SetShowExpandedEvents(true).
		// Without this the stream blocks for as long as the server likes, and a
		// reader that never returns cannot report that it is alive.
		SetMaxAwaitTime(idleHeartbeat)

	if !from.IsZero() {
		token, err := decodeToken(from)
		if err != nil {
			return err
		}
		opts.SetResumeAfter(token)
	}

	stream, err := r.Client.Watch(ctx, pipeline, opts)
	if err != nil {
		if positionLost(err) {
			return domain.Unrecoverable(
				"the change stream cannot be resumed from the position this task holds "+
					"(%v). The oplog no longer reaches back that far, so a fresh copy is "+
					"needed: clear the stored checkpoint. Until then nothing is being "+
					"replicated", err)
		}
		return fmt.Errorf("open the change stream: %w", err)
	}
	r.stream = stream
	return nil
}

// Next hands over the next event.
//
// The change stream is a cursor, so this drains a whole source transaction
// before returning its first event: the events of one transaction share an lsid
// and a txnNumber, and handing them over together is what stops a batch being
// cut inside one.
func (r *Reader) Next(ctx context.Context) (*domain.Event, error) {
	for {
		if len(r.tx) > 0 {
			event := r.tx[0]
			r.tx = r.tx[1:]
			return event, nil
		}

		if err := r.fill(ctx); err != nil {
			return nil, err
		}
	}
}

// fill reads until it has a complete source transaction, or until the stream
// goes quiet and a heartbeat is due.
func (r *Reader) fill(ctx context.Context) error {
	for {
		if r.stream.TryNext(ctx) {
			if err := r.take(r.stream.Current); err != nil {
				return err
			}
			if r.settled() {
				return nil
			}
			continue
		}

		if err := r.stream.Err(); err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return ctx.Err()
			}
			metrics.CountDisconnect(r.Labels)
			if positionLost(err) {
				return domain.Unrecoverable(
					"the change stream cannot continue from the position this task holds "+
						"(%v). The oplog no longer reaches back that far, so a fresh copy "+
						"is needed: clear the stored checkpoint", err)
			}
			return err
		}

		// TryNext returned nothing and the stream is healthy. If a transaction
		// is open it is still being delivered, so keep reading; otherwise this
		// is the quiet the heartbeat exists for.
		if len(r.tx) > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		token := r.stream.ResumeToken()
		if token == nil {
			continue
		}
		payload, err := encodeToken(token)
		if err != nil {
			return err
		}
		// The heartbeat carries the source's own clock, not just a token.
		//
		// It means "everything up to here has been delivered", which is what a
		// re-copy needs in order to know the stream has passed the point its
		// chunk was read at. Without it a quiet source leaves the stream's clock
		// at zero and a re-copy waits for ever — which is how this was found.
		at, clockErr := r.sourceClock(ctx)
		if clockErr != nil {
			r.Logger.Warnf("[MongoDB] Could not read the source's cluster time for a "+
				"heartbeat: %v", clockErr)
		}
		r.tx = []*domain.Event{{
			Heartbeat:       true,
			EndsTransaction: true,
			Pos:             domain.Position{Payload: payload},
			SourceTime:      at,
		}}
		return nil
	}
}

// sourceClock reads the source's own time, for a heartbeat to carry.
func (r *Reader) sourceClock(ctx context.Context) (time.Time, error) {
	raw, err := r.Client.Database("admin").
		RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Raw()
	if err != nil {
		return time.Time{}, err
	}
	at, err := clusterTimeFrom(raw)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(int64(at.T), 0), nil
}

// settled reports whether the buffered events form a complete transaction.
//
// A change stream delivers an event at a time. Everything outside a transaction
// is its own boundary; inside one, the boundary is where the lsid and txnNumber
// stop matching — which is only visible once the next event arrives, so a
// transaction is held until something that is not part of it turns up.
func (r *Reader) settled() bool {
	if len(r.tx) == 0 {
		return false
	}
	return r.txID == ""
}

// take converts one raw change stream document and buffers it.
func (r *Reader) take(raw bson.Raw) error {
	ns, ok := namespaceOf(raw)
	if !ok {
		return nil
	}
	if !r.replicates(ns.Object) {
		return nil
	}

	if isSchemaEvent(raw) {
		return r.takeSchemaChange(raw, ns)
	}

	model, err := r.conv.convertRawBSONToWriteModel(raw, ns.DB, ns.Object)
	if err != nil {
		// A change that cannot be converted must not be skipped: the two sides
		// would diverge from here on with only a log line to show it.
		return domain.Unrecoverable(
			"a change to %s could not be turned into a write for the target (%v). "+
				"Replication has stopped rather than skip it", ns, err)
	}
	if model == nil {
		// The task asked for this operation to be ignored, which is a
		// configured decision rather than a failure.
		return nil
	}

	token, err := encodeToken(raw.Lookup("_id").Document())
	if err != nil {
		return err
	}

	at, _ := eventClusterTime(raw)
	if !at.IsZero() {
		r.lastAt = at
		metrics.SetReadLag(r.Labels, time.Since(at).Seconds())
	}

	txID := transactionOf(raw)
	event := &domain.Event{
		NS:         ns,
		Op:         opOf(raw),
		Key:        keyOf(raw),
		Payload:    model,
		Bytes:      len(raw),
		Pos:        domain.Position{Payload: token},
		SourceTime: at,
	}

	// The previous transaction ends where this event's identity differs from it.
	if r.txID != "" && r.txID != txID && len(r.tx) > 0 {
		r.tx[len(r.tx)-1].EndsTransaction = true
	}
	r.txID = txID
	if txID == "" {
		// Not part of a multi-document transaction, so it is its own boundary.
		event.EndsTransaction = true
	}
	r.tx = append(r.tx, event)
	return nil
}

// watchedOperations are the change stream events this reader asks for.
//
// The four row operations, plus the schema changes that keep the target's shape
// in step with the source's. "invalidate" is asked for because it is how the
// server says the stream cannot continue — dropped unnoticed, the task would
// look healthy and replicate nothing.
var watchedOperations = []string{
	"insert", "update", "replace", "delete",
	"create", "modify", "createIndexes", "dropIndexes",
	"drop", "dropDatabase", "rename",
	"shardCollection", "reshardCollection", "refineCollectionShardKey",
	"invalidate",
}

// rowOperations are the events that carry data rather than shape.
var rowOperations = map[string]bool{
	"insert": true, "update": true, "replace": true, "delete": true,
}

// isSchemaEvent reports whether an event changes the shape of the data rather
// than the data.
func isSchemaEvent(raw bson.Raw) bool {
	kind, ok := raw.Lookup("operationType").StringValueOK()
	if !ok {
		return false
	}
	return !rowOperations[kind]
}

// takeSchemaChange decides what to do with one DDL event and buffers it.
func (r *Reader) takeSchemaChange(raw bson.Raw, ns domain.Namespace) error {
	kind, _ := raw.Lookup("operationType").StringValueOK()

	if kind == "invalidate" {
		// The stream is over: the collection or database it watched is gone. A
		// resume token from an invalidated stream cannot be used, so carrying on
		// is not possible and pretending otherwise replicates nothing quietly.
		return domain.Unrecoverable(
			"the change stream was invalidated, which means what it watched no longer " +
				"exists. A fresh copy is needed: clear the stored checkpoint")
	}

	change, decision, reason := planSchemaChange(raw, ns.Object)
	switch decision {
	case ddlSkip:
		r.Logger.Infof("[MongoDB][DDL] Not replicating a change to %s: it %s", ns, reason)
		return nil
	case ddlStop:
		return domain.Unrecoverable(
			"refusing to replicate a schema change to %s: it %s. Replication has stopped "+
				"so the change can be made on the target deliberately", ns, reason)
	}

	token, err := encodeToken(raw.Lookup("_id").Document())
	if err != nil {
		return err
	}
	at, _ := eventClusterTime(raw)

	// A schema change is its own transaction boundary and gets a batch of its
	// own: MongoDB's catalogue is not transactional, so it cannot share one with
	// rows.
	if len(r.tx) > 0 {
		r.tx[len(r.tx)-1].EndsTransaction = true
	}
	r.txID = ""
	r.tx = append(r.tx, &domain.Event{
		NS:              ns,
		Op:              domain.OpSchema,
		Payload:         change,
		Bytes:           len(raw),
		Pos:             domain.Position{Payload: token},
		SourceTime:      at,
		EndsTransaction: true,
	})
	return nil
}

// Close releases the stream. Calling it more than once is safe.
func (r *Reader) Close() error {
	var err error
	r.closeOnce.Do(func() {
		if r.stream != nil {
			err = r.stream.Close(context.Background())
		}
	})
	return err
}

// replicates reports whether a collection is one this task carries.
func (r *Reader) replicates(collection string) bool {
	if len(r.mapped) == 0 {
		// The task lists nothing, so everything is replicated under its own
		// name — apart from the syncer's own bookkeeping.
		return !isInternal(collection)
	}
	return r.mapped[strings.ToLower(collection)]
}

func (r *Reader) mappedCollections() map[string]bool {
	mapped := map[string]bool{}
	for _, mapping := range r.Config.Mappings {
		for _, table := range mapping.Tables {
			mapped[strings.ToLower(table.SourceTable)] = true
		}
	}
	return mapped
}

// ------------------------------------------------------------- event reading

// namespaceOf reads which collection a change touched.
func namespaceOf(raw bson.Raw) (domain.Namespace, bool) {
	value, err := raw.LookupErr("ns")
	if err != nil {
		return domain.Namespace{}, false
	}
	doc, ok := value.DocumentOK()
	if !ok {
		return domain.Namespace{}, false
	}
	db, _ := doc.Lookup("db").StringValueOK()
	coll, _ := doc.Lookup("coll").StringValueOK()
	if db == "" || coll == "" {
		return domain.Namespace{}, false
	}
	return domain.Namespace{DB: db, Object: coll}, true
}

// opOf reads what happened.
func opOf(raw bson.Raw) domain.Op {
	kind, _ := raw.Lookup("operationType").StringValueOK()
	switch kind {
	case "insert":
		return domain.OpInsert
	case "update", "replace":
		return domain.OpUpdate
	case "delete":
		return domain.OpDelete
	}
	return domain.OpSchema
}

// keyOf identifies the document a change touched.
//
// It is the whole documentKey, not just the _id. On a sharded collection
// documentKey carries the shard key as well, and that is what the target has to
// be addressed by: an updateOne whose filter omits the shard key cannot be
// routed to one shard, so mongos broadcasts it to every one of them — one wasted
// round trip per shard, per document, for the life of the task.
func keyOf(raw bson.Raw) string {
	value, err := raw.LookupErr("documentKey")
	if err != nil {
		return ""
	}
	doc, ok := value.DocumentOK()
	if !ok {
		return ""
	}
	elements, err := doc.Elements()
	if err != nil {
		return ""
	}
	var b strings.Builder
	for _, element := range elements {
		fmt.Fprintf(&b, "%s=%v\x00", element.Key(), element.Value())
	}
	return b.String()
}

// transactionOf identifies the multi-document transaction a change belongs to,
// empty when it belongs to none.
//
// Every event of one transaction carries the same session id and transaction
// number, which is what makes the transaction's boundary visible from outside.
func transactionOf(raw bson.Raw) string {
	number, err := raw.LookupErr("txnNumber")
	if err != nil {
		return ""
	}
	session, err := raw.LookupErr("lsid")
	if err != nil {
		return ""
	}
	n, ok := number.AsInt64OK()
	if !ok {
		return ""
	}
	return fmt.Sprintf("%x/%d", session.Value, n)
}

// ------------------------------------------------------------------- tokens

func encodeToken(token bson.Raw) (string, error) {
	if token == nil {
		return "", nil
	}
	encoded, err := bson.MarshalExtJSON(bson.Raw(token), true, false)
	if err != nil {
		return "", fmt.Errorf("encode the resume token: %w", err)
	}
	return string(encoded), nil
}

func decodeToken(pos domain.Position) (bson.Raw, error) {
	var token bson.Raw
	if err := bson.UnmarshalExtJSON([]byte(pos.Payload), true, &token); err != nil {
		return nil, fmt.Errorf("read the stored resume token: %w", err)
	}
	return token, nil
}

// isInternal reports whether a collection is the syncer's own bookkeeping,
// which must never be replicated: doing so writes the target's own checkpoint
// back over itself.
func isInternal(collection string) bool {
	switch collection {
	case "_sync_checkpoint", "_sync_direction", "_sync_dead_letter":
		return true
	}
	return strings.HasPrefix(collection, "system.")
}
