package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
)

// Reader turns one MongoDB deployment's change stream into a stream of events.
// One stream, opened on the client rather than on a collection, covers every
// collection the task maps: a stream per collection made the server scan the
// oplog once for each, and the oplog has no index. On a sharded cluster mongos
// merges the shards' streams in cluster time order, so one stream also gives the
// single global ordering a payment ledger across collections needs.
type Reader struct {
	Client *mongo.Client
	Config config.SyncConfig
	Logger logrus.FieldLogger
	Labels metrics.Labels

	// conv renders events into write models. It needs the task's mappings for
	// field security and nothing else.
	conv *MongoDBSyncer

	stream *mongo.ChangeStream
	// nudge keeps the source's idle shards advancing so mongos can release
	// events instead of holding them for the server's no-op writer. Nil on a
	// replica set, where nothing merges and nothing is held.
	nudge *nudger
	// mapped names the collections to replicate, empty when the task lists none
	// and every collection of a mapped database is replicated.
	mapped map[string]bool
	// databases names the source databases to read. Anything outside them is
	// another tenant's data, or the target's own.
	databases map[string]bool

	// open holds the events of the transaction currently being delivered, and
	// openID identifies it. A change stream reports every event of a
	// multi-document transaction with the same lsid and txnNumber, so a
	// transaction's events are held together and a batch is never cut inside one.
	//
	// ready holds complete units — a finished transaction, a standalone change, a
	// schema change, a heartbeat — waiting to be handed over. The two were one
	// list, with "the transaction ended" judged by an event belonging to no
	// transaction arriving, so fifty transactions back to back delivered nothing
	// at all.
	open   []*domain.Event
	openID string
	ready  []*domain.Event
	lastAt time.Time

	closeOnce sync.Once
}

// idleHeartbeat is how long the stream may return nothing before the reader
// reports that it is nonetheless alive. A stream delivering nothing looks
// exactly like one that is up to date.
const idleHeartbeat = 10 * time.Second

// Open starts the change stream at a position, or at the current end when there
// is none.
func (r *Reader) Open(ctx context.Context, from domain.Position) error {
	if r.Client == nil {
		return fmt.Errorf("no source connection")
	}
	r.conv = &MongoDBSyncer{cfg: r.Config, logger: r.Logger}
	r.mapped = r.mappedCollections()
	r.databases = r.mappedDatabaseSet()

	// The schema changes are asked for as well as the rows. An index added at
	// the source used never to reach the target — indexes were copied once, by
	// the snapshot — so the moment the target most needed an index was exactly
	// the moment it did not have it.
	match := bson.D{{Key: "operationType", Value: bson.M{"$in": watchedOperations}}}
	if dbs := r.mappedDatabases(); len(dbs) > 0 {
		// Filtered on the server as well as here. A deployment-level stream sees
		// every database on the cluster — including the target's, when the two
		// are on one cluster, and including any other database that happens to
		// hold a collection of the same name.
		match = append(match, bson.E{Key: "ns.db", Value: bson.M{"$in": dbs}})
	}
	pipeline := mongo.Pipeline{{{Key: "$match", Value: match}}}

	opts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup).
		// MongoDB 6.0 and later report DDL on a stream that asks for it.
		SetShowExpandedEvents(true).
		// Without this the stream blocks for as long as the server likes, and a
		// reader that never returns cannot report that it is alive.
		SetMaxAwaitTime(idleHeartbeat)

	if !from.IsZero() {
		stored, err := decodePosition(from)
		if err != nil {
			return err
		}
		switch {
		case stored.Token != "":
			// The stream has delivered something before, so resume exactly after
			// it.
			token, err := stored.token()
			if err != nil {
				return err
			}
			opts.SetResumeAfter(token)
		case stored.Cluster != 0:
			// Nothing has been delivered yet: this is the cluster time the
			// snapshot pinned before it copied. Starting there replays the writes
			// made while the copy was running, which the copy itself could not
			// see. Starting from now instead would lose that window silently.
			at := bson.Timestamp{T: stored.Cluster, I: stored.Increment}
			r.Logger.Infof("[MongoDB] Starting the stream at the snapshot's cluster "+
				"time %d.%d", at.T, at.I)
			opts.SetStartAtOperationTime(&at)
		default:
			return domain.Unrecoverable(
				"the stored position holds neither a resume token nor a cluster time, so "+
					"there is nowhere to resume from: %q", from.Payload)
		}
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
	if r.nudge == nil {
		r.nudge = startNudging(ctx, r.Client, r.Logger)
	}
	metrics.SetConnected(r.Labels, true)
	metrics.SetCapturedTables(r.Labels, r.capturedCollections())
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
		if len(r.ready) > 0 {
			event := r.ready[0]
			r.ready = r.ready[1:]
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
			if len(r.ready) > 0 {
				return nil
			}
			continue
		}

		if err := r.stream.Err(); err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return ctx.Err()
			}
			metrics.CountDisconnect(r.Labels)
			metrics.SetConnected(r.Labels, false)
			if positionLost(err) {
				return domain.Unrecoverable(
					"the change stream cannot continue from the position this task holds "+
						"(%v). The oplog no longer reaches back that far, so a fresh copy "+
						"is needed: clear the stored checkpoint", err)
			}
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// TryNext returned nothing and the stream is healthy, so the cursor is
		// exhausted. A change stream delivers a committed transaction's events
		// contiguously, so an open transaction with nothing behind it is
		// complete and may be handed over.
		if len(r.open) > 0 {
			r.seal()
			return nil
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
		r.ready = append(r.ready, &domain.Event{
			Heartbeat:       true,
			EndsTransaction: true,
			Pos:             domain.Position{Payload: payload},
			SourceTime:      at,
		})
		return nil
	}
}

// capturedCollections counts the objects this task watches.
//
// Debezium: CapturedTables. The number changing on its own is how a mapping
// edit that dropped a collection shows up; nothing else says so.
func (r *Reader) capturedCollections() int {
	n := 0
	for _, mapping := range r.Config.Mappings {
		n += len(mapping.Tables)
	}
	return n
}

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

// seal closes the open transaction and moves it to the events waiting to be
// handed over, marking its last event as the boundary a batch may be cut at.
func (r *Reader) seal() {
	if len(r.open) == 0 {
		return
	}
	r.open[len(r.open)-1].EndsTransaction = true
	r.ready = append(r.ready, r.open...)
	r.open = nil
	r.openID = ""
}

func (r *Reader) take(raw bson.Raw) error {
	ns, ok := namespaceOf(raw)
	if !ok {
		return nil
	}
	if !r.replicates(ns) {
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

	event := &domain.Event{
		NS:         ns,
		Op:         opOf(raw),
		Key:        keyOf(raw),
		Payload:    model,
		Bytes:      len(raw),
		Pos:        domain.Position{Payload: token},
		SourceTime: at,
	}

	txID := transactionOf(raw)
	switch {
	case txID == "":
		// Not part of a multi-document transaction, so whatever was open before
		// it has ended, and this is its own boundary.
		r.seal()
		event.EndsTransaction = true
		r.ready = append(r.ready, event)
	case txID != r.openID:
		// A different transaction, so the one before it has ended.
		r.seal()
		r.openID = txID
		r.open = append(r.open, event)
	default:
		r.open = append(r.open, event)
	}
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
	r.seal()
	r.ready = append(r.ready, &domain.Event{
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
		r.nudge.Stop()
		if r.stream != nil {
			err = r.stream.Close(context.Background())
		}
	})
	return err
}

// replicates reports whether a namespace is one this task carries.
//
// The database is part of the answer, and leaving it out was a defect a real
// cluster found within seconds. A deployment-level change stream sees every
// database on the cluster, so matching on the collection name alone let through:
//
//   - the target's own writes, when source and target are two databases on one
//     cluster. The syncer read back what it had just written and applied it
//     again — idempotent, so it converged, but doing twice the work and
//     replicating its own bookkeeping.
//   - any other database's collection of the same name. On a cluster hosting
//     twenty-odd databases, a name like "orders" colliding is not a possibility
//     but an expectation, and those changes would be written into the
//     disaster-recovery copy of a payment database.
//
// This is the same defect as matching a DDL statement on its table name and
// discarding the database it named, which was fixed on the MySQL side. It was
// then written again here, for row events.
func (r *Reader) replicates(ns domain.Namespace) bool {
	if !r.databases[strings.ToLower(ns.DB)] {
		return false
	}
	if len(r.mapped) == 0 {
		// The task lists no collections, so every collection of a mapped
		// database is replicated under its own name — apart from the syncer's
		// own bookkeeping.
		return !isInternal(ns.Object)
	}
	return r.mapped[strings.ToLower(ns.Object)]
}

// mappedCollections lists the collections the task names, empty when it names
// none and replicates whatever it finds.
func (r *Reader) mappedCollections() map[string]bool {
	mapped := map[string]bool{}
	for _, mapping := range r.Config.Mappings {
		for _, table := range mapping.Tables {
			mapped[strings.ToLower(table.SourceTable)] = true
		}
	}
	return mapped
}

// mappedDatabaseSet lists the source databases the task reads, for the check
// above.
func (r *Reader) mappedDatabaseSet() map[string]bool {
	dbs := map[string]bool{}
	for _, name := range r.mappedDatabases() {
		dbs[strings.ToLower(name)] = true
	}
	return dbs
}

// mappedDatabases lists the source databases the task reads, in the spelling the
// server uses, for the server-side filter.
func (r *Reader) mappedDatabases() []string {
	seen := map[string]bool{}
	var dbs []string
	fallback := dsn.GetDatabaseName(r.Config.Type, r.Config.SourceConnection)
	for _, mapping := range r.Config.Mappings {
		name := mapping.SourceDatabase
		if name == "" {
			name = fallback
		}
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		dbs = append(dbs, name)
	}
	if len(dbs) == 0 && fallback != "" {
		dbs = append(dbs, fallback)
	}
	return dbs
}

// ------------------------------------------------------------- event reading

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

// keyOf identifies the document a change touched: the whole documentKey, not
// just the _id. On a sharded collection documentKey carries the shard key, and
// that is what the target has to be addressed by — an updateOne whose filter
// omits it cannot be routed, so mongos broadcasts to every shard.
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

// streamPosition is how a MongoDB stream's position is stored. Two kinds, not
// interchangeable, which is what this type exists to stop anybody forgetting:
// before the stream has delivered anything the only thing to resume from is the
// cluster time the snapshot pinned; once it has, the resume token is exact.
// Guessing at the far end got "Bad resume token" over and over while the task
// looked like it was restarting for a transient reason.
type streamPosition struct {
	// Token is a resume token as extended JSON, once the stream has delivered an
	// event.
	Token string `json:"token,omitempty"`
	// Cluster and Increment are a pinned cluster time, set by the snapshot
	// before the stream has delivered anything.
	Cluster   uint32 `json:"cluster,omitempty"`
	Increment uint32 `json:"increment,omitempty"`
}

func (p streamPosition) token() (bson.Raw, error) {
	var token bson.Raw
	if err := bson.UnmarshalExtJSON([]byte(p.Token), true, &token); err != nil {
		return nil, fmt.Errorf("read the stored resume token: %w", err)
	}
	return token, nil
}

func encodeToken(token bson.Raw) (string, error) {
	if token == nil {
		return "", nil
	}
	encoded, err := bson.MarshalExtJSON(bson.Raw(token), true, false)
	if err != nil {
		return "", fmt.Errorf("encode the resume token: %w", err)
	}
	return checkpoint.Encode(streamPosition{Token: string(encoded)})
}

func encodeClusterTime(at bson.Timestamp) (string, error) {
	return checkpoint.Encode(streamPosition{Cluster: at.T, Increment: at.I})
}

func decodePosition(pos domain.Position) (streamPosition, error) {
	var stored streamPosition
	if _, err := checkpoint.Decode(pos.Payload, &stored); err != nil {
		return streamPosition{}, fmt.Errorf("read the stored position: %w", err)
	}
	return stored, nil
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
