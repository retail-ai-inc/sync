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
	"github.com/retail-ai-inc/sync/internal/replication/app/pipeline"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
	"github.com/retail-ai-inc/sync/internal/replication/infra/discovery"
)

// Reader turns one MongoDB deployment's change stream into a stream of events,
// opened on the client so one stream covers every mapped collection: the oplog
// has no index, and mongos merges the shards in cluster time order, which is
// the global ordering a ledger needs.
type Reader struct {
	Client *mongo.Client
	Config config.SyncConfig
	Logger logrus.FieldLogger
	Labels metrics.Labels

	// conv renders events into write models, needing the task's mappings for field
	// security and nothing else.
	conv *MongoDBSyncer

	stream *mongo.ChangeStream
	// nudge advances the source's idle shards so mongos releases events instead of
	// holding them. Nil on a replica set, where nothing merges.
	nudge *nudger
	// mapped names the collections to replicate, empty when the task lists none
	// and every collection of a mapped database goes.
	mapped map[string]bool
	// databases names the source databases to read; anything outside them is
	// another tenant's data, or the target's own.
	databases map[string]bool

	// open holds the transaction being delivered and openID identifies it — every
	// event of one carries the same lsid and txnNumber, which is what keeps a
	// batch from being cut inside it. ready holds complete units waiting to be
	// handed over; as one list, fifty transactions back to back delivered nothing
	// at all.
	open   []*domain.Event
	openID string
	ready  []*domain.Event
	lastAt time.Time
	// lastBeat is when the last heartbeat was emitted. The await window is much
	// shorter than the heartbeat interval, so without this every empty window
	// would produce one.
	lastBeat time.Time

	// deltas reports that this reader has read the stream to its end at least
	// once, after which an update may be applied as the fields it touched. See
	// dropTheLookup for why that is the condition.
	deltas bool

	closeOnce sync.Once
}

// idleHeartbeat is how long the stream may return nothing before the reader
// says it is alive anyway: silence looks exactly like being up to date.
//
// It is also what every lag figure for this task resolves to while the source
// is quiet. The gauges report the age of the newest thing seen, and on an idle
// source the newest thing is the last heartbeat -- so the reported lag walks
// from zero up to this value and drops back, whatever the real delay is. At ten
// seconds that read as a steady three to five seconds of lag on a link whose
// measured end-to-end delay was around a tenth of a second, and it hid the
// difference between "nothing is being written" and "we have stopped reading"
// for ten seconds at a time.
//
// One second costs one getMore return per second on an idle stream, which is
// the cadence the shard nudger already runs at.
const idleHeartbeat = time.Second

// streamAwait is how long the server may hold a getMore that has nothing to
// return. It is separate from the heartbeat cadence because it is not a
// liveness setting: it is the delay.
//
// mongos does not return an event as it arrives. It returns when the current
// await window ends, so this is a floor under every change's latency on a
// sharded source. Measured against the real cluster with a change stream of its
// own and no replication involved: at one second events arrived after 1011 to
// 2013ms, at 200ms after 412 to 1825ms, at 100ms after 414 to 940ms. What does
// not go away is the merge -- mongos may not release an event stamped T until
// every shard has reported past T -- so this buys the difference and not the
// whole of it.
//
// The cost is one getMore per interval on one cluster-wide stream, not one per
// collection.
const streamAwait = 200 * time.Millisecond

// Open starts the change stream at a position, or at the current end when there
// is none.
func (r *Reader) Open(ctx context.Context, from domain.Position) error {
	if r.Client == nil {
		return fmt.Errorf("no source connection")
	}
	r.conv = &MongoDBSyncer{cfg: r.Config, logger: r.Logger}
	r.mapped = r.mappedCollections()
	r.databases = r.mappedDatabaseSet()

	// Schema changes are asked for as well as rows: indexes were copied once by
	// the snapshot, so the moment the target most needed an index was when it did
	// not have it.
	match := bson.D{{Key: "operationType", Value: bson.M{"$in": watchedOperations}}}
	if dbs := r.mappedDatabases(); len(dbs) > 0 {
		// Filtered on the server as well as here: a deployment-level stream sees
		// every database on the cluster, the target's included when the two share
		// one.
		match = append(match, bson.E{Key: "ns.db", Value: bson.M{"$in": dbs}})
	}
	stages := mongo.Pipeline{{{Key: "$match", Value: match}}}

	opts := options.ChangeStream().
		// MongoDB 6.0 and later report DDL on a stream that asks for it.
		SetShowExpandedEvents(true).
		// Without this the stream blocks for as long as the server likes, and a
		// reader that never returns cannot report that it is alive.
		SetMaxAwaitTime(pipeline.Await(streamAwait))

	if !r.deltas {
		// Every update carries the document it produced, at the cost of a lookup
		// per update on the source and the whole document over the link. It is
		// what the stream is opened with until the reader has caught up, because
		// until then the target may hold a document newer than the change being
		// replayed -- see dropTheLookup.
		opts.SetFullDocument(options.UpdateLookup)
	}

	if !from.IsZero() {
		stored, err := decodePosition(from)
		if err != nil {
			return err
		}
		switch {
		case stored.Token != "":
			// The stream has delivered something before, so resume exactly after it.
			token, err := stored.token()
			if err != nil {
				return err
			}
			opts.SetResumeAfter(token)
		case stored.Cluster != 0:
			// Nothing delivered yet: this is the cluster time the snapshot pinned, so
			// starting there replays the writes made while the copy ran. Starting from
			// now loses that window silently.
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

	stream, err := r.Client.Watch(ctx, stages, opts)
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
	// Whether this stream is paying for a lookup per update, which is worth
	// seeing rather than inferring from the throughput.
	metrics.SetWholeDocumentMode(r.Labels, !r.deltas)
	// Only when the task lists its collections. One that lists none replicates
	// the database as a whole, and this counts the task's list, so it would
	// report nothing captured while every collection was being replicated. The
	// syncer's scan of the source publishes that case.
	if n := r.capturedCollections(); n > 0 {
		metrics.SetCapturedTables(r.Labels, n)
	}
	return nil
}

// Next drains a whole source transaction before returning its first event: they
// share an lsid and txnNumber, and handing them over together is what stops a
// batch being cut inside one.
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

// fill reads until it has a complete source transaction, or the stream goes
// quiet and a heartbeat is due.
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

		// TryNext returned nothing on a healthy stream, so the cursor is exhausted —
		// and a committed transaction's events arrive contiguously, so an open one
		// with nothing behind it is complete.
		if len(r.open) > 0 {
			r.seal()
			return nil
		}

		// Nothing left to deliver is also the one moment it is known that the
		// target is not ahead of the stream, which is what an update written as
		// a delta needs.
		if !r.deltas && !pipeline.MongoWholeDocuments() {
			if err := r.dropTheLookup(ctx); err != nil {
				return err
			}
			continue
		}

		// The await window is short so that an event is not held, but a heartbeat
		// is a different thing: it writes a position to the target and asks the
		// source for its clock, and doing that five times a second buys nothing.
		if !r.lastBeat.IsZero() && time.Since(r.lastBeat) < idleHeartbeat {
			continue
		}

		token := r.stream.ResumeToken()
		if token == nil {
			continue
		}
		// The heartbeat carries the source's clock, not just a token: it means
		// everything up to here was delivered, which is how a re-copy knows the
		// stream passed the point its chunk was read at.
		at, clockErr := r.sourceClock(ctx)
		if clockErr != nil {
			r.Logger.Warnf("[MongoDB] Could not read the source's cluster time for a "+
				"heartbeat: %v", clockErr)
		}
		payload, err := encodeTokenAt(token, at)
		if err != nil {
			return err
		}
		r.lastBeat = time.Now()
		r.ready = append(r.ready, &domain.Event{
			Heartbeat:       true,
			EndsTransaction: true,
			Pos:             domain.Position{Payload: payload},
			SourceTime:      at,
		})
		return nil
	}
}

// dropTheLookup reopens the stream without asking the server to attach the
// whole document to every update, so that an update is replicated as the
// fields it touched. A megabyte document whose status flipped was a megabyte
// read on the source, a megabyte over the link and a megabyte written to the
// target for the sake of one field.
//
// It waits for the stream to run out because a delta may only be applied to a
// document the target holds at the same point the change was made from. That
// is true of everything the stream delivers from here on, and it is not true
// of the catch-up: the first copy reads each document as it reaches it and the
// stream then restarts from before the copy began, so a document on the target
// can be newer than the change being replayed. A whole document written twice
// is the same document; a delta replayed over a newer one silently puts old
// values back, and nothing later in the stream mentions those fields again.
//
// Reopening costs one request. The alternative -- deciding per event by its
// cluster time -- would need the document read back for every event of the
// catch-up, one round trip each, and would lose the bound the batch's byte
// limit puts on how much a batch holds.
func (r *Reader) dropTheLookup(ctx context.Context) error {
	token := r.stream.ResumeToken()
	if token == nil {
		// Nothing delivered yet, so there is nothing to resume after. The next
		// quiet moment will have one.
		return nil
	}
	payload, err := encodeTokenAt(token, r.lastAt)
	if err != nil {
		return err
	}
	if err := r.stream.Close(ctx); err != nil {
		r.Logger.Warnf("[MongoDB] Could not close the change stream before reopening "+
			"it to stop asking for whole documents: %v", err)
	}

	r.deltas = true
	if err := r.Open(ctx, domain.Position{Payload: payload}); err != nil {
		return fmt.Errorf("reopen the change stream to replicate updates as the "+
			"fields they touch: %w", err)
	}
	r.Logger.Info("[MongoDB] Caught up, so updates are replicated as the fields " +
		"they change rather than as whole documents")
	return nil
}

// capturedCollections counts the objects this task watches (Debezium:
// CapturedTables) — the number moving on its own is how a dropped mapping shows
// up.
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

// seal closes the open transaction and moves it to the waiting events, marking
// its last one as the boundary a batch may be cut at.
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
		// A change that cannot be converted must not be skipped: the two sides would
		// diverge from here with only a log line to show it.
		return domain.Unrecoverable(
			"a change to %s could not be turned into a write for the target (%v). "+
				"Replication has stopped rather than skip it", ns, err)
	}
	if model == nil {
		// The task asked for this operation to be ignored, which is a decision rather
		// than a failure.
		return nil
	}

	at, _ := eventClusterTime(raw)
	token, err := encodeTokenAt(raw.Lookup("_id").Document(), at)
	if err != nil {
		return err
	}

	if !at.IsZero() {
		r.lastAt = at
	}

	wall, _ := eventWallTime(raw)
	event := &domain.Event{
		NS:         ns,
		Op:         opOf(raw),
		Key:        keyOf(raw),
		Payload:    model,
		Bytes:      len(raw),
		Pos:        domain.Position{Payload: token},
		SourceTime: at,
		WallTime:   wall,
	}

	txID := transactionOf(raw)
	switch {
	case txID == "":
		// Not part of a multi-document transaction, so whatever was open has ended
		// and this is its own boundary.
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

// watchedOperations are the four row operations plus the schema changes that
// keep the target's shape in step. "invalidate" is asked for because it is how
// the server says the stream cannot continue.
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
		// The stream is over: what it watched is gone. A token from an invalidated
		// stream cannot be used, so pretending otherwise replicates nothing quietly.
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

	at, _ := eventClusterTime(raw)
	token, err := encodeTokenAt(raw.Lookup("_id").Document(), at)
	if err != nil {
		return err
	}

	// A schema change is its own boundary and gets its own batch: MongoDB's
	// catalogue is not transactional, so it cannot share one with rows.
	r.seal()
	wall, _ := eventWallTime(raw)
	r.ready = append(r.ready, &domain.Event{
		NS:              ns,
		Op:              domain.OpSchema,
		Payload:         change,
		Bytes:           len(raw),
		Pos:             domain.Position{Payload: token},
		SourceTime:      at,
		WallTime:        wall,
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
		// The task lists no collections, so every collection of a mapped database is
		// replicated under its own name, apart from the syncer's own bookkeeping.
		// The snapshot skips by discovery's rule; any other rule here lets the stream
		// copy the source's direction claim over the target's.
		return !discovery.IsInternal(ns.Object)
	}
	return r.mapped[strings.ToLower(ns.Object)]
}

// mappedCollections lists the collections the task names, empty when it names
// none.
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

// mappedDatabases lists those databases in the server's spelling, for the
// server-side filter.
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

// keyOf identifies the document a change touched: the whole documentKey,
// because on a sharded collection it carries the shard key and a filter without
// it cannot be routed to one shard.
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
// empty when it belongs to none: every event of one carries the same session id
// and transaction number.
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

// streamPosition holds either kind of position, which is what stops anybody
// confusing them: before the stream delivers anything only the snapshot's
// pinned cluster time exists; after, the resume token is exact. Guessing got
// "Bad resume token" while the task looked transiently unhealthy.
type streamPosition struct {
	// Token is a resume token as extended JSON, once the stream has delivered an
	// event.
	Token string `json:"token,omitempty"`
	// Cluster and Increment are a pinned cluster time, set by the snapshot before
	// delivery.
	Cluster   uint32 `json:"cluster,omitempty"`
	Increment uint32 `json:"increment,omitempty"`
	// At is the cluster time of the event the token belongs to, in seconds.
	//
	// It is recorded beside the token and never used to resume: a resume token
	// is exact and a timestamp is not, so resuming from the timestamp would
	// re-deliver every event that shared its second. It is here to be read --
	// a token is opaque, so with only a token stored there is no way to answer
	// "has the target applied what the source had at this moment", which is the
	// question a switch-over asks.
	At int64 `json:"at,omitempty"`
}

func (p streamPosition) token() (bson.Raw, error) {
	var token bson.Raw
	if err := bson.UnmarshalExtJSON([]byte(p.Token), true, &token); err != nil {
		return nil, fmt.Errorf("read the stored resume token: %w", err)
	}
	return token, nil
}

// encodeTokenAt stores a resume token together with the cluster time of the
// event it came from, so the stored position can be read as well as resumed
// from.
func encodeTokenAt(token bson.Raw, at time.Time) (string, error) {
	if token == nil {
		return "", nil
	}
	encoded, err := bson.MarshalExtJSON(bson.Raw(token), true, false)
	if err != nil {
		return "", fmt.Errorf("encode the resume token: %w", err)
	}
	position := streamPosition{Token: string(encoded)}
	if !at.IsZero() {
		position.At = at.Unix()
	}
	return checkpoint.Encode(position)
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
