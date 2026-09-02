package mongodb

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
)

// Applier writes one batch to a MongoDB target, by default inside a transaction
// so the batch and its position land together or not at all.
//
// MongoDB gives atomicity for a single document and nothing wider without a
// transaction. A batch applied as bare bulk writes therefore commits an
// operation at a time: a failure part way through — and a partly failed
// BulkWrite is an ordinary occurrence, not a crash — leaves the target holding
// some of the batch. Because the events of one batch may touch several
// collections in the order the source wrote them, what remains can be a state
// the source was never in: the payment present and the order it belongs to
// absent. Nothing downstream is written to cope with that, and a reconciliation
// by document count does not see it.
//
// A secondary has this property for free. It applies an oplog batch in parallel
// and holds its readable timestamp at the batch boundary, so no reader ever sees
// the middle of one. A client cannot hold anybody's read timestamp; a
// transaction is the only way it can say "these writes become visible together".
type Applier struct {
	Client *mongo.Client
	// TargetDatabase is the database the events are written to.
	TargetDatabase string
	// Mappings resolve a source collection to its target name.
	Mappings []config.DatabaseMapping
	// Checkpoints records the position. Nil means the runner records it, which
	// is the weaker at-least-once guarantee.
	Checkpoints   *checkpoint.MongoStore
	CheckpointKey string
	Logger        logrus.FieldLogger
	Labels        metrics.Labels

	// NoTransaction applies the batch as bare bulk writes.
	//
	// The zero value keeps the transaction, because the safe setting is the one
	// an operator gets without knowing to ask for it. Turning it off is the
	// trade AWS DMS spells BatchApplyEnabled: more throughput, and — in their
	// words — temporary lapses in transactional integrity. On a sharded cluster
	// a batch spanning shards is a two-phase commit, so the cost is real and
	// worth measuring; the default is still the correct one.
	NoTransaction bool

	// bulk remembers whether the target has the cross-collection bulkWrite
	// command, so a target without it is discovered once rather than on every
	// batch.
	bulk clientBulkSupport
}

func (a *Applier) Apply(ctx context.Context, runs [][]*domain.Event, pos domain.Position) (bool, error) {
	if a.Client == nil {
		return false, fmt.Errorf("no target connection")
	}
	if len(runs) == 0 {
		return false, nil
	}

	// A schema change stands alone in its batch and runs outside a transaction:
	// MongoDB's catalogue is not transactional, so a DDL inside one is refused.
	if schema, ok := onlySchemaChange(runs); ok {
		return false, a.applySchemaChange(ctx, schema)
	}

	started := time.Now()
	events, namespaces := shapeOf(runs)

	if a.NoTransaction {
		trips, err := a.write(ctx, runs)
		if err != nil {
			return false, err
		}
		metrics.ObserveBatch(a.Labels, time.Since(started), 0, trips, namespaces, events)
		return false, nil
	}

	session, err := a.Client.StartSession()
	if err != nil {
		return false, fmt.Errorf("start a session for the batch: %w", err)
	}
	defer session.EndSession(ctx)

	committed := false
	trips := 0
	// writing is the time inside the transaction's body. What is left of the
	// total is the commit, which on a sharded target is a two-phase protocol
	// when the batch spans shards — the number that decides whether applying
	// batches concurrently could help or would only make commits contend.
	var writing time.Duration

	// Majority on both sides: a batch acknowledged by less than a majority can
	// be rolled back by an election, and this target exists to survive one.
	txOpts := options.Transaction().
		SetReadConcern(readconcern.Snapshot()).
		SetWriteConcern(writeconcern.Majority())

	// The callback is handed a context carrying the session, so every write it
	// makes joins the transaction. In the driver's v1 this was a distinct
	// SessionContext type; in v2 it is an ordinary context and the session is
	// recovered from it when needed.
	_, err = session.WithTransaction(ctx, func(sc context.Context) (interface{}, error) {
		bodyStarted := time.Now()
		written, err := a.write(sc, runs)
		if err != nil {
			return nil, err
		}
		trips = written
		if a.Checkpoints != nil && !pos.IsZero() {
			if err := a.Checkpoints.SaveIn(sc, a.CheckpointKey, pos.Payload); err != nil {
				return nil, err
			}
			trips++
			committed = true
		}
		writing = time.Since(bodyStarted)
		return nil, nil
	}, txOpts)
	if err != nil {
		// WithTransaction has already aborted, so nothing of the batch is on the
		// target. Reporting committed as false would be a lie of a different
		// kind — the caller must not record a position for a batch that did not
		// land — so it is reset here.
		return false, fmt.Errorf("apply the batch: %w", err)
	}

	total := time.Since(started)
	commit := total - writing
	if commit < 0 {
		commit = 0
	}
	metrics.ObserveBatch(a.Labels, total, commit, trips, namespaces, events)
	return committed, nil
}

// shapeOf reports how many changes a batch carries and how many objects they
// touch. The second number is what decides whether a write that spans objects
// would save a round trip, and how often a sharded commit spans shards.
func shapeOf(runs [][]*domain.Event) (events, namespaces int) {
	seen := map[string]bool{}
	for _, run := range runs {
		for _, event := range run {
			events++
			seen[event.NS.String()] = true
		}
	}
	return events, len(seen)
}

// onlySchemaChange reports the batch's single schema change, when that is all it
// holds. The pipeline gives a schema change a batch of its own, so anything else
// alongside one is a bug worth failing on rather than guessing at.
func onlySchemaChange(runs [][]*domain.Event) (*domain.Event, bool) {
	var found *domain.Event
	count := 0
	for _, run := range runs {
		for _, event := range run {
			count++
			if event.Op == domain.OpSchema {
				found = event
			}
		}
	}
	return found, found != nil && count == 1
}

// write applies the runs in order. Within a run no document appears twice, so
// its operations may go out together; between runs they may not.
func (a *Applier) write(ctx context.Context, runs [][]*domain.Event) (roundTrips int, err error) {
	for _, run := range runs {
		// One request for the whole run, whatever collections it touches. A
		// batch spanning two collections cost two requests before, against a
		// 7 ms round trip and a 30 ms batch — two thirds of the time a batch
		// took was waiting for the network.
		if !a.bulk.unsupported.Load() {
			trips, err := a.writeRunAsOne(ctx, run)
			if err == nil {
				roundTrips += trips
				continue
			}
			if !lacksClientBulkWrite(err) {
				return roundTrips + trips, err
			}
			// The target has no bulkWrite command, which means it is older than
			// 8.0. Noted once, and written per collection from here on.
			a.bulk.unsupported.Store(true)
			if a.Logger != nil {
				a.Logger.Warnf("[MongoDB] The target has no bulkWrite command, so each "+
					"collection in a batch takes its own request: %v", err)
			}
		}

		for _, group := range groupByCollection(run) {
			target := a.targetFor(group.collection)
			collection := a.Client.Database(a.TargetDatabase).Collection(target)

			// Ordered, for the reason writeRunAsOne is: two documents are not
			// independent when a unique index relates them, so the order they
			// were read in is the only one known to be correct.
			roundTrips++
			if _, err := collection.BulkWrite(ctx, group.models, options.BulkWrite().SetOrdered(true)); err != nil {
				return roundTrips, fmt.Errorf("write %d changes to %s.%s: %w",
					len(group.models), a.TargetDatabase, target, err)
			}
		}
	}
	return roundTrips, nil
}

type collectionGroup struct {
	collection string
	models     []mongo.WriteModel
}

// groupByCollection splits a run into consecutive stretches of one collection.
//
// BulkWrite addresses one collection, so a run spanning several needs one call
// each, and the calls are made in sequence.
//
// The stretches have to be consecutive. Gathering every change to a collection
// into one group, wherever in the run it appeared, reorders the run: all of one
// collection's writes go before all of another's. That was harmless while a run
// held at most one change per document and the batch was split into runs that
// separated them; it is not harmless now that a run is the whole batch, in the
// order it was read. A payment and the order it belongs to live in different
// collections, and which lands first is the difference between a target that
// was never in a state the source was not.
//
// The cost is a request per stretch rather than per collection, and only where
// a batch interleaves them. The path this feeds is itself the fallback, for a
// target older than 8.0; a current one sends the whole run in one request.
func groupByCollection(run []*domain.Event) []collectionGroup {
	var groups []collectionGroup

	for _, event := range run {
		model, ok := event.Payload.(mongo.WriteModel)
		if !ok || model == nil {
			continue
		}
		if len(groups) == 0 || groups[len(groups)-1].collection != event.NS.Object {
			groups = append(groups, collectionGroup{collection: event.NS.Object})
		}
		groups[len(groups)-1].models = append(groups[len(groups)-1].models, model)
	}
	return groups
}

func (a *Applier) targetFor(source string) string {
	for _, mapping := range a.Mappings {
		for _, table := range mapping.Tables {
			if strings.EqualFold(table.SourceTable, source) {
				if table.TargetTable != "" {
					return table.TargetTable
				}
				return table.SourceTable
			}
		}
	}
	// The task lists nothing, so everything is replicated under its own name.
	return source
}

// noTransaction reads the escape hatch from the environment.
//
// SYNC_MONGO_NO_TRANSACTION=1 applies batches as bare bulk writes. It exists
// because on a sharded cluster a batch that spans shards is a two-phase commit,
// and whether that cost is affordable is a measurement rather than an opinion.
// The default is the safe one: an operator who has not measured gets the
// guarantee, and one who has measured can trade it away deliberately.
func noTransaction() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("SYNC_MONGO_NO_TRANSACTION"))) {
	case "1", "true", "yes":
		return true
	}
	return false
}

// describeNoTransaction reports what setting the escape hatch costs, or "" when
// it is not set.
//
// Separate from noTransaction so it can be tested without a cluster, and said at
// startup rather than left to whoever reads the code: the variable is read once,
// deep in here, and a task started with it set looked exactly like a task
// without it.
func describeNoTransaction(bare bool, taskID int) string {
	if !bare {
		return ""
	}
	return fmt.Sprintf("[MongoDB] Task %d: SYNC_MONGO_NO_TRANSACTION is set, so batches "+
		"are applied as bare bulk writes. A batch interrupted part way is then applied "+
		"in part while the position moves past it, so the target is quietly missing "+
		"changes and the next consistency check is what finds them — if one is running. "+
		"Unset it unless the two-phase commit cost has been measured on this cluster and "+
		"traded away deliberately, and set SYNC_VERIFY_INTERVAL while it is set.", taskID)
}
