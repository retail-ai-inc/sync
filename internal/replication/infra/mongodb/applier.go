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

// Applier writes one batch to a MongoDB target, by default in a transaction so
// the batch and its position land together or not at all. MongoDB gives
// atomicity for one document and nothing wider, and a partly failed BulkWrite
// is ordinary — leaving a state the source was never in, the payment present
// and its order absent, which a count comparison cannot see. A secondary gets
// this free by holding its readable timestamp at the batch boundary; a client
// can only get it from a transaction.
type Applier struct {
	Client *mongo.Client
	// TargetDatabase is the database the events are written to.
	TargetDatabase string
	// Mappings resolve a source collection to its target name.
	Mappings []config.DatabaseMapping
	// Checkpoints records the position. Nil means the runner does, which is the
	// weaker at-least-once guarantee.
	Checkpoints   *checkpoint.MongoStore
	CheckpointKey string
	Logger        logrus.FieldLogger
	Labels        metrics.Labels

	// NoTransaction applies the batch as bare bulk writes. The zero value keeps
	// the transaction, because the safe setting is the one an operator gets
	// without asking. Turning it off is the trade AWS DMS calls BatchApplyEnabled.
	NoTransaction bool

	// bulk remembers whether the target has the cross-collection bulkWrite
	// command, so a target without it is discovered once rather than per batch.
	bulk clientBulkSupport
}

func (a *Applier) Apply(ctx context.Context, runs [][]*domain.Event, pos domain.Position) (bool, error) {
	if a.Client == nil {
		return false, fmt.Errorf("no target connection")
	}
	if len(runs) == 0 {
		return false, nil
	}

	// A schema change stands alone and runs outside a transaction: MongoDB's
	// catalogue is not transactional, so a DDL inside one is refused.
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
	// writing is the time inside the transaction body; the rest is the commit,
	// which on a sharded target is two-phase when the batch spans shards.
	var writing time.Duration

	// Majority on both sides: a batch acknowledged by less than a majority can be
	// rolled back by an election, and this target exists to survive one.
	txOpts := options.Transaction().
		SetReadConcern(readconcern.Snapshot()).
		SetWriteConcern(writeconcern.Majority())

	// The callback gets a context carrying the session, so every write joins the
	// transaction. In driver v1 this was a SessionContext type; in v2 it is an
	// ordinary context.
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
		// target and committed is reset: the caller must not record a position for a
		// batch that did not land.
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
// touch — the second decides whether a cross-object write saves a round trip,
// and how often a sharded commit spans shards.
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

// onlySchemaChange reports the batch's single schema change when that is all it
// holds. The pipeline gives one its own batch, so anything alongside is a bug
// worth failing on.
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
			// independent when a unique index relates them.
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

// groupByCollection splits a run into consecutive stretches of one collection,
// because BulkWrite addresses one. Consecutive, not gathered: grouping every
// change to a collection wherever it appeared would send all of one
// collection's writes before another's, and a payment and its order live in
// different collections.
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

// noTransaction reads the escape hatch. SYNC_MONGO_NO_TRANSACTION=1 applies
// bare bulk writes; it exists because a batch spanning shards is a two-phase
// commit, and that cost is a measurement rather than an opinion.
func noTransaction() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("SYNC_MONGO_NO_TRANSACTION"))) {
	case "1", "true", "yes":
		return true
	}
	return false
}

// describeNoTransaction reports what the escape hatch costs, or "" when unset.
// Separate so it can be tested without a cluster, and said at startup: the
// variable is read once, deep in here, and a task with it set looked like one
// without.
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
