package mongodb

import (
	"context"
	"fmt"
	"os"
	"strings"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
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

	// NoTransaction applies the batch as bare bulk writes.
	//
	// The zero value keeps the transaction, because the safe setting is the one
	// an operator gets without knowing to ask for it. Turning it off is the
	// trade AWS DMS spells BatchApplyEnabled: more throughput, and — in their
	// words — temporary lapses in transactional integrity. On a sharded cluster
	// a batch spanning shards is a two-phase commit, so the cost is real and
	// worth measuring; the default is still the correct one.
	NoTransaction bool
}

// Apply writes every run of the batch, then the position.
func (a *Applier) Apply(ctx context.Context, runs [][]*domain.Event, pos domain.Position) (bool, error) {
	if a.Client == nil {
		return false, fmt.Errorf("no target connection")
	}
	if len(runs) == 0 {
		return false, nil
	}

	if a.NoTransaction {
		if err := a.write(ctx, runs); err != nil {
			return false, err
		}
		return false, nil
	}

	session, err := a.Client.StartSession()
	if err != nil {
		return false, fmt.Errorf("start a session for the batch: %w", err)
	}
	defer session.EndSession(ctx)

	committed := false
	// Majority on both sides: a batch acknowledged by less than a majority can
	// be rolled back by an election, and this target exists to survive one.
	txOpts := options.Transaction().
		SetReadConcern(readconcern.Snapshot()).
		SetWriteConcern(writeconcern.Majority())

	_, err = session.WithTransaction(ctx, func(sc mongo.SessionContext) (interface{}, error) {
		if err := a.write(sc, runs); err != nil {
			return nil, err
		}
		if a.Checkpoints != nil && !pos.IsZero() {
			if err := a.Checkpoints.SaveIn(sc, a.CheckpointKey, pos.Payload); err != nil {
				return nil, err
			}
			committed = true
		}
		return nil, nil
	}, txOpts)
	if err != nil {
		// WithTransaction has already aborted, so nothing of the batch is on the
		// target. Reporting committed as false would be a lie of a different
		// kind — the caller must not record a position for a batch that did not
		// land — so it is reset here.
		return false, fmt.Errorf("apply the batch: %w", err)
	}
	return committed, nil
}

// write applies the runs in order. Within a run no document appears twice, so
// its operations may go out together; between runs they may not.
func (a *Applier) write(ctx context.Context, runs [][]*domain.Event) error {
	for _, run := range runs {
		for _, group := range groupByCollection(run) {
			target := a.targetFor(group.collection)
			collection := a.Client.Database(a.TargetDatabase).Collection(target)

			// Unordered: no document appears twice in a run, so the server is
			// free to apply them in any order — which is the same freedom a
			// secondary's writer threads get, and for the same reason.
			_, err := collection.BulkWrite(ctx, group.models, options.BulkWrite().SetOrdered(false))
			if err != nil {
				return fmt.Errorf("write %d changes to %s.%s: %w",
					len(group.models), a.TargetDatabase, target, err)
			}
		}
	}
	return nil
}

// collectionGroup is the operations of one run that belong to one collection.
type collectionGroup struct {
	collection string
	models     []mongo.WriteModel
}

// groupByCollection splits a run by collection, keeping the order the
// collections first appeared in.
//
// BulkWrite addresses one collection, so a run spanning several needs one call
// each. Order between the calls is kept because a run may hold a change to a
// document in one collection that a change in another depends on having landed.
func groupByCollection(run []*domain.Event) []collectionGroup {
	var groups []collectionGroup
	index := map[string]int{}

	for _, event := range run {
		model, ok := event.Payload.(mongo.WriteModel)
		if !ok || model == nil {
			continue
		}
		at, seen := index[event.NS.Object]
		if !seen {
			index[event.NS.Object] = len(groups)
			groups = append(groups, collectionGroup{collection: event.NS.Object})
			at = len(groups) - 1
		}
		groups[at].models = append(groups[at].models, model)
	}
	return groups
}

// targetFor resolves a source collection to the name it is written under.
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
