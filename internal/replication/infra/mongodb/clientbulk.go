package mongodb

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// Writing a run of changes in one request rather than one per collection.
//
// A batch spans several collections — one stream carries them all, and a source
// transaction that writes an order and its payment touches two. BulkWrite
// addresses one collection, so such a batch cost one request per collection: the
// measurement on a three-shard 8.0 cluster put it at 2.33 requests per batch,
// against a 7 ms round trip and a 30 ms batch. Two thirds of the time a batch
// took was waiting for the network.
//
// MongoDB 8.0 added a bulkWrite command that carries the namespace on each write
// instead, so a whole run goes in one request whatever it touches. That is the
// only reason the driver was taken to v2; this is where the reason is spent.
//
// A server that does not have the command is not an error worth stopping for.
// The applier notices once and writes per collection from then on, which is what
// it did before.

// clientBulkUnsupported is set once a target has been found not to have the
// bulkWrite command, so the fallback is chosen without another failed attempt.
type clientBulkSupport struct{ unsupported atomic.Bool }

// writeRunAsOne writes one run in a single request, whatever collections it
// touches, and reports how many requests it took.
func (a *Applier) writeRunAsOne(ctx context.Context, run []*domain.Event) (int, error) {
	writes := make([]mongo.ClientBulkWrite, 0, len(run))
	for _, event := range run {
		model, ok := event.Payload.(mongo.WriteModel)
		if !ok || model == nil {
			continue
		}
		clientModel, err := clientModelOf(model)
		if err != nil {
			return 0, err
		}
		writes = append(writes, mongo.ClientBulkWrite{
			Database:   a.TargetDatabase,
			Collection: a.targetFor(event.NS.Object),
			Model:      clientModel,
		})
	}
	if len(writes) == 0 {
		return 0, nil
	}

	// Ordered, because the order is the only one known to be correct.
	//
	// It used to be unordered, on the grounds that no document appears twice in
	// a run so the server could apply them in any order. That reasoning holds
	// for two changes to two documents and nothing else: two documents are not
	// independent when a unique index relates them, and handing a unique value
	// from one to another is an ordinary thing to do. Applied the wrong way
	// round, the write that takes the value runs before the one that frees it —
	// and because every write here is an upsert, the result is not an error but
	// a document rewritten where it should have been inserted.
	//
	// The cost is that the server applies the batch in sequence rather than
	// concurrently. It is not paid in round trips: a whole batch now goes in one
	// request, where before it took one per run.
	_, err := a.Client.BulkWrite(ctx, writes, options.ClientBulkWrite().SetOrdered(true))
	if err != nil {
		return 1, err
	}
	return 1, nil
}

// clientModelOf turns a collection-level write model into the client-level one
// that carries its own namespace.
//
// The fields are the same on both; what differs is that the client-level model
// is addressed by a namespace given alongside it rather than by the collection
// the call was made on.
func clientModelOf(model mongo.WriteModel) (mongo.ClientWriteModel, error) {
	switch m := model.(type) {
	case *mongo.ReplaceOneModel:
		out := mongo.NewClientReplaceOneModel().
			SetFilter(m.Filter).
			SetReplacement(m.Replacement)
		if m.Upsert != nil {
			out.SetUpsert(*m.Upsert)
		}
		if m.Hint != nil {
			out.SetHint(m.Hint)
		}
		return out, nil

	case *mongo.UpdateOneModel:
		out := mongo.NewClientUpdateOneModel().
			SetFilter(m.Filter).
			SetUpdate(m.Update)
		if m.Upsert != nil {
			out.SetUpsert(*m.Upsert)
		}
		if m.ArrayFilters != nil {
			out.SetArrayFilters(m.ArrayFilters)
		}
		if m.Hint != nil {
			out.SetHint(m.Hint)
		}
		return out, nil

	case *mongo.DeleteOneModel:
		out := mongo.NewClientDeleteOneModel().SetFilter(m.Filter)
		if m.Hint != nil {
			out.SetHint(m.Hint)
		}
		return out, nil

	case *mongo.InsertOneModel:
		return mongo.NewClientInsertOneModel().SetDocument(m.Document), nil
	}

	// Not a shape this produces. Guessing at it would write something nobody
	// asked for, so it stops instead.
	return nil, domain.Unrecoverable(
		"a %T is not a write this knows how to send as part of a cross-collection "+
			"request", model)
}

// lacksClientBulkWrite reports whether a failure means the target has no
// bulkWrite command, rather than that the write was refused.
//
// CommandNotFound is what a server before 8.0 answers. The text check covers
// the proxies and older mongos builds that report it differently.
func lacksClientBulkWrite(err error) bool {
	if err == nil {
		return false
	}
	var serverErr mongo.ServerError
	if errors.As(err, &serverErr) && serverErr.HasErrorCode(59) {
		return true
	}
	text := err.Error()
	for _, marker := range []string{
		"no such command: 'bulkWrite'",
		"Unrecognized command: bulkWrite",
		"command bulkWrite is not supported",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}
