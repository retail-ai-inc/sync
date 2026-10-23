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

// Writing a run of changes in one request rather than one per collection. A
// batch spans several collections — one stream carries them all, and a source
// transaction that writes an order and its payment touches two.

// clientBulkSupport remembers that a target lacks the bulkWrite command, so the
// fallback is chosen without another failed attempt.
type clientBulkSupport struct{ unsupported atomic.Bool }

// writeRunAsOne writes one run in a single request, whatever collections it
// touches, and reports how many requests it took and how many of the documents
// its updates addressed were there to be written.
func (a *Applier) writeRunAsOne(ctx context.Context, run []*domain.Event) (int, int64, error) {
	writes := make([]mongo.ClientBulkWrite, 0, len(run))
	for _, event := range run {
		if pending, ok := event.Payload.(*fullDocumentRead); ok {
			// The applier resolves these before writing, so one here means the
			// resolution was skipped. Sending the rest of the batch would record
			// a position past a change the target never received.
			return 0, 0, domain.Unrecoverable(
				"a change to %s still needs its document read from the source (%s) "+
					"when the batch is being written", event.NS, pending.reason)
		}
		model, ok := event.Payload.(mongo.WriteModel)
		if !ok || model == nil {
			continue
		}
		clientModel, err := clientModelOf(model)
		if err != nil {
			return 0, 0, err
		}
		writes = append(writes, mongo.ClientBulkWrite{
			Database:   a.TargetDatabase,
			Collection: a.targetFor(event.NS.Object),
			Model:      clientModel,
		})
	}
	if len(writes) == 0 {
		return 0, 0, nil
	}

	// Ordered, because the order is the only one known to be correct. It used to
	// be unordered, on the grounds that no document appears twice in a run so the
	// server could apply them in any order.
	result, err := a.Client.BulkWrite(ctx, writes, options.ClientBulkWrite().SetOrdered(true))
	if err != nil {
		return 1, 0, err
	}
	return 1, result.MatchedCount + result.UpsertedCount, nil
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
