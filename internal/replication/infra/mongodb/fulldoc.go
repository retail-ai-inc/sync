package mongodb

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// Applying a change as the whole document instead of as the fields it touched.
//
// An update is normally written as a delta, which is what the event describes
// and is the difference between a megabyte and a few bytes for a document
// whose status flipped. Two cases cannot be written that way, and both end
// here: a change the event does not describe exactly, and a change addressing
// a document the target does not have — a delta carries the fields that
// changed, so it cannot create one, and an update that matched nothing is a
// divergence with nothing to show for it.

// fullDocumentRead is an event's payload until the document has been read. The
// applier resolves it into a write; a run still holding one when it is written
// is a bug, and the applier says so rather than passing over the change.
type fullDocumentRead struct {
	// filter addresses the document, as the whole documentKey.
	filter bson.M
	// reason is why the delta could not be used, as a bounded label.
	reason string
	// why is the detail for the log, which is free text and stays out of the
	// metric.
	why string
}

const (
	// reasonUndescribed: the event did not describe the change in a way that
	// can be written as one update.
	reasonUndescribed = "undescribed"
	// reasonMissing: the target does not hold the document the update
	// addressed.
	reasonMissing = "missing_on_target"
)

// maxFullDocumentBytes bounds what one batch may pull back from the source.
//
// In a steady state a batch pulls back nothing: an update describes itself and
// the target holds what it addresses. There is no bound on how many documents
// a broken state would ask for, though, and the batch's own byte limit does
// not cover these because a delta is small and the document it stands for is
// not. Without this a batch of five hundred deltas against a collection the
// target never received would read five hundred whole documents into one
// transaction.
const maxFullDocumentBytes = 64 << 20

// resolveFullDocuments turns every pending read in a run into a write.
//
// One read per document rather than one query per collection: an _id may be a
// document, which cannot be matched back to its event by comparison as
// reliably as by asking for it, and this path is the rare one.
func (a *Applier) resolveFullDocuments(ctx context.Context, run []*domain.Event) error {
	pending := 0
	for _, event := range run {
		if _, ok := event.Payload.(*fullDocumentRead); ok {
			pending++
		}
	}
	if pending == 0 {
		return nil
	}
	if a.Source == nil {
		return domain.Unrecoverable(
			"%d changes in this batch have to be applied as whole documents, and "+
				"this applier has no connection to the source to read them from",
			pending)
	}

	read, byReason := 0, map[string]int{}
	held := 0
	for _, event := range run {
		want, ok := event.Payload.(*fullDocumentRead)
		if !ok {
			continue
		}

		raw, err := a.Source.Database(event.NS.DB).Collection(event.NS.Object).
			FindOne(ctx, want.filter).Raw()
		if errors.Is(err, mongo.ErrNoDocuments) {
			// Gone from the source since the change. A delete for it is further
			// along the same stream, so there is nothing to write: writing the
			// delta would leave the target holding a document the source does
			// not have.
			event.Payload = nil
			continue
		}
		if err != nil {
			return fmt.Errorf("read %s to apply a change to it whole: %w", event.NS, err)
		}

		held += len(raw)
		if held > maxFullDocumentBytes {
			return domain.Unrecoverable(
				"applying this batch needs more than %d bytes of whole documents read "+
					"back from the source (%d changes so far, %s). A batch that large "+
					"means the target is missing the documents these changes address, "+
					"and re-copying that collection is the fix. Replication has stopped "+
					"rather than hold the whole of it in one transaction",
				maxFullDocumentBytes, pending, event.NS)
		}

		var document bson.M
		if err := bson.Unmarshal(raw, &document); err != nil {
			return fmt.Errorf("read the document %s returned: %w", event.NS, err)
		}

		event.Payload = mongo.NewReplaceOneModel().
			SetFilter(want.filter).
			SetReplacement(a.mask(event.NS.Object, document)).
			SetUpsert(true)
		read++
		byReason[want.reason]++
		if a.Logger != nil && want.why != "" {
			a.Logger.Debugf("[MongoDB] %s: read whole because %s", event.NS, want.why)
		}
	}

	for reason, n := range byReason {
		metrics.CountWholeDocumentReads(a.Labels, reason, n)
	}
	if read > 0 && a.Logger != nil {
		a.Logger.Warnf("[MongoDB] %d of this batch's changes were applied by reading "+
			"the whole document from the source: %v", read, byReason)
	}
	return nil
}

// mask applies the task's field security to a document read here, so a
// document that came back the long way is treated exactly as one that arrived
// on the stream.
func (a *Applier) mask(collection string, document bson.M) bson.M {
	if a.Mask == nil {
		return document
	}
	masked, ok := a.Mask(collection, document).(bson.M)
	if !ok {
		return document
	}
	return masked
}

// repairMissing writes whole documents for the deltas of a run that matched
// nothing.
//
// The bulk result says how many documents were matched, not which, so the
// deltas are checked against the target one at a time. That is only worth
// doing when the count is short — in a steady state it never is, and this
// returns without a single round trip.
func (a *Applier) repairMissing(ctx context.Context, run []*domain.Event, landed int64) (int, error) {
	addressed := 0
	for _, event := range run {
		switch event.Payload.(type) {
		case *mongo.UpdateOneModel, *mongo.ReplaceOneModel:
			addressed++
		}
	}
	if landed >= int64(addressed) {
		return 0, nil
	}

	var missing []*domain.Event
	for _, event := range run {
		model, ok := event.Payload.(*mongo.UpdateOneModel)
		if !ok {
			continue
		}
		target := a.Client.Database(a.TargetDatabase).Collection(a.targetFor(event.NS.Object))
		err := target.FindOne(ctx, model.Filter,
			options.FindOne().SetProjection(bson.M{"_id": 1})).Err()
		if errors.Is(err, mongo.ErrNoDocuments) {
			event.Payload = &fullDocumentRead{
				filter: documentOf(model.Filter),
				reason: reasonMissing,
			}
			missing = append(missing, event)
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("check whether the target holds the document a change "+
				"to %s addressed: %w", event.NS, err)
		}
	}
	if len(missing) == 0 {
		// Short by the count and yet every document is there: a bulk result that
		// does not add up is not something to guess at.
		return 0, fmt.Errorf("the target reported %d of %d changes applied and holds "+
			"every document they addressed", landed, addressed)
	}

	if a.Logger != nil {
		a.Logger.Warnf("[MongoDB] %d of this batch's %d changes addressed documents the "+
			"target does not have, so they are being written whole. The target is "+
			"missing data these updates assume; a consistency check will say how much",
			len(missing), addressed)
	}
	if err := a.resolveFullDocuments(ctx, missing); err != nil {
		return 0, err
	}
	trips, _, err := a.writeRun(ctx, missing)
	return trips, err
}
