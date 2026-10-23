package mongodb

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// convertRawBSONToWriteModel turns one buffered change stream event into the
// write that applies it. It reports four things apart: a write to make, a
// change that needs the whole document read from the source before it can be
// written, nothing to do — an event this does not replicate, or a delete the
// task is configured to ignore — and an event it could not read.
//
// The write is returned as the event's payload rather than as a
// mongo.WriteModel, because one of the four is not a write yet.
func (s *MongoDBSyncer) convertRawBSONToWriteModel(rawData bson.Raw, sourceDB, collectionName string) (interface{}, error) {
	var event bson.M
	if err := bson.Unmarshal(rawData, &event); err != nil {
		return nil, fmt.Errorf("read a buffered change: %w", err)
	}

	opType, _ := event["operationType"].(string)
	if opType == "" {
		return nil, fmt.Errorf("a buffered change carries no operation type")
	}

	switch opType {
	case "insert":
		// An upsert rather than an insert: the stream resumes from the cluster
		// time the snapshot pinned, so the inserts made while the copy was
		// running arrive again for documents the copy already wrote. A plain
		// insert would fail on every one of them.
		fullDoc, ok := event["fullDocument"]
		if !ok {
			return nil, fmt.Errorf("an insert event for %s.%s carries no document",
				sourceDB, collectionName)
		}
		fullDoc = s.maskValue(sourceDB, collectionName, fullDoc)
		filter := filterOf(event)
		if filter == nil {
			if id := idOf(fullDoc); id != nil {
				filter = bson.M{"_id": id}
			}
		}
		if filter == nil {
			return mongo.NewInsertOneModel().SetDocument(fullDoc), nil
		}
		return mongo.NewReplaceOneModel().
			SetFilter(filter).
			SetReplacement(fullDoc).
			SetUpsert(true), nil

	case "update", "replace":
		filter := filterOf(event)
		if filter == nil {
			return nil, fmt.Errorf("an %s event for %s.%s names no document",
				opType, sourceDB, collectionName)
		}

		// A replace carries the new document, and so does an update while the
		// stream is asking for the lookup. The server sends the field as null
		// rather than leaving it out when the document was deleted between the
		// change and the lookup, so a present-but-null field is not a document.
		if fullDoc, ok := event["fullDocument"]; ok && fullDoc != nil {
			return mongo.NewReplaceOneModel().
				SetFilter(filter).
				SetReplacement(s.maskValue(sourceDB, collectionName, fullDoc)).
				SetUpsert(true), nil
		}

		// Otherwise the fields the change touched, which is what the target
		// needs: a megabyte document whose status flipped cost a megabyte
		// through the lookup, over the link and into the target for the sake of
		// one field.
		update, err := updateFromDescription(event)
		if err != nil {
			// Not everything can be expressed as a delta. Such a change is
			// applied as the whole document, read from the source when the batch
			// is written rather than skipped.
			return &fullDocumentRead{
				filter: filter,
				reason: reasonUndescribed,
				why:    err.Error(),
			}, nil
		}
		if set, ok := update["$set"].(bson.M); ok {
			update["$set"] = s.maskFields(sourceDB, collectionName, set)
		}
		return mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(update), nil

	case "delete":
		// Check if delete operations should be ignored for this collection
		advancedSettings := s.findTableAdvancedSettings(collectionName)
		if advancedSettings.IgnoreDeleteOps {
			s.warnAboutDroppedDeletes(sourceDB, collectionName)
			return nil, nil
		}

		filter := filterOf(event)
		if filter == nil {
			return nil, fmt.Errorf("a delete event for %s.%s names no document",
				sourceDB, collectionName)
		}
		return mongo.NewDeleteOneModel().SetFilter(filter), nil
	}

	// Collection-level events — drop, rename, invalidate — are not replicated.
	// They are the same decision the MySQL side makes about a destructive DDL,
	// and they are not row changes, so there is nothing to write here.
	s.logger.Warnf("[MongoDB] %s.%s: not replicating a %q event", sourceDB, collectionName, opType)
	return nil, nil
}

// updateFromDescription builds the update an event describes.
//
// It reports an error for anything it cannot express exactly. The caller reads
// the whole document instead, which is always right and merely slower — the
// alternative is writing an update that is nearly the change, and a payment
// row that is nearly right is wrong.
func updateFromDescription(event bson.M) (bson.M, error) {
	description := documentOf(event["updateDescription"])
	if description == nil {
		return nil, fmt.Errorf("an update event carries neither the document nor a " +
			"description of what changed")
	}

	update := bson.M{}
	var touched []string

	if set := documentOf(description["updatedFields"]); len(set) > 0 {
		update["$set"] = set
		touched = append(touched, fieldsOf(set)...)
	}
	if removed, ok := description["removedFields"].(bson.A); ok && len(removed) > 0 {
		unset := bson.M{}
		for _, field := range removed {
			if name, ok := field.(string); ok {
				unset[name] = ""
			}
		}
		if len(unset) > 0 {
			update["$unset"] = unset
			touched = append(touched, fieldsOf(unset)...)
		}
	}
	// An array the change shortened. The event says how long the array is now,
	// not which elements went, and $push with an empty $each and a $slice is
	// how an update says "keep the first n of it". Without this the target
	// would keep elements the source has dropped and nothing later in the
	// stream would mention that array again.
	if truncated, ok := description["truncatedArrays"].(bson.A); ok && len(truncated) > 0 {
		push := bson.M{}
		for _, entry := range truncated {
			cut := documentOf(entry)
			field, _ := cut["field"].(string)
			size, sized := sizeOf(cut["newSize"])
			if field == "" || !sized || size < 0 {
				return nil, fmt.Errorf("an update shortened an array without saying " +
					"which one, or to what length")
			}
			push[field] = bson.M{"$each": bson.A{}, "$slice": size}
			touched = append(touched, field)
		}
		update["$push"] = push
	}

	if len(update) == 0 {
		return nil, fmt.Errorf("an update event describes no change")
	}
	// MongoDB refuses one update that writes both a field and something inside
	// it, and the refusal would arrive as a failed batch. The change is real
	// either way, so it is read as a whole document instead.
	if overlap := overlappingPath(touched); overlap != "" {
		return nil, fmt.Errorf("the fields an update touched overlap at %q, which "+
			"cannot be written as one update", overlap)
	}
	return update, nil
}

// fieldsOf names the paths a stage of an update writes to.
func fieldsOf(stage bson.M) []string {
	names := make([]string, 0, len(stage))
	for field := range stage {
		names = append(names, field)
	}
	return names
}

// overlappingPath reports a path that is another's parent, or "" when none is.
// "items" and "items.0.qty" overlap; "items.0" and "items.1" do not.
func overlappingPath(paths []string) string {
	sorted := make([]string, len(paths))
	copy(sorted, paths)
	sort.Strings(sorted)
	for i := 1; i < len(sorted); i++ {
		parent, child := sorted[i-1], sorted[i]
		if parent == child || strings.HasPrefix(child, parent+".") {
			return child
		}
	}
	return ""
}

// sizeOf reads a length the server sent, whichever integer type it arrived as.
func sizeOf(value interface{}) (int, bool) {
	switch held := value.(type) {
	case int:
		return held, true
	case int32:
		return int(held), true
	case int64:
		return int(held), true
	case float64:
		if held != math.Trunc(held) {
			return 0, false
		}
		return int(held), true
	}
	return 0, false
}

// filterOf is how the target addresses the document a change touched. It is
// the whole documentKey, not just the _id.
func filterOf(event bson.M) bson.M {
	key := documentOf(event["documentKey"])
	if len(key) == 0 {
		return nil
	}
	if _, hasID := key["_id"]; !hasID {
		// Every documentKey carries an _id. One that does not is not something
		// to guess at.
		return nil
	}
	return key
}

// documentOf renders a nested BSON document as a map, whichever shape the driver
// decoded it into.
func documentOf(value interface{}) bson.M {
	switch held := value.(type) {
	case bson.M:
		out := make(bson.M, len(held))
		for field, v := range held {
			out[field] = v
		}
		return out
	case map[string]interface{}:
		out := make(bson.M, len(held))
		for field, v := range held {
			out[field] = v
		}
		return out
	case bson.D:
		out := make(bson.M, len(held))
		for _, element := range held {
			out[element.Key] = element.Value
		}
		return out
	case bson.Raw:
		var out bson.M
		if err := bson.Unmarshal(held, &out); err != nil {
			return nil
		}
		return out
	}
	return nil
}
