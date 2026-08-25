package mongodb

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// convertRawBSONToWriteModel turns one buffered change stream event into the
// write that applies it.
//
// It reports three things apart: a write to make, nothing to do — an event this
// does not replicate, or a delete the task is configured to ignore — and an
// event it could not read. The third used to be indistinguishable from the
// second: every failure logged and returned nil, so a change nobody could parse
// left the target without it and the batch went on to be recorded as applied.
func (s *MongoDBSyncer) convertRawBSONToWriteModel(rawData bson.Raw, sourceDB, collectionName string) (mongo.WriteModel, error) {
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
		fullDoc = s.maskValue(collectionName, fullDoc)
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

		if fullDoc, ok := event["fullDocument"]; ok {
			return mongo.NewReplaceOneModel().
				SetFilter(filter).
				SetReplacement(s.maskValue(collectionName, fullDoc)).
				SetUpsert(true), nil
		}

		// The stream is opened with fullDocument=updateLookup, so the document
		// is normally attached. It is not when the document was deleted between
		// the update and the lookup — and it used to be dropped there, silently,
		// leaving the target on the older revision. The change itself is in the
		// event, so it can be applied without the lookup.
		update, err := updateFromDescription(event)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", sourceDB, collectionName, err)
		}
		if set, ok := update["$set"].(bson.M); ok {
			if masked, ok := s.maskValue(collectionName, set).(bson.M); ok {
				update["$set"] = masked
			}
		}
		return mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(update), nil

	case "delete":
		// Check if delete operations should be ignored for this collection
		advancedSettings := s.findTableAdvancedSettings(collectionName)
		if advancedSettings.IgnoreDeleteOps {
			s.logger.Debugf("[MongoDB] Ignoring delete operation for %s.%s (ignoreDeleteOps=true)",
				sourceDB, collectionName)
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

// updateFromDescription builds the update an event describes, for the case
// where the full document was not attached.
func updateFromDescription(event bson.M) (bson.M, error) {
	description := documentOf(event["updateDescription"])
	if description == nil {
		return nil, fmt.Errorf("an update event carries neither the document nor a " +
			"description of what changed")
	}

	update := bson.M{}
	if set := documentOf(description["updatedFields"]); len(set) > 0 {
		update["$set"] = set
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
		}
	}
	if len(update) == 0 {
		return nil, fmt.Errorf("an update event describes no change")
	}
	return update, nil
}

// filterOf is how the target addresses the document a change touched.
//
// It is the whole documentKey, not just the _id. On a sharded collection
// documentKey carries the shard key as well, and that is what mongos routes by:
// a filter holding only the _id cannot be routed, so an updateOne or a deleteOne
// is broadcast to every shard and an upsert — which is what a replayed insert
// has to be — is refused outright with "could not extract exact shard key". A
// collection sharded on anything but its _id would stop replication on the first
// document.
//
// The reasoning was written down against keyOf, which orders a batch, and the
// writes went on being addressed by the _id alone. The tests missed it because
// they sharded on {_id: hashed}, where the two are the same thing.
//
// Both document shapes are accepted, and that is not defensiveness. Unmarshalling
// into a bson.M gives nested documents as bson.M under the driver's v1 and as
// bson.D under its v2, so the type assertion that read the documentKey stopped
// matching the moment the driver was upgraded — and an event whose documentKey
// cannot be read is refused, which stopped every update and delete. idOf next
// door was already written to take either; this was not.
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
