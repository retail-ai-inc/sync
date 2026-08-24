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
		id := idOf(fullDoc)
		if id == nil {
			if dk, ok := event["documentKey"].(bson.M); ok {
				id = dk["_id"]
			}
		}
		if id == nil {
			return mongo.NewInsertOneModel().SetDocument(fullDoc), nil
		}
		return mongo.NewReplaceOneModel().
			SetFilter(bson.M{"_id": id}).
			SetReplacement(fullDoc).
			SetUpsert(true), nil

	case "update", "replace":
		dk, ok := event["documentKey"].(bson.M)
		if !ok {
			return nil, fmt.Errorf("an %s event for %s.%s names no document",
				opType, sourceDB, collectionName)
		}
		docID := dk["_id"]

		if fullDoc, ok := event["fullDocument"]; ok {
			return mongo.NewReplaceOneModel().
				SetFilter(bson.M{"_id": docID}).
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
			SetFilter(bson.M{"_id": docID}).
			SetUpdate(update), nil

	case "delete":
		// Check if delete operations should be ignored for this collection
		advancedSettings := s.findTableAdvancedSettings(collectionName)
		if advancedSettings.IgnoreDeleteOps {
			s.logger.Debugf("[MongoDB] Ignoring delete operation for %s.%s (ignoreDeleteOps=true)",
				sourceDB, collectionName)
			return nil, nil
		}

		dk, ok := event["documentKey"].(bson.M)
		if !ok {
			return nil, fmt.Errorf("a delete event for %s.%s names no document",
				sourceDB, collectionName)
		}
		return mongo.NewDeleteOneModel().SetFilter(bson.M{"_id": dk["_id"]}), nil
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
	description, ok := event["updateDescription"].(bson.M)
	if !ok {
		return nil, fmt.Errorf("an update event carries neither the document nor a " +
			"description of what changed")
	}

	update := bson.M{}
	if set, ok := description["updatedFields"].(bson.M); ok && len(set) > 0 {
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
