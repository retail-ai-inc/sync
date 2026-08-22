package mongodb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/platform/resilience"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

// flushWriteModels applies a batch of changes to the target.
//
// The batch is split into runs that hold at most one write per document, and
// the runs are applied in sequence, so two changes to the same document in one
// batch cannot land the wrong way round while writes to unrelated documents
// still go out together.
func (s *MongoDBSyncer) flushWriteModels(ctx context.Context, targetColl *mongo.Collection, models []mongo.WriteModel, sourceDB, collectionName string) error {
	if len(models) == 0 {
		return nil
	}

	runs := orderedRuns(models)
	if len(runs) > 1 {
		s.logger.Debugf("[MongoDB] Batch for %s.%s touches some documents more than "+
			"once; applying it as %d ordered runs", sourceDB, collectionName, len(runs))
	}
	for _, run := range runs {
		if err := s.flushOneRun(ctx, targetColl, run, sourceDB, collectionName); err != nil {
			return err
		}
	}
	return nil
}

// flushOneRun applies one run, in which no document appears twice.
func (s *MongoDBSyncer) flushOneRun(ctx context.Context, targetColl *mongo.Collection, models []mongo.WriteModel, sourceDB, collectionName string) error {
	if len(models) == 0 {
		return nil
	}

	// First attempt: Try bulk write with unordered operations
	err := resilience.RetryMongoOperation(ctx, s.logger, fmt.Sprintf("BulkWrite to %s.%s",
		targetColl.Database().Name(), targetColl.Name()),
		func() error {
			res, err := targetColl.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
			if err != nil {
				return err
			}
			s.logger.Debugf(
				"[MongoDB] BulkWrite => table=%s.%s inserted=%d matched=%d modified=%d upserted=%d deleted=%d",
				targetColl.Database().Name(),
				targetColl.Name(),
				res.InsertedCount,
				res.MatchedCount,
				res.ModifiedCount,
				res.UpsertedCount,
				res.DeletedCount,
			)
			return nil
		})

	if err == nil {
		// Bulk write succeeded completely
		return nil
	}

	// Check if it's a bulk write error with write errors
	if bulkWriteErr, ok := err.(mongo.BulkWriteException); ok {
		s.logger.Warnf("[MongoDB] BulkWrite partially failed for %s.%s: %d write errors out of %d operations",
			sourceDB, collectionName, len(bulkWriteErr.WriteErrors), len(models))

		// Log detailed error information
		for _, writeErr := range bulkWriteErr.WriteErrors {
			s.logger.Warnf("[MongoDB] Write error at index %d: code=%d, message=%s",
				writeErr.Index, writeErr.Code, writeErr.Message)
		}

		// Handle specific error types
		return s.handleBulkWriteErrors(ctx, targetColl, models, bulkWriteErr, sourceDB, collectionName)
	}

	// For other types of errors, try individual operations
	s.logger.Warnf("[MongoDB] BulkWrite failed with non-bulk error for %s.%s: %v, falling back to individual operations",
		sourceDB, collectionName, err)

	return s.handleBulkWriteWithIndividualOps(ctx, targetColl, models, sourceDB, collectionName)
}

// handleBulkWriteErrors handles specific bulk write errors with intelligent recovery
func (s *MongoDBSyncer) handleBulkWriteErrors(ctx context.Context, targetColl *mongo.Collection, models []mongo.WriteModel, bulkErr mongo.BulkWriteException, sourceDB, collectionName string) error {
	// Create a map of failed indices for quick lookup
	failedIndices := make(map[int]bool)
	errorMap := make(map[int]mongo.BulkWriteError)

	for _, writeErr := range bulkErr.WriteErrors {
		failedIndices[writeErr.Index] = true
		errorMap[writeErr.Index] = writeErr
	}

	// Separate successful and failed operations
	var successfulModels []mongo.WriteModel
	var failedModels []mongo.WriteModel
	var failedErrors []mongo.BulkWriteError

	for i, model := range models {
		if failedIndices[i] {
			failedModels = append(failedModels, model)
			if err, exists := errorMap[i]; exists {
				failedErrors = append(failedErrors, err)
			}
		} else {
			successfulModels = append(successfulModels, model)
		}
	}

	s.logger.Infof("[MongoDB] Bulk write analysis for %s.%s: %d successful, %d failed",
		sourceDB, collectionName, len(successfulModels), len(failedModels))

	// Try to retry failed operations individually
	var stillFailedModels []mongo.WriteModel
	var stillFailedErrors []mongo.BulkWriteError

	if len(failedModels) > 0 {
		s.logger.Infof("[MongoDB] Attempting to retry %d failed operations individually for %s.%s",
			len(failedModels), sourceDB, collectionName)

		retrySuccess, retryFailedModels, retryFailedErrors := s.retryFailedOperationsWithDetails(ctx, targetColl, failedModels, failedErrors, sourceDB, collectionName)

		if retrySuccess > 0 {
			s.logger.Infof("[MongoDB] Successfully retried %d operations for %s.%s",
				retrySuccess, sourceDB, collectionName)
		}

		stillFailedModels = retryFailedModels
		stillFailedErrors = retryFailedErrors

		if len(stillFailedModels) > 0 {
			s.logger.Warnf("[MongoDB] %d operations still failed after retry for %s.%s",
				len(stillFailedModels), sourceDB, collectionName)
		}
	}

	return s.setAside(stillFailedModels, stillFailedErrors, len(models), len(successfulModels), sourceDB, collectionName)
}

// setAside puts the operations that could not be applied where they can be
// retried, and reports when it cannot.
//
// Both failure paths used to return nil no matter what happened. With the dead
// letter queue turned off, or with the write to it failing, the operations were
// dropped on the floor and the batch was reported as applied: the checkpoint
// moved past changes the target never received, so nothing would ever send them
// again and the only trace was a warning in a log.
func (s *MongoDBSyncer) setAside(models []mongo.WriteModel, errs []mongo.BulkWriteError,
	total, applied int, sourceDB, collectionName string) error {

	if len(models) == 0 {
		return nil
	}

	metrics.Failed(s.metricLabels(collectionName), len(models))

	if !s.enableDeadLetterQueue {
		return fmt.Errorf("%d of %d changes to %s.%s could not be applied to the "+
			"target and the dead letter queue is turned off, so there is nowhere to "+
			"keep them: %v", len(models), total, sourceDB, collectionName, firstError(errs))
	}

	if err := s.storeToDeadLetterQueue(models, errs, total, applied, sourceDB, collectionName); err != nil {
		return fmt.Errorf("%d of %d changes to %s.%s could not be applied to the "+
			"target, and could not be written to the dead letter queue either: %w",
			len(models), total, sourceDB, collectionName, err)
	}

	// An error, not a warning: these are changes the source has and the target
	// does not, and the file they are in is on this pod's disk.
	s.logger.Errorf("[MongoDB] %d of %d changes to %s.%s could not be applied and "+
		"are held in the dead letter queue at %s. Until they are retried the target "+
		"is missing them.", len(models), total, sourceDB, collectionName, s.deadLetterDir)

	if pending, _, err := s.getDeadLetterQueueStats(sourceDB, collectionName); err == nil {
		metrics.SetDeadLettered(s.metricLabels(collectionName), float64(pending))
	}
	return nil
}

// firstError names one of the reasons, for a message somebody has to act on.
func firstError(errs []mongo.BulkWriteError) string {
	for _, e := range errs {
		if e.Message != "" {
			return e.Message
		}
	}
	return "no reason was reported"
}

// serializeWriteModel converts a WriteModel to JSON for storage
func (s *MongoDBSyncer) serializeWriteModel(model mongo.WriteModel) (json.RawMessage, error) {
	switch m := model.(type) {
	case *mongo.InsertOneModel:
		data := map[string]interface{}{
			"type":     "insert",
			"document": m.Document,
		}
		return json.Marshal(data)
	case *mongo.UpdateOneModel:
		data := map[string]interface{}{
			"type":   "update",
			"filter": m.Filter,
			"update": m.Update,
			"upsert": true, // Default to upsert for safety
		}
		return json.Marshal(data)
	case *mongo.ReplaceOneModel:
		data := map[string]interface{}{
			"type":        "replace",
			"filter":      m.Filter,
			"replacement": m.Replacement,
			"upsert":      true, // Default to upsert for safety
		}
		return json.Marshal(data)
	case *mongo.DeleteOneModel:
		data := map[string]interface{}{
			"type":   "delete",
			"filter": m.Filter,
		}
		return json.Marshal(data)
	default:
		return nil, fmt.Errorf("unsupported WriteModel type: %T", model)
	}
}

// deserializeWriteModel converts JSON back to WriteModel
func (s *MongoDBSyncer) deserializeWriteModel(data json.RawMessage, collectionName string) (mongo.WriteModel, error) {
	var modelData map[string]interface{}
	if err := json.Unmarshal(data, &modelData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal model data: %w", err)
	}

	opType, ok := modelData["type"].(string)
	if !ok {
		return nil, fmt.Errorf("missing or invalid operation type")
	}

	switch opType {
	case "insert":
		document, ok := modelData["document"]
		if !ok {
			return nil, fmt.Errorf("missing document for insert operation")
		}
		return mongo.NewInsertOneModel().SetDocument(document), nil

	case "update":
		filter, ok := modelData["filter"]
		if !ok {
			return nil, fmt.Errorf("missing filter for update operation")
		}
		update, ok := modelData["update"]
		if !ok {
			return nil, fmt.Errorf("missing update for update operation")
		}
		upsert, _ := modelData["upsert"].(bool)

		updateModel := mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update)
		if upsert {
			updateModel.SetUpsert(true)
		}
		return updateModel, nil

	case "replace":
		filter, ok := modelData["filter"]
		if !ok {
			return nil, fmt.Errorf("missing filter for replace operation")
		}
		replacement, ok := modelData["replacement"]
		if !ok {
			return nil, fmt.Errorf("missing replacement for replace operation")
		}
		upsert, _ := modelData["upsert"].(bool)

		replaceModel := mongo.NewReplaceOneModel().SetFilter(filter).SetReplacement(replacement)
		if upsert {
			replaceModel.SetUpsert(true)
		}
		return replaceModel, nil

	case "delete":
		// Check if delete operations should be ignored for this collection
		advancedSettings := s.findTableAdvancedSettings(collectionName)
		if advancedSettings.IgnoreDeleteOps {
			s.logger.Debugf("[MongoDB] Ignoring delete operation during retry for %s (ignoreDeleteOps=true)",
				collectionName)
			return nil, nil
		}

		filter, ok := modelData["filter"]
		if !ok {
			return nil, fmt.Errorf("missing filter for delete operation")
		}
		return mongo.NewDeleteOneModel().SetFilter(filter), nil

	default:
		return nil, fmt.Errorf("unsupported operation type: %s", opType)
	}
}

// getOperationType returns the operation type string for a WriteModel
func (s *MongoDBSyncer) getOperationType(model mongo.WriteModel) string {
	switch model.(type) {
	case *mongo.InsertOneModel:
		return "insert"
	case *mongo.UpdateOneModel:
		return "update"
	case *mongo.ReplaceOneModel:
		return "replace"
	case *mongo.DeleteOneModel:
		return "delete"
	default:
		return "unknown"
	}
}

// retryFailedOperationsWithDetails retries failed operations individually and returns detailed results
func (s *MongoDBSyncer) retryFailedOperationsWithDetails(ctx context.Context, targetColl *mongo.Collection, failedModels []mongo.WriteModel, failedErrors []mongo.BulkWriteError, sourceDB, collectionName string) (int, []mongo.WriteModel, []mongo.BulkWriteError) {
	successCount := 0
	var stillFailedModels []mongo.WriteModel
	var stillFailedErrors []mongo.BulkWriteError

	for i, model := range failedModels {
		opType := s.getOperationType(model)
		err := s.executeIndividualOperation(ctx, targetColl, model)

		if err != nil {
			stillFailedModels = append(stillFailedModels, model)
			if i < len(failedErrors) {
				stillFailedErrors = append(stillFailedErrors, failedErrors[i])
			}

			// Log specific error information
			if i < len(failedErrors) {
				s.logger.Warnf("[MongoDB] Individual retry failed for %s operation: code=%d, message=%s, error=%v",
					opType, failedErrors[i].Code, failedErrors[i].Message, err)
			} else {
				s.logger.Warnf("[MongoDB] Individual retry failed for %s operation: %v", opType, err)
			}
		} else {
			successCount++
			s.logger.Debugf("[MongoDB] Individual retry succeeded for %s operation", opType)
		}
	}

	return successCount, stillFailedModels, stillFailedErrors
}

// executeIndividualOperation executes a single WriteModel operation
func (s *MongoDBSyncer) executeIndividualOperation(ctx context.Context, targetColl *mongo.Collection, model mongo.WriteModel) error {
	switch m := model.(type) {
	case *mongo.InsertOneModel:
		_, err := targetColl.InsertOne(ctx, m.Document)
		return err
	case *mongo.UpdateOneModel:
		_, err := targetColl.UpdateOne(ctx, m.Filter, m.Update, options.Update().SetUpsert(true))
		return err
	case *mongo.ReplaceOneModel:
		_, err := targetColl.ReplaceOne(ctx, m.Filter, m.Replacement, options.Replace().SetUpsert(true))
		return err
	case *mongo.DeleteOneModel:
		_, err := targetColl.DeleteOne(ctx, m.Filter)
		return err
	default:
		return fmt.Errorf("unsupported WriteModel type: %T", model)
	}
}

// retryFailedOperations retries failed operations individually with specific error handling
func (s *MongoDBSyncer) retryFailedOperations(ctx context.Context, targetColl *mongo.Collection, failedModels []mongo.WriteModel, failedErrors []mongo.BulkWriteError, sourceDB, collectionName string) (int, int) {
	successCount, stillFailedModels, _ := s.retryFailedOperationsWithDetails(ctx, targetColl, failedModels, failedErrors, sourceDB, collectionName)
	return successCount, len(stillFailedModels)
}

// handleBulkWriteWithIndividualOps handles cases where bulk write fails completely
func (s *MongoDBSyncer) handleBulkWriteWithIndividualOps(ctx context.Context, targetColl *mongo.Collection, models []mongo.WriteModel, sourceDB, collectionName string) error {
	s.logger.Infof("[MongoDB] Falling back to individual operations for %s.%s: %d operations",
		sourceDB, collectionName, len(models))

	successCount := 0
	var failedModels []mongo.WriteModel

	for i, model := range models {
		opType := s.getOperationType(model)
		err := s.executeIndividualOperation(ctx, targetColl, model)

		if err != nil {
			failedModels = append(failedModels, model)
			s.logger.Warnf("[MongoDB] Individual operation failed for %s operation at index %d: %v",
				opType, i, err)
		} else {
			successCount++
		}
	}

	s.logger.Infof("[MongoDB] Individual operations completed for %s.%s: %d successful, %d failed",
		sourceDB, collectionName, successCount, len(failedModels))

	// No bulk write errors to carry here: the batch failed before the server
	// reported per-operation reasons.
	return s.setAside(failedModels, make([]mongo.BulkWriteError, len(failedModels)),
		len(models), successCount, sourceDB, collectionName)
}
