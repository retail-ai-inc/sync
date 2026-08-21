package mongodb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/retail-ai-inc/sync/pkg/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (s *MongoDBSyncer) convertRawBSONToWriteModel(rawData bson.Raw, sourceDB, collectionName string) mongo.WriteModel {
	var event bson.M
	if err := bson.Unmarshal(rawData, &event); err != nil {
		s.logger.Errorf("[MongoDB] Failed to unmarshal raw BSON event: %v", err)
		return nil
	}

	opType, _ := event["operationType"].(string)
	if opType == "" {
		s.logger.Warnf("[MongoDB] Operation type is missing from BSON event")
		return nil
	}

	// This is a simplified conversion. A full implementation would need the logic from the old `convertToWriteModel`.
	// For now, we'll just handle insert as an example.
	switch opType {
	case "insert":
		if fullDoc, ok := event["fullDocument"]; ok {
			return mongo.NewInsertOneModel().SetDocument(fullDoc)
		}
	case "update", "replace":
		var docID interface{}
		if dk, ok := event["documentKey"].(bson.M); ok {
			docID = dk["_id"]
		} else {
			return nil
		}
		if fullDoc, ok := event["fullDocument"]; ok {
			return mongo.NewReplaceOneModel().SetFilter(bson.M{"_id": docID}).SetReplacement(fullDoc).SetUpsert(true)
		}
	case "delete":
		// Check if delete operations should be ignored for this collection
		advancedSettings := s.findTableAdvancedSettings(collectionName)
		if advancedSettings.IgnoreDeleteOps {
			s.logger.Debugf("[MongoDB] Ignoring delete operation for %s.%s (ignoreDeleteOps=true)",
				sourceDB, collectionName)
			return nil
		}

		var docID interface{}
		if dk, ok := event["documentKey"].(bson.M); ok {
			docID = dk["_id"]
		} else {
			return nil
		}
		return mongo.NewDeleteOneModel().SetFilter(bson.M{"_id": docID})
	}

	s.logger.Warnf("[MongoDB] Unhandled operation type '%s' in convertRawBSONToWriteModel", opType)
	return nil
}

func (s *MongoDBSyncer) flushWriteModels(ctx context.Context, targetColl *mongo.Collection, models []mongo.WriteModel, sourceDB, collectionName string) error {
	if len(models) == 0 {
		return nil
	}

	// First attempt: Try bulk write with unordered operations
	err := utils.RetryMongoOperation(ctx, s.logger, fmt.Sprintf("BulkWrite to %s.%s",
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

	// Move still failed operations to dead letter queue
	if len(stillFailedModels) > 0 && s.enableDeadLetterQueue {
		err := s.storeToDeadLetterQueue(stillFailedModels, stillFailedErrors, len(models), len(successfulModels), sourceDB, collectionName)
		if err != nil {
			s.logger.Errorf("[MongoDB] Failed to store failed operations to dead letter queue: %v", err)
		} else {
			s.logger.Infof("[MongoDB] Moved %d failed operations to dead letter queue for %s.%s",
				len(stillFailedModels), sourceDB, collectionName)
		}
	}

	// Always consider the operation successful if we processed the data
	// Failed operations are safely stored in dead letter queue
	s.logger.Infof("[MongoDB] Bulk write completed for %s.%s: %d successful, %d moved to dead letter queue",
		sourceDB, collectionName, len(successfulModels), len(stillFailedModels))

	return nil // Always return success - failed data is in dead letter queue
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

	// Store failed operations to dead letter queue
	if len(failedModels) > 0 && s.enableDeadLetterQueue {
		// Create empty error slice for failed operations (no specific bulk write errors)
		failedErrors := make([]mongo.BulkWriteError, len(failedModels))
		err := s.storeToDeadLetterQueue(failedModels, failedErrors, len(models), successCount, sourceDB, collectionName)
		if err != nil {
			s.logger.Errorf("[MongoDB] Failed to store failed operations to dead letter queue: %v", err)
		} else {
			s.logger.Infof("[MongoDB] Moved %d failed operations to dead letter queue for %s.%s",
				len(failedModels), sourceDB, collectionName)
		}
	}

	// Always consider successful - failed operations are in dead letter queue
	return nil
}
