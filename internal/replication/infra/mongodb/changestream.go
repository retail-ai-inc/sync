package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// streamEvent represents the data passed from the Change Stream reader to the disk writer.
// It contains the raw BSON data and the corresponding resume token.
type streamEvent struct {
	RawData     bson.Raw
	ResumeToken bson.Raw
}

// watchChanges follows one collection's change stream until the context is
// cancelled or the stream ends. The returned error is what the guardian decides
// on: an ErrUnrecoverable means the position is gone and retrying is pointless.
func (s *MongoDBSyncer) watchChanges(ctx context.Context, sourceColl, targetColl *mongo.Collection, sourceDB, collectionName string) error {
	key := fmt.Sprintf("%s.%s", sourceDB, collectionName)

	s.processorMutex.Lock()
	if cancelFunc, exists := s.activeProcessors[key]; exists {
		cancelFunc()
	}
	processorCtx, cancel := context.WithCancel(ctx)
	s.activeProcessors[key] = cancel
	s.processorMutex.Unlock()

	defer func() {
		s.processorMutex.Lock()
		delete(s.activeProcessors, key)
		s.processorMutex.Unlock()
		cancel()
	}()

	eventChannel := make(chan streamEvent, s.channelCapacity)

	go s.diskWriter(processorCtx, eventChannel, sourceDB, collectionName)
	go s.processPersistentBuffer(processorCtx, sourceDB, collectionName, targetColl.Database().Name(), targetColl.Name())

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "ns.db", Value: sourceDB},
			{Key: "ns.coll", Value: collectionName},
			{Key: "operationType", Value: bson.M{"$in": []string{"insert", "update", "replace", "delete"}}},
		}}},
	}
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	switch resumeToken := s.loadMongoDBResumeToken(sourceDB, collectionName); {
	case resumeToken != nil:
		opts.SetResumeAfter(resumeToken)
	default:
		// No token yet, so the stream has never delivered an event. The
		// snapshot pinned the cluster time it read from; starting there
		// replays the writes made while the copy was running, which the
		// copy itself could not see. Without it the stream would start
		// from now and that window would be lost.
		if startAt := s.loadStartTime(sourceDB, collectionName); !startAt.IsZero() {
			s.logger.Infof("[MongoDB] Starting the change stream for %s.%s at the "+
				"snapshot's cluster time %d.%d", sourceDB, collectionName, startAt.T, startAt.I)
			opts.SetStartAtOperationTime(&startAt)
		}
	}

	cs, err := sourceColl.Watch(ctx, pipeline, opts)
	if err != nil {
		if positionLost(err) {
			return domain.Unrecoverable("the change stream for %s.%s cannot be resumed "+
				"from the position this task holds (%v). The oplog no longer reaches "+
				"back that far, so a fresh copy is needed: clear the stored checkpoint. "+
				"Until then nothing is being replicated", sourceDB, collectionName, err)
		}
		return fmt.Errorf("open the change stream for %s.%s: %w", sourceDB, collectionName, err)
	}
	defer cs.Close(ctx)
	s.logger.Infof("[MongoDB] Watching changes => %s.%s", sourceDB, collectionName)

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("[MongoDB] Context cancelled, stopping change stream for %s.%s", sourceDB, collectionName)
			return nil
		default:
			if !cs.Next(ctx) {
				err := cs.Err()
				switch {
				case err == nil:
					s.logger.Infof("[MongoDB] Change stream ended normally for %s.%s", sourceDB, collectionName)
					return nil
				case positionLost(err):
					return domain.Unrecoverable("the change stream for %s.%s cannot be "+
						"resumed from the position this task holds (%v). The oplog no "+
						"longer reaches back that far, so a fresh copy is needed: clear "+
						"the stored checkpoint. Until then nothing is being replicated",
						sourceDB, collectionName, err)
				case errors.Is(err, context.Canceled), ctx.Err() != nil:
					// The task is being stopped. A shutdown is not a failure,
					// and logging one as an error means every deploy leaves
					// something in the log that looks like a fault — which is
					// how a real fault goes unnoticed.
					s.logger.Infof("[MongoDB] Change stream for %s.%s closed on shutdown",
						sourceDB, collectionName)
					return nil
				case isRecoverableError(err):
					s.logger.Warnf("[MongoDB] Recoverable error detected for %s.%s, will be retried by guardian", sourceDB, collectionName)
					return err
				default:
					s.logger.Errorf("[MongoDB] Change stream error for %s.%s: %v", sourceDB, collectionName, err)
					return err
				}
			}

			event := streamEvent{
				RawData:     cs.Current,
				ResumeToken: cs.ResumeToken(),
			}
			if at, ok := eventClusterTime(cs.Current); ok {
				metrics.SetReadLag(s.metricLabels(collectionName), time.Since(at).Seconds())
			}

			select {
			case eventChannel <- event:
				// Event successfully sent
			case <-ctx.Done():
				return nil
			case <-time.After(10 * time.Second):
				return fmt.Errorf("the pipeline for %s.%s has been blocked for ten "+
					"seconds, so the change stream is being closed rather than read "+
					"further ahead of what can be applied", sourceDB, collectionName)
			}
		}
	}
}

// watchChangesWithRetry is a guardian wrapper around watchChanges that provides automatic retry and recovery
func (s *MongoDBSyncer) watchChangesWithRetry(ctx context.Context, sourceColl, targetColl *mongo.Collection, sourceDB, collectionName string) {
	// Get retry settings from configuration
	advancedSettings := s.findTableAdvancedSettings(collectionName)
	maxRetries := advancedSettings.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 10 // Default value
	}

	baseDelay := advancedSettings.BaseRetryDelay
	if baseDelay <= 0 {
		baseDelay = 5 * time.Second // Default value
	}

	maxDelay := advancedSettings.MaxRetryDelay
	if maxDelay <= 0 {
		maxDelay = 5 * time.Minute // Default value
	}

	currentDelay := baseDelay
	retryCount := 0

	s.logger.Infof("[MongoDB] Starting guardian loop for %s.%s", sourceDB, collectionName)

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("[MongoDB] Guardian loop cancelled for %s.%s", sourceDB, collectionName)
			return
		default:
			s.logger.Infof("[MongoDB] Attempting to start watchChanges for %s.%s (attempt %d)", sourceDB, collectionName, retryCount+1)

			// Start the actual watchChanges in a separate goroutine
			watchCtx, watchCancel := context.WithCancel(ctx)
			watchDone := make(chan struct{})
			var watchErr error

			go func() {
				defer close(watchDone)
				watchErr = s.watchChanges(watchCtx, sourceColl, targetColl, sourceDB, collectionName)
			}()

			// Wait for watchChanges to complete or context to be cancelled
			select {
			case <-watchDone:
				// watchChanges has exited; release its context either way.
				watchCancel()

				// A position that no longer exists is not something the guardian
				// can retry its way out of: every attempt fails identically, and
				// looping hides the one thing somebody needs to be told.
				if domain.IsUnrecoverable(watchErr) {
					s.report(watchErr)
					return
				}

				// Check whether it was due to context cancellation.
				select {
				case <-ctx.Done():
					s.logger.Infof("[MongoDB] Guardian loop stopping due to context cancellation for %s.%s", sourceDB, collectionName)
					return
				default:
					// watchChanges exited due to error, retry
					retryCount++
					if retryCount > maxRetries {
						s.report(fmt.Errorf("the change stream for %s.%s failed %d times "+
							"in a row, most recently with: %v", sourceDB, collectionName,
							maxRetries, watchErr))
						return
					}

					s.logger.Warnf("[MongoDB] watchChanges exited for %s.%s, retrying in %v (attempt %d/%d)",
						sourceDB, collectionName, currentDelay, retryCount, maxRetries)

					// Exponential backoff with jitter
					select {
					case <-ctx.Done():
						return
					case <-time.After(currentDelay):
						// Increase delay for next retry
						currentDelay = time.Duration(float64(currentDelay) * 1.5)
						if currentDelay > maxDelay {
							currentDelay = maxDelay
						}
					}
				}
			case <-ctx.Done():
				watchCancel()
				<-watchDone // Wait for watchChanges to clean up
				return
			}
		}
	}
}

// isRecoverableError determines if a MongoDB error is recoverable and should trigger a retry
func isRecoverableError(err error) bool {
	if err == nil {
		return false
	}

	// The driver's own judgement first. It knows which of its errors are network
	// failures and which are timeouts without anybody having to guess at the
	// wording, and the wording changes between driver releases: the list below
	// missed "connection() error occurred during connection handshake", "socket
	// was unexpectedly closed" and "client is disconnected", which are among the
	// commonest things it says while a replica set elects a new primary. A
	// watcher that read those as fatal gave up on the stream instead of
	// reconnecting to it.
	if mongo.IsNetworkError(err) || mongo.IsTimeout(err) {
		return true
	}

	// A cancelled context is this process stopping, not the cluster failing.
	if errors.Is(err, context.Canceled) {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// Network-related errors that are typically recoverable
	recoverablePatterns := []string{
		"server selection timeout",
		"server selection error",
		"connection refused",
		"connection handshake",
		"socket was unexpectedly closed",
		"client is disconnected",
		"network timeout",
		"no reachable servers",
		"connection pool exhausted",
		"pool is closed",
		"write concern timeout",
		"read concern timeout",
		"cursor not found",
		"interrupted at shutdown",
		"interruptedatshutdown",
		"shutdown in progress",
		"host unreachable",
		"connection reset",
		"connection closed",
		"broken pipe",
		"i/o timeout",
		"context deadline exceeded",
		"not master",
		"not primary",
		"node is recovering",
		"primary stepped down",
		"unable to target",
		"eof",
	}

	for _, pattern := range recoverablePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	// Check for specific MongoDB error codes that are recoverable
	for _, code := range []int{
		11600, // InterruptedAtShutdown
		11602, // InterruptedDueToReplStateChange
		10107, // NotWritablePrimary
		13435, // NotPrimaryNoSecondaryOk
		13436, // NotPrimaryOrSecondary
		189,   // PrimarySteppedDown
		91,    // ShutdownInProgress
		7,     // HostNotFound
		6,     // HostUnreachable
		89,    // NetworkTimeout
		9001,  // SocketException
		262,   // ExceededTimeLimit
	} {
		if hasServerCode(err, code) {
			return true
		}
	}

	return false
}

// hasServerCode reports whether the server returned a particular error code,
// by the driver's own accounting rather than by looking for the number in the
// message text.
func hasServerCode(err error, code int) bool {
	var server mongo.ServerError
	if errors.As(err, &server) {
		return server.HasErrorCode(code)
	}
	return strings.Contains(err.Error(), fmt.Sprintf("error code %d", code))
}

// positionLost reports whether an error says the change stream cannot be
// resumed from the token or cluster time this task holds.
//
// The oplog is a capped collection: a task stopped for longer than it covers
// comes back to find its resume point gone. Retrying cannot help — the entries
// are not there — and the tempting repair, dropping the token and watching from
// now, silently skips everything in between. So it is reported instead.
func positionLost(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	for _, marker := range []string{
		"changestreamhistorylost",
		"resume of change stream was not possible",
		"resume point may no longer be in the oplog",
		"invalid resume token",
		"the resume point may no longer be in the oplog",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}
