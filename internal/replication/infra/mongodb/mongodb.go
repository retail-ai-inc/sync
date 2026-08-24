package mongodb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/platform/resilience"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
	"github.com/retail-ai-inc/sync/internal/replication/infra/directionlock"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBSyncer struct {
	sourceClient  *mongo.Client
	targetClient  *mongo.Client
	cfg           config.SyncConfig
	logger        logrus.FieldLogger
	resumeTokens  map[string]bson.Raw
	resumeTokensM sync.RWMutex
	// New fields for persistent buffer
	bufferDir     string
	bufferEnabled bool
	// Fields for goroutine lifecycle management
	activeProcessors map[string]context.CancelFunc
	processorMutex   sync.RWMutex
	// New fields for async pipeline
	channelCapacity int
	// Smart batch controller configuration
	targetBatchSizeBytes int64
	maxFilesPerBatch     int
	minFilesPerBatch     int
	// Dead letter queue configuration
	deadLetterDir         string
	maxRetryAttempts      int
	retryInterval         time.Duration
	enableDeadLetterQueue bool
	// Global configuration for accessing Slack settings
	globalConfig *config.Config
	// checkpoints is where the resume tokens and start times are recorded.
	checkpoints checkpoint.Store
	// connectErr is why the constructor could not reach one side, kept so Start
	// can report it rather than the caller having to notice a nil syncer.
	connectErr error
	// faults carries the first reason a watcher gave up, so Start can report it
	// rather than blocking on collections that have all stopped. It holds one
	// value: the first reason is the one worth acting on and the rest follow
	// from it.
	faults chan error
}

// NewMongoDBSyncer builds the syncer for one task.
//
// It never returns nil. It used to, for a connection string the driver would
// not parse — and the caller then called Start on that nil pointer, which
// dereferences it. The panic happened in the goroutine one task runs in, where
// nothing recovers it, so one mistyped URI took down every other task and the
// API with it. A syncer that could not connect is returned carrying the reason
// instead, and Start reports it.
func NewMongoDBSyncer(cfg config.SyncConfig, globalConfig *config.Config, logger *logrus.Logger) *MongoDBSyncer {
	ctx := context.Background()

	sourceClient, sourceErr := connectMongo(ctx, cfg.SourceConnection)
	if sourceErr != nil {
		logger.Errorf("[MongoDB] Could not connect to the source: %v", sourceErr)
	}
	targetClient, targetErr := connectMongo(ctx, cfg.TargetConnection)
	if targetErr != nil {
		logger.Errorf("[MongoDB] Could not connect to the target: %v", targetErr)
	}
	connectErr := sourceErr
	if connectErr == nil {
		connectErr = targetErr
	}

	resumeMap := make(map[string]bson.Raw)
	if cfg.MongoDBResumeTokenPath != "" {
		if e2 := os.MkdirAll(cfg.MongoDBResumeTokenPath, os.ModePerm); e2 != nil {
			logger.Warnf("[MongoDB] Failed to create resume token dir %s: %v", cfg.MongoDBResumeTokenPath, e2)
		}
	}

	// Create buffer directory for persistent storage
	bufferDir := cfg.MongoDBResumeTokenPath
	if bufferDir == "" {
		bufferDir = "./mongodb_buffer"
	} else {
		bufferDir = filepath.Join(bufferDir, "buffer")
	}

	if err := os.MkdirAll(bufferDir, os.ModePerm); err != nil {
		logger.Warnf("[MongoDB] Failed to create buffer directory %s: %v", bufferDir, err)
	}

	// Create dead letter directory for failed data
	deadLetterDir := cfg.MongoDBResumeTokenPath
	if deadLetterDir == "" {
		deadLetterDir = "./mongodb_dead_letter"
	} else {
		deadLetterDir = filepath.Join(deadLetterDir, "dead_letter")
	}

	if err := os.MkdirAll(deadLetterDir, os.ModePerm); err != nil {
		logger.Warnf("[MongoDB] Failed to create dead letter directory %s: %v", deadLetterDir, err)
	}

	return &MongoDBSyncer{
		sourceClient:     sourceClient,
		targetClient:     targetClient,
		connectErr:       connectErr,
		cfg:              cfg,
		logger:           logger.WithField("sync_task_id", cfg.ID),
		resumeTokens:     resumeMap,
		bufferDir:        bufferDir,
		bufferEnabled:    true, // Enable persistent buffer by default
		activeProcessors: make(map[string]context.CancelFunc),
		// Initialize async pipeline components
		channelCapacity: 200, // A smaller, safer default to prevent OOM.
		// Initialize smart batch controller configuration
		targetBatchSizeBytes: 256 * 1024 * 1024, // 256MB - Increased batch size for higher throughput
		maxFilesPerBatch:     1000,
		minFilesPerBatch:     5,
		// Initialize dead letter queue configuration
		deadLetterDir:         deadLetterDir,
		maxRetryAttempts:      3,
		retryInterval:         time.Second * 5,
		enableDeadLetterQueue: true,
		globalConfig:          globalConfig,
	}
}

// connectMongo dials one side, retrying while the failure looks like something
// a later attempt could survive. A URI the driver rejects is not: it is
// returned immediately so the caller can say so.
func connectMongo(ctx context.Context, uri string) (*mongo.Client, error) {
	var client *mongo.Client

	err := resilience.Retry(ctx, 5, 2*time.Second, 2.0, func() error {
		var connErr error
		client, connErr = mongo.Connect(ctx, options.Client().ApplyURI(uri))
		if isURIError(connErr) {
			return permanentURI{connErr}
		}
		return connErr
	})
	if err != nil {
		return nil, err
	}
	return client, nil
}

// isURIError reports whether the driver refused the connection string itself.
func isURIError(err error) bool {
	if err == nil {
		return false
	}
	text := err.Error()
	return strings.Contains(text, "scheme must be") ||
		strings.Contains(text, "error parsing uri") ||
		strings.Contains(text, "invalid connection string")
}

// permanentURI stops Retry: no amount of waiting makes a malformed URI parse.
type permanentURI struct{ error }

func (p permanentURI) Unwrap() error   { return p.error }
func (p permanentURI) Permanent() bool { return true }

func (s *MongoDBSyncer) ensureCollectionExists(ctx context.Context, db *mongo.Database, collName string) error {
	collections, err := db.ListCollectionNames(ctx, bson.M{"name": collName})
	if err != nil {
		s.logger.Errorf("[MongoDB] ListCollectionNames failed => %v", err)
		return err
	}
	if len(collections) == 0 {
		s.logger.Infof("[MongoDB] Creating collection => %s.%s", db.Name(), collName)
		if createErr := db.CreateCollection(ctx, collName); createErr != nil {
			return createErr
		}
	}
	return nil
}

func (s *MongoDBSyncer) copyIndexes(ctx context.Context, sourceColl, targetColl *mongo.Collection) error {
	cursor, err := sourceColl.Indexes().List(ctx)
	if err != nil {
		return fmt.Errorf("list source indexes fail: %w", err)
	}
	defer cursor.Close(ctx)

	var indexDocs []bson.M
	if err2 := cursor.All(ctx, &indexDocs); err2 != nil {
		return fmt.Errorf("read indexes fail: %w", err2)
	}

	targetCursor, err := targetColl.Indexes().List(ctx)
	if err != nil {
		s.logger.Warnf("[MongoDB] Failed to list existing indexes: %v", err)
	}

	existingIndexes := make(map[string]bool)
	if targetCursor != nil {
		var targetIdxDocs []bson.M
		if err := targetCursor.All(ctx, &targetIdxDocs); err == nil {
			for _, idx := range targetIdxDocs {
				if name, ok := idx["name"].(string); ok {
					existingIndexes[name] = true
				}
			}
		}
		targetCursor.Close(ctx)
	}

	indexesCreated := 0
	indexesSkipped := 0

	for _, idx := range indexDocs {
		if name, ok := idx["name"].(string); ok && name == "_id_" {
			continue
		}

		var name string
		if nameStr, ok := idx["name"].(string); ok {
			name = nameStr
			if existingIndexes[name] {
				s.logger.Debugf("[MongoDB] Index %s already exists, skipping", name)
				indexesSkipped++
				continue
			}
		}

		keyDoc := bson.D{}
		if keys, ok := idx["key"].(bson.M); ok {
			for field, direction := range keys {
				fixedDirection := direction
				if strVal, isString := direction.(string); isString {
					if strVal == "1" {
						fixedDirection = int32(1)
					} else if strVal == "-1" {
						fixedDirection = int32(-1)
					}
				} else if floatVal, isFloat := direction.(float64); isFloat {
					fixedDirection = int32(floatVal)
				}

				keyDoc = append(keyDoc, bson.E{Key: field, Value: fixedDirection})
			}
		} else {
			s.logger.Warnf("[MongoDB] Invalid index key format: %v", idx["key"])
			continue
		}

		indexOptions := options.Index()

		if uniqueVal, hasUnique := idx["unique"]; hasUnique {
			if uv, isBool := uniqueVal.(bool); isBool && uv {
				indexOptions.SetUnique(true)
			}
		}

		if nameVal, hasName := idx["name"]; hasName {
			if nameStr, ok := nameVal.(string); ok {
				name = nameStr
				indexOptions.SetName(nameStr)
			}
		}

		indexModel := mongo.IndexModel{
			Keys:    keyDoc,
			Options: indexOptions,
		}

		_, errC := targetColl.Indexes().CreateOne(ctx, indexModel)
		if errC != nil {
			if strings.Contains(errC.Error(), "already exists") {
				indexesSkipped++
			} else {
				s.logger.Warnf("[MongoDB] Create index %s fail: %v", name, errC)
			}
		} else {
			indexesCreated++
		}
	}

	s.logger.Infof("[MongoDB] Index creation summary for %s: created=%d, skipped=%d",
		targetColl.Name(), indexesCreated, indexesSkipped)

	return nil
}

func (s *MongoDBSyncer) findTableAdvancedSettings(collName string) config.AdvancedSettings {
	for _, m := range s.cfg.Mappings {
		for _, t := range m.Tables {
			if t.SourceTable == collName {
				return t.AdvancedSettings
			}
		}
	}
	return config.AdvancedSettings{}
}

// claimDirection records which way this task replicates, on both databases, and
// keeps the claims refreshed for as long as it runs.
func (s *MongoDBSyncer) claimDirection(ctx context.Context, sourceDBName, targetDBName string) (func(), error) {
	guard := &directionlock.Guard{
		TaskID: s.cfg.ID,
		Source: &directionlock.MongoStore{
			Database: s.sourceClient.Database(sourceDBName),
			Address:  dsn.Endpoint(s.cfg.Type, s.cfg.SourceConnection),
		},
		Target: &directionlock.MongoStore{
			Database: s.targetClient.Database(targetDBName),
			Address:  dsn.Endpoint(s.cfg.Type, s.cfg.TargetConnection),
		},
	}

	return directionlock.Hold(ctx, guard, s.logger, "MongoDB")
}

// discoveryInterval is how often a task with no configured collections looks
// for ones that have appeared since it started.
const discoveryInterval = time.Minute

// hasConfiguredCollections reports whether the task names any collection. The
// unlistedScanEvery is how often a task that names its collections is compared
// against what the source actually holds.
const unlistedScanEvery = 5 * time.Minute
