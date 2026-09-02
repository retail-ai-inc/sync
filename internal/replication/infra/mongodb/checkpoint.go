package mongodb

import (
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
)

// checkpointStore is where this task records what it has applied. It writes to
// the target database as well as the configured directory: the directory alone
// dies with the region the syncer runs in. See package checkpoint.
func (s *MongoDBSyncer) checkpointStore(targetDBName string) checkpoint.Store {
	var stores []checkpoint.Store
	if s.targetClient != nil && targetDBName != "" {
		stores = append(stores, &checkpoint.MongoStore{
			Database: s.targetClient.Database(targetDBName),
			TaskID:   s.cfg.ID,
		})
	}
	if s.cfg.MongoDBResumeTokenPath != "" {
		stores = append(stores, &checkpoint.FileStore{
			Path: s.cfg.MongoDBResumeTokenPath + "/checkpoint",
		})
	}
	return &checkpoint.Layered{
		Stores:  stores,
		OnError: func(err error) { s.logger.Warnf("[MongoDB] Checkpoint store: %v", err) },
	}
}
