package mongodb

import (
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
)

// checkpointStore is where this task records what it has applied.
//
// It writes to the target database as well as the configured directory. The
// directory alone was the problem: the syncer runs beside the source, so the
// outage this setup exists to survive takes the resume tokens with it, and a
// replacement started in the other region has no way to find out where to
// resume from. It would re-copy every collection, or start the stream from the
// current moment and silently skip whatever was in flight.
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
