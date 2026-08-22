package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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

// store reports the checkpoint store, falling back to a file-only one so the
// helpers work before Start has connected.
func (s *MongoDBSyncer) store() checkpoint.Store {
	if s.checkpoints != nil {
		return s.checkpoints
	}
	return s.checkpointStore("")
}

// tokenKey and startKey name one collection's two checkpoints.
func tokenKey(db, coll string) string { return fmt.Sprintf("%s.%s.token", db, coll) }
func startKey(db, coll string) string { return fmt.Sprintf("%s.%s.start", db, coll) }

func (s *MongoDBSyncer) loadMongoDBResumeToken(db, coll string) bson.Raw {
	key := fmt.Sprintf("%s.%s", db, coll)

	s.resumeTokensM.RLock()
	if token, exists := s.resumeTokens[key]; exists {
		s.resumeTokensM.RUnlock()
		return token
	}
	s.resumeTokensM.RUnlock()

	payload, err := s.store().Load(context.Background(), tokenKey(db, coll))
	if err != nil {
		s.logger.Warnf("[MongoDB] Could not read the resume token for %s: %v", key, err)
		return nil
	}
	if payload == "" {
		return nil
	}

	var token bson.Raw
	if err := json.Unmarshal([]byte(payload), &token); err != nil {
		s.logger.Errorf("[MongoDB] unmarshal resume token fail => %v", err)
		s.removeMongoDBResumeToken(db, coll)
		return nil
	}

	s.resumeTokensM.Lock()
	s.resumeTokens[key] = token
	s.resumeTokensM.Unlock()

	return token
}

func (s *MongoDBSyncer) saveMongoDBResumeToken(db, coll string, token bson.Raw) {
	if token == nil {
		return
	}

	key := fmt.Sprintf("%s.%s", db, coll)
	s.resumeTokensM.Lock()
	s.resumeTokens[key] = token
	s.resumeTokensM.Unlock()

	payload, err := json.Marshal(token)
	if err != nil {
		s.logger.Errorf("[MongoDB] marshal resume token fail => %v", err)
		return
	}
	if err := s.store().Save(context.Background(), tokenKey(db, coll), string(payload)); err != nil {
		s.logger.Errorf("[MongoDB] Could not record the resume token for %s: %v", key, err)
	}
}

func (s *MongoDBSyncer) removeMongoDBResumeToken(db, coll string) {
	s.resumeTokensM.Lock()
	delete(s.resumeTokens, fmt.Sprintf("%s.%s", db, coll))
	s.resumeTokensM.Unlock()

	if err := s.store().Save(context.Background(), tokenKey(db, coll), ""); err != nil {
		s.logger.Warnf("[MongoDB] Could not clear the resume token for %s.%s: %v", db, coll, err)
	}
	s.logger.Infof("[MongoDB] removed invalid resume token => %s.%s", db, coll)
}

// saveStartTime records the cluster time the change stream must resume from.
func (s *MongoDBSyncer) saveStartTime(db, coll string, ts primitive.Timestamp) {
	payload, err := checkpoint.Encode(ts)
	if err != nil {
		s.logger.Errorf("[MongoDB] marshal start time fail => %v", err)
		return
	}
	if err := s.store().Save(context.Background(), startKey(db, coll), payload); err != nil {
		s.logger.Errorf("[MongoDB] Could not record the start time for %s.%s: %v", db, coll, err)
	}
}

// loadStartTime reports the recorded start time, or the zero timestamp when
// there is none.
func (s *MongoDBSyncer) loadStartTime(db, coll string) primitive.Timestamp {
	payload, err := s.store().Load(context.Background(), startKey(db, coll))
	if err != nil {
		s.logger.Warnf("[MongoDB] Could not read the start time for %s.%s: %v", db, coll, err)
		return primitive.Timestamp{}
	}

	var ts primitive.Timestamp
	if _, err := checkpoint.Decode(payload, &ts); err != nil {
		s.logger.Errorf("[MongoDB] unmarshal start time fail => %v", err)
		return primitive.Timestamp{}
	}
	return ts
}

// snapshotDone reports whether the initial copy has already been made.
//
// A checkpoint — either a resume token from a running stream or the start time
// the snapshot pinned — means a previous run got past the copy. A target
// collection that merely holds documents means no such thing: that is exactly
// what an interrupted copy leaves behind, and treating it as "done" is why a
// copy that failed halfway could never be finished.
func (s *MongoDBSyncer) snapshotDone(db, coll string) bool {
	if s.loadMongoDBResumeToken(db, coll) != nil {
		return true
	}
	return !s.loadStartTime(db, coll).IsZero()
}

// getResumeTokenPath names the file one collection's resume token used to live
// in. It is kept because the buffer directory is derived from the same setting.
func (s *MongoDBSyncer) getResumeTokenPath(db, coll string) string {
	return filepath.Join(s.cfg.MongoDBResumeTokenPath, collectionKey(db, coll)+".json")
}
