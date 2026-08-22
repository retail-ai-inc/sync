package mongodb

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (s *MongoDBSyncer) loadMongoDBResumeToken(db, coll string) bson.Raw {
	key := fmt.Sprintf("%s.%s", db, coll)

	s.resumeTokensM.RLock()
	if token, exists := s.resumeTokens[key]; exists {
		s.resumeTokensM.RUnlock()
		return token
	}
	s.resumeTokensM.RUnlock()

	if s.cfg.MongoDBResumeTokenPath == "" {
		return nil
	}

	path := s.getResumeTokenPath(db, coll)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	if len(data) <= 1 {
		return nil
	}

	var token bson.Raw
	if errU := json.Unmarshal(data, &token); errU != nil {
		s.logger.Errorf("[MongoDB] unmarshal resume token fail => %v", errU)
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

	if s.cfg.MongoDBResumeTokenPath == "" {
		return
	}

	path := s.getResumeTokenPath(db, coll)
	data, err := json.Marshal(token)
	if err != nil {
		s.logger.Errorf("[MongoDB] marshal resume token fail => %v", err)
		return
	}
	if errW := os.WriteFile(path, data, 0644); errW != nil {
		s.logger.Errorf("[MongoDB] write resume token file => %v", errW)
	}
}

func (s *MongoDBSyncer) removeMongoDBResumeToken(db, coll string) {
	if s.cfg.MongoDBResumeTokenPath == "" {
		return
	}
	path := s.getResumeTokenPath(db, coll)
	_ = os.Remove(path)
	s.resumeTokensM.Lock()
	delete(s.resumeTokens, fmt.Sprintf("%s.%s", db, coll))
	s.resumeTokensM.Unlock()
	s.logger.Infof("[MongoDB] removed invalid resume token => %s", path)
}

func (s *MongoDBSyncer) getResumeTokenPath(db, coll string) string {
	fileName := fmt.Sprintf("%s_%s.json", db, coll)
	return filepath.Join(s.cfg.MongoDBResumeTokenPath, fileName)
}

// startTimePath names the file holding the cluster time a collection's change
// stream should start from. It sits beside the resume token: the token is what
// a running stream saves, the start time is what the snapshot pins before it
// copies a single document.
func (s *MongoDBSyncer) startTimePath(db, coll string) string {
	return filepath.Join(s.cfg.MongoDBResumeTokenPath, fmt.Sprintf("%s_%s.start", db, coll))
}

// saveStartTime records the cluster time the change stream must resume from.
func (s *MongoDBSyncer) saveStartTime(db, coll string, ts primitive.Timestamp) {
	if s.cfg.MongoDBResumeTokenPath == "" {
		return
	}
	data, err := json.Marshal(ts)
	if err != nil {
		s.logger.Errorf("[MongoDB] marshal start time fail => %v", err)
		return
	}
	if err := os.WriteFile(s.startTimePath(db, coll), data, 0o644); err != nil {
		s.logger.Errorf("[MongoDB] write start time file => %v", err)
	}
}

// loadStartTime reports the recorded start time, or the zero timestamp when
// there is none.
func (s *MongoDBSyncer) loadStartTime(db, coll string) primitive.Timestamp {
	if s.cfg.MongoDBResumeTokenPath == "" {
		return primitive.Timestamp{}
	}
	data, err := os.ReadFile(s.startTimePath(db, coll))
	if err != nil {
		return primitive.Timestamp{}
	}
	var ts primitive.Timestamp
	if err := json.Unmarshal(data, &ts); err != nil {
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
	if s.cfg.MongoDBResumeTokenPath == "" {
		return false
	}
	if s.loadMongoDBResumeToken(db, coll) != nil {
		return true
	}
	return !s.loadStartTime(db, coll).IsZero()
}
