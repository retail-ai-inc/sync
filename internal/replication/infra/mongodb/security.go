package mongodb

import (
	"github.com/retail-ai-inc/sync/internal/replication/infra/security"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// A task can name fields to mask or encrypt, and on this engine nothing did it.
//
// The MySQL and PostgreSQL paths apply the policy; the MongoDB path never called
// the security package at all, so a collection configured to hide an email
// address replicated it in the clear — and the configuration said otherwise, on
// screen, to whoever set it.

// maskDocument applies a collection's field security to one document, returning
// a copy. A collection with no policy is returned as it is.
func (s *MongoDBSyncer) maskDocument(collectionName string, document bson.M) bson.M {
	policy := security.FindTableSecurityFromMappings(collectionName, s.cfg.Mappings)
	if !policy.SecurityEnabled || len(policy.FieldSecurity) == 0 {
		return document
	}

	processed, ok := security.ProcessValue(map[string]interface{}(document), "", policy).(map[string]interface{})
	if !ok {
		return document
	}
	return bson.M(processed)
}

// maskValue applies the policy to a value that may or may not be a document,
// which is what a change stream event's fullDocument is.
func (s *MongoDBSyncer) maskValue(collectionName string, value interface{}) interface{} {
	policy := security.FindTableSecurityFromMappings(collectionName, s.cfg.Mappings)
	if !policy.SecurityEnabled || len(policy.FieldSecurity) == 0 {
		return value
	}

	switch typed := value.(type) {
	case bson.M:
		if processed, ok := security.ProcessValue(map[string]interface{}(typed), "", policy).(map[string]interface{}); ok {
			return bson.M(processed)
		}
	case map[string]interface{}:
		return security.ProcessValue(typed, "", policy)
	}
	return value
}
