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
	case bson.D:
		// Unmarshalling into a bson.M gives nested documents as bson.M under the
		// driver's v1 and as bson.D under its v2, so a change stream event's
		// fullDocument arrives here in the second shape now. It used to fall through
		// and be returned untouched — the masking a task had configured simply
		// stopped happening, with nothing to show it, which is the worst way for a
		// security setting to fail.
		document := make(map[string]interface{}, len(typed))
		for _, element := range typed {
			document[element.Key] = element.Value
		}
		if processed, ok := security.ProcessValue(document, "", policy).(map[string]interface{}); ok {
			return bson.M(processed)
		}
	}
	return value
}
