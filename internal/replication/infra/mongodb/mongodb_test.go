package mongodb

import (
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// TestATextIndexIsRebuiltFromItsWeights: the server reports a text index's key
// as {_fts, _ftsx} and keeps the indexed fields in the weights, and creating
// one from the reported key is refused.
func TestATextIndexIsRebuiltFromItsWeights(t *testing.T) {
	reported := bson.M{"_fts": "text", "_ftsx": int32(1)}
	key, ok := indexKeyOf(reported)
	if !ok {
		t.Fatal("the reported key could not be read")
	}
	if !textIndexKey(key) {
		t.Fatalf("key %v was not recognised as a text index", key)
	}

	rebuilt, weights, ok := textIndexFrom(bson.M{"ItemName": int32(1), "ItemId": int32(1)})
	if !ok {
		t.Fatal("the weights could not be read")
	}
	if len(rebuilt) != 2 || len(weights) != 2 {
		t.Fatalf("rebuilt = %v, weights = %v", rebuilt, weights)
	}
	for _, e := range rebuilt {
		if e.Value != "text" {
			t.Errorf("%s = %v, want \"text\"", e.Key, e.Value)
		}
		if e.Key != "ItemName" && e.Key != "ItemId" {
			t.Errorf("unexpected field %q in the rebuilt key", e.Key)
		}
	}
	if textIndexKey(rebuilt) {
		t.Errorf("the rebuilt key is still the server's shape: %v", rebuilt)
	}
}

// An ordinary index must not be mistaken for a text one.
func TestAnOrdinaryIndexIsLeftAlone(t *testing.T) {
	key, _ := indexKeyOf(bson.M{"sku": int32(1), "shop": int32(-1)})
	if textIndexKey(key) {
		t.Errorf("key %v was taken for a text index", key)
	}
}

// TestNoDirectoriesAreMadeForNothing: the syncer used to create a buffer and a
// dead-letter directory on every start, alongside ten fields that were set from
// them and read by nothing. The deployment notes told operators to size a volume
// for a buffer that never held anything.
func TestNoDirectoriesAreMadeForNothing(t *testing.T) {
	root := t.TempDir()
	cfg := config.SyncConfig{ID: 1, Type: "mongodb", MongoDBResumeTokenPath: root}

	log := logrus.New()
	log.SetLevel(logrus.PanicLevel)
	_ = NewMongoDBSyncer(cfg, nil, log)

	for _, unwanted := range []string{"buffer", "dead_letter"} {
		if _, err := os.Stat(filepath.Join(root, unwanted)); err == nil {
			t.Errorf("%s was created; nothing reads it", unwanted)
		}
	}
}
