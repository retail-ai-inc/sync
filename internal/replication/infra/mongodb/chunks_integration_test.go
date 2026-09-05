//go:build integration

package mongodb

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/test/harness"
)

func TestChunksWalkTheCollectionInIDOrderWithoutRepeating(t *testing.T) {
	client := connect(t, harness.MongoSource)
	database := harness.UniqueName("chunks")
	collection := "documents"
	ctx := context.Background()
	t.Cleanup(func() { _ = client.Database(database).Drop(context.Background()) })

	const total = 25
	coll := client.Database(database).Collection(collection)
	for i := 0; i < total; i++ {
		if _, err := coll.InsertOne(ctx, bson.M{"_id": i, "n": i}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	chunks := &Chunks{Client: client, Database: database, Masker: &MongoDBSyncer{}}
	ns := domain.Namespace{Object: collection}

	seen := map[int]bool{}
	after := ""
	for round := 0; round < 10; round++ {
		chunk, err := chunks.NextChunk(ctx, ns, after, 10)
		if err != nil {
			t.Fatalf("NextChunk: %v", err)
		}
		if len(chunk.Events) == 0 {
			break
		}
		for _, event := range chunk.Events {
			// The key is what the applier addresses the document by, so it is
			// what has to be unique across the walk.
			id, err := strconv.Atoi(strings.TrimSuffix(
				strings.TrimPrefix(event.Key, "_id="), "\x00"))
			if err != nil {
				t.Fatalf("a chunk carried the key %q: %v", event.Key, err)
			}
			if seen[id] {
				t.Fatalf("_id %d was read twice, so the cursor did not advance", id)
			}
			seen[id] = true
		}
		if chunk.After == "" {
			t.Fatal("a chunk with documents in it reported no cursor to carry on from")
		}
		after = chunk.After
	}
	if len(seen) != total {
		t.Errorf("the walk read %d of %d documents", len(seen), total)
	}

	// Past the end: an empty chunk rather than an error, which is what stops the
	// copy.
	last, err := chunks.NextChunk(ctx, ns, after, 10)
	if err != nil {
		t.Fatalf("NextChunk past the end: %v", err)
	}
	if len(last.Events) != 0 {
		t.Errorf("reading past the end returned %d documents", len(last.Events))
	}
}

func TestAChunkOfACollectionThatIsNotThereIsEmpty(t *testing.T) {
	client := connect(t, harness.MongoSource)
	chunks := &Chunks{Client: client, Database: harness.UniqueName("chunks-absent"),
		Masker: &MongoDBSyncer{}}

	chunk, err := chunks.NextChunk(context.Background(),
		domain.Namespace{Object: "nothing"}, "", 10)
	if err != nil {
		t.Fatalf("NextChunk: %v", err)
	}
	if len(chunk.Events) != 0 {
		t.Errorf("a collection that does not exist yielded %d documents", len(chunk.Events))
	}
}

func TestAChunkRefusesACursorItCannotRead(t *testing.T) {
	client := connect(t, harness.MongoSource)
	chunks := &Chunks{Client: client, Database: harness.UniqueName("chunks-cursor"),
		Masker: &MongoDBSyncer{}}

	// A cursor is where the copy resumes from. Carrying on from a value that
	// cannot be decoded would start again at the beginning, which re-copies
	// what has already landed.
	if _, err := chunks.NextChunk(context.Background(),
		domain.Namespace{Object: "documents"}, "not-a-cursor", 10); err == nil {
		t.Error("an unreadable cursor was accepted")
	}
}

func TestTheOplogEdgesBracketTheWindow(t *testing.T) {
	client := connect(t, harness.MongoSource)
	ctx := context.Background()

	oldest, err := oplogEdgeOf(ctx, client, 1)
	if err != nil {
		t.Fatalf("read the oldest oplog entry: %v", err)
	}
	newest, err := oplogEdgeOf(ctx, client, -1)
	if err != nil {
		t.Fatalf("read the newest oplog entry: %v", err)
	}
	if oldest.T > newest.T || (oldest.T == newest.T && oldest.I > newest.I) {
		t.Errorf("the oldest oplog entry (%v) is after the newest (%v)", oldest, newest)
	}
}
