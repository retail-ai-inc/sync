//go:build integration

package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	driverevent "go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

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

func TestAWalkCrossesIDTypes(t *testing.T) {
	client := connect(t, harness.MongoSource)
	database := harness.UniqueName("chunks-types")
	ctx := context.Background()
	t.Cleanup(func() { _ = client.Database(database).Drop(context.Background()) })

	ids := []interface{}{
		int32(1), int32(2), "$a", "b", bson.D{{Key: "x", Value: int32(1)}},
		bson.NewObjectIDFromTimestamp(time.Unix(1700000000, 0)),
		bson.NewObjectIDFromTimestamp(time.Unix(1700000001, 0)),
	}
	coll := client.Database(database).Collection("documents")
	for _, id := range ids {
		if _, err := coll.InsertOne(ctx, bson.M{"_id": id}); err != nil {
			t.Fatalf("seed %v: %v", id, err)
		}
	}

	chunks := &Chunks{Client: client, Database: database, Masker: &MongoDBSyncer{}}
	ns := domain.Namespace{Object: "documents"}
	var seen []string
	after := ""
	for round := 0; ; round++ {
		if round > len(ids) {
			t.Fatalf("the walk had not finished after %d chunks: %v", round, seen)
		}
		chunk, err := chunks.NextChunk(ctx, ns, after, 2)
		if err != nil {
			t.Fatalf("NextChunk: %v", err)
		}
		for _, event := range chunk.Events {
			id := event.Payload.(*mongo.ReplaceOneModel).Replacement.(bson.M)["_id"]
			seen = append(seen, fmt.Sprintf("%T %v", id, id))
		}
		if chunk.Done {
			break
		}
		after = chunk.After
	}

	// Short means a re-copy stops at the end of one BSON type and reports done without the rest.
	want := make([]string, len(ids))
	for i, id := range ids {
		want[i] = fmt.Sprintf("%T %v", id, id)
	}
	if fmt.Sprint(seen) != fmt.Sprint(want) {
		t.Errorf("the walk read %v, want %v", seen, want)
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

func TestAChunkIsDatedNoLaterThanItsFirstDocumentWasRead(t *testing.T) {
	writer := connect(t, harness.MongoSource)
	database := harness.UniqueName("chunks-readat")
	ctx := context.Background()
	t.Cleanup(func() { _ = writer.Database(database).Drop(context.Background()) })

	const total = 150
	coll := writer.Database(database).Collection("documents")
	documents := make([]interface{}, total)
	for i := range documents {
		documents[i] = bson.M{"_id": i, "n": 0}
	}
	if _, err := coll.InsertMany(ctx, documents); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var changed bson.Timestamp
	var changeErr error
	var once sync.Once
	changeBetweenBatches := func() {
		changed, changeErr = writtenAt(ctx, writer, func(sc context.Context) error {
			_, err := coll.UpdateOne(sc, bson.M{"_id": 0}, bson.M{"$set": bson.M{"n": 1}})
			return err
		})
		deadline := time.Now().Add(5 * time.Second)
		for changeErr == nil {
			var later bson.Timestamp
			later, changeErr = writtenAt(ctx, writer, func(sc context.Context) error {
				_, err := writer.Database(database).Collection("clock").InsertOne(sc, bson.M{})
				return err
			})
			if later.T > changed.T {
				return
			}
			if time.Now().After(deadline) {
				changeErr = errors.New("the source's clock did not reach the next second")
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	reader := connect(t, harness.MongoSource, options.Client().SetMonitor(&driverevent.CommandMonitor{
		Started: func(_ context.Context, e *driverevent.CommandStartedEvent) {
			if e.CommandName == "getMore" {
				once.Do(changeBetweenBatches)
			}
		},
	}))

	chunks := &Chunks{Client: reader, Database: database, Masker: &MongoDBSyncer{}}
	chunk, err := chunks.NextChunk(ctx, domain.Namespace{Object: "documents"}, "", total+1)
	if err != nil {
		t.Fatalf("NextChunk: %v", err)
	}
	if changeErr != nil {
		t.Fatalf("change a document between the batches: %v", changeErr)
	}
	if changed.T == 0 {
		t.Fatal("the chunk was read without a getMore, so no document changed during the read")
	}
	if len(chunk.Events) != total {
		t.Fatalf("the chunk holds %d of %d documents", len(chunk.Events), total)
	}
	first := chunk.Events[0].Payload.(*mongo.ReplaceOneModel).Replacement.(bson.M)
	if first["_id"] != int32(0) || first["n"] != int32(0) {
		t.Fatalf("the chunk's first document is %v, want _id 0 as it was before the change", first)
	}

	// Dated before ReadAt, the change is queued ahead of the chunk, whose older copy then overwrites it.
	if at := time.Unix(int64(changed.T), 0); at.Before(chunk.ReadAt) {
		t.Errorf("a change made after the chunk read the document is dated %v, before the "+
			"chunk's ReadAt %v", at, chunk.ReadAt)
	}
}

func writtenAt(ctx context.Context, client *mongo.Client, write func(context.Context) error) (bson.Timestamp, error) {
	session, err := client.StartSession()
	if err != nil {
		return bson.Timestamp{}, err
	}
	defer session.EndSession(ctx)
	if err := mongo.WithSession(ctx, session, write); err != nil {
		return bson.Timestamp{}, err
	}
	at := session.OperationTime()
	if at == nil {
		return bson.Timestamp{}, errors.New("the server reported no operation time for a write")
	}
	return *at, nil
}
