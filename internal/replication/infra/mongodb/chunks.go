package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/retail-ai-inc/sync/internal/replication/app/pipeline"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// Chunks reads a collection in _id order, for a re-copy that runs alongside the
// stream. A chunk's ReadAt is the server's operation time from before the read,
// not this machine's clock: the pipeline orders the chunk against the stream by
// it, so it has to be in the source's terms.
type Chunks struct {
	Client *mongo.Client
	// Database is the source database.
	Database string
	// Masker applies the task's field security to the documents, so a re-copy
	// does not undo what the stream path masks.
	Masker *MongoDBSyncer
}

func (c *Chunks) NextChunk(ctx context.Context, ns domain.Namespace, after string, size int) (pipeline.Chunk, error) {
	session, err := c.Client.StartSession()
	if err != nil {
		return pipeline.Chunk{}, fmt.Errorf("start a session for the chunk: %w", err)
	}
	defer session.EndSession(ctx)

	// How the target addresses these documents. A copy has no documentKey to
	// take it from, so the shard key is read once here and its values are taken
	// out of each document; without them a sharded target refuses every upsert.
	address, err := addressOf(ctx, c.Client, c.Database+"."+ns.Object)
	if err != nil {
		return pipeline.Chunk{}, fmt.Errorf("read how %s is partitioned: %w", ns, err)
	}

	var chunk pipeline.Chunk
	err = mongo.WithSession(ctx, session, func(sc context.Context) error {
		// Taken before the Find: ReadAt must not be later than any document was
		// read, and the session's time moves on with every getMore of the cursor.
		if err := c.Client.Database(c.Database).RunCommand(sc,
			bson.D{{Key: "ping", Value: 1}}).Err(); err != nil {
			return fmt.Errorf("read the source's clock before reading %s: %w", ns, err)
		}
		at := session.OperationTime()
		if at == nil {
			return fmt.Errorf("the server reported no operation time before the read "+
				"of %s, so the chunk cannot be ordered against the stream", ns)
		}
		chunk.ReadAt = time.Unix(int64(at.T), 0)

		filter := bson.M{}
		if after != "" {
			id, err := decodeID(after)
			if err != nil {
				return err
			}
			// An aggregation comparison, because a query $gt only matches its own
			// BSON type; $literal keeps a "$"-string or document _id a value.
			filter["$expr"] = bson.M{"$gt": bson.A{"$_id", bson.M{"$literal": id}}}
		}

		cursor, err := c.Client.Database(c.Database).Collection(ns.Object).Find(sc, filter,
			options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(size)))
		if err != nil {
			return fmt.Errorf("read %s: %w", ns, err)
		}
		defer cursor.Close(sc)

		var last bson.RawValue
		for cursor.Next(sc) {
			var document bson.M
			if err := cursor.Decode(&document); err != nil {
				return fmt.Errorf("decode a document of %s: %w", ns, err)
			}
			id, ok := document["_id"]
			if !ok {
				return fmt.Errorf("a document of %s carries no _id, so a re-copy cannot "+
					"address it on the target", ns)
			}
			where, err := address.filter(document)
			if err != nil {
				return fmt.Errorf("a document of %s cannot be addressed on the target: %w",
					ns, err)
			}
			masked := c.Masker.maskDocument(c.Database, ns.Object, document)

			chunk.Events = append(chunk.Events, &domain.Event{
				NS:  ns,
				Op:  domain.OpInsert,
				Key: fmt.Sprintf("_id=%v\x00", id),
				Payload: mongo.NewReplaceOneModel().
					SetFilter(where).
					SetReplacement(masked).
					SetUpsert(true),
			})
			last = rawOf(id)
		}
		if err := cursor.Err(); err != nil {
			return fmt.Errorf("read %s: %w", ns, err)
		}

		if len(chunk.Events) > 0 {
			chunk.After, err = encodeID(last)
			if err != nil {
				return err
			}
		}
		chunk.Done = len(chunk.Events) < size
		return nil
	})
	if err != nil {
		return pipeline.Chunk{}, err
	}
	return chunk, nil
}

func rawOf(id interface{}) bson.RawValue {
	kind, data, err := bson.MarshalValue(id)
	if err != nil {
		return bson.RawValue{}
	}
	return bson.RawValue{Type: kind, Value: data}
}

func encodeID(id bson.RawValue) (string, error) {
	if id.Type == 0 {
		return "", nil
	}
	wrapped, err := bson.MarshalExtJSON(bson.D{{Key: "id", Value: id}}, true, false)
	if err != nil {
		return "", fmt.Errorf("encode the chunk's last key: %w", err)
	}
	return string(wrapped), nil
}

func decodeID(stored string) (interface{}, error) {
	var wrapper struct {
		ID interface{} `bson:"id"`
	}
	if err := bson.UnmarshalExtJSON([]byte(stored), true, &wrapper); err != nil {
		return nil, fmt.Errorf("read the re-copy's last key: %w", err)
	}
	return wrapper.ID, nil
}
