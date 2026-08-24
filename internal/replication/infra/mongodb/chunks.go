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
// stream.
//
// It reads inside a session so the read's own operation time can be taken from
// the server rather than guessed from this machine's clock. That timestamp is
// what the pipeline holds the chunk against: until the stream has been read past
// it, applying the chunk could put a record back to an older value.
type Chunks struct {
	Client *mongo.Client
	// Database is the source database.
	Database string
	// Masker applies the task's field security to the documents, so a re-copy
	// does not undo what the stream path masks.
	Masker *MongoDBSyncer
}

// NextChunk reads the documents after a key.
func (c *Chunks) NextChunk(ctx context.Context, ns domain.Namespace, after string, size int) (pipeline.Chunk, error) {
	session, err := c.Client.StartSession()
	if err != nil {
		return pipeline.Chunk{}, fmt.Errorf("start a session for the chunk: %w", err)
	}
	defer session.EndSession(ctx)

	var chunk pipeline.Chunk
	err = mongo.WithSession(ctx, session, func(sc context.Context) error {
		filter := bson.M{}
		if after != "" {
			id, err := decodeID(after)
			if err != nil {
				return err
			}
			filter["_id"] = bson.M{"$gt": id}
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
			masked := c.Masker.maskDocument(ns.Object, document)

			chunk.Events = append(chunk.Events, &domain.Event{
				NS:  ns,
				Op:  domain.OpInsert,
				Key: fmt.Sprintf("_id=%v\x00", id),
				Payload: mongo.NewReplaceOneModel().
					SetFilter(bson.M{"_id": id}).
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

		// The server's own clock at the moment of the read. Taken here rather
		// than from this process's clock because the two can differ, and the
		// comparison the pipeline makes has to be in the source's terms.
		if at := session.OperationTime(); at != nil {
			chunk.ReadAt = time.Unix(int64(at.T), 0)
		} else {
			return fmt.Errorf("the server reported no operation time for the read of "+
				"%s, so the chunk cannot be ordered against the stream", ns)
		}
		return nil
	})
	if err != nil {
		return pipeline.Chunk{}, err
	}
	return chunk, nil
}

// rawOf renders an _id as a raw value, so it can be stored and compared.
func rawOf(id interface{}) bson.RawValue {
	kind, data, err := bson.MarshalValue(id)
	if err != nil {
		return bson.RawValue{}
	}
	return bson.RawValue{Type: kind, Value: data}
}

// encodeID stores an _id as text, so the re-copy's progress survives a restart.
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

// decodeID reads a stored _id back.
func decodeID(stored string) (interface{}, error) {
	var wrapper struct {
		ID interface{} `bson:"id"`
	}
	if err := bson.UnmarshalExtJSON([]byte(stored), true, &wrapper); err != nil {
		return nil, fmt.Errorf("read the re-copy's last key: %w", err)
	}
	return wrapper.ID, nil
}
