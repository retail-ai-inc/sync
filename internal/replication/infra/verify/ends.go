package verify

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// --------------------------------------------------------------- MongoDB

// MongoEnd streams and looks up documents of one collection.
//
// The key is the BSON encoding of the document's _id rather than its printed
// form. An _id can be an ObjectId, a string or a number, and the order the
// server sorts those in is not the order their printed forms sort in — which is
// why the comparison this feeds only ever tests keys for equality.
type MongoEnd struct {
	Coll *mongo.Collection

	cursor *mongo.Cursor
	done   bool
}

func (e *MongoEnd) Name() string {
	return e.Coll.Database().Name() + "." + e.Coll.Name()
}

func (e *MongoEnd) Next(ctx context.Context, limit int) ([]Row, error) {
	if e.done {
		return nil, nil
	}
	if e.cursor == nil {
		cursor, err := e.Coll.Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
		if err != nil {
			return nil, err
		}
		e.cursor = cursor
	}

	var batch []Row
	for len(batch) < limit && e.cursor.Next(ctx) {
		row, err := rowFromDocument(e.cursor.Current)
		if err != nil {
			return nil, err
		}
		batch = append(batch, row)
	}
	if err := e.cursor.Err(); err != nil {
		return nil, err
	}
	if len(batch) == 0 {
		e.done = true
		_ = e.cursor.Close(ctx)
	}
	return batch, nil
}

func (e *MongoEnd) Lookup(ctx context.Context, keys []string) (map[string]Row, error) {
	if len(keys) == 0 {
		return map[string]Row{}, nil
	}

	ids := make([]interface{}, 0, len(keys))
	for _, key := range keys {
		id, err := idFromKey(key)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	cursor, err := e.Coll.Find(ctx, bson.M{"_id": bson.M{"$in": ids}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	found := make(map[string]Row, len(keys))
	for cursor.Next(ctx) {
		row, err := rowFromDocument(cursor.Current)
		if err != nil {
			return nil, err
		}
		found[row.Key] = row
	}
	return found, cursor.Err()
}

// rowFromDocument renders one document as a key and a digest.
func rowFromDocument(raw bson.Raw) (Row, error) {
	value, err := raw.LookupErr("_id")
	if err != nil {
		return Row{}, fmt.Errorf("a document in the comparison has no _id")
	}

	key, err := keyFromID(value)
	if err != nil {
		return Row{}, err
	}
	return Row{Key: key, Digest: documentDigest(raw)}, nil
}

// keyFromID renders an _id as a string that is equal for equal ids and
// different for different ones. The BSON encoding is used because it captures
// the type as well as the value: the string "1" and the number 1 are different
// documents and must not compare equal.
func keyFromID(value bson.RawValue) (string, error) {
	encoded, err := bson.Marshal(bson.D{{Key: "_id", Value: value}})
	if err != nil {
		return "", fmt.Errorf("render an _id for comparison: %w", err)
	}
	return hex.EncodeToString(encoded), nil
}

// idFromKey reverses keyFromID, so one side can look up the keys the other
// produced.
func idFromKey(key string) (interface{}, error) {
	decoded, err := hex.DecodeString(key)
	if err != nil {
		return nil, fmt.Errorf("read the comparison key %q: %w", key, err)
	}
	value, err := bson.Raw(decoded).LookupErr("_id")
	if err != nil {
		return nil, fmt.Errorf("read the comparison key %q: %w", key, err)
	}
	return value, nil
}

// describeID renders an _id the way somebody reading an alert would write it:
// an ObjectId as its hex, a string as itself, a number as its digits.
func describeID(id interface{}) string {
	value, ok := id.(bson.RawValue)
	if !ok {
		return fmt.Sprint(id)
	}
	if oid, ok := value.ObjectIDOK(); ok {
		return oid.Hex()
	}
	if s, ok := value.StringValueOK(); ok {
		return s
	}
	// Everything else — numbers, dates, subdocuments — has a rendering of its
	// own already, and it is short enough to put in an alert.
	return value.String()
}

// documentDigest hashes a document.
//
// The fields are sorted before hashing, because BSON preserves the order they
// were written in and two servers can hold the same document with its fields in
// different orders. Reporting that as a difference would send an operator
// looking for data loss that is not there.
func documentDigest(raw bson.Raw) string {
	var doc bson.M
	if err := bson.Unmarshal(raw, &doc); err != nil {
		// An unreadable document is hashed as its bytes, which at least
		// distinguishes it from a different unreadable document.
		sum := sha256.Sum256(raw)
		return hex.EncodeToString(sum[:])
	}

	h := sha256.New()
	h.Write([]byte(canonical(doc)))
	return hex.EncodeToString(h.Sum(nil))
}

// canonical renders a BSON value with its fields in a fixed order, so the same
// document always produces the same string.
func canonical(v interface{}) string {
	switch value := v.(type) {
	case bson.M:
		keys := make([]string, 0, len(value))
		for k := range value {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%d:%s=%s", len(k), k, canonical(value[k])))
		}
		return "{" + strings.Join(parts, ",") + "}"

	case bson.A:
		parts := make([]string, 0, len(value))
		for _, item := range value {
			parts = append(parts, canonical(item))
		}
		return "[" + strings.Join(parts, ",") + "]"

	case bson.ObjectID:
		return "oid(" + value.Hex() + ")"
	case bson.DateTime:
		return fmt.Sprintf("date(%d)", int64(value))
	case bson.Timestamp:
		return fmt.Sprintf("ts(%d.%d)", value.T, value.I)
	case bson.Binary:
		return "bin(" + hex.EncodeToString(value.Data) + ")"
	case nil:
		return "null"
	case string:
		return fmt.Sprintf("s%d:%s", len(value), value)
	default:
		// Numbers, booleans and anything else print unambiguously enough: the
		// type name is included so 1 and "1" cannot collide.
		return fmt.Sprintf("%T:%v", v, v)
	}
}

// MongoRepairer copies documents from a source collection to a target.
type MongoRepairer struct {
	Source *mongo.Collection
	Target *mongo.Collection
}

// Repair re-reads each named document from the source and writes it to the
// target, removing the ones the source no longer has.
//
// It is deliberately document by document. A repair runs after something has
// already gone wrong, so being slow and obvious beats being fast and hard to
// reason about.
func (r *MongoRepairer) Repair(ctx context.Context, differences []Difference) (int, error) {
	fixed := 0
	for _, d := range differences {
		id, err := idFromKey(d.Key)
		if err != nil {
			return fixed, err
		}

		if d.Kind == Extra {
			if _, err := r.Target.DeleteOne(ctx, bson.M{"_id": id}); err != nil {
				return fixed, fmt.Errorf("repair %s %s: %w", d.Kind, d.Key, err)
			}
			fixed++
			continue
		}

		var doc bson.Raw
		switch err := r.Source.FindOne(ctx, bson.M{"_id": id}).Decode(&doc); {
		case err == mongo.ErrNoDocuments:
			// The source has lost the document since the comparison, so the
			// target should not have it either.
			if _, err := r.Target.DeleteOne(ctx, bson.M{"_id": id}); err != nil {
				return fixed, fmt.Errorf("repair %s %s: %w", d.Kind, d.Key, err)
			}
			fixed++
			continue
		case err != nil:
			return fixed, fmt.Errorf("repair %s %s: %w", d.Kind, d.Key, err)
		}

		if _, err := r.Target.ReplaceOne(ctx, bson.M{"_id": id}, doc,
			options.Replace().SetUpsert(true)); err != nil {
			return fixed, fmt.Errorf("repair %s %s: %w", d.Kind, d.Key, err)
		}
		fixed++
	}
	return fixed, nil
}
