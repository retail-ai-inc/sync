package mongodb

import (
	"strconv"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// idOf reports the _id of a document, or nil when it does not carry one.
func idOf(doc interface{}) interface{} {
	switch d := doc.(type) {
	case bson.M:
		return d["_id"]
	case map[string]interface{}:
		return d["_id"]
	case bson.D:
		for _, e := range d {
			if e.Key == "_id" {
				return e.Value
			}
		}
	case bson.Raw:
		if v, err := d.LookupErr("_id"); err == nil {
			return v
		}
	}
	return nil
}

// documentKey identifies the document a write model touches.
//
// The key is the BSON encoding of the _id rather than its Go value, so an
// ObjectID and the string that prints the same way are told apart, and values
// that are not comparable in Go can still be used as map keys.
func documentKey(model mongo.WriteModel) (string, bool) {
	var id interface{}
	switch m := model.(type) {
	case *mongo.InsertOneModel:
		id = idOf(m.Document)
	case *mongo.ReplaceOneModel:
		id = idOf(m.Filter)
	case *mongo.UpdateOneModel:
		id = idOf(m.Filter)
	case *mongo.DeleteOneModel:
		id = idOf(m.Filter)
	default:
		return "", false
	}
	if id == nil {
		return "", false
	}
	encoded, err := bson.Marshal(bson.D{{Key: "_id", Value: id}})
	if err != nil {
		return "", false
	}
	return string(encoded), true
}

// orderedRuns splits models into runs that hold at most one write per document.
//
// An unordered bulk write lets the server apply the batch in any order it
// likes, so two changes to the same document inside one batch could land the
// wrong way round and the target would keep the older value — silently, and
// for good. Ordering the whole batch instead would serialise writes to
// unrelated documents and cost most of the throughput the batching exists for.
//
// Splitting by document keeps both: no run contains the same _id twice, so the
// runs can go to the server unordered, and running them in sequence replays
// each document's changes in the order the source produced them. Documents that
// change once in a batch — which is the common case — all land in the first run.
//
// A model whose document cannot be identified is given a run of its own and
// closes the runs before it, so it cannot be reordered against anything.
func orderedRuns(models []mongo.WriteModel) [][]mongo.WriteModel {
	var runs [][]mongo.WriteModel
	var keys []map[string]struct{}
	// firstOpen is the earliest run that may still take another model. It moves
	// forward when a model that cannot be identified acts as a barrier.
	firstOpen := 0

	for _, model := range models {
		key, ok := documentKey(model)
		if !ok {
			runs = append(runs, []mongo.WriteModel{model})
			keys = append(keys, nil)
			firstOpen = len(runs)
			continue
		}

		placed := false
		for i := firstOpen; i < len(runs); i++ {
			if keys[i] == nil {
				continue
			}
			if _, clash := keys[i][key]; clash {
				// Every earlier run already holds this document, so this write
				// has to go after them.
				continue
			}
			runs[i] = append(runs[i], model)
			keys[i][key] = struct{}{}
			placed = true
			break
		}
		if !placed {
			runs = append(runs, []mongo.WriteModel{model})
			keys = append(keys, map[string]struct{}{key: {}})
		}
	}
	return runs
}

// eventClusterTime reports when the source made the change a raw event
// describes. Change stream documents carry it as clusterTime.
func eventClusterTime(raw bson.Raw) (time.Time, bool) {
	value, err := raw.LookupErr("clusterTime")
	if err != nil {
		return time.Time{}, false
	}
	seconds, _, ok := value.TimestampOK()
	if !ok || seconds == 0 {
		return time.Time{}, false
	}
	return time.Unix(int64(seconds), 0), true
}

// metricLabels identify one collection of one task in the metrics. The
// endpoints are named without their credentials, because the exposition is
// scraped and stored.
func (s *MongoDBSyncer) metricLabels(collection string) metrics.Labels {
	return metrics.Labels{
		"task":       strconv.Itoa(s.cfg.ID),
		"engine":     "mongodb",
		"collection": collection,
		"source":     dsn.Endpoint(s.cfg.Type, s.cfg.SourceConnection),
		"target":     dsn.Endpoint(s.cfg.Type, s.cfg.TargetConnection),
	}
}

// eventResumeToken reports the resume token of a change stream event.
//
// A change stream document's _id *is* its resume token, so a buffered event
// already carries the value the stream would continue from — no extra
// bookkeeping in the buffer file is needed to record it.
func eventResumeToken(raw bson.Raw) (bson.Raw, bool) {
	value, err := raw.LookupErr("_id")
	if err != nil {
		return nil, false
	}
	doc, ok := value.DocumentOK()
	if !ok {
		return nil, false
	}
	return bson.Raw(doc), true
}
