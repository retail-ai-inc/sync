package mongodb

import (
	"time"

	"go.mongodb.org/mongo-driver/bson"
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
