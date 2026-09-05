package mongodb

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

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
// eventWallTime reads the primary's own clock at the moment of the change.
//
// MongoDB 6.0 and later put it on every change stream event. It is not an
// ordering clock -- clusterTime is -- but it has millisecond resolution, and
// clusterTime counts whole seconds: a delay measured from clusterTime carries
// up to a second of error that is an artefact of the timestamp rather than
// anything the replication did.
func eventWallTime(raw bson.Raw) (time.Time, bool) {
	value, err := raw.LookupErr("wallTime")
	if err != nil {
		return time.Time{}, false
	}
	milliseconds, ok := value.DateTimeOK()
	if !ok || milliseconds == 0 {
		return time.Time{}, false
	}
	return time.UnixMilli(milliseconds), true
}

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
