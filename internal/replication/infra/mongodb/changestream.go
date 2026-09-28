package mongodb

import (
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// streamEvent represents the data passed from the Change Stream reader to the disk writer.
// It contains the raw BSON data and the corresponding resume token.
type streamEvent struct {
	RawData     bson.Raw
	ResumeToken bson.Raw
}

// positionLost reports whether an error says the change stream cannot be
// resumed from the token or cluster time this task holds. The oplog is capped,
// so a task stopped for longer than it covers finds its resume point gone.
// Retrying cannot help, and the tempting repair — dropping the token and
// watching from now — silently skips everything in between, so it is reported.
func positionLost(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	for _, marker := range []string{
		"changestreamhistorylost",
		"resume of change stream was not possible",
		"resume point may no longer be in the oplog",
		"invalid resume token",
		"the resume point may no longer be in the oplog",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}
